import asyncio
import http
import os
import urllib.parse
import websockets

ROOMS = {}

async def health_check(path, request_headers):
    if path == "/healthz" or path == "/":
        return http.HTTPStatus.OK, [], b"OK\n"

async def handler(websocket):
    req_path = getattr(websocket, "path", None)
    if not req_path and hasattr(websocket, "request"):
        req_path = websocket.request.path
    if not req_path:
        req_path = "/"
        
    parsed = urllib.parse.urlparse(req_path)
    params = urllib.parse.parse_qs(parsed.query)
    path_parts = [p for p in parsed.path.split("/") if p and p != "ws"]
    room_id = path_parts[0] if path_parts else params.get("room", ["default"])[0]
    role = params.get("role", ["subscriber"])[0]

    if room_id not in ROOMS:
        ROOMS[room_id] = {"publisher": None, "subscribers": set(), "last_snapshot": None}
    room = ROOMS[room_id]
    
    if role == "publisher":
        room["publisher"] = websocket
        try:
            async for message in websocket:
                if '"list"' in message or '"add"' in message:
                    room["last_snapshot"] = message
                dead = set()
                for sub in list(room["subscribers"]):
                    try:
                        await sub.send(message)
                    except Exception:
                        dead.add(sub)
                room["subscribers"] -= dead
        finally:
            if room["publisher"] == websocket:
                room["publisher"] = None
    else:
        room["subscribers"].add(websocket)
        if room["last_snapshot"]:
            try:
                await websocket.send(room["last_snapshot"])
            except Exception:
                pass
        try:
            async for _ in websocket:
                pass
        finally:
            room["subscribers"].discard(websocket)

async def main():
    port = int(os.environ.get("PORT", 8080))
    async with websockets.serve(handler, "0.0.0.0", port, process_request=health_check):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
