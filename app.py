import asyncio
import json
import logging
import os
import sys
import urllib.parse
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

ROOMS = {}

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
        logging.info(f"[NADAJNIK POŁĄCZONY] Pokój: '{room_id}'")
        try:
            async for message in websocket:
                msg_type = "Dane partii"
                if '"list"' in message or '"add"' in message:
                    room["last_snapshot"] = message
                    msg_type = "MIGAWKA (Snapshot turnieju)"
                elif '"chessMove"' in message or '"moves"' in message or '"halfMoves"' in message:
                    msg_type = "Ruch szachowy"
                elif '"clock"' in message or '"whiteClock"' in message or '"blackClock"' in message:
                    msg_type = "Czas zegara"
                elif '"result"' in message:
                    msg_type = "Wynik partii"

                logging.info(f"[PAKIET] Pokój: '{room_id}' | {msg_type} ({len(message)} bajtów) -> do {len(room['subscribers'])} widzów")
                
                # Broadcast live change to all viewers in this room
                dead = set()
                for sub in list(room["subscribers"]):
                    try:
                        await sub.send(message)
                    except Exception:
                        dead.add(sub)
                room["subscribers"] -= dead
        except Exception as e:
            logging.warning(f"[BŁĄD NADAJNIKA] Pokój '{room_id}': {e}")
        finally:
            if room["publisher"] == websocket:
                room["publisher"] = None
            logging.info(f"[NADAJNIK ROZŁĄCZONY] Pokój: '{room_id}'")
    else:
        room["subscribers"].add(websocket)
        logging.info(f"[WIDZ POŁĄCZONY] Pokój: '{room_id}' (Razem widzów: {len(room['subscribers'])})")
        if room["last_snapshot"]:
            try:
                await websocket.send(room["last_snapshot"])
                logging.info(f"[MIGAWKA WYSŁANA DO WIDZA] Pokój: '{room_id}'")
            except Exception:
                pass
        try:
            async for _ in websocket:
                pass
        except Exception:
            pass
        finally:
            room["subscribers"].discard(websocket)
            logging.info(f"[WIDZ ROZŁĄCZONY] Pokój: '{room_id}'")

HTML_FILE = os.path.join(os.path.dirname(__file__), "viewer.html")

def get_viewer_html():
    if os.path.exists(HTML_FILE):
        try:
            with open(HTML_FILE, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            pass
    return "<!DOCTYPE html><html><body><h1>LazyArbiter Relay Server</h1></body></html>"

def process_request(connection_or_path, request_or_headers=None):
    html = get_viewer_html()
    if hasattr(connection_or_path, "respond"):
        req = request_or_headers
        if req and hasattr(req, "headers"):
            upgrade = req.headers.get("Upgrade", "")
            if "websocket" in upgrade.lower():
                return None
        resp = connection_or_path.respond(200, html)
        resp.headers["Content-Type"] = "text/html; charset=utf-8"
        return resp
    else:
        headers = request_or_headers
        if headers:
            upgrade = headers.get("Upgrade", "")
            if "websocket" in upgrade.lower():
                return None
        from http import HTTPStatus
        return (HTTPStatus.OK, [("Content-Type", "text/html; charset=utf-8")], html.encode("utf-8"))

async def main():
    port = int(os.environ.get("PORT", 8088))
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass

    print("=" * 60)
    print(f"  LazyArbiter WebSocket & Web Viewer Relay Server")
    print(f"  Nasłuchiwanie na porcie: {port}")
    print(f"  URL dla przeglądarki/smartfona: http://<ADRES_IP>:{port}/<NAZWA_POKOJU>")
    print(f"  URL dla WebSocket Viewera: ws://<ADRES_IP>:{port}/<NAZWA_POKOJU>")
    print("=" * 60)
    
    async with websockets.serve(handler, "0.0.0.0", port, process_request=process_request):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
