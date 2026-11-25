"""
LazyArbiter Cloud Relay
=======================

Przekaźnik WebSocket między nadajnikiem (LiveChess + CloudStreamManager) a odbiornikami
(LazyArbiter Viewer w Javie oraz viewer WWW).

Protokół danych to oryginalny protokół LiveChess (/data):
  {"list":   [snapshot, ...]}   pełny stan wszystkich turniejów nadajnika
  {"add":    [snapshot, ...]}   nowy turniej
  {"remove": ["uuid", ...]}     usunięty turniej
  {"change": [delta, ...]}      zmiany (tylko zmienione obiekty / pola)
gdzie snapshot/delta = {"uuid","version","base","root","objects":{"<id>@<typ>;<wersja>": {...}}}

Relay składa zmiany w pełny, aktualny stan pokoju, więc widz, który dołączy później,
dostaje od razu aktualne szachownice (a nie migawkę sprzed godziny).

Komunikaty kontrolne relay -> klienci (stare viewery Java je ignorują):
  {"type":"hb", ...}            heartbeat co HEARTBEAT_SECONDS (watchdog po stronie klienta)
  {"type":"room_status", ...}   liczba nadajników / widzów
  {"type":"room_cleared"}       bufor pokoju wyczyszczony
  {"type":"need_snapshot"}      (do nadajnika) prośba o ponowne wysłanie pełnego stanu

Uruchomienie:  python relay_server.py [port]      (domyślnie PORT z env albo 8088)
Zmienne środowiskowe (opcjonalne):
  PUBLISH_KEY  - hasło nadajnika (?key=... w adresie), domyślnie 21b03c; pusty = bez hasła
  ADMIN_KEY    - hasło czyszczenia bufora (?key=... / {"key":...}), domyślnie takie jak PUBLISH_KEY
"""
import asyncio
import json
import logging
import os
import re
import sys
import time
import unicodedata
import urllib.parse
from http import HTTPStatus

import websockets

try:  # websockets >= 13 (nowe API asyncio)
    from websockets.asyncio.server import serve as ws_serve
    try:
        from websockets.asyncio.server import broadcast as ws_broadcast
    except ImportError:  # pragma: no cover
        from websockets import broadcast as ws_broadcast
    NEW_API = True
except ImportError:  # starsze websockets (legacy)
    ws_serve = websockets.serve
    ws_broadcast = getattr(websockets, "broadcast", None)
    NEW_API = False

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("relay")
logging.getLogger("websockets").setLevel(logging.WARNING)

DEFAULT_ROOM = "turniej1"
HEARTBEAT_SECONDS = 15
ROOM_IDLE_EXPIRY = 24 * 3600          # pokój bez połączeń i bez zmian znika po 24 h
SNAPSHOT_REQUEST_COOLDOWN = 20
# Hasło nadajnika i czynności administracyjnych (czyszczenie bufora). Można nadpisać zmiennymi środowiskowymi.
PUBLISH_KEY = os.environ.get("PUBLISH_KEY", "21b03c") or None
ADMIN_KEY = os.environ.get("ADMIN_KEY", PUBLISH_KEY or "") or None
HTML_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "viewer.html")

_PL = str.maketrans("ąćęłńóśźżĄĆĘŁŃÓŚŹŻ", "acelnoszzACELNOSZZ")


def normalize_room(raw):
    """Nazwa pokoju: małe litery, bez polskich znaków i spacji. 'Turniej 1' -> 'turniej-1'."""
    if not raw:
        return DEFAULT_ROOM
    s = urllib.parse.unquote(str(raw)).strip().translate(_PL)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower()
    s = re.sub(r"[^a-z0-9_-]+", "-", s).strip("-")
    s = re.sub(r"-{2,}", "-", s)[:64]
    return s or DEFAULT_ROOM


def room_from_path(path):
    parsed = urllib.parse.urlparse(path or "/")
    params = urllib.parse.parse_qs(parsed.query)
    parts = [p for p in parsed.path.split("/") if p and p != "ws" and not p.endswith(".html")]
    raw = params.get("room", [None])[0] or (parts[0] if parts else None)
    return normalize_room(raw), params


def obj_id(key):
    return key.split("@", 1)[0]


class Tournament:
    """Pełny, scalany stan jednego turnieju (snapshot + kolejne delty)."""

    def __init__(self, snap, owner):
        self.owner = owner
        self.head = {k: v for k, v in snap.items() if k != "objects"}
        self.objects = {}
        self.index = {}  # id -> aktualny klucz "id@typ;wersja"
        for key, obj in (snap.get("objects") or {}).items():
            self.objects[key] = dict(obj) if isinstance(obj, dict) else obj
            self.index[obj_id(key)] = key
        self.updated = time.time()

    @property
    def uuid(self):
        return self.head.get("uuid")

    def merge(self, delta):
        if "version" in delta:
            self.head["version"] = delta["version"]
        for key, obj in (delta.get("objects") or {}).items():
            oid = obj_id(key)
            old_key = self.index.get(oid)
            if old_key is not None:
                base = self.objects.pop(old_key, {})
                if isinstance(base, dict) and isinstance(obj, dict):
                    base.update(obj)
                else:
                    base = obj
            else:
                base = dict(obj) if isinstance(obj, dict) else obj
            self.objects[key] = base
            self.index[oid] = key
        self.updated = time.time()

    def reachable(self):
        """Tylko obiekty osiągalne z korzenia (usuwa np. skasowane partie / stare pary)."""
        root = self.head.get("root")
        if root is None:
            return dict(self.objects)
        seen, todo = set(), [str(root)]
        while todo:
            oid = todo.pop()
            if oid in seen:
                continue
            seen.add(oid)
            key = self.index.get(oid)
            obj = self.objects.get(key) if key else None
            if not isinstance(obj, dict):
                continue
            for field, val in obj.items():
                if not field.startswith("*"):
                    continue
                vals = val if isinstance(val, list) else [val]
                for v in vals:
                    if isinstance(v, (int, str)) and str(v) not in seen:
                        todo.append(str(v))
        out = {}
        for oid in seen:
            key = self.index.get(oid)
            if key in self.objects:
                out[key] = self.objects[key]
        # sprzątanie pamięci
        if len(out) < len(self.objects):
            self.objects = dict(out)
            self.index = {obj_id(k): k for k in out}
        return out

    def snapshot(self):
        snap = dict(self.head)
        snap["base"] = 0
        snap["objects"] = self.reachable()
        return snap

    def name(self):
        key = self.index.get(str(self.head.get("root")))
        obj = self.objects.get(key) if key else None
        return obj.get("name") if isinstance(obj, dict) else None


class Room:
    def __init__(self, name):
        self.name = name
        self.publishers = {}      # ws -> owner id
        self.subscribers = set()
        self.tournaments = {}     # uuid -> Tournament
        self.last_publish = 0.0
        self.last_snapshot_request = 0.0
        self.touched = time.time()
        self.announcement = None  # ostatni komunikat sędziego z LiveChess - dostaje go też nowy widz
        self.announcement_at = 0.0

    def full_list(self):
        return json.dumps({"list": [t.snapshot() for t in self.tournaments.values()]},
                          ensure_ascii=False, separators=(",", ":"))

    def status(self):
        return {
            "type": "room_status",
            "room": self.name,
            "publishers": len(self.publishers),
            "subscribers": len(self.subscribers),
            "tournaments": len(self.tournaments),
            "lastPublishAgo": round(time.time() - self.last_publish, 1) if self.last_publish else None,
        }


ROOMS = {}


def get_room(name):
    room = ROOMS.get(name)
    if room is None:
        room = ROOMS[name] = Room(name)
    room.touched = time.time()
    return room


def send_all(conns, message):
    conns = [c for c in conns]
    if not conns:
        return
    if ws_broadcast is not None:
        ws_broadcast(conns, message)
    else:  # bardzo stare websockets
        for c in conns:
            asyncio.ensure_future(_safe_send(c, message))


async def _safe_send(ws, message):
    try:
        await ws.send(message)
    except Exception:
        pass


def notify_status(room):
    st = json.dumps(room.status())
    send_all(list(room.publishers) + list(room.subscribers), st)


def request_snapshot(room, force=False):
    now = time.time()
    if not force and now - room.last_snapshot_request < SNAPSHOT_REQUEST_COOLDOWN:
        return
    room.last_snapshot_request = now
    if room.publishers:
        log.info("[%s] prośba do nadajnika o pełny stan (need_snapshot)", room.name)
        send_all(list(room.publishers), json.dumps({"type": "need_snapshot", "room": room.name}))


def clear_room(room, who):
    room.tournaments.clear()
    log.info("[%s] BUFOR WYCZYSZCZONY (%s)", room.name, who)
    send_all(room.subscribers, json.dumps({"type": "room_cleared", "room": room.name}))
    request_snapshot(room, force=True)


def is_clear_command(data):
    return isinstance(data, dict) and (data.get("action") == "clear" or data.get("type") == "clear_room")


def admin_ok(key):
    return ADMIN_KEY is None or key == ADMIN_KEY


def handle_publisher_message(room, ws, raw):
    owner = room.publishers.get(ws, "default")
    try:
        data = json.loads(raw)
    except Exception:
        log.warning("[%s] niepoprawny JSON od nadajnika (%d B) - pomijam", room.name, len(raw))
        return
    if not isinstance(data, dict):
        return
    if is_clear_command(data):
        clear_room(room, "nadajnik")
        return
    if data.get("type") == "announce":
        # Komunikat / timer sędziego wysłany z LiveChess - do wszystkich ekranów w pokoju
        data["ageMs"] = 0
        room.announcement = data
        room.announcement_at = time.time()
        log.info("[%s] KOMUNIKAT: %s", room.name, (data.get("text") or "")[:60] if data.get("enabled") else "(wyłączony)")
        send_all(room.subscribers, json.dumps(data, ensure_ascii=False))
        return
    if "type" in data:          # inne komunikaty kontrolne nadajnika (hello/ping) - nie przekazujemy
        return

    room.last_publish = time.time()
    if "list" in data and isinstance(data["list"], list):
        new = {}
        for snap in data["list"]:
            if isinstance(snap, dict) and snap.get("uuid"):
                new[snap["uuid"]] = Tournament(snap, owner)
        for uuid in [u for u, t in room.tournaments.items() if t.owner == owner and u not in new]:
            del room.tournaments[uuid]
        room.tournaments.update(new)
        log.info("[%s] MIGAWKA od nadajnika: %d turniej(e), %d B -> %d widzów",
                 room.name, len(new), len(raw), len(room.subscribers))
        # Widzom wysyłamy pełną listę pokoju (z turniejami innych nadajników, jeśli są)
        send_all(room.subscribers, room.full_list() if len(room.publishers) > 1 else raw)
        return
    if "add" in data and isinstance(data["add"], list):
        for snap in data["add"]:
            if isinstance(snap, dict) and snap.get("uuid"):
                room.tournaments[snap["uuid"]] = Tournament(snap, owner)
        log.info("[%s] nowy turniej (add) -> %d widzów", room.name, len(room.subscribers))
        send_all(room.subscribers, raw)
        return
    if "remove" in data and isinstance(data["remove"], list):
        for uuid in data["remove"]:
            room.tournaments.pop(uuid, None)
        send_all(room.subscribers, raw)
        return
    if "change" in data and isinstance(data["change"], list):
        if not data["change"]:
            return  # keepalive LiveChess - heartbeat relay i tak utrzymuje połączenia
        unknown = False
        for delta in data["change"]:
            if not isinstance(delta, dict):
                continue
            t = room.tournaments.get(delta.get("uuid"))
            if t is None:
                unknown = True
            else:
                t.merge(delta)
        if unknown:
            request_snapshot(room)
        send_all(room.subscribers, raw)
        return
    log.info("[%s] nieznany pakiet nadajnika (%d B) - przekazuję", room.name, len(raw))
    send_all(room.subscribers, raw)


async def handler(ws):
    path = getattr(ws, "path", None)
    if path is None and hasattr(ws, "request"):
        path = ws.request.path
    room_name, params = room_from_path(path or "/")
    role = (params.get("role", ["subscriber"])[0] or "subscriber").lower()
    room = get_room(room_name)
    peer = getattr(ws, "remote_address", None)

    if role == "publisher":
        if PUBLISH_KEY and params.get("key", [None])[0] != PUBLISH_KEY:
            log.warning("[%s] odrzucono nadajnik bez poprawnego klucza (%s)", room_name, peer)
            await ws.close(4003, "bad publish key")
            return
        owner = params.get("pub", ["default"])[0] or "default"
        # Ten sam nadajnik (owner) połączony ponownie -> stare połączenie zamykamy
        for other, other_owner in list(room.publishers.items()):
            if other_owner == owner:
                room.publishers.pop(other, None)
                asyncio.ensure_future(other.close(4000, "replaced by new connection"))
        room.publishers[ws] = owner
        log.info("[%s] NADAJNIK POŁĄCZONY (%s, id=%s)", room_name, peer, owner)
        notify_status(room)
        try:
            async for raw in ws:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", "replace")
                handle_publisher_message(room, ws, raw)
        except Exception as e:
            log.info("[%s] nadajnik: %s", room_name, e)
        finally:
            room.publishers.pop(ws, None)
            log.info("[%s] NADAJNIK ROZŁĄCZONY (%s)", room_name, peer)
            notify_status(room)
    else:
        room.subscribers.add(ws)
        log.info("[%s] WIDZ POŁĄCZONY (%s) - widzów: %d", room_name, peer, len(room.subscribers))
        try:
            if room.tournaments:
                await ws.send(room.full_list())
            if room.announcement:
                # ageMs - ile czasu minęło od wysłania (żeby timer u spóźnionego widza pokazywał właściwy czas)
                ann = dict(room.announcement)
                ann["ageMs"] = int((time.time() - room.announcement_at) * 1000)
                await ws.send(json.dumps(ann, ensure_ascii=False))
            notify_status(room)  # status trafia też do nowego widza
            async for raw in ws:
                if isinstance(raw, bytes):
                    continue
                try:
                    data = json.loads(raw)
                except Exception:
                    continue
                if is_clear_command(data):
                    if admin_ok(data.get("key")):
                        clear_room(room, "panel sędziowski")
                    else:
                        await ws.send(json.dumps({"type": "error", "message": "Brak uprawnień do czyszczenia bufora"}))
        except Exception as e:
            log.info("[%s] widz: %s", room_name, e)
        finally:
            room.subscribers.discard(ws)
            log.info("[%s] WIDZ ROZŁĄCZONY - widzów: %d", room_name, len(room.subscribers))
            notify_status(room)


async def heartbeat():
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        now = time.time()
        for name, room in list(ROOMS.items()):
            conns = list(room.publishers) + list(room.subscribers)
            if conns:
                room.touched = now
                send_all(conns, json.dumps({"type": "hb", "t": int(now * 1000),
                                            "publishers": len(room.publishers),
                                            "subscribers": len(room.subscribers)}))
            elif now - room.touched > ROOM_IDLE_EXPIRY:
                del ROOMS[name]


# ---------------------------------------------------------------- HTTP
_html_cache = {"mtime": None, "body": None}


def viewer_html():
    try:
        mtime = os.path.getmtime(HTML_FILE)
        if _html_cache["mtime"] != mtime:
            with open(HTML_FILE, "r", encoding="utf-8") as f:
                _html_cache["body"] = f.read()
            _html_cache["mtime"] = mtime
        return _html_cache["body"]
    except Exception:
        return "<!DOCTYPE html><html><body><h1>LazyArbiter Relay Server</h1><p>Brak viewer.html</p></body></html>"


def http_response(path):
    """Zwraca (status, content_type, body) albo None dla upgrade'u WebSocket."""
    parsed = urllib.parse.urlparse(path)
    params = urllib.parse.parse_qs(parsed.query)
    p = parsed.path.rstrip("/")
    if p in ("/health", "/healthz"):
        return HTTPStatus.OK, "text/plain; charset=utf-8", "ok"
    if p == "/api/status":
        room = ROOMS.get(normalize_room(params.get("room", [None])[0]))
        if room is None:
            body = {"type": "room_status", "room": normalize_room(params.get("room", [None])[0]),
                    "publishers": 0, "subscribers": 0, "tournaments": 0, "lastPublishAgo": None}
        else:
            body = room.status()
            body["tournamentList"] = [{"uuid": t.uuid, "name": t.name(), "version": t.head.get("version")}
                                      for t in room.tournaments.values()]
        return HTTPStatus.OK, "application/json; charset=utf-8", json.dumps(body, ensure_ascii=False)
    if p in ("/api/clear", "/clear"):
        name = normalize_room(params.get("room", [None])[0])
        if not admin_ok(params.get("key", [None])[0]):
            return HTTPStatus.FORBIDDEN, "application/json; charset=utf-8", json.dumps({"status": "forbidden"})
        if name in ROOMS:
            clear_room(ROOMS[name], "HTTP API")
        return HTTPStatus.OK, "application/json; charset=utf-8", json.dumps({"status": "ok", "room": name, "cleared": True})
    if p == "/favicon.ico":
        return HTTPStatus.NO_CONTENT, "text/plain", ""
    return HTTPStatus.OK, "text/html; charset=utf-8", viewer_html()


def _is_upgrade(headers):
    return headers is not None and "websocket" in str(headers.get("Upgrade", "")).lower()


if NEW_API:
    def process_request(connection, request):
        if _is_upgrade(request.headers):
            return None
        status, ctype, body = http_response(request.path)
        resp = connection.respond(status, body)
        resp.headers["Content-Type"] = ctype
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Cache-Control"] = "no-cache"
        return resp
else:
    def process_request(path, request_headers):
        if _is_upgrade(request_headers):
            return None
        status, ctype, body = http_response(path)
        return (status, [("Content-Type", ctype), ("Access-Control-Allow-Origin", "*"),
                         ("Cache-Control", "no-cache")], body.encode("utf-8"))


async def main():
    port = int(os.environ.get("PORT", 8088))
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass
    print("=" * 64)
    print("  LazyArbiter Cloud Relay (WebSocket + viewer WWW)")
    print(f"  Port: {port}   websockets {websockets.__version__}")
    print(f"  Viewer WWW:  http://<ADRES>:{port}/<pokoj>")
    print(f"  Nadajnik:    ws://<ADRES>:{port}/<pokoj>?role=publisher")
    print(f"  Odbiornik:   ws://<ADRES>:{port}/<pokoj>")
    print("=" * 64, flush=True)
    async with ws_serve(handler, "0.0.0.0", port, process_request=process_request,
                        max_size=32 * 1024 * 1024, ping_interval=20, ping_timeout=30):
        asyncio.ensure_future(heartbeat())
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
