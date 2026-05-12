#!/usr/bin/env python3
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

HOST = "127.0.0.1"
PORT = 9010
STATE_FILE = os.path.join(os.path.dirname(__file__), ".airflow_monitor_state.json")


def read_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def write_state(data):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, STATE_FILE)


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        p = urlparse(self.path)
        if p.path != "/state":
            self._send(404, {"ok": False, "error": "not found"})
            return
        key = parse_qs(p.query).get("key", ["default"])[0]
        all_state = read_state()
        self._send(200, {"ok": True, "key": key, "state": all_state.get(key)})

    def do_POST(self):
        p = urlparse(self.path)
        if p.path != "/state":
            self._send(404, {"ok": False, "error": "not found"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            self._send(400, {"ok": False, "error": "invalid json"})
            return

        key = payload.get("key", "default")
        state = payload.get("state")
        all_state = read_state()
        all_state[key] = state
        write_state(all_state)
        self._send(200, {"ok": True})


if __name__ == "__main__":
    print(f"Airflow state server running on http://{HOST}:{PORT}")
    HTTPServer((HOST, PORT), Handler).serve_forever()
