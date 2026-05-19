#!/usr/bin/env python3
import base64
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

HOST = "127.0.0.1"
PORT = 9010
STATE_FILE = os.path.join(os.path.dirname(__file__), ".airflow_monitor_state.json")
AIRFLOW_PROXY_PREFIX = "/airflow"


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
    def _set_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,PATCH,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _send(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._set_cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, code, body, content_type="text/plain; charset=utf-8"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self._set_cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return None

    def _proxy_airflow(self):
        p = urlparse(self.path)
        if not p.path.startswith(AIRFLOW_PROXY_PREFIX):
            self._send(404, {"ok": False, "error": "not found"})
            return

        payload = self._read_json_body()
        if payload is None:
            self._send(400, {"ok": False, "error": "invalid json"})
            return

        upstream_method = str((payload or {}).get("method") or "GET").upper()
        base_url = str((payload or {}).get("baseUrl") or "").strip().rstrip("/")
        path = str((payload or {}).get("path") or "").strip()
        username = str((payload or {}).get("username") or "")
        password = str((payload or {}).get("password") or "")
        upstream_body = (payload or {}).get("body")
        extra_headers = dict((payload or {}).get("headers") or {})

        if upstream_method not in {"GET", "POST", "PATCH"}:
            self._send(400, {"ok": False, "error": f"unsupported method: {upstream_method}"})
            return
        if not base_url:
            self._send(400, {"ok": False, "error": "missing baseUrl"})
            return
        if not path.startswith("/"):
            self._send(400, {"ok": False, "error": "path must start with /"})
            return

        target_url = f"{base_url}{path}"
        request_headers = {"Accept": extra_headers.pop("Accept", "application/json")}
        if username or password:
            token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            request_headers["Authorization"] = f"Basic {token}"
        request_headers.update(extra_headers)

        request_data = None
        if upstream_body is not None:
            if isinstance(upstream_body, str):
                request_data = upstream_body.encode("utf-8")
            else:
                request_data = json.dumps(upstream_body).encode("utf-8")
            request_headers.setdefault("Content-Type", "application/json")

        req = Request(target_url, data=request_data, method=upstream_method, headers=request_headers)
        try:
            with urlopen(req, timeout=30) as res:
                body = res.read()
                content_type = res.headers.get("Content-Type", "application/json; charset=utf-8")
                self._send_text(res.status, body, content_type=content_type)
                return
        except HTTPError as e:
            body = e.read()
            content_type = e.headers.get("Content-Type", "text/plain; charset=utf-8")
            self._send_text(e.code, body, content_type=content_type)
            return
        except URLError as e:
            self._send(502, {"ok": False, "error": f"upstream unreachable: {e.reason}"})
            return
        except Exception as e:
            self._send(500, {"ok": False, "error": f"proxy failure: {e}"})
            return

    def do_OPTIONS(self):
        self.send_response(204)
        self._set_cors_headers()
        self.end_headers()

    def do_GET(self):
        p = urlparse(self.path)
        if p.path == "/state":
            key = parse_qs(p.query).get("key", ["default"])[0]
            all_state = read_state()
            self._send(200, {"ok": True, "key": key, "state": all_state.get(key)})
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        p = urlparse(self.path)
        if p.path == "/state":
            payload = self._read_json_body()
            if payload is None:
                self._send(400, {"ok": False, "error": "invalid json"})
                return

            key = payload.get("key", "default")
            state = payload.get("state")
            all_state = read_state()
            all_state[key] = state
            write_state(all_state)
            self._send(200, {"ok": True})
            return
        if p.path == f"{AIRFLOW_PROXY_PREFIX}/request":
            self._proxy_airflow()
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_PATCH(self):
        self._send(405, {"ok": False, "error": "use POST /airflow/request with method in payload"})


if __name__ == "__main__":
    print(f"Airflow state server running on http://{HOST}:{PORT}")
    HTTPServer((HOST, PORT), Handler).serve_forever()
