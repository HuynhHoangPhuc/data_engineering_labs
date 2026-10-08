#!/usr/bin/env python3
"""A tiny webhook receiver that stands in for Slack / PagerDuty / Opsgenie / Teams.

Runs in the `alert-receiver` container (docker compose --profile monitor up -d) or on your laptop:

    python scripts/webhook_receiver.py --port 9099 --log data/alerts.jsonl

  POST /alert   JSON body -> printed as one line + appended to the JSONL log, answers 200 {"ok": true}
  GET  /        the last 50 alerts as plain text (open http://localhost:9099 in a browser)
  GET  /health  "ok"

Standard library only (http.server), so it runs in the 30 MB python:3.12-alpine image.
"""
import argparse
import json
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ALERTS: list[dict] = []
LOG: Path | None = None


class Handler(BaseHTTPRequestHandler):
    def _reply(self, code: int, body: str, ctype: str = "text/plain; charset=utf-8") -> None:
        data = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # noqa: N802
        if self.path == "/health":
            return self._reply(200, "ok\n")
        lines = [f"{len(ALERTS)} alert(s) received since start\n"]
        for a in ALERTS[-50:]:
            lines.append(f"{a['received_at']}  [{a.get('severity', '?').upper():8}] {a.get('check', '?'):12} "
                         f"{a.get('dataset', '')} {a.get('partition', '')}: {a.get('message', '')}")
        return self._reply(200, "\n".join(lines) + "\n")

    def do_POST(self):  # noqa: N802
        if self.path != "/alert":
            return self._reply(404, "POST to /alert\n")
        length = int(self.headers.get("Content-Length", 0))
        try:
            alert = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(alert, dict):
                alert = {"payload": alert}
        except json.JSONDecodeError:
            return self._reply(400, '{"ok": false, "error": "body must be JSON"}\n', "application/json")
        alert["received_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        ALERTS.append(alert)
        print(f"ALERT [{alert.get('severity', '?').upper()}] {alert.get('check', '?')} "
              f"{alert.get('dataset', '')} {alert.get('partition', '')}: {alert.get('message', '')}", flush=True)
        if LOG:
            with LOG.open("a") as f:
                f.write(json.dumps(alert) + "\n")
        return self._reply(200, '{"ok": true}\n', "application/json")

    def log_message(self, fmt, *args):  # keep the console for alerts only
        pass


def main() -> None:
    global LOG
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9099)
    ap.add_argument("--log", default="", help="append every alert to this JSONL file")
    args = ap.parse_args()
    if args.log:
        LOG = Path(args.log)
        LOG.parent.mkdir(parents=True, exist_ok=True)
    print(f"webhook receiver listening on :{args.port}  (POST /alert, GET /)", flush=True)
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
