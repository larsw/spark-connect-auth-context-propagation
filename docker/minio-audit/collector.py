"""A minimal sink for MinIO's audit webhook.

MinIO can only ship audit events to a webhook or Kafka, so proving which credential the
executor actually used needs somewhere for those events to land. This appends each event as one
JSON line to a shared volume that the test suite reads.
"""
from __future__ import annotations

import http.server
import json
import pathlib
import threading

AUDIT_FILE = pathlib.Path("/audit/audit.jsonl")
_lock = threading.Lock()


class Collector(http.server.BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        with _lock:
            with AUDIT_FILE.open("a") as handle:
                for line in body.decode("utf-8", errors="replace").splitlines():
                    if line.strip():
                        handle.write(line.strip() + "\n")
        self.send_response(200)
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802 - health check
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args) -> None:
        pass  # the audit file is the output; stdout noise helps nobody


if __name__ == "__main__":
    AUDIT_FILE.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_FILE.touch(exist_ok=True)
    http.server.ThreadingHTTPServer(("0.0.0.0", 8888), Collector).serve_forever()
