"""HTTP wrapper so the same handler runs on any open-source FaaS platform (OpenFaaS 'of-watchdog' http mode,
Knative, Fission) or standalone:  python server.py 8080   (set STORE_BACKEND / SQLITE_PATH / POSTGRES_DSN)."""
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import handler as h

_inited = False


class H(BaseHTTPRequestHandler):
    def do_POST(self):
        global _inited
        cold, init_ms = False, 0.0
        if not _inited:
            t = time.time()
            h.init()
            _inited, cold, init_ms = True, True, (time.time() - t) * 1e3
        ev = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        out = json.dumps({"response": h.handler(ev), "cold": cold, "init_ms": init_ms}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1]) if len(sys.argv) > 1 else 8080), H).serve_forever()
