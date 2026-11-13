"""API de práctica con registros ficticios y latencia controlada."""

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


SAMPLES = {
    "/ventas": [
        {"pedido": "V-01", "sku": "AL-01", "unidades": 2, "fecha": "2026-11-12"},
        {"pedido": "V-02", "sku": "AL-02", "unidades": 1, "fecha": "2026-11-12"},
    ],
    "/inventario": [
        {"sku": "AL-01", "sucursal": "Centro", "stock": 14},
        {"sku": "AL-02", "sucursal": "Centro", "stock": 6},
    ],
}


class DemoHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        records = SAMPLES.get(self.path)
        if records is None:
            self.send_error(404)
            return

        time.sleep(0.45)
        payload = json.dumps(records).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    with ThreadingHTTPServer(("127.0.0.1", args.port), DemoHandler) as server:
        print(f"API de práctica en http://127.0.0.1:{server.server_port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
