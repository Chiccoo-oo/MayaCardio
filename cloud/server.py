"""
Cloud HTTP REST server for MayaCardio pipeline.
Zero external dependencies (uses standard library ThreadingHTTPServer).
Provides ingest (with gzip support), sync, online window processing, export, and status endpoints.
"""

import argparse
import gzip
import json
import urllib.parse
from http.server import HTTPServer, ThreadingHTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any
from cloud.storage import CloudStorage

class CloudRequestHandler(BaseHTTPRequestHandler):
    storage: CloudStorage = None

    def _set_headers(self, status: int = 200, content_type: str = "application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Content-Encoding")
        self.end_headers()

    def do_OPTIONS(self):
        self._set_headers(204)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        session_id = query.get("session", [None])[0]

        if path == "/health":
            self._set_headers(200, "application/json")
            self.wfile.write(b'{"status":"ok"}')
            return

        if path == "/api/status":
            count = self.storage.get_sample_count(session_id)
            resp = {
                "status": "ok",
                "session_id": session_id,
                "sample_count": count
            }
            self._set_headers(200, "application/json")
            self.wfile.write(json.dumps(resp).encode("utf-8"))
            return

        if path == "/api/results":
            if not session_id:
                self._set_headers(400, "application/json")
                self.wfile.write(b'{"error":"Missing session parameter"}')
                return

            results = self.storage.get_results(session_id)
            self._set_headers(200, "application/x-ndjson")
            for r in results:
                self.wfile.write((json.dumps(r, separators=(',', ':')) + "\n").encode("utf-8"))
            return

        if path == "/api/export":
            if not session_id:
                self._set_headers(400, "application/json")
                self.wfile.write(b'{"error":"Missing session parameter"}')
                return

            self._set_headers(200, "application/x-ndjson")
            for line in self.storage.export_data_generator(session_id):
                self.wfile.write((line + "\n").encode("utf-8"))
            return

        self._set_headers(404, "application/json")
        self.wfile.write(b'{"error":"Not Found"}')

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/api/ingest":
            content_length = int(self.headers.get("Content-Length", 0))
            raw_body = self.rfile.read(content_length)

            content_encoding = self.headers.get("Content-Encoding", "").lower()
            if "gzip" in content_encoding:
                try:
                    raw_body = gzip.decompress(raw_body)
                except Exception as e:
                    self._set_headers(400, "application/json")
                    self.wfile.write(json.dumps({"error": f"Gzip decompress error: {e}"}).encode("utf-8"))
                    return

            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except Exception as e:
                self._set_headers(400, "application/json")
                self.wfile.write(json.dumps({"error": f"Invalid JSON payload: {e}"}).encode("utf-8"))
                return

            session_id = payload.get("session_id")
            samples = payload.get("samples", [])
            lost_items = payload.get("lost", [])
            results_items = payload.get("results", [])

            if not session_id:
                self._set_headers(400, "application/json")
                self.wfile.write(b'{"error":"Missing session_id"}')
                return

            inserted = self.storage.ingest_samples(session_id, samples)
            if lost_items:
                self.storage.ingest_lost(session_id, lost_items)
            if results_items:
                self.storage.ingest_results(session_id, results_items)

            # Online processing within < 5 seconds
            self.storage.process_pending_windows(session_id)

            resp = {
                "status": "ok",
                "session_id": session_id,
                "inserted": inserted,
                "total_cloud": self.storage.get_sample_count(session_id)
            }
            self._set_headers(200, "application/json")
            self.wfile.write(json.dumps(resp).encode("utf-8"))
            return

        self._set_headers(404, "application/json")
        self.wfile.write(b'{"error":"Not Found"}')

    def log_message(self, format, *args):
        # Silence default request logging to keep CLI and tests clean
        pass

def run_server(port: int = 8000, db_path: str = "maya_cloud.db"):
    storage = CloudStorage(db_path=db_path)
    CloudRequestHandler.storage = storage
    server = ThreadingHTTPServer(("0.0.0.0", port), CloudRequestHandler)
    print(f"[maya-cloud] Server running on port {port} (db={db_path})")
    server.serve_forever()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MayaCardio Cloud Server")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on")
    parser.add_argument("--db", type=str, default="maya_cloud.db", help="Cloud SQLite database file")
    args = parser.parse_args()
    run_server(port=args.port, db_path=args.db)
