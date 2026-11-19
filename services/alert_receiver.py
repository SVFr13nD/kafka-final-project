"""Local notification destination, inspectable without Telegram credentials."""
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import json
import time
OUT=Path("/evidence/alerts.jsonl")
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b"ok")
    def do_POST(self):
        size=int(self.headers.get("Content-Length","0"))
        if size>1024*1024:
            self.send_error(413);return
        try:
            payload=json.loads(self.rfile.read(size))
        except ValueError:
            self.send_error(400);return
        OUT.parent.mkdir(parents=True,exist_ok=True)
        with OUT.open("a") as f:
            f.write(json.dumps({"received_at":time.time(),"payload":payload})+"\n")
        print("notification_received",payload.get("status"),flush=True)
        self.send_response(200); self.end_headers(); self.wfile.write(b"received")
HTTPServer(("0.0.0.0",8080),Handler).serve_forever()
