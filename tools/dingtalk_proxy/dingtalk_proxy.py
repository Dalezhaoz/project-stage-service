"""
钉钉消息转发代理 - 运行在能访问外网的机器上
服务器发消息到这个代理，代理转发到钉钉 API

用法: python dingtalk_proxy.py [端口，默认9100]
"""

import sys
import json
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler


class ProxyHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            payload = json.loads(body)

            target_url = payload.get("targetUrl", "")
            message = payload.get("message", {})

            if not target_url:
                self._respond(400, {"error": "缺少 targetUrl"})
                return

            # Forward to DingTalk
            data = json.dumps(message).encode("utf-8")
            req = urllib.request.Request(
                target_url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read())

            self._respond(200, result)
            print(f"[OK] Forwarded to DingTalk -> errcode={result.get('errcode', '?')}")

        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8", errors="replace")
            self._respond(e.code, {"error": error_body})
            print(f"[ERR] DingTalk returned {e.code}: {error_body}")
        except Exception as e:
            self._respond(500, {"error": str(e)})
            print(f"[ERR] {e}")

    def _respond(self, code, data):
        body = json.dumps(data).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # suppress default logs


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9100
    server = HTTPServer(("0.0.0.0", port), ProxyHandler)
    print(f"钉钉转发代理已启动: http://0.0.0.0:{port}")
    print("等待服务器消息...")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")


if __name__ == "__main__":
    main()
