"""
钉钉消息转发代理 - 运行在能访问外网的机器上
服务器发消息到这个代理，代理转发到钉钉 API
启动后自动向主服务注册，定时心跳保活，VPN IP 变化自动更新。

用法: python dingtalk_proxy.py [--port 9100] [--server http://主服务地址] [--token 密钥] [--detect-target 内网IP]

也可以通过环境变量配置:
  PROXY_PORT=9100
  MAIN_SERVER=http://xxx.xxx.xxx.xxx:5000
  PROXY_TOKEN=你的钉钉签名密钥
"""

import sys
import os
import json
import socket
import threading
import time
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler


# ---------- Configuration ----------

def parse_args():
    """Parse command line arguments."""
    port = int(os.environ.get("PROXY_PORT", "9100"))
    server = os.environ.get("MAIN_SERVER", "")
    token = os.environ.get("PROXY_TOKEN", "")
    proxy_ip = os.environ.get("PROXY_IP", "")
    detect_target = os.environ.get("DETECT_TARGET", "")

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--port" and i + 1 < len(args):
            port = int(args[i + 1])
            i += 2
        elif args[i] == "--server" and i + 1 < len(args):
            server = args[i + 1]
            i += 2
        elif args[i] == "--token" and i + 1 < len(args):
            token = args[i + 1]
            i += 2
        elif args[i] == "--proxy-ip" and i + 1 < len(args):
            proxy_ip = args[i + 1]
            i += 2
        elif args[i] == "--detect-target" and i + 1 < len(args):
            detect_target = args[i + 1]
            i += 2
        elif args[i].isdigit():
            port = int(args[i])  # backward compatible: positional port
            i += 1
        else:
            i += 1

    return port, server, token, proxy_ip, detect_target


# ---------- Heartbeat ----------

def get_local_ip(target_host):
    """Get the local IP address used to reach the target host."""
    try:
        # Parse host from URL
        host = target_host.split("://")[-1].split("/")[0].split(":")[0]
        port = 80
        parts = target_host.split("://")[-1].split("/")[0].split(":")
        if len(parts) == 2:
            port = int(parts[1])
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect((host, port))
            return s.getsockname()[0]
    except Exception:
        return None


def heartbeat_loop(port, server_url, token, proxy_ip="", detect_target=""):
    """Periodically register this proxy with the main server."""
    if not server_url:
        print("[心跳] 未配置 --server，跳过自动注册。需手动在主服务配置 ProxyUrl。")
        return

    register_url = server_url.rstrip("/") + "/api/dingtalk/register-proxy"
    last_ip = None
    fail_count = 0

    while True:
        try:
            # detect_target 用内网 IP 探测本机 VPN 地址，避免域名走公网出口
            local_ip = proxy_ip or get_local_ip(detect_target or server_url)
            if not local_ip:
                print("[心跳] 无法获取本机 IP，等待重试...")
                time.sleep(30)
                continue

            proxy_url = f"http://{local_ip}:{port}"

            payload = json.dumps({
                "proxyUrl": proxy_url,
                "token": token
            }).encode("utf-8")

            req = urllib.request.Request(
                register_url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                resp.read()

            if local_ip != last_ip:
                if last_ip:
                    print(f"[心跳] ✅ IP 已变更 {last_ip} → {local_ip}，已更新注册")
                else:
                    print(f"[心跳] ✅ 已注册到主服务: {proxy_url}")
                last_ip = local_ip
            fail_count = 0

        except Exception as e:
            fail_count += 1
            if fail_count <= 3 or fail_count % 10 == 0:
                print(f"[心跳] ❌ 注册失败 (#{fail_count}): {e}")

        time.sleep(60)


# ---------- Proxy Handler ----------

class ProxyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self._respond(200, {"status": "ok", "message": "钉钉转发代理运行中"})

    def do_POST(self):
        try:
            # Support both Content-Length and chunked transfer encoding
            transfer_encoding = self.headers.get("Transfer-Encoding", "")
            content_length = int(self.headers.get("Content-Length", 0))

            if "chunked" in transfer_encoding.lower():
                body = self._read_chunked()
            else:
                body = self.rfile.read(content_length)

            print(f"[REQ] {self.command} {self.path} len={len(body)} TE={transfer_encoding}")

            if not body:
                self._respond(400, {"error": "请求体为空"})
                print("[ERR] Empty body")
                return

            payload = json.loads(body)
            target_url = payload.get("targetUrl", "")
            message = payload.get("message", {})

            if not target_url:
                self._respond(400, {"error": "缺少 targetUrl"})
                print("[ERR] Missing targetUrl")
                return

            print(f"[FWD] -> {target_url[:80]}...")

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
            print(f"[OK] Forwarded -> errcode={result.get('errcode', '?')}")

        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8", errors="replace")
            self._respond(e.code, {"error": error_body})
            print(f"[ERR] DingTalk returned {e.code}: {error_body}")
        except json.JSONDecodeError as e:
            self._respond(400, {"error": f"JSON 解析失败: {e}"})
            print(f"[ERR] JSON decode failed: {e}")
        except Exception as e:
            self._respond(500, {"error": str(e)})
            print(f"[ERR] {e}")

    def _read_chunked(self):
        """Read chunked transfer encoding body."""
        data = b""
        while True:
            line = self.rfile.readline().strip()
            chunk_size = int(line, 16)
            if chunk_size == 0:
                self.rfile.readline()  # trailing CRLF
                break
            data += self.rfile.read(chunk_size)
            self.rfile.readline()  # trailing CRLF after chunk
        return data

    def _respond(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # suppress default access logs


# ---------- Main ----------

def main():
    port, server, token, proxy_ip, detect_target = parse_args()

    print(f"钉钉转发代理已启动: http://0.0.0.0:{port}")
    if proxy_ip:
        print(f"指定注册 IP: {proxy_ip}")
    if detect_target:
        print(f"IP 探测目标: {detect_target}")
    if server:
        print(f"主服务地址: {server}")
        print(f"心跳间隔: 60 秒")
        # Start heartbeat in background thread
        t = threading.Thread(target=heartbeat_loop, args=(port, server, token, proxy_ip, detect_target), daemon=True)
        t.start()
    else:
        print("未配置 --server，仅作为转发代理运行（手动配置模式）")

    print("等待服务器消息...")
    http_server = HTTPServer(("0.0.0.0", port), ProxyHandler)
    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")


if __name__ == "__main__":
    main()
