"""
钉钉消息转发代理 - 运行在能访问外网的机器上
服务器发消息到这个代理，代理转发到钉钉 API
启动后自动向主服务注册，定时心跳保活，VPN IP 变化自动更新。

配置优先级: config.ini > 环境变量 > 命令行参数

config.ini 示例:
  [proxy]
  port = 9100
  server = http://your-server-ip:5000
  token = your_dingtalk_secret

命令行用法:
  python dingtalk_proxy.py [--port 9100] [--server http://主服务地址] [--token 密钥]
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

HEARTBEAT_INTERVAL = 30  # seconds


# ---------- Logging ----------

def log(tag, msg):
    now = time.strftime("%H:%M:%S")
    print(f"[{now}] [{tag}] {msg}", flush=True)


# ---------- Configuration ----------

def load_config_ini():
    """Load config.ini from the same directory as this script."""
    ini_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
    if not os.path.exists(ini_path):
        return {}

    config = {}
    try:
        import configparser
        cp = configparser.ConfigParser()
        cp.read(ini_path, encoding="utf-8")
        if cp.has_section("proxy"):
            for key in ("port", "server", "token", "proxy_ip", "detect_target"):
                if cp.has_option("proxy", key):
                    config[key] = cp.get("proxy", key).strip()
    except Exception as e:
        log("配置", f"读取 config.ini 失败: {e}")
    return config


def parse_args():
    """Merge config.ini, environment variables, and CLI arguments (later overrides earlier)."""
    ini = load_config_ini()

    port = int(ini.get("port") or os.environ.get("PROXY_PORT") or 9100)
    server = ini.get("server") or os.environ.get("MAIN_SERVER") or ""
    token = ini.get("token") or os.environ.get("PROXY_TOKEN") or ""
    proxy_ip = ini.get("proxy_ip") or os.environ.get("PROXY_IP") or ""
    detect_target = ini.get("detect_target") or os.environ.get("DETECT_TARGET") or ""

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--port" and i + 1 < len(args):
            port = int(args[i + 1]); i += 2
        elif args[i] == "--server" and i + 1 < len(args):
            server = args[i + 1]; i += 2
        elif args[i] == "--token" and i + 1 < len(args):
            token = args[i + 1]; i += 2
        elif args[i] == "--proxy-ip" and i + 1 < len(args):
            proxy_ip = args[i + 1]; i += 2
        elif args[i] == "--detect-target" and i + 1 < len(args):
            detect_target = args[i + 1]; i += 2
        elif args[i].isdigit():
            port = int(args[i]); i += 1
        else:
            i += 1

    return port, server, token, proxy_ip, detect_target


# ---------- Heartbeat ----------

def get_local_ip(target_host):
    """Get the local IP address used to reach the target host."""
    try:
        host = target_host.split("://")[-1].split("/")[0].split(":")[0]
        parts = target_host.split("://")[-1].split("/")[0].split(":")
        port = int(parts[1]) if len(parts) == 2 else 80
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect((host, port))
            return s.getsockname()[0]
    except Exception:
        return None


def heartbeat_loop(port, server_url, token, proxy_ip="", detect_target=""):
    """Periodically register this proxy with the main server."""
    if not server_url:
        log("心跳", "未配置 server，跳过自动注册。需手动在主服务配置 ProxyUrl。")
        return

    register_url = server_url.rstrip("/") + "/api/dingtalk/register-proxy"
    last_ip = None
    fail_count = 0

    while True:
        try:
            local_ip = proxy_ip or get_local_ip(detect_target or server_url)
            if not local_ip:
                log("心跳", "无法获取本机 IP，等待重试...")
                time.sleep(HEARTBEAT_INTERVAL)
                continue

            proxy_url = f"http://{local_ip}:{port}"
            payload = json.dumps({"proxyUrl": proxy_url, "token": token}).encode("utf-8")
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
                    log("心跳", f"✅ IP 已变更 {last_ip} → {local_ip}，已更新注册")
                else:
                    log("心跳", f"✅ 已注册到主服务: {proxy_url}")
                last_ip = local_ip
            fail_count = 0

        except Exception as e:
            fail_count += 1
            if fail_count <= 3 or fail_count % 10 == 0:
                log("心跳", f"❌ 注册失败 (#{fail_count}): {e}")

        time.sleep(HEARTBEAT_INTERVAL)


# ---------- Proxy Handler ----------

class ProxyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self._respond(200, {"status": "ok", "message": "钉钉转发代理运行中"})

    def do_POST(self):
        try:
            transfer_encoding = self.headers.get("Transfer-Encoding", "")
            content_length = int(self.headers.get("Content-Length", 0))

            if "chunked" in transfer_encoding.lower():
                body = self._read_chunked()
            else:
                body = self.rfile.read(content_length)

            if not body:
                self._respond(400, {"error": "请求体为空"})
                return

            payload = json.loads(body)
            target_url = payload.get("targetUrl", "")
            message = payload.get("message", {})

            if not target_url:
                self._respond(400, {"error": "缺少 targetUrl"})
                return

            log("转发", f"-> {target_url[:80]}...")

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
            errcode = result.get("errcode", "?")
            if errcode == 0:
                log("转发", f"✅ 成功")
            else:
                log("转发", f"⚠️  钉钉返回 errcode={errcode}: {result.get('errmsg', '')}")

        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8", errors="replace")
            self._respond(e.code, {"error": error_body})
            log("转发", f"❌ 钉钉返回 {e.code}: {error_body}")
        except json.JSONDecodeError as e:
            self._respond(400, {"error": f"JSON 解析失败: {e}"})
            log("转发", f"❌ JSON 解析失败: {e}")
        except Exception as e:
            self._respond(500, {"error": str(e)})
            log("转发", f"❌ {e}")

    def _read_chunked(self):
        data = b""
        while True:
            line = self.rfile.readline().strip()
            chunk_size = int(line, 16)
            if chunk_size == 0:
                self.rfile.readline()
                break
            data += self.rfile.read(chunk_size)
            self.rfile.readline()
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

    log("启动", f"钉钉转发代理 http://0.0.0.0:{port}")
    if proxy_ip:
        log("启动", f"指定注册 IP: {proxy_ip}")
    if detect_target:
        log("启动", f"IP 探测目标: {detect_target}")
    if server:
        log("启动", f"主服务: {server}，心跳间隔: {HEARTBEAT_INTERVAL}s")
        t = threading.Thread(
            target=heartbeat_loop,
            args=(port, server, token, proxy_ip, detect_target),
            daemon=True,
        )
        t.start()
    else:
        log("启动", "未配置 server，仅转发模式（手动在主服务填写 ProxyUrl）")

    log("启动", "等待消息...")
    http_server = HTTPServer(("0.0.0.0", port), ProxyHandler)
    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        log("启动", "已停止")


if __name__ == "__main__":
    main()
