#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bridge.py - 节点测速桥接（支持 sing-box / mihomo 两种内核）

两种模式：
  1. sing-box（默认，NekoBox 风格）
     每代理独立 sing-box 实例 → 请求 cp.cloudflare.com → 测量 RTT
  2. mihomo（单实例批量测速）
     全部代理载入单个 mihomo 实例 → API 批量测延迟

在下面配置区修改 KERNEL 即可切换。
"""

import base64
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
import zipfile
import gzip
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ── 配置 ──────────────────────────────────────────────
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(PROJECT_DIR, "box.txt")
DEFAULT_SOURCE = os.path.join(PROJECT_DIR, "nodes", "neokongbox.txt")

# 内核选择
KERNEL = "sing-box"   # "sing-box": NekoBox风格(每代理独立实例)
                       # "mihomo":  单实例批量测速

# 测速参数（两种内核共用）
TEST_URL = "http://cp.cloudflare.com/"
TIMEOUT = 5           # 超时 5s（NekoBox 默认 5000ms）
MAX_CONCURRENT = 5    # 并发 5（NekoBox 默认）
LATENCY_TEST = True   # 是否测延迟
LIMIT = 500

# BAN 列表
BAN = ["中国", "China", "CN", "电信", "移动", "联通"]

CLASH_API_PORT = 19090  # 用不常用的端口避免冲突
CLASH_API_HOST = "127.0.0.1"


# ══════════════════════════════════════════════════════
# sing-box 内核下载（NekoBox 使用的内核，SagerNet/sing-box）
# ══════════════════════════════════════════════════════

def get_singbox_binary():
    """下载/定位 sing-box 二进制（NekoBox 使用的内核）"""
    sys_plat = platform.system().lower()
    arch_map = {"amd64": "amd64", "x86_64": "amd64", "arm64": "arm64", "aarch64": "arm64"}
    arch = arch_map.get(platform.machine().lower(), "amd64")

    if sys_plat == "windows":
        binary_name = "sing-box.exe"
        zip_pattern = f"{sys_plat}-{arch}"
        ext = ".zip"
    elif sys_plat == "linux":
        binary_name = "sing-box"
        zip_pattern = f"{sys_plat}-{arch}"
        ext = ".tar.gz"
    elif sys_plat == "darwin":
        binary_name = "sing-box"
        zip_pattern = f"{sys_plat}-{arch}"
        ext = ".tar.gz"
    else:
        raise OSError(f"不支持的操作系统: {sys_plat}")

    binary_path = os.path.join(PROJECT_DIR, binary_name)
    if os.path.exists(binary_path):
        print(f"[OK] sing-box 内核已存在: {binary_path}")
        return binary_path

    print(f"[*] 下载 sing-box 内核 ({zip_pattern})...")
    try:
        import requests
        resp = requests.get(
            "https://api.github.com/repos/SagerNet/sing-box/releases/latest",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=15
        )
        resp.raise_for_status()
        data = resp.json()

        dl_url = None
        for asset in data.get("assets", []):
            name = asset.get("name", "")
            if zip_pattern in name and name.endswith(ext) and "glibc" not in name and "musl" not in name:
                dl_url = asset["browser_download_url"]
                break
                dl_url = asset["browser_download_url"]
                break

        if not dl_url:
            print(f"[!] 未找到包含 {zip_pattern} 的 {ext} 文件")
            return ""

        print(f"[*] 下载: {dl_url}")
        resp = requests.get(dl_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=120)
        resp.raise_for_status()

        tmp_file = os.path.join(PROJECT_DIR, f"sg_dl{ext}")
        with open(tmp_file, "wb") as f:
            f.write(resp.content)

        import tarfile
        if ext == ".zip":
            with zipfile.ZipFile(tmp_file) as zf:
                zf.extractall(PROJECT_DIR)
                for name in zf.namelist():
                    if name.endswith(f"/{binary_name}") or name == binary_name:
                        src = os.path.join(PROJECT_DIR, name)
                        if os.path.isfile(src):
                            shutil.move(src, binary_path)
                            break
        elif ext == ".tar.gz":
            with tarfile.open(tmp_file, "r:gz") as tf:
                tf.extractall(PROJECT_DIR)
                for name in tf.getnames():
                    if name.endswith(f"/{binary_name}") or name == binary_name:
                        src = os.path.join(PROJECT_DIR, name)
                        if os.path.isfile(src):
                            shutil.move(src, binary_path)
                            break

        os.remove(tmp_file)
        if sys_plat != "windows":
            os.chmod(binary_path, 0o755)
        print(f"[OK] sing-box 下载完成: {binary_path}")
        return binary_path

    except Exception as e:
        print(f"[!] sing-box 下载失败: {e}")
        return ""


# ══════════════════════════════════════════════════════
# URL 解析（复用）
# ══════════════════════════════════════════════════════

VALID_PROTOCOLS = ("vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "hy2://")

def preprocess_input(lines):
    filtered = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith(VALID_PROTOCOLS):
            filtered.append(line)
    return filtered


def parse_proxy_url(url):
    try:
        if url.startswith("vmess://"):
            b64 = url[8:]
            padded = b64 + "=" * (-len(b64) % 4)
            data = json.loads(base64.b64decode(padded))
            return {"name": data.get("ps", data.get("add", "vmess")), "type": "vmess",
                    "server": data.get("add", ""), "port": int(data.get("port", 0)),
                    "uuid": data.get("id", ""), "alterId": int(data.get("aid", "0")),
                    "cipher": data.get("scy", "auto"), "udp": True,
                    "network": data.get("net", "tcp"),
                    "tls": bool(data.get("tls")),
                    "sni": data.get("sni", "")}
        elif url.startswith("vless://"):
            body = url[8:]
            frag = body.split("#", 1)
            name = urllib.parse.unquote(frag[1]) if len(frag) > 1 else ""
            ui, hi = frag[0].split("@", 1)
            hp = hi.split("?")[0]
            port = hp.split(":")[-1] if ":" in hp else "443"
            server = hp.split(":")[0] if ":" in hp else hp
            q = urllib.parse.parse_qs(hi.split("?")[1]) if "?" in hi else {}
            proxy = {"name": name or f"vless-{server}", "type": "vless",
                     "server": server, "port": int(port), "uuid": ui, "udp": True,
                     "network": q.get("type", ["tcp"])[0],
                     "tls": q.get("security", ["none"])[0] == "tls",
                     "sni": q.get("sni", [""])[0]}
            path = q.get("path", [""])[0]
            host = q.get("host", [""])[0]
            if path: proxy["ws-opts"] = {"path": path, "headers": {"Host": host}} if host else {"path": path}
            if host: proxy["ws-headers"] = {"Host": host}
            return proxy
        elif url.startswith("trojan://"):
            body = url[9:]
            frag = body.split("#", 1)
            name = urllib.parse.unquote(frag[1]) if len(frag) > 1 else ""
            ui, hi = frag[0].split("@", 1)
            pw = ui.split(":")[-1] if ":" in ui else ui
            hp = hi.split("?")[0]
            server = hp.split(":")[0] if ":" in hp else hp
            port = hp.split(":")[-1] if ":" in hp else "443"
            q = urllib.parse.parse_qs(hi.split("?")[1]) if "?" in hi else {}
            return {"name": name or f"trojan-{server}", "type": "trojan", "udp": True,
                    "server": server, "port": int(port), "password": pw,
                    "sni": q.get("sni", [""])[0]}
        elif url.startswith("ss://"):
            body = url[5:]
            frag = body.split("#", 1)
            name = urllib.parse.unquote(frag[1]) if len(frag) > 1 else ""
            raw = frag[0]
            if "@" in raw:
                cp, si = raw.split("@", 1)
                si = si.split("?")[0]
                server = si.rsplit(":", 1)[0] if ":" in si else si
                port = si.rsplit(":", 1)[-1] if ":" in si else ""
                raw_b64 = urllib.parse.unquote(cp)
                padded = raw_b64 + "=" * (-len(raw_b64) % 4)
                try:
                    dec = base64.urlsafe_b64decode(padded).decode("utf-8")
                except Exception:
                    dec = raw_b64 if ":" in raw_b64 else None
                if not dec: return None
                cipher, password = dec.split(":", 1) if ":" in dec else (dec, "")
            else:
                r = urllib.parse.unquote(raw)
                padded = r + "=" * (-len(r) % 4)
                try:
                    dec = base64.urlsafe_b64decode(padded).decode("utf-8")
                except Exception:
                    return None
                if "@" not in dec: return None
                ap, address = dec.split("@", 1)
                cipher = ap.split(":", 1)[0] if ":" in ap else ""
                server = address.rsplit(":", 1)[0] if ":" in address else address
                port = address.rsplit(":", 1)[-1] if ":" in address else ""
            return {"name": name or f"ss-{server}", "type": "ss", "udp": True,
                    "server": server, "port": int(port) if port else 0,
                    "cipher": cipher, "password": password}
        elif url.startswith(("hysteria2://", "hy2://")):
            proto = "hysteria2://" if url.startswith("hysteria2://") else "hy2://"
            body = url[len(proto):]
            frag = body.split("#", 1)
            name = urllib.parse.unquote(frag[1]) if len(frag) > 1 else ""
            parts = frag[0].split("@", 1)
            if len(parts) != 2: return None
            _, rest = parts
            hp = rest.split("?")[0]
            server = hp.split(":")[0]
            port = hp.split(":")[-1] if ":" in hp else "443"
            q = urllib.parse.parse_qs(rest.split("?")[1]) if "?" in rest else {}
            return {"name": name or f"hy2-{server}", "type": "hysteria2", "udp": True,
                    "server": server, "port": int(port), "password": parts[0],
                    "sni": q.get("sni", [""])[0]}
    except Exception:
        return None
    return None


def not_contains_ban(name):
    return not any(k in name for k in BAN)


def generate_config(urls):
    proxies = []
    seen_names = set()
    name_counter = {}
    skipped = {"ban": 0, "parse_fail": 0}
    for url in urls:
        proxy = parse_proxy_url(url)
        if not proxy:
            skipped["parse_fail"] += 1
            continue
        if not not_contains_ban(proxy["name"]):
            skipped["ban"] += 1
            continue
        name = proxy["name"]
        if name in seen_names:
            name_counter[name] = name_counter.get(name, 1) + 1
            proxy["name"] = f"{name}-{name_counter[name]}"
        seen_names.add(proxy["name"])
        proxies.append(proxy)
    unique = {}
    for p in proxies:
        key = (p["server"], p["port"], p["type"], p.get("password", ""), p.get("sni", ""))
        if key not in unique:
            unique[key] = p
    dup_count = len(proxies) - len(unique)
    proxies = list(unique.values())
    print(f"[*] URL 解析完成: {len(proxies)} 个节点")
    if skipped.get("parse_fail"): print(f"    [!] 解析失败: {skipped['parse_fail']}")
    if skipped.get("ban"): print(f"    [!] BAN 过滤: {skipped['ban']}")
    if dup_count: print(f"    [!] 节点去重: {dup_count}")
    return proxies


# ══════════════════════════════════════════════════════
# NekoBox 风格：每个代理独立 sing-box 实例测速
# ══════════════════════════════════════════════════════

def proxy_to_singbox_outbound(proxy):
    """将 Clash 格式 proxy dict 转为 sing-box outbound 配置"""
    t = proxy["type"]
    tag = proxy["name"]

    if t == "vmess":
        out = {
            "type": "vmess", "tag": tag,
            "server": proxy["server"], "server_port": proxy["port"],
            "uuid": proxy.get("uuid", ""),
            "security": proxy.get("cipher", "auto"),
            "alter_id": proxy.get("alterId", 0),
        }
        net = proxy.get("network", "tcp")
        if net == "ws":
            ws_path = (proxy.get("ws-opts") or {}).get("path", "")
            ws_host = (proxy.get("ws-opts") or {}).get("headers", {}).get("Host", "")
            out["transport"] = {"type": "ws"}
            if ws_path: out["transport"]["path"] = ws_path
            if ws_host: out["transport"]["headers"] = {"Host": ws_host}
        if proxy.get("tls"):
            out["tls"] = {"enabled": True}
            if proxy.get("sni"): out["tls"]["server_name"] = proxy["sni"]
        return out

    elif t == "vless":
        out = {
            "type": "vless", "tag": tag,
            "server": proxy["server"], "server_port": proxy["port"],
            "uuid": proxy.get("uuid", ""),
            "flow": "",
        }
        net = proxy.get("network", "tcp")
        if net == "ws":
            ws_path = (proxy.get("ws-opts") or {}).get("path", "")
            ws_host = (proxy.get("ws-opts") or {}).get("headers", {}).get("Host", "") or proxy.get("ws-headers", {}).get("Host", "")
            out["transport"] = {"type": "ws"}
            if ws_path: out["transport"]["path"] = ws_path
            if ws_host: out["transport"]["headers"] = {"Host": ws_host}
        elif net == "grpc":
            svc = (proxy.get("grpc-opts") or {}).get("grpc-service-name", "")
            out["transport"] = {"type": "grpc"}
            if svc: out["transport"]["service_name"] = svc
        if proxy.get("tls"):
            out["tls"] = {"enabled": True}
            if proxy.get("sni"): out["tls"]["server_name"] = proxy["sni"]
        return out

    elif t == "trojan":
        out = {
            "type": "trojan", "tag": tag,
            "server": proxy["server"], "server_port": proxy["port"],
            "password": proxy.get("password", ""),
        }
        if proxy.get("sni"):
            out["tls"] = {"enabled": True, "server_name": proxy["sni"]}
        return out

    elif t == "ss":
        out = {
            "type": "shadowsocks", "tag": tag,
            "server": proxy["server"], "server_port": proxy["port"],
            "method": proxy.get("cipher", ""),
            "password": proxy.get("password", ""),
        }
        return out

    elif t in ("hysteria2", "hy2"):
        out = {
            "type": "hysteria2", "tag": tag,
            "server": proxy["server"], "server_port": proxy["port"],
            "password": proxy.get("password", ""),
        }
        if proxy.get("sni"):
            out["tls"] = {"enabled": True, "server_name": proxy["sni"]}
        return out

    return None


def build_singbox_config(proxy, api_port):
    """为单个代理生成最小 sing-box 配置"""
    outbound = proxy_to_singbox_outbound(proxy)
    if not outbound:
        return None

    config = {
        "log": {"disabled": True},
        "dns": {"final": "local"},
        "inbounds": [],
        "outbounds": [
            {"type": "direct", "tag": "direct"},
            {"type": "block", "tag": "block"},
            outbound,
        ],
        "route": {
            "final": outbound["tag"],
            "rules": [],
        },
        "experimental": {
            "clash_api": {
                "external_controller": f"{CLASH_API_HOST}:{api_port}",
                "secret": "",
            }
        },
    }
    return config


_port_counter = 19090
_port_lock = __import__("threading").Lock()

def _next_port():
    global _port_counter
    with _port_lock:
        _port_counter += 1
        result = _port_counter
    return result


def test_single_proxy(proxy, binary, work_dir):
    """
    为单个代理：启动独立 sing-box → Clash API 测延迟 → 关闭。
    完全复刻 NekoBox 的 URL Test 方式。
    返回 (name, delay_ms) 或 (name, None)
    """
    name = proxy["name"]
    api_port = _next_port()
    config = build_singbox_config(proxy, api_port)
    if not config:
        return name, None

    # 写临时配置
    config_file = os.path.join(work_dir, f"config_{api_port}.json")
    with open(config_file, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False)

    proc = None
    try:
        proc = subprocess.Popen(
            [binary, "-f", config_file, "-d", work_dir],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # 等待 API 就绪（最长 3s）
        ready = False
        for _ in range(6):
            time.sleep(0.5)
            try:
                import requests
                r = requests.get(f"http://{CLASH_API_HOST}:{api_port}/version", timeout=1)
                if r.status_code == 200:
                    ready = True
                    break
            except Exception:
                continue

        if not ready:
            return name, None

        # 测延迟（NekoBox URL Test 方式）
        try:
            import requests
            r = requests.get(
                f"http://{CLASH_API_HOST}:{api_port}/proxies/{urllib.parse.quote(name, safe='')}/delay",
                params={"url": TEST_URL, "timeout": TIMEOUT * 1000},
                timeout=TIMEOUT + 1,
            )
            if r.status_code == 200:
                delay = r.json().get("delay")
                return name, delay
        except Exception:
            pass

        return name, None

    finally:
        if proc:
            try:
                proc.kill()
                proc.wait(timeout=3)
            except Exception:
                pass
        # 清理
        try:
            if os.path.exists(config_file):
                os.remove(config_file)
        except Exception:
            pass


def test_proxies_nekobox(proxies, binary):
    """NekoBox 风格：并发 5，每代理独立实例 URL Test"""
    work_dir = os.path.join(PROJECT_DIR, "nekobox_temp")
    os.makedirs(work_dir, exist_ok=True)

    results = {}
    total = len(proxies)

    print(f"[*] NekoBox URL Test: {total} 个节点")
    print(f"    测试 URL: {TEST_URL}")
    print(f"    超时: {TIMEOUT}s | 并发: {MAX_CONCURRENT}")

    done = 0
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
        futures = {
            executor.submit(test_single_proxy, p, binary, work_dir): p
            for p in proxies
        }
        for future in as_completed(futures):
            p = futures[future]
            try:
                name, delay = future.result()
            except Exception:
                name, delay = p["name"], None
            done += 1
            if delay is not None:
                results[name] = delay
            if done % 5 == 0 or done == total:
                print(f"\r  进度: {done}/{total} (可达: {len(results)})", end="", flush=True)
    print()

    # 清理工作目录
    try:
        shutil.rmtree(work_dir, ignore_errors=True)
    except Exception:
        pass

    return results


# ══════════════════════════════════════════════════════
# proxy → URL 转换
# ══════════════════════════════════════════════════════

def proxy_to_url(proxy):
    name = urllib.parse.quote(proxy.get("name", ""), safe="")
    t = proxy.get("type", "")

    if t == "vless":
        params = {}
        if proxy.get("sni"): params["sni"] = proxy["sni"]
        net = proxy.get("network", "tcp")
        if net != "tcp": params["type"] = net
        path = (proxy.get("ws-opts") or {}).get("path", "")
        if path: params["path"] = path
        host = (proxy.get("ws-opts") or {}).get("headers", {}).get("Host", "") or proxy.get("ws-headers", {}).get("Host", "")
        if host: params["host"] = host
        qs = urllib.parse.urlencode(params) if params else ""
        url = f"vless://{proxy.get('uuid','')}@{proxy['server']}:{proxy['port']}"
        if qs: url += f"?{qs}"
        url += f"#{name}"
        return url
    elif t == "vmess":
        v = {"v":"2","ps":proxy.get("name",""),"add":proxy["server"],"port":str(proxy["port"]),
             "id":proxy.get("uuid",""),"aid":str(proxy.get("alterId",0)),"scy":proxy.get("cipher","auto"),
             "net":proxy.get("network","tcp"),"type":"none","host":"","path":"",
             "tls":"tls" if proxy.get("tls") else ""}
        ws = proxy.get("ws-opts") or {}
        if ws.get("path"): v["path"] = ws["path"]
        if ws.get("headers",{}).get("Host"): v["host"] = ws["headers"]["Host"]
        if v["tls"] and proxy.get("sni"): v["sni"] = proxy["sni"]
        enc = base64.b64encode(json.dumps(v, ensure_ascii=False, separators=(",",":")).encode()).decode()
        return f"vmess://{enc}"
    elif t == "trojan":
        pw = urllib.parse.quote(proxy.get("password",""), safe="")
        return f"trojan://{pw}@{proxy['server']}:{proxy['port']}#{name}"
    elif t == "ss":
        mp = f"{proxy.get('cipher','')}:{proxy.get('password','')}"
        enc = base64.urlsafe_b64encode(mp.encode()).decode().rstrip("=")
        return f"ss://{enc}@{proxy['server']}:{proxy['port']}#{name}"
    elif t in ("hysteria2", "hy2"):
        pw = urllib.parse.quote(proxy.get("password",""), safe="")
        params = {}
        if proxy.get("sni"): params["sni"] = proxy["sni"]
        qs = urllib.parse.urlencode(params) if params else ""
        url = f"hysteria2://{pw}@{proxy['server']}:{proxy['port']}"
        if qs: url += f"?{qs}"
        url += f"#{name}"
        return url
    return None


def proxy_to_url_full(p):
    return proxy_to_url(p)


# ══════════════════════════════════════════════════════
# mihomo 方式：单实例批量测速
# ══════════════════════════════════════════════════════

def get_mihomo_binary():
    """下载/定位 mihomo 内核"""
    sys_plat = platform.system().lower()
    if sys_plat == "windows":
        binary_name = "clash.exe"
        target = "mihomo-windows-amd64-compatible"
        ext = ".zip"
    elif sys_plat == "linux":
        binary_name = "clash"
        target = "mihomo-linux-amd64-compatible"
        ext = ".gz"
    elif sys_plat == "darwin":
        binary_name = "clash"
        target = "mihomo-darwin-amd64-compatible"
        ext = ".gz"
    else:
        raise OSError(f"不支持: {sys_plat}")

    bp = os.path.join(PROJECT_DIR, binary_name)
    if os.path.exists(bp):
        return bp

    import requests
    resp = requests.get("https://api.github.com/repos/MetaCubeX/mihomo/releases/latest",
                        headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    resp.raise_for_status()
    dl_url = None
    for a in resp.json().get("assets", []):
        n = a["name"]
        if target in n and n.endswith(ext):
            dl_url = a["browser_download_url"]
            break
    if not dl_url:
        return ""
    resp = requests.get(dl_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=120)
    resp.raise_for_status()
    tmp = os.path.join(PROJECT_DIR, f"dl{ext}")
    with open(tmp, "wb") as f: f.write(resp.content)
    if ext == ".zip":
        with zipfile.ZipFile(tmp) as zf:
            zf.extractall(PROJECT_DIR)
            for n in zf.namelist():
                s = os.path.join(PROJECT_DIR, n)
                if os.path.isfile(s): shutil.move(s, bp); break
    else:
        out = tmp[:-3]
        with gzip.open(tmp, "rb") as gz, open(out, "wb") as o: shutil.copyfileobj(gz, o)
        os.rename(out, bp)
    os.remove(tmp)
    if sys_plat != "windows": os.chmod(bp, 0o755)
    return bp


def test_proxies_mihomo(proxies, binary):
    """单 mihomo 实例，载入所有代理，API 批量测速"""
    # 生成 Clash 格式配置
    config_json = os.path.join(PROJECT_DIR, "clash_test.json")
    clash_proxies = []
    for p in proxies:
        cp = {"name": p["name"], "type": p["type"], "server": p["server"],
              "port": p["port"], "udp": True}
        if p["type"] == "vmess":
            cp.update({"uuid": p.get("uuid",""), "alterId": p.get("alterId",0), "cipher": p.get("cipher","auto")})
            if p.get("tls"): cp["tls"] = True
            if p.get("network"): cp["network"] = p["network"]
            if p.get("ws-opts"): cp["ws-opts"] = p["ws-opts"]
        elif p["type"] == "vless":
            cp.update({"uuid": p.get("uuid",""), "network": p.get("network","tcp")})
            if p.get("tls"): cp["tls"] = True
            if p.get("sni"): cp["sni"] = p["sni"]
            if p.get("ws-opts"): cp["ws-opts"] = p["ws-opts"]
        elif p["type"] == "trojan":
            cp.update({"password": p.get("password","")})
            if p.get("sni"): cp["sni"] = p["sni"]
        elif p["type"] == "ss":
            cp.update({"cipher": p.get("cipher",""), "password": p.get("password","")})
        clash_proxies.append(cp)

    config = {
        "port": 0, "socks-port": 0, "allow-lan": False, "mode": "rule",
        "log-level": "silent",
        "external-controller": f"{CLASH_API_HOST}:{CLASH_API_PORT}",
        "dns": {"enable": False},
        "proxies": clash_proxies,
        "proxy-groups": [{"name": "Proxy", "type": "select", "proxies": [p["name"] for p in clash_proxies]}],
        "rules": ["MATCH,Proxy"],
    }
    with open(config_json, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False)

    # 启动 mihomo
    proc = subprocess.Popen([binary, "-f", config_json, "-d", PROJECT_DIR],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        import requests
        for i in range(12):
            time.sleep(0.5)
            try:
                r = requests.get(f"http://{CLASH_API_HOST}:{CLASH_API_PORT}/version", timeout=1)
                if r.status_code == 200: break
            except: pass
        else:
            return {}

        proxy_names = [p["name"] for p in proxies]
        results = {}
        total = len(proxy_names)
        print(f"[*] mihomo 单实例测速: {total} 个节点 (并发 {MAX_CONCURRENT}, 超时 {TIMEOUT}s)")

        done = 0
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
            def test_one(name):
                try:
                    r = requests.get(
                        f"http://{CLASH_API_HOST}:{CLASH_API_PORT}/proxies/{urllib.parse.quote(name, safe='')}/delay",
                        params={"url": TEST_URL, "timeout": TIMEOUT * 1000},
                        timeout=TIMEOUT + 1,
                    )
                    if r.status_code == 200:
                        return name, r.json().get("delay")
                except: pass
                return name, None

            futures = {executor.submit(test_one, n): n for n in proxy_names}
            for f in as_completed(futures):
                n, d = f.result()
                done += 1
                if d is not None: results[n] = d
                if done % 10 == 0 or done == total:
                    print(f"\r  进度: {done}/{total} (可达: {len(results)})", end="", flush=True)
        print()
        return results
    finally:
        proc.kill()
        proc.wait(timeout=3)
        try: os.remove(config_json)
        except: pass


# ══════════════════════════════════════════════════════
# 输出
# ══════════════════════════════════════════════════════

def save_output(sorted_list, all_proxies):
    name_map = {p["name"]: p for p in all_proxies}
    urls = []
    total_delay = 0
    for name, delay in sorted_list:
        p = name_map.get(name)
        if p:
            url = proxy_to_url_full(p)
            if url:
                urls.append((url, delay))
                total_delay += delay

    plaintext = "\n".join(u for u, _ in urls)
    raw = base64.b64encode(plaintext.encode("utf-8")).decode("ascii")
    encoded = "\n".join(raw[i:i+76] for i in range(0, len(raw), 76))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(encoded + "\n")

    avg = total_delay / len(urls) if urls else 0
    print(f"\n{'='*60}")
    print(f"[OK] 已输出: {OUTPUT_FILE}")
    print(f"    节点数: {len(urls)}")
    print(f"    平均延迟: {avg:.0f}ms")
    print(f"{'='*60}")

    types = {}
    for u, _ in urls:
        p = u.split("://")[0]
        types[p] = types.get(p, 0) + 1
    print("\n节点类型分布:")
    for t, c in sorted(types.items()):
        print(f"  {t}: {c}")

    buckets = {"<100ms": 0, "100-200ms": 0, "200-500ms": 0, "500-1000ms": 0, ">1000ms": 0}
    for _, d in urls:
        if d < 100: buckets["<100ms"] += 1
        elif d < 200: buckets["100-200ms"] += 1
        elif d < 500: buckets["200-500ms"] += 1
        elif d < 1000: buckets["500-1000ms"] += 1
        else: buckets[">1000ms"] += 1
    print("\n延迟分布:")
    for k, v in buckets.items():
        if v:
            bar = "█" * max(1, v * 20 // len(urls))
            print(f"  {k}: {v:4d} {bar}")


def save_all_output(proxies):
    urls = [proxy_to_url_full(p) for p in proxies if proxy_to_url_full(p)]
    plaintext = "\n".join(urls)
    raw = base64.b64encode(plaintext.encode("utf-8")).decode("ascii")
    encoded = "\n".join(raw[i:i+76] for i in range(0, len(raw), 76))
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(encoded + "\n")
    print(f"\n[OK] 已输出: {OUTPUT_FILE} ({len(urls)} 个节点)")


# ══════════════════════════════════════════════════════
# 主入口
# ══════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser(description="NekoBox 风格 URL Test 测速（每代理独立实例）")
    parser.add_argument("--input", default=DEFAULT_SOURCE)
    parser.add_argument("--skip-test", action="store_true", help="跳过测速，直接输出全部节点")
    args = parser.parse_args()

    # 读取输入
    if not os.path.exists(args.input):
        print(f"[!] 输入文件不存在: {args.input}")
        sys.exit(1)

    for enc in ("utf-8", "gbk", "latin-1"):
        try:
            with open(args.input, "r", encoding=enc) as f:
                raw_lines = f.readlines()
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
    else:
        print("[!] 无法解码输入文件")
        sys.exit(1)

    clean = preprocess_input(raw_lines)
    print(f"[*] 源文件: {len(raw_lines)} 行 -> 有效节点: {len(clean)}")

    proxies = generate_config(clean)
    if not proxies:
        print("[!] 没有可用节点")
        sys.exit(1)

    if args.skip_test or not LATENCY_TEST:
        print("\n=== 跳过测速，直接输出全部节点 ===")
        save_all_output(proxies)
        return

    # 根据 KERNEL 选择测速方式
    if "sing" in KERNEL.lower():
        print("\n=== 方式: sing-box（每代理独立实例，NekoBox 风格）===")
        binary = get_singbox_binary()
        if not binary:
            print("[!] sing-box 下载失败")
            sys.exit(1)
        results = test_proxies_nekobox(proxies, binary)
    else:
        print("\n=== 方式: mihomo（单实例批量测速）===")
        binary = get_mihomo_binary()
        if not binary:
            print("[!] mihomo 下载失败")
            sys.exit(1)
        results = test_proxies_mihomo(proxies, binary)

    if not results:
        print("[!] 所有节点均不可达")
        sys.exit(1)

    sorted_list = sorted(results.items(), key=lambda x: x[1])
    alive = len(sorted_list)
    dead = len(proxies) - alive
    print(f"\n[OK] 测速完成: 可达 {alive} / 共 {len(proxies)} (失效 {dead})")

    print("\n  最快节点 TOP 5:")
    for name, delay in sorted_list[:5]:
        try:
            print(f"    {delay:5d}ms  {name}")
        except UnicodeEncodeError:
            print(f"    {delay:5d}ms  (名称含特殊字符)")

    print("\n=== 输出 Base64 订阅文件 ===")
    save_output(sorted_list, proxies)


if __name__ == "__main__":
    main()
