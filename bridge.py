#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bridge.py - ClashForge + node_downloader 桥接脚本 V2

将 node_downloader 下载的 neokongbox.txt 送入 mihomo 内核，
自动完成：启动真实 mihomo → 测延迟 → 移除失效节点 → 按延迟排序 →
输出回 NeoKongBox 兼容的明文 URL 订阅文件。

用法：
  python bridge.py
  python bridge.py --input path/to/input.txt
  python bridge.py --no-download   # 如果 mihomo 已存在，跳过下载
"""

import base64
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import zipfile
import gzip
from datetime import datetime
from pathlib import Path

# ── 配置 ──────────────────────────────────────────────
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(PROJECT_DIR, "input")
NODES_DIR = os.path.join(PROJECT_DIR, "nodes")
CONFIG_JSON = os.path.join(PROJECT_DIR, "clash_config.yaml.json")
OUTPUT_FILE = os.path.join(PROJECT_DIR, "box.txt")
DEFAULT_SOURCE = os.path.join(PROJECT_DIR, "nodes", "neokongbox.txt")

CLASH_API_PORT = 9090
CLASH_API_HOST = "127.0.0.1"
TIMEOUT = 10          # 测延迟超时（秒），免费节点普遍慢，10s 更合理
MAX_CONCURRENT = 15   # 并发数（GitHub Action 2核，15适中）
LIMIT = 500           # 最多保留节点数

# BAN 列表（名称包含这些关键词的节点跳过）
BAN = ["中国", "China", "CN", "电信", "移动", "联通"]

# mihomo 下载配置
MIHOMO_GITHUB = "https://ghfast.top/https://api.github.com/repos/MetaCubeX/mihomo/releases/latest"
MIHOMO_DOWNLOAD_BASE = "https://ghfast.top/https://github.com/MetaCubeX/mihomo/releases/download"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


# ══════════════════════════════════════════════════════
# 阶段 0: 输入预处理
# ══════════════════════════════════════════════════════

def preprocess_input(lines: list[str]) -> list[str]:
    """
    过滤无效行：
    - 去掉 # 注释行
    - 去掉空行
    - 保留支持的协议 (vless://, vmess://, trojan://, ss://, hysteria2://, hy2://)
    """
    VALID_PROTOCOLS = ("vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "hy2://")
    filtered = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith(VALID_PROTOCOLS):
            filtered.append(line)
    return filtered


# ══════════════════════════════════════════════════════
# 阶段 1: 准备输入
# ══════════════════════════════════════════════════════

def prepare_input(source_file: str) -> list[str]:
    """读取源文件，过滤后写入 input/ 目录"""
    os.makedirs(INPUT_DIR, exist_ok=True)
    # 清空 input/
    for f in Path(INPUT_DIR).iterdir():
        if f.is_file():
            f.unlink()

    if not os.path.exists(source_file):
        print(f"[!] 源文件不存在: {source_file}")
        return []

    # 读取并过滤
    for enc in ("utf-8", "gbk", "latin-1"):
        try:
            with open(source_file, "r", encoding=enc) as f:
                raw_lines = f.readlines()
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
    else:
        print(f"[!] 无法解码源文件: {source_file}")
        return []

    clean_lines = preprocess_input(raw_lines)
    print(f"[*] 源文件: {len(raw_lines)} 行 -> 有效节点: {len(clean_lines)} (过滤了 {len(raw_lines) - len(clean_lines)} 行)")

    # 写入过滤后的文件到 input/
    clean_file = os.path.join(INPUT_DIR, "neokongbox.txt")
    with open(clean_file, "w", encoding="utf-8") as f:
        for line in clean_lines:
            f.write(line + "\n")
    print(f"[OK] 已写入: {clean_file}")

    return clean_lines


# ══════════════════════════════════════════════════════
# 阶段 2: URL → Clash proxy dict 转换
# ══════════════════════════════════════════════════════

def parse_proxy_url(url: str) -> dict | None:
    """将 v2ray URL 解析为 Clash proxy dict"""
    try:
        if url.startswith("vmess://"):
            return _parse_vmess(url)
        elif url.startswith("vless://"):
            return _parse_vless(url)
        elif url.startswith("trojan://"):
            return _parse_trojan(url)
        elif url.startswith("ss://"):
            return _parse_ss(url)
        elif url.startswith(("hysteria2://", "hy2://")):
            return _parse_hy2(url)
    except Exception:
        return None
    return None


def _parse_vmess(url: str) -> dict:
    b64 = url[8:]
    # 补齐 padding
    padded = b64 + "=" * (-len(b64) % 4)
    data = json.loads(base64.b64decode(padded))
    proxy = {
        "name": data.get("ps", data.get("add", "vmess-unknown")),
        "type": "vmess",
        "server": data.get("add", ""),
        "port": int(data.get("port", 0)),
        "uuid": data.get("id", ""),
        "alterId": int(data.get("aid", "0")),
        "cipher": data.get("scy", "auto"),
        "udp": True,
    }
    net = data.get("net", "tcp")
    proxy["network"] = net
    if data.get("tls"):
        proxy["tls"] = True
    if data.get("sni"):
        proxy["sni"] = data["sni"]
    if net in ("ws", "websocket"):
        proxy["ws-opts"] = {"path": data.get("path", ""), "headers": {"Host": data.get("host", "")}}
        proxy["ws-path"] = data.get("path", "")
        proxy["ws-headers"] = {"Host": data.get("host", "")}
    return proxy


def _split_fragment(link: str) -> tuple[str, str]:
    """分离 URL 的片段名，如果没有则返回空字符串"""
    if "#" in link:
        parts = link.split("#", 1)
        return parts[0], parts[1]
    return link, ""


def _split_query(link: str) -> tuple[str, dict]:
    """分离 URL 的查询参数"""
    if "?" in link:
        parts = link.split("?", 1)
        return parts[0], urllib.parse.parse_qs(parts[1])
    return link, {}


def _parse_vless(url: str) -> dict:
    body = url[8:]
    body, name = _split_fragment(body)
    user_info, host_info = body.split("@", 1)
    uuid = user_info
    host_part, query = _split_query(host_info)
    port = host_part.split(":")[-1] if ":" in host_part else "80"
    server = host_part.split(":")[0] if ":" in host_part else host_part
    proxy = {
        "name": urllib.parse.unquote(name) if name else f"vless-{server}",
        "type": "vless",
        "server": server,
        "port": int(port),
        "uuid": uuid,
        "network": query.get("type", ["tcp"])[0],
        "tls": query.get("security", ["none"])[0] == "tls",
        "udp": True,
    }
    # 保留 encryption 参数（默认 none，但输出时需要保留）
    enc = query.get("encryption", ["none"])[0]
    if enc:
        proxy["encryption"] = enc
    if query.get("security", ["none"])[0] != "none":
        proxy["security"] = query["security"][0]
    if query.get("sni", [""])[0]:
        proxy["sni"] = query["sni"][0]
    if query.get("skip-cert-verify", ["false"])[0] == "true":
        proxy["skip-cert-verify"] = True
    path = query.get("path", [""])[0]
    host = query.get("host", [""])[0]
    if path:
        proxy["ws-opts"] = {"path": path, "headers": {"Host": host}} if host else {"path": path}
        proxy["ws-path"] = path
    if host:
        proxy["ws-headers"] = {"Host": host}
    return proxy


def _parse_trojan(url: str) -> dict:
    body = url[9:]
    body, name = _split_fragment(body)
    user_info, host_info = body.split("@", 1)
    password = user_info.split(":")[-1] if ":" in user_info else user_info
    host_part, query = _split_query(host_info)
    server = host_part.split(":")[0] if ":" in host_part else host_part
    port = host_part.split(":")[-1] if ":" in host_part else "443"
    return {
        "name": urllib.parse.unquote(name) if name else f"trojan-{server}",
        "type": "trojan",
        "server": server,
        "port": int(port),
        "password": password,
        "sni": query.get("sni", [""])[0],
        "udp": True,
    }


def _parse_ss(url: str) -> dict:
    body = url[5:]
    body, name = _split_fragment(body)

    # 标准格式: ss://base64(method:password)@server:port
    if "@" in body:
        config_part, server_info = body.split("@", 1)
        # 去掉 server_info 中的查询参数（plugin 等）
        if "?" in server_info:
            server_info = server_info.split("?")[0]
        if ":" in server_info:
            server = server_info.rsplit(":", 1)[0]
            port = server_info.rsplit(":", 1)[-1].rstrip("?")
        else:
            server, port = server_info, ""

        # Base64 解码 cipher:password（先 URL-decode 处理 %2B 等情况）
        raw_b64 = urllib.parse.unquote(config_part)
        padded = raw_b64 + "=" * (-len(raw_b64) % 4)
        try:
            decoded = base64.urlsafe_b64decode(padded).decode("utf-8")
        except Exception:
            # 可能不是 Base64 而是明文格式: ss://method:password@server:port
            if ":" in raw_b64:
                decoded = raw_b64
            else:
                return None
        cipher, password = decoded.split(":", 1) if ":" in decoded else (decoded, "")
    else:
        # 非标准格式: ss://base64(method:password@server:port)#name
        # server:port 嵌在 base64 里面
        raw = urllib.parse.unquote(body)
        padded = raw + "=" * (-len(raw) % 4)
        try:
            decoded = base64.urlsafe_b64decode(padded).decode("utf-8")
        except Exception:
            return None
        # 格式: cipher:password@server:port
        if "@" not in decoded:
            return None
        auth_part, address = decoded.split("@", 1)
        if ":" in auth_part:
            cipher, password = auth_part.split(":", 1)
        else:
            cipher, password = auth_part, ""
        if ":" in address:
            server, port = address.rsplit(":", 1)
        else:
            server, port = address, ""

    return {
        "name": urllib.parse.unquote(name) if name else f"ss-{server}",
        "type": "ss",
        "server": server,
        "port": int(port) if port else 0,
        "cipher": cipher,
        "password": password,
        "udp": True,
    }


def _parse_hy2(url: str) -> dict:
    proto_prefix = "hysteria2://" if url.startswith("hysteria2://") else "hy2://"
    body = url[len(proto_prefix):]
    body, name = _split_fragment(body)
    parts = body.split("@", 1)
    if len(parts) != 2:
        return None
    password, rest = parts
    host_part, query = _split_query(rest)
    server = host_part.split(":")[0]
    port = host_part.split(":")[-1] if ":" in host_part else ""
    return {
        "name": urllib.parse.unquote(name) if name else f"hy2-{server}",
        "type": "hysteria2",
        "server": server,
        "port": int(port),
        "password": password,
        "sni": query.get("sni", [""])[0],
        "udp": True,
    }


# ══════════════════════════════════════════════════════
# 阶段 3: 生成 Clash 配置并写文件
# ══════════════════════════════════════════════════════

def build_clash_config(proxies: list[dict]) -> dict:
    """构建完整的 Clash 配置（最小化，避免启动阻塞）"""
    config = {
        "port": 7890,
        "socks-port": 7891,
        "allow-lan": False,
        "mode": "rule",
        "log-level": "silent",
        "external-controller": f"{CLASH_API_HOST}:{CLASH_API_PORT}",
        "geodata-mode": True,
        "geox-url": {
            "geoip": "https://slink.ltd/https://raw.githubusercontent.com/Loyalsoldier/geoip/release/geoip.dat",
            "mmdb": "https://slink.ltd/https://raw.githubusercontent.com/Loyalsoldier/geoip/release/GeoLite2-Country.mmdb",
        },
        "dns": {"enable": False},
        "proxies": proxies,
        "proxy-groups": [
            {"name": "Proxy", "type": "select", "proxies": [p["name"] for p in proxies]},
        ],
        "rules": ["MATCH,Proxy"],
    }
    return config


def not_contains_ban(name: str) -> bool:
    return not any(k in name for k in BAN)


def validate_proxy(proxy: dict) -> bool:
    """检查 proxy dict 是否合法（mihomo 接受的最小要求）"""
    if not proxy.get("server") or not proxy.get("port"):
        return False
    # vmess 必须有 cipher
    if proxy["type"] == "vmess":
        cipher = proxy.get("cipher", "")
        if not cipher:
            proxy["cipher"] = "auto"
    return True


def generate_config(urls: list[str]) -> list[dict]:
    """生成代理列表，过滤 BAN 和不支持的协议，去重名称"""
    proxies = []
    seen_names = set()
    name_counter = {}  # for adding suffixes
    skipped = {"ban": 0, "parse_fail": 0, "invalid": 0}
    for url in urls:
        proxy = parse_proxy_url(url)
        if not proxy:
            skipped["parse_fail"] += 1
            continue
        if not not_contains_ban(proxy["name"]):
            skipped["ban"] += 1
            continue
        if not validate_proxy(proxy):
            skipped["invalid"] += 1
            continue
        # 去重名称（添加后缀）
        name = proxy["name"]
        if name in seen_names:
            name_counter[name] = name_counter.get(name, 1) + 1
            proxy["name"] = f"{name}-{name_counter[name]}"
        seen_names.add(proxy["name"])
        proxies.append(proxy)

    # 全局去重（server:port:type + 路由参数，避免不同 SNI/path 的节点被误删）
    unique = {}
    for p in proxies:
        # 构建区分 key：server+port+type+password+network+sni+path+host
        key = (
            p["server"], p["port"], p["type"],
            p.get("password", ""),
            p.get("network", "tcp"),
            p.get("sni", ""),
            (p.get("ws-opts") or {}).get("path", "") or p.get("ws-path", ""),
            (p.get("ws-opts") or {}).get("headers", {}).get("Host", "") or p.get("ws-headers", {}).get("Host", ""),
        )
        if key not in unique:
            unique[key] = p
    dup_count = len(proxies) - len(unique)
    proxies = list(unique.values())

    print(f"[*] URL 解析完成: {len(proxies)} 个节点")
    if skipped["parse_fail"]:
        print(f"    [!] 解析失败: {skipped['parse_fail']}")
    if skipped["ban"]:
        print(f"    [!] BAN 过滤: {skipped['ban']}")
    if skipped["invalid"]:
        print(f"    [!] 参数校验失败: {skipped['invalid']}")
    if dup_count:
        print(f"    [!] 节点去重: {dup_count}")

    config = build_clash_config(proxies)
    # 写 JSON
    with open(CONFIG_JSON, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    print(f"[OK] Clash 配置已生成: {CONFIG_JSON} ({len(proxies)} 个去重后节点)")
    return proxies


# ══════════════════════════════════════════════════════
# 阶段 4: 下载/检查 mihomo 内核
# ══════════════════════════════════════════════════════

def get_mihomo_binary() -> str:
    """返回 mihomo 二进制路径，不存在则下载"""
    sys_plat = platform.system().lower()
    if sys_plat == "windows":
        binary_name = "clash.exe"
        target_name = "mihomo-windows-amd64-compatible"
        ext = ".zip"
    elif sys_plat == "linux":
        binary_name = "clash"
        target_name = "mihomo-linux-amd64-compatible"
        ext = ".gz"
    elif sys_plat == "darwin":
        binary_name = "clash"
        target_name = "mihomo-darwin-amd64-compatible"
        ext = ".gz"
    else:
        raise OSError(f"不支持的操作系统: {sys_plat}")

    binary_path = os.path.join(PROJECT_DIR, binary_name)
    if os.path.exists(binary_path):
        print(f"[OK] mihomo 内核已存在: {binary_path}")
        return binary_path

    print(f"[*] 下载 mihomo 内核 ({target_name})...")

    # 通过 ghfast.top 代理获取 latest release 信息
    try:
        import requests
        resp = requests.get(MIHOMO_GITHUB, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        for asset in data.get("assets", []):
            name = asset.get("name", "")
            if target_name in name and name.endswith(ext):
                download_url = asset["browser_download_url"]
                break
        else:
            print("[!] 未找到匹配的内核文件")
            return ""

        # 使用代理下载
        dl_url = f"https://ghfast.top/{download_url}"
        print(f"[*] 下载: {dl_url}")
        resp = requests.get(dl_url, headers=HEADERS, timeout=120)
        resp.raise_for_status()

        # 保存
        tmp_file = os.path.join(PROJECT_DIR, f"download{ext}")
        with open(tmp_file, "wb") as f:
            f.write(resp.content)

        # 解压
        if ext == ".zip":
            with zipfile.ZipFile(tmp_file) as zf:
                zf.extractall(PROJECT_DIR)
                for name in zf.namelist():
                    src = os.path.join(PROJECT_DIR, name)
                    if os.path.isfile(src):
                        os.rename(src, binary_path)
                        break
        elif ext == ".gz":
            out_name = tmp_file[:-3]
            with gzip.open(tmp_file, "rb") as gz:
                with open(out_name, "wb") as out:
                    shutil.copyfileobj(gz, out)
            os.rename(out_name, binary_path)

        os.remove(tmp_file)
        print(f"[OK] mihomo 下载完成: {binary_path}")
        return binary_path

    except Exception as e:
        print(f"[!] mihomo 下载失败: {e}")
        return ""


# ══════════════════════════════════════════════════════
# 阶段 5: 启动 mihomo + 测速
# ══════════════════════════════════════════════════════

def start_mihomo(binary: str) -> subprocess.Popen | None:
    """启动 mihomo 内核，返回进程对象"""
    # 清理可能的旧 GeoIP（避免启动阻塞）
    for f in ("geoip.dat", "GeoLite2-Country.mmdb", "cache.db"):
        p = os.path.join(PROJECT_DIR, f)
        if os.path.exists(p):
            os.remove(p)

    print(f"[*] 启动 mihomo: {binary} -f {CONFIG_JSON}")
    try:
        proc = subprocess.Popen(
            [binary, "-f", CONFIG_JSON, "-d", PROJECT_DIR],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # 等待 API 就绪
        for i in range(60):
            time.sleep(1)
            try:
                import requests
                r = requests.get(f"http://{CLASH_API_HOST}:{CLASH_API_PORT}/version",
                                 timeout=2, headers=HEADERS)
                if r.status_code == 200:
                    print(f"[OK] mihomo API 就绪 (端口 {CLASH_API_PORT})")
                    return proc
            except Exception:
                continue
        print("[!] mihomo API 未就绪")
        proc.kill()
        return None
    except Exception as e:
        print(f"[!] 启动 mihomo 失败: {e}")
        return None


def test_proxies(proxy_names: list[str]) -> dict[str, int]:
    """批量测延迟，返回 name -> delay_ms 映射"""
    import requests
    import concurrent.futures

    results = {}
    total = len(proxy_names)

    def test_one(name: str) -> tuple[str, int | None]:
        try:
            r = requests.get(
                f"http://{CLASH_API_HOST}:{CLASH_API_PORT}/proxies/{urllib.parse.quote(name, safe='')}/delay",
                params={"url": "http://www.gstatic.com/generate_204", "timeout": TIMEOUT * 1000},
                timeout=TIMEOUT + 1,
                headers=HEADERS,
            )
            if r.status_code == 200:
                return name, r.json().get("delay", 99999)
        except Exception:
            pass
        return name, None

    print(f"[*] 测速: {total} 个节点 (并发 {MAX_CONCURRENT}, 超时 {TIMEOUT}s)")
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
        futures = {executor.submit(test_one, name): name for name in proxy_names}
        for future in concurrent.futures.as_completed(futures):
            name, delay = future.result()
            done += 1
            if delay is not None:
                results[name] = delay
            if done % 20 == 0 or done == total:
                print(f"\r  进度: {done}/{total} (可达: {len(results)})", end="", flush=True)
    print()
    return results


def stop_mihomo(proc: subprocess.Popen):
    """停止 mihomo"""
    if proc:
        proc.kill()
        proc.wait()
        print("[OK] mihomo 已停止")


# ══════════════════════════════════════════════════════
# 阶段 6: Clash proxy dict → 明文 URL（仅保留测速可达且排序后的）
# ══════════════════════════════════════════════════════

def proxy_to_url(proxy: dict) -> str | None:
    """Clash proxy dict → 标准 v2ray URL"""
    ptype = proxy.get("type", "")
    if ptype == "vless":
        return _proxy_to_vless(proxy)
    elif ptype == "vmess":
        return _proxy_to_vmess(proxy)
    elif ptype == "trojan":
        return _proxy_to_trojan(proxy)
    elif ptype == "ss":
        return _proxy_to_ss(proxy)
    elif ptype in ("hysteria2", "hy2"):
        return _proxy_to_hy2(proxy)
    return None


def _proxy_to_vless(p: dict) -> str:
    name = urllib.parse.quote(p.get("name", ""), safe="")
    params = {}
    if p.get("encryption") and p["encryption"] != "none":
        params["encryption"] = p["encryption"]
    if p.get("security") and p["security"] != "none":
        params["security"] = p["security"]
    if p.get("sni"):
        params["sni"] = p["sni"]
    if p.get("skip-cert-verify"):
        params["skip-cert-verify"] = "true"
    net = p.get("network", "tcp")
    if net and net != "tcp":
        params["type"] = net
    ws_path = (p.get("ws-opts") or {}).get("path", "") or p.get("ws-path", "")
    if ws_path:
        params["path"] = ws_path
    ws_host = ((p.get("ws-opts") or {}).get("headers", {}).get("Host", "")
               or (p.get("ws-headers") or {}).get("Host", ""))
    if ws_host:
        params["host"] = ws_host
    qs = urllib.parse.urlencode(params) if params else ""
    url = f"vless://{p['uuid']}@{p['server']}:{p['port']}"
    if qs:
        url += f"?{qs}"
    url += f"#{name}"
    return url


def _proxy_to_vmess(p: dict) -> str:
    vmess = {
        "v": "2",
        "ps": p.get("name", ""),
        "add": p.get("server", ""),
        "port": str(p.get("port", 0)),
        "id": p.get("uuid", ""),
        "aid": str(p.get("alterId", 0)),
        "scy": p.get("cipher", "auto"),
        "net": p.get("network", "tcp"),
        "type": "none",
        "host": "",
        "path": "",
        "tls": "tls" if p.get("tls") else "",
    }
    ws_opts = p.get("ws-opts") or {}
    if ws_opts.get("path"):
        vmess["path"] = ws_opts["path"]
    if ws_opts.get("headers", {}).get("Host"):
        vmess["host"] = ws_opts["headers"]["Host"]
    if vmess["tls"] and p.get("sni"):
        vmess["sni"] = p["sni"]
    encoded = base64.b64encode(
        json.dumps(vmess, ensure_ascii=False, separators=(",", ":")).encode()
    ).decode()
    return f"vmess://{encoded}"


def _proxy_to_trojan(p: dict) -> str:
    name = urllib.parse.quote(p.get("name", ""), safe="")
    params = {}
    if p.get("sni"):
        params["sni"] = p["sni"]
    if p.get("skip-cert-verify"):
        params["allowInsecure"] = "1"
    qs = urllib.parse.urlencode(params) if params else ""
    pw = urllib.parse.quote(p.get("password", ""), safe="")
    url = f"trojan://{pw}@{p['server']}:{p['port']}"
    if qs:
        url += f"?{qs}"
    url += f"#{name}"
    return url


def _proxy_to_ss(p: dict) -> str:
    name = urllib.parse.quote(p.get("name", ""), safe="")
    method_pass = f"{p['cipher']}:{p['password']}"
    encoded = base64.urlsafe_b64encode(method_pass.encode()).decode().rstrip("=")
    return f"ss://{encoded}@{p['server']}:{p['port']}#{name}"


def _proxy_to_hy2(p: dict) -> str:
    name = urllib.parse.quote(p.get("name", ""), safe="")
    pw = urllib.parse.quote(p.get("password", ""), safe="")
    params = {}
    if p.get("skip-cert-verify"):
        params["insecure"] = "1"
    if p.get("sni"):
        params["sni"] = p["sni"]
    qs = urllib.parse.urlencode(params) if params else ""
    url = f"hysteria2://{pw}@{p['server']}:{p['port']}"
    if qs:
        url += f"?{qs}"
    url += f"#{name}"
    return url


# ══════════════════════════════════════════════════════
# 组装最终输出
# ══════════════════════════════════════════════════════

def save_sorted_output(sorted_proxies: list[tuple[str, int]], name_to_proxy: dict[str, dict]):
    """
    按测速结果排序输出 Base64 编码的订阅文件。
    sorted_proxies: [(name, delay_ms), ...]
    """
    urls = []
    total_delay = 0
    for name, delay in sorted_proxies:
        proxy = name_to_proxy.get(name)
        if proxy:
            url = proxy_to_url(proxy)
            if url:
                urls.append((url, delay))
                total_delay += delay

    # 拼接明文，然后整体 Base64 编码（标准 v2ray 订阅格式，每 76 字符换行）
    plaintext = "\n".join(url for url, _ in urls)
    raw = base64.b64encode(plaintext.encode("utf-8")).decode("ascii")
    # 每 76 字符换行
    encoded = "\n".join(raw[i:i+76] for i in range(0, len(raw), 76))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(encoded + "\n")

    avg = total_delay / len(urls) if urls else 0
    size = sum(len(u[0].encode("utf-8")) for u in urls)

    print(f"\n{'='*60}")
    print(f"[OK] 已输出: {OUTPUT_FILE}")
    print(f"    节点数: {len(urls)}")
    print(f"    平均延迟: {avg:.0f}ms")
    print(f"    文件大小: {size:,} 字节")
    print(f"{'='*60}")

    # 类型分布
    types = {}
    for url, _ in urls:
        proto = url.split("://")[0]
        types[proto] = types.get(proto, 0) + 1
    print("\n节点类型分布:")
    for proto, count in sorted(types.items()):
        print(f"  {proto}: {count} 个")

    # 延迟分段统计
    buckets = {"<100ms": 0, "100-200ms": 0, "200-500ms": 0, "500-1000ms": 0, ">1000ms": 0}
    for _, d in urls:
        if d < 100:
            buckets["<100ms"] += 1
        elif d < 200:
            buckets["100-200ms"] += 1
        elif d < 500:
            buckets["200-500ms"] += 1
        elif d < 1000:
            buckets["500-1000ms"] += 1
        else:
            buckets[">1000ms"] += 1
    print("\n延迟分布:")
    for k, v in buckets.items():
        if v:
            bar = "█" * (v * 20 // len(urls) if urls else 0)
            print(f"  {k}: {v:4d} {bar}")

def save_all_output(proxies: list[dict]):
    """不测速，直接全部输出为 Base64 订阅文件"""
    urls = []
    for p in proxies:
        url = proxy_to_url(p)
        if url:
            urls.append(url)

    plaintext = "\n".join(urls)
    raw = base64.b64encode(plaintext.encode("utf-8")).decode("ascii")
    encoded = "\n".join(raw[i:i+76] for i in range(0, len(raw), 76))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(encoded + "\n")

    print(f"\n{'='*60}")
    print(f"[OK] 已输出: {OUTPUT_FILE}")
    print(f"    节点数: {len(urls)}")
    print(f"    文件大小: {len(raw):,} 字节")
    print(f"{'='*60}")

    types = {}
    for u in urls:
        p = u.split("://")[0]
        types[p] = types.get(p, 0) + 1
    print("\n节点类型分布:")
    for t, c in sorted(types.items()):
        print(f"  {t}: {c}")


# ══════════════════════════════════════════════════════
# 主入口
# ══════════════════════════════════════════════════════

def main():
    # 解决 Windows GBK 控制台无法打印 emoji 的问题
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    import argparse
    parser = argparse.ArgumentParser(description="mihomo 测速桥接: 去失效 + 按延迟排序")
    parser.add_argument("--input", default=DEFAULT_SOURCE, help=f"输入文件（默认: {DEFAULT_SOURCE}）")
    parser.add_argument("--no-download", action="store_true", help="跳过 mihomo 下载（如果已存在）")
    parser.add_argument("--skip-test", action="store_true", help="跳过测速，直接输出全部节点")
    parser.add_argument("--output", default=None, help="输出文件路径")
    args = parser.parse_args()

    output_path = args.output or OUTPUT_FILE

    # ── 阶段 1: 准备输入 ──
    print("=== 阶段 1: 读取并过滤输入 ===")
    clean_urls = prepare_input(args.input)
    if not clean_urls:
        print("[!] 没有有效节点")
        sys.exit(1)

    # ── 阶段 2: 生成 Clash 配置 ──
    print("\n=== 阶段 2: 生成 Clash 配置 ===")
    proxies = generate_config(clean_urls)
    if not proxies:
        print("[!] 没有可用的代理节点")
        sys.exit(1)

    # ── 跳过测速模式 ──
    if args.skip_test:
        print("\n=== 跳过测速，直接输出全部节点 ===")
        save_all_output(proxies)
        return

    # ── 阶段 3: 准备 mihomo ──
    print("\n=== 阶段 3: 准备 mihomo 内核 ===")
    binary = get_mihomo_binary()
    if not binary:
        print("[!] mihomo 内核不可用，无法测速")
        print("    试试 --no-download (如果已有 mihomo)")
        sys.exit(1)

    # ── 阶段 4: 启动 mihomo 并测速 ──
    print("\n=== 阶段 4: 启动 mihomo + 测延迟 ===")
    mihomo_proc = start_mihomo(binary)
    if not mihomo_proc:
        print("[!] mihomo 启动失败")
        sys.exit(1)

    try:
        proxy_names = [p["name"] for p in proxies]
        results = test_proxies(proxy_names)

        if not results:
            print("[!] 所有节点均不可达")
            stop_mihomo(mihomo_proc)
            sys.exit(1)

        # 按延迟排序（从小到大）
        sorted_proxies = sorted(results.items(), key=lambda x: x[1])
        alive = len(sorted_proxies)
        dead = len(proxy_names) - alive
        print(f"\n[OK] 测速完成: 可达 {alive} / 共 {len(proxy_names)} (失效 {dead})")

        top_n = sorted_proxies[:LIMIT]
        print(f"    保留延迟最低的 {len(top_n)} 个节点")

        # 显示最快的几个
        print("\n  最快节点 TOP 5:")
        for name, delay in top_n[:5]:
            print(f"    {delay:5d}ms  {name}")

        # ── 阶段 5: 输出 ──
        print("\n=== 阶段 5: 输出 NeoKongBox 兼容文件 ===")
        name_to_proxy = {p["name"]: p for p in proxies}
        save_sorted_output(top_n, name_to_proxy)

    finally:
        stop_mihomo(mihomo_proc)


if __name__ == "__main__":
    main()
