#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
节点订阅链接自动下载器 + NeoKongBox 合并工具

只保留 .txt 格式的 v2ray 订阅链接，支持下载、汇总、去重、Base64 转明文。
一键模式默认为：下载所有站点 -> 合并去重 -> 转明文 -> 清理临时文件。

支持站点：
  1. 米贝77 (mibei77.com)
     - 文章格式: https://www.mibei77.com/{ID}.html
     - 节点链接以纯文本形式写在 <p> 标签中
     - URL 模式: https://mm.mibei77.com/{path}/{date}

  2. ClashGitHub (clashgithub.com)
     - 文章格式: https://clashgithub.com/clashnode-{YYYYMMDD}.html
     - 节点链接以 <a> 标签形式存在
     - URL 模式: https://clashgithub.com/wp-content/uploads/rss/{date}

  3. OneClash (oneclash.cc)
     - 文章格式: https://oneclash.cc/a/{ID}.html
     - 节点链接格式: https://oss.oneclash.cc/{YYYY}/{MM}/{date}.txt
     - 日期在链接文本中，如 "6月9日"

  4. V2RayShare (v2rayshare.net)
     - 文章格式: https://v2rayshare.net/p/{ID}.html
     - 节点链接格式: https://static.v2rayshare.net/{YYYY}/{MM}/{date}.txt
     - 日期在链接文本中，如 "6月9日"

  5. 玩转喵 (wanzhuanmi.com)
     - 文章格式: https://wanzhuanmi.com/archives/{ID}
     - 节点链接格式: http://wanzhuanmi.cczzuu.top/node/{date}.txt
     - 日期在链接文本中，如 "2026年06月09日"

  6. CFMem (cfmem.com)
     - 首页有 Cloudflare 保护，不解析首页
     - 直接使用已知文章 URL 模式
     - 节点链接: https://nodebuf.com/files/public/{hash}/preview

  7. YoYaPaI (yoyapai.com)
     - 文章格式: https://yoyapai.com/{ID}
     - 节点链接格式: https://freenode.yoyapai.com/{path}/{date}.{ext}
     - 从 category 页面 /category/mianfeijiedian 提取文章

  8. StairNode (stairnode.com)
     - 文章格式: https://www.stairnode.com/archives/{ID}.html
     - 节点链接格式: http://stairnode.cczzuu.top/node/{YYYYMMDD}.{txt|yaml}
     - 日期格式: 2026年06月09日
     - 注意: 首页需要 -k 跳过 SSL 证书验证

   9. ClashNode (clashnode.cc)
      - 文章格式: https://clashnode.cc/free-node/{date}-free-node-subscribe-links.htm
      - 节点链接格式: https://node.clashnode.cc/uploads/{YYYY}/{MM}/{N}-{YYYYMMDD}.{txt|yaml|json}
      - 日期格式: 6月9日 或 2026-6-9
      - 支持 v2ray/clash/sing-box 三种格式

   10. 直连订阅源 (3 个)
      - freev2.net: https://xmxosfepggzm.503403.xyz/ (NekoBox UA, vless://)
      - ssrsub: https://gh-proxy.com/raw/.../ssrsub/ssr/master/v2ray (vless/vmess/ss/trojan)
      - daozhangnb: https://ss.daozhangnb.dpdns.org/ (15 条 ss://)

用法：
  # 下载模式
  python node_downloader.py --site mibei77 --download
  python node_downloader.py --all --download
  python node_downloader.py --all --download --outdir ./nodes

  # 一键全自动：下载 + 合并去重 + 明文转换 + 清理临时文件
  python node_downloader.py --all --download --merge

  # NeoKongBox 合并模式（仅合并，不下载）
  python node_downloader.py --merge
  python node_downloader.py --merge --outdir ./nodes

  python node_downloader.py --help
"""

import argparse
import base64
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("请安装依赖：pip install requests beautifulsoup4")
    sys.exit(1)


# ── 站点配置 ──────────────────────────────────────────────
SITES = {
    "mibei77": {
        "name": "米贝77",
        "home_url": "https://www.mibei77.com/",
        "article_pattern": re.compile(r'(https?://www\.mibei77\.com/\d+\.html)'),
        "date_pattern": re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日"),
        "link_url_pattern": re.compile(r'https?://mm\.mibei77\.com/\S+\.txt', re.IGNORECASE),
        "file_ext_map": {".txt": "txt"},
        "desc_map": {".txt": "v2ray 订阅链接"},
        "extract_links": "mibei77_style",
    },
    "clash": {
        "name": "ClashGitHub",
        "home_url": "https://clashgithub.com/",
        "article_pattern": re.compile(r'(https?://clashgithub\.com/clashnode-\d+\.html)'),
        "date_pattern": re.compile(r"clashnode-(\d{8})\.html"),
        "link_url_pattern": re.compile(r'https?://clashgithub\.com/wp-content/uploads/rss/\d{8}\.txt', re.IGNORECASE),
        "file_ext_map": {".txt": "txt"},
        "desc_map": {".txt": "v2ray 订阅链接"},
        "extract_links": "p_tag",
    },
    "oneclash": {
        "name": "OneClash",
        "home_url": "https://oneclash.cc/",
        "category_url": None,
        "article_pattern": re.compile(r'(https?://oneclash\.cc/a/\d+\.html)'),
        "date_pattern": re.compile(r'(\d{1,2})月(\d{1,2})日'),
        "link_url_pattern": re.compile(r'https?://oss\.oneclash\.cc/\d{4}/\d{2}/\d{8}\.txt', re.IGNORECASE),
        "file_ext_map": {".txt": "txt"},
        "desc_map": {".txt": "v2ray 订阅链接"},
        "extract_links": "p_tag",
    },
    "v2rayshare": {
        "name": "V2RayShare",
        "home_url": "https://v2rayshare.net/",
        "category_url": None,
        "article_pattern": re.compile(r'(https?://v2rayshare\.net/p/\d+\.html)'),
        "date_pattern": re.compile(r'(\d{1,2})月(\d{1,2})日'),
        "link_url_pattern": re.compile(r'https?://static\.v2rayshare\.net/\d{4}/\d{2}/\d{8}\.txt', re.IGNORECASE),
        "file_ext_map": {".txt": "txt"},
        "desc_map": {".txt": "V2RayShare 节点订阅"},
        "extract_links": "p_tag",
    },
    "wanzhuanmi": {
        "name": "玩转喵",
        "home_url": "https://wanzhuanmi.com/",
        "category_url": None,
        "article_pattern": re.compile(r'(https?://wanzhuanmi\.com/freenode/[\d-]+)'),
        "date_pattern": re.compile(r'(\d{4})年(\d{2})月(\d{2})日'),
        "link_url_pattern": re.compile(r'https?://wanzhuanmi\.cczzuu\.top/node/\d{8}\.txt', re.IGNORECASE),
        "file_ext_map": {".txt": "txt"},
        "desc_map": {".txt": "玩转喵 节点订阅"},
        "extract_links": "both",
    },
    "yoyapai": {
        "name": "YoYaPaI",
        "home_url": "https://yoyapai.com/category/mianfeijiedian",
        "article_pattern": re.compile(r'(https?://yoyapai\.com/\d+)'),
        "date_pattern": re.compile(r'(\d{1,2})月(\d{1,2})日'),
        "link_url_pattern": re.compile(r'https?://freenode\.yoyapai\.com/\d{4}/\d{2}/[\d\-]+[^ \s]*\.txt', re.IGNORECASE),
        "file_ext_map": {".txt": "txt"},
        "desc_map": {".txt": "v2ray 订阅链接"},
        "extract_links": "both",
    },
    "stairnode": {
        "name": "StairNode",
        "home_url": "https://www.stairnode.com/freenode",
        "article_pattern": re.compile(r'(https?://www\.stairnode\.com/archives/\d+\.html)'),
        "date_pattern": re.compile(r'(\d{4})年(\d{1,2})月(\d{1,2})日'),
        "link_url_pattern": re.compile(r'https?://stairnode\.cczzuu\.top/node/\d{8}\.txt', re.IGNORECASE),
        "file_ext_map": {".txt": "txt"},
        "desc_map": {".txt": "v2ray 订阅链接"},
        "extract_links": "p_tag",
    },
    "clashnode": {
        "name": "ClashNode",
        "home_url": "https://clashnode.cc/",
        "article_pattern": re.compile(r'(https?://clashnode\.cc/free-node/[\w\-]+\.htm)'),
        "date_pattern": re.compile(r'(\d{1,2})月(\d{1,2})日'),
        "link_url_pattern": re.compile(r'https?://node\.clashnode\.cc/uploads/\d{4}/\d{2}/[\d-]+\d{8}\.txt', re.IGNORECASE),
        "file_ext_map": {".txt": "txt"},
        "desc_map": {".txt": "v2ray 订阅链接"},
        "extract_links": "p_tag",
    },
}

# GitHub Raw 直链（手动维护，需要定期更新 URL）
GITHUB_RAW_URLS = [
    {
        "name": "GitHub cn-news",
        "url": "https://ghfast.top/https://raw.githubusercontent.com/hello-world-1989/cn-news/refs/heads/main/end-gfw-together",
#        "url": "https://raw.githubusercontent.com/hello-world-1989/cn-news/main/end-gfw-together",
        "desc": "GitHub Raw: hello-world-1989/cn-news",
    },
    {
        "name": "GitHub v2ray (via ghfast.top)",
        "url": "https://ghfast.top/https://raw.githubusercontent.com/free18/v2ray/refs/heads/main/v.txt",
        "desc": "GitHub Raw: free18/v2ray (代理加速)",
    },
]

# Nekobox/subscription 直连订阅源（无文章解析，直接下载）
DIRECT_SUBSCRIPTION_URLS = [
    {
        "name": "freev2.net",
        "url": "https://xmxosfepggzm.503403.xyz/",
        "desc": "freev2.net 订阅 (NekoBox UA)",
    },
    {
        "name": "ssrsub",
        "url": "https://gh-proxy.com/raw.githubusercontent.com/ssrsub/ssr/master/v2ray",
        "desc": "ssrsub GitHub mirror (via gh-proxy)",
    },
    {
        "name": "daozhangnb",
        "url": "https://ss.daozhangnb.dpdns.org/",
        "desc": "daozhangnb 订阅",
    },
]

CFMEM_SITE = {
    "name": "CFMem",
    "home_url": "https://www.cfmem.com/",
    "link_url_pattern": re.compile(r'nodebuf\.com/files/public/([a-zA-Z0-9_]+)/preview', re.IGNORECASE),
    "file_ext_map": {".txt": "txt"},
    "desc_map": {".txt": "CFMem 节点预览"},
    "extract_links": "cfmem",
}


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)
NEKODOX_UA = "NekoBox/1.3.1"
REQUEST_TIMEOUT = 15
VERIFY_SSL = True


# ── NeoKongBox 合并相关 ─────────────────────────────────

PROTOCOL_PREFIXES = [
    'vless://',
    'trojan://',
    'vmess://',
    'ss://',
    'socks://',
    'http://',
    'https://',
]

BASE64_PATTERN = re.compile(r'^[A-Za-z0-9+/=]{20,}$')


def is_valid_node_line(line: str) -> bool:
    """判断一行是否为 NeoKongBox 可用的节点行。"""
    line = line.strip()
    if not line:
        return False
    for prefix in PROTOCOL_PREFIXES:
        if line.startswith(prefix):
            return True
    if BASE64_PATTERN.match(line):
        return True
    return False


def decode_to_plaintext(line: str) -> str:
    """
    将节点行解码为明文 URL（单行，仅返回第一个有效 URL）。
    对于多节点 base64 块，请使用 decode_to_plaintext_all()。
    """
    line = line.strip()

    for prefix in PROTOCOL_PREFIXES:
        if line.startswith(prefix):
            q_pos = line.find('?')
            if q_pos != -1 and '#' in line[q_pos:]:
                return line[:q_pos + line[q_pos:].find('#')]
            return line

    try:
        decoded = base64.b64decode(line).decode('utf-8', errors='ignore')
        if decoded:
            decoded = decoded.strip()
            for prefix in PROTOCOL_PREFIXES:
                if decoded.startswith(prefix):
                    q_pos = decoded.find('?')
                    if q_pos != -1 and '#' in decoded[q_pos:]:
                        return decoded[:q_pos + decoded[q_pos:].find('#')]
                    return decoded
            for part in decoded.split('\n'):
                part = part.strip()
                for prefix in PROTOCOL_PREFIXES:
                    if part.startswith(prefix):
                        q_pos = part.find('?')
                        if q_pos != -1 and '#' in part[q_pos:]:
                            return part[:q_pos + part[q_pos:].find('#')]
                        return part
    except Exception:
        pass

    return None


def decode_to_plaintext_all(line: str) -> list[str]:
    """
    将节点行解码为所有明文 URL（处理多节点 base64 块）。
    返回解码后的有效 URL 列表，空列表表示解码失败或无有效节点。
    """
    line = line.strip()
    results = []

    for prefix in PROTOCOL_PREFIXES:
        if line.startswith(prefix):
            results.append(line)
            return results

    try:
        decoded = base64.b64decode(line).decode('utf-8', errors='ignore')
    except Exception:
        return results

    for part_line in decoded.split('\n'):
        part_line = part_line.strip()
        if not part_line:
            continue
        for prefix in PROTOCOL_PREFIXES:
            if part_line.startswith(prefix):
                q_pos = part_line.find('?')
                if q_pos != -1 and '#' in part_line[q_pos:]:
                    part_line = part_line[:q_pos + part_line[q_pos:].find('#')]
                results.append(part_line)
                break

    return results


def merge_nodes(nodes_dir: str, output_file: str):
    """合并所有 .txt 文件为 NeoKongBox 兼容格式。"""
    nodes_path = Path(nodes_dir)
    txt_files = sorted(nodes_path.glob('*.txt'))

    if not txt_files:
        print(f"未找到 {nodes_dir} 目录下的 .txt 文件")
        return

    seen = set()
    deduped = []
    stats = {
        'sources': 0,
        'base64_decoded': 0,
        'base64_failed': 0,
        'valid_nodes': 0,
        'duplicates': 0,
        'filtered': 0,
    }

    for txt_file in txt_files:
        try:
            for encoding in ['utf-8', 'gbk', 'gb2312', 'latin-1']:
                try:
                    with open(txt_file, 'r', encoding=encoding) as f:
                        lines = f.readlines()
                    break
                except (UnicodeDecodeError, UnicodeError):
                    continue
            else:
                print(f"[!] 跳过无法解码的文件: {txt_file.name}")
                continue

            for line in lines:
                if is_valid_node_line(line):
                    raw = line.rstrip('\n\r')
                    is_b64 = bool(BASE64_PATTERN.match(raw))
                    plain_lines = decode_to_plaintext_all(raw) if is_b64 else [decode_to_plaintext(raw)]
                    for plaintext in plain_lines:
                        if plaintext:
                            if plaintext not in seen:
                                seen.add(plaintext)
                                deduped.append(plaintext)
                                stats['valid_nodes'] += 1
                                if is_b64:
                                    stats['base64_decoded'] += 1
                            else:
                                stats['duplicates'] += 1

                elif line.strip() and not line.strip().startswith('#'):
                    stats['filtered'] += 1

            stats['sources'] += 1

        except Exception as e:
            print(f"[!] 读取文件失败 {txt_file.name}: {e}")

    # 写入输出文件
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# ============================================\n")
        f.write("# NeoKongBox 订阅文件（明文格式）\n")
        f.write(f"# 来源文件数: {stats['sources']}\n")
        f.write(f"# 去重后节点数: {stats['valid_nodes']}\n")
        f.write(f"# Base64 解码数: {stats['base64_decoded']}\n")
        f.write(f"# Base64 解码失败: {stats['base64_failed']}\n")
        f.write(f"# 重复节点数: {stats['duplicates']}\n")
        f.write(f"# 过滤行数: {stats['filtered']}\n")
        f.write("# ============================================\n")
        for line in deduped:
            f.write(line + '\n')

    print(f"\n[OK] 合并完成！")
    print(f"   来源文件数: {stats['sources']}")
    print(f"   去重后节点数: {stats['valid_nodes']}")
    print(f"   Base64 解码: {stats['base64_decoded']}")
    print(f"   Base64 解码失败: {stats['base64_failed']}")
    print(f"   重复节点数: {stats['duplicates']}")
    print(f"   过滤行数: {stats['filtered']}")
    print(f"   输出文件: {output_file}")


def cleanup_txt_files(outdir: str) -> bool:
    """清理 nodes/ 目录下的所有 .txt 文件（保留 neokongbox.txt）"""
    nodes_dir = Path(outdir)
    if not nodes_dir.exists():
        print(f"[!] 清理: 目录不存在 {outdir}")
        return False

    txt_files = sorted(nodes_dir.glob('*.txt'))
    if not txt_files:
        print(f"[OK] 清理: 没有 .txt 文件需要清理")
        return True

    # 排除 neokongbox.txt 和 neokongbox_subscription.txt
    exclude_names = {'neokongbox.txt', 'neokongbox_subscription.txt'}
    files_to_delete = [f for f in txt_files if f.name not in exclude_names]

    if not files_to_delete:
        print(f"[OK] 清理: 没有临时文件需要删除")
        return True

    print(f"\n{'='*60}")
    print(f"  清理临时文件")
    print(f"{'='*60}")
    print(f"[*] 准备删除 {len(files_to_delete)} 个 .txt 文件:")

    deleted_count = 0
    for f in files_to_delete:
        try:
            f.unlink()
            deleted_count += 1
            print(f"  [OK] 已删除: {f.name}")
        except Exception as e:
            print(f"  [!] 删除失败 {f.name}: {e}")

    print(f"\n[OK] 清理完成: 已删除 {deleted_count}/{len(files_to_delete)} 个文件")
    return deleted_count == len(files_to_delete)


# ── 工具函数 ──────────────────────────────────────────

def get_session(verify_ssl: bool = True) -> requests.Session:
    """创建带默认头部的 session"""
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    })
    s.verify = verify_ssl
    return s


def extract_article_id(href: str) -> int:
    """从文章 URL 中提取数字 ID，用于排序（ID 越大越新）"""
    m = re.search(r'/(\d+)\.?', href)
    if m:
        return int(m.group(1))
    return 0


# ── 日期归一化工具 ──────────────────────────────────────

def normalize_date(date_pattern, text):
    """
    从文本中提取日期并归一化为 YYYYMMDD 格式。
    返回 (YYYYMMDD_str, date_int) 或 (None, 0)。
    """
    m = date_pattern.search(text)
    if not m:
        return None, 0

    groups = m.groups()
    if len(groups) == 3:
        try:
            date_str = f"{groups[0].zfill(4)}{groups[1].zfill(2)}{groups[2].zfill(2)}"
            return date_str, int(date_str)
        except (ValueError, IndexError):
            pass
    elif len(groups) == 2:
        try:
            date_str = f"2026{groups[0].zfill(2)}{groups[1].zfill(2)}"
            return date_str, int(date_str)
        except (ValueError, IndexError):
            pass

    return None, 0


# ── 提取链接工具 ──────────────────────────────────────

def find_latest_article_url_from_homepage(site_config: dict, session: requests.Session, verify_ssl: bool = True) -> str | None:
    """
    从首页（或指定页面）提取最新文章链接。
    返回最新文章的完整 URL。
    策略：优先按日期排序，日期相同时按 ID 排序，都失败时取 ID 最大的。
    """
    target_url = site_config.get("home_url", site_config.get("home_url"))
    print(f"[*] 访问页面: {target_url}")

    resp = session.get(target_url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    article_pattern = site_config["article_pattern"]
    date_pattern = site_config["date_pattern"]

    raw_records = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)
        full_href = urljoin(target_url, href)

        if not article_pattern.search(full_href):
            continue

        date_str, date_int = normalize_date(date_pattern, text)
        if date_str is None:
            date_str, date_int = normalize_date(date_pattern, full_href)
        if date_str is None:
            date_str, date_int = normalize_date(date_pattern, a.parent.get_text(strip=True) if a.parent else "")

        article_id = extract_article_id(full_href)
        raw_records.append((date_str, date_int, article_id, full_href, text))

    # 去重：同一 URL 只保留有日期的那条
    seen_urls = {}
    for rec in raw_records:
        full_href = rec[3]
        if full_href not in seen_urls:
            seen_urls[full_href] = rec
        elif rec[1] > 0 and seen_urls[full_href][1] == 0:
            seen_urls[full_href] = rec
    article_links = list(seen_urls.values())

    if not article_links:
        print("[!] 页面未找到节点文章")
        return None

    # 按日期降序，日期为 None 或 0 时 fallback 到 ID 降序
    has_date = any(x[1] > 0 for x in article_links)

    site_key = None
    for k, v in SITES.items():
        if v.get("home_url") == target_url or v.get("home_url") == target_url.rstrip('/'):
            site_key = k
            break

    if site_key == "oneclash":
        article_links_filtered = [x for x in article_links if x[2] > 10 and x[1] > 0]
        if article_links_filtered:
            return article_links_filtered[0][3]
    elif has_date:
        article_links.sort(key=lambda x: (x[1] if x[1] > 0 else 0, x[2]), reverse=True)
    else:
        article_links.sort(key=lambda x: x[2], reverse=True)

    latest = article_links[0]
    preview = latest[4][:80] if latest[4] else latest[3]
    print(f"[*] 最新文章: {preview}")
    print(f"[*] 日期: {latest[0]}")

    return latest[3]


def extract_subscriber_links_a_tag(site_config: dict, resp_text: str, article_url: str) -> list[dict]:
    """从 <a> 标签中提取链接"""
    soup = BeautifulSoup(resp_text, "html.parser")
    link_pattern = site_config["link_url_pattern"]
    file_ext_map = site_config["file_ext_map"]
    desc_map = site_config["desc_map"]

    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not link_pattern.match(href):
            continue

        for ext, link_type in file_ext_map.items():
            if href.endswith(ext):
                desc = desc_map.get(ext, link_type)
                links.append({"type": link_type, "url": href, "desc": desc})
                break

    return links


def extract_subscriber_links_p_tag(site_config: dict, resp_text: str, article_url: str) -> list[dict]:
    """从 <p> 标签的纯文本中提取链接"""
    soup = BeautifulSoup(resp_text, "html.parser")
    link_pattern = site_config["link_url_pattern"]
    file_ext_map = site_config["file_ext_map"]
    desc_map = site_config["desc_map"]

    links = []
    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        for url in link_pattern.findall(text):
            url = url.strip()
            if any(l["url"] == url for l in links):
                continue

            for ext, link_type in file_ext_map.items():
                if url.endswith(ext):
                    desc = desc_map.get(ext, link_type)
                    links.append({"type": link_type, "url": url, "desc": desc})
                    break

    return links


def extract_subscriber_links_pre_tag(site_config: dict, resp_text: str, article_url: str) -> list[dict]:
    """从 <pre> 和 <code> 标签的纯文本中提取链接（用于 yoyapai 等站点）"""
    soup = BeautifulSoup(resp_text, "html.parser")
    link_pattern = site_config["link_url_pattern"]
    file_ext_map = site_config["file_ext_map"]
    desc_map = site_config["desc_map"]

    links = []
    for tag_name in ("pre", "code"):
        for tag in soup.find_all(tag_name):
            text = tag.get_text(strip=True)
            for url in link_pattern.findall(text):
                url = url.strip()
                if any(l["url"] == url for l in links):
                    continue

                for ext, link_type in file_ext_map.items():
                    if url.endswith(ext):
                        desc = desc_map.get(ext, link_type)
                        links.append({"type": link_type, "url": url, "desc": desc})
                        break

    return links


def extract_subscriber_links_both(site_config: dict, resp_text: str, article_url: str) -> list[dict]:
    """同时尝试 <a> 标签、<p> 标签和 <pre>/<code> 标签提取"""
    links = extract_subscriber_links_a_tag(site_config, resp_text, article_url)
    if not links:
        links = extract_subscriber_links_p_tag(site_config, resp_text, article_url)
    if not links:
        links = extract_subscriber_links_pre_tag(site_config, resp_text, article_url)
    return links


def extract_cfm_subscriber_links(site_config: dict, resp_text: str, article_url: str) -> list[dict]:
    """CFMem 特殊处理：从 nodebuf preview 链接中提取"""
    soup = BeautifulSoup(resp_text, "html.parser")
    links = []

    for a in soup.find_all('a', href=True):
        href = a["href"]
        preview_match = re.search(r'nodebuf\.com/files/public/([a-zA-Z0-9_]+)/preview', href, re.I)
        if preview_match:
            file_id = preview_match.group(1)
            links.append({
                "type": "cfmem_preview",
                "url": f"https://nodebuf.com/files/public/{file_id}/preview",
                "desc": f"CFMem 节点预览 ({file_id[:12]}...)",
                "file_id": file_id,
            })
    return links


def extract_subscriber_links(site_config: dict, article_url: str, session: requests.Session) -> list[dict]:
    """进入文章页，提取订阅链接"""
    print(f"[*] 访问文章页: {article_url[:80]}...")
    resp = session.get(article_url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    extract_method = site_config.get("extract_links", "both")

    if extract_method == "a_tag":
        links = extract_subscriber_links_a_tag(site_config, resp.text, article_url)
    elif extract_method == "mibei77_style":
        links = extract_subscriber_links_p_tag(site_config, resp.text, article_url)
    elif extract_method == "cfmem":
        links = extract_cfm_subscriber_links(site_config, resp.text, article_url)
    elif extract_method == "both":
        links = extract_subscriber_links_both(site_config, resp.text, article_url)
    else:
        links = extract_subscriber_links_a_tag(site_config, resp.text, article_url)
        if not links:
            links = extract_subscriber_links_p_tag(site_config, resp.text, article_url)

    return links


def download_links(site_key: str, site_config: dict, links: list[dict], outdir: str, session: requests.Session) -> list[str]:
    """下载订阅链接的文件内容，保存到本地"""
    os.makedirs(outdir, exist_ok=True)
    saved_files = []

    for link in links:
        link_type = link["type"]
        url = link["url"]
        print(f"[*] 下载 {link_type.upper()}: {url[:70]}...")

        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"[!] 下载失败: {e}")
            continue

        if "file_id" in link:
            filename = f"cfmem_{link['file_id'][:16]}.txt"
        else:
            filename = f"{site_key}_{Path(url).name}"

        filepath = os.path.join(outdir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(resp.text)

        print(f"[OK] 已保存: {filepath} ({len(resp.text):,} 字节)")
        saved_files.append(filepath)

        time.sleep(1)

    return saved_files


def download_github_raw(outdir: str, session: requests.Session) -> dict:
    """下载 GitHub Raw 直链文件"""
    if not GITHUB_RAW_URLS:
        return {"status": "EMPTY", "files": []}

    print(f"\n{'='*60}")
    print(f"  站点: GitHub Raw 直链")
    print(f"{'='*60}")

    os.makedirs(outdir, exist_ok=True)
    saved_files = []

    for item in GITHUB_RAW_URLS:
        url = item["url"]
        name = item["name"]
        print(f"[*] 下载 {name}: {url[:70]}...")

        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"[!] 下载失败: {e}")
            continue

        filename = Path(url).name
        if not filename or filename == "":
            parts = url.rstrip("/").split("/")
            filename = parts[-1] if parts else "github_raw"
            if not filename.endswith((".txt", ".yaml", ".yml")):
                filename += ".txt"

        filepath = os.path.join(outdir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(resp.text)

        print(f"[OK] 已保存: {filepath} ({len(resp.text):,} 字节)")
        saved_files.append(filepath)
        time.sleep(1)

    return {"status": "OK" if saved_files else "FAIL", "files": saved_files}


def download_direct_subscriptions(outdir: str, session: requests.Session) -> dict:
    """下载直连订阅源文件（无文章解析，用 NekoBox UA 请求）"""
    session.headers.update({"User-Agent": NEKODOX_UA})
    if not DIRECT_SUBSCRIPTION_URLS:
        return {"status": "EMPTY", "files": []}

    print(f"\n{'='*60}")
    print(f"  直连订阅源")
    print(f"{'='*60}")

    os.makedirs(outdir, exist_ok=True)
    saved_files = []

    for item in DIRECT_SUBSCRIPTION_URLS:
        url = item["url"]
        name = item["name"]
        print(f"[*] 下载 {name}: {url[:60]}...")

        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"[!] 下载失败: {e}")
            continue

        filename = f"direct_{name}_{Path(url).name or 'subscription'}.txt"
        filepath = os.path.join(outdir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(resp.text)

        print(f"[OK] 已保存: {filepath} ({len(resp.text):,} 字节)")
        saved_files.append(filepath)
        time.sleep(1)

    return {"status": "OK" if saved_files else "FAIL", "files": saved_files}


# ── 站点处理 ──────────────────────────────────────────

def process_site(site_key: str, download: bool, outdir: str, verify_ssl: bool = True) -> dict:
    """处理单个站点"""
    site_config = SITES[site_key]
    print(f"\n{'='*60}")
    print(f"  站点: {site_config['name']} ({site_key})")
    print(f"{'='*60}")

    session = get_session(verify_ssl)

    article_url = find_latest_article_url_from_homepage(site_config, session)
    if not article_url:
        return {"status": "NO_ARTICLE", "links": [], "files": []}

    subscriber_links = extract_subscriber_links(site_config, article_url, session)
    if not subscriber_links:
        return {"status": "NO_LINKS", "links": [], "files": []}

    print(f"\n[*] 找到 {len(subscriber_links)} 个订阅链接:\n")
    for link in subscriber_links:
        print(f"  [{link['type'].upper():16s}] {link['url']}")
        print(f"         {link['desc']}")

    saved_files = []
    if download:
        saved = download_links(site_key, site_config, subscriber_links, outdir, session)
        saved_files = saved

    return {
        "status": "OK",
        "article_url": article_url,
        "links": subscriber_links,
        "files": saved_files,
    }


CFMEM_ARTICLE_PATTERN = re.compile(r'https://www\.cfmem\.com/\d{4}/\d{2}/[\w-]+\.html')


def find_latest_cfmem_article(session: requests.Session) -> str | None:
    """从 cfmem.com sitemap 获取最新文章 URL"""
    print("[*] 获取 sitemap 索引...")
    try:
        resp = session.get("https://www.cfmem.com/sitemap.xml", timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        page_urls = re.findall(r'<loc>(https://www\.cfmem\.com/sitemap\.xml\?page=\d+)</loc>', resp.text)
        if not page_urls:
            print("[!] sitemap 索引未找到分页")
            return None
        # 第1页是最新的
        sitemap_page = page_urls[0]
    except requests.RequestException as e:
        print(f"[!] 获取 sitemap 索引失败: {e}")
        return None

    print(f"[*] 获取 sitemap 分页: {sitemap_page}")
    try:
        resp = session.get(sitemap_page, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        articles = CFMEM_ARTICLE_PATTERN.findall(resp.text)
        if not articles:
            print("[!] sitemap 分页未找到文章")
            return None
        latest = articles[0]
        print(f"[*] 最新文章: {latest}")
        return latest
    except requests.RequestException as e:
        print(f"[!] 获取 sitemap 分页失败: {e}")
        return None


def process_cfmem(download: bool, outdir: str) -> dict:
    """处理 CFMem 站点（通过 sitemap 自动获取最新文章）"""
    site_config = CFMEM_SITE
    print(f"\n{'='*60}")
    print(f"  站点: {site_config['name']} (cfmem)")
    print(f"{'='*60}")

    session = get_session()
    article_url = find_latest_cfmem_article(session)
    if not article_url:
        return {"status": "NO_ARTICLE", "links": [], "files": []}

    print(f"[*] 文章 URL: {article_url}")
    subscriber_links = extract_subscriber_links(site_config, article_url, session)

    if not subscriber_links:
        return {"status": "NO_LINKS", "links": [], "files": []}

    print(f"\n[*] 找到 {len(subscriber_links)} 个节点链接:\n")
    for link in subscriber_links:
        print(f"  [{link['type'].upper():16s}] {link['url']}")
        print(f"         {link['desc']}")

    saved_files = []
    if download:
        saved = download_links("cfmem", site_config, subscriber_links, outdir, session)
        saved_files = saved

    return {
        "status": "OK",
        "article_url": article_url,
        "links": subscriber_links,
        "files": saved_files,
    }


def process_all_sites(download: bool, outdir: str, verify_ssl: bool = True) -> dict:
    """处理所有站点"""
    results = {}

    for site_key in SITES:
        result = process_site(site_key, download, outdir, verify_ssl)
        results[site_key] = result
        time.sleep(2)

    results["cfmem"] = process_cfmem(download, outdir)
    time.sleep(2)

    if download:
        gh_result = download_github_raw(outdir, get_session())
        results["github_raw"] = gh_result
        time.sleep(1)
        direct_result = download_direct_subscriptions(outdir, get_session())
        results["direct_sub"] = direct_result

    return results


# ── 主程序 ────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="节点订阅链接自动下载器 + NeoKongBox 合并工具（只保留 .txt v2ray 订阅，支持 9 个站点 + CFMem + GitHub Raw + 直连订阅源）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
站点列表：
  mibei77      - 米贝77 (mibei77.com)
  clash        - ClashGitHub (clashgithub.com)
  oneclash     - OneClash (oneclash.cc)
  v2rayshare   - V2RayShare (v2rayshare.net)
  wanzhuanmi   - 玩转喵 (wanzhuanmi.com)
  yoyapai      - YoYaPaI (yoyapai.com)
  stairnode    - StairNode (stairnode.com)
  clashnode    - ClashNode (clashnode.cc)
  cfmem        - CFMem (cfmem.com)
  all          - 所有站点（含直连订阅源 freev2.net / ssrsub / daozhangnb）

 模式：
  --download   下载文件内容到本地
  --merge      合并 nodes/ 目录下所有 .txt 文件为 NeoKongBox 兼容格式
  --neokongbox 同 --merge

示例：
  python node_downloader.py                             # 一键全自动：下载+合并+清理
  python node_downloader.py --site mibei77              # 查看链接，不下载
  python node_downloader.py --site clash --download     # 查看并下载文件
  python node_downloader.py --all --download            # 处理所有站点
  python node_downloader.py --merge                     # 仅合并已有文件
        """,
    )
    parser.add_argument("--site",
                        choices=["mibei77", "clash", "oneclash", "v2rayshare", "wanzhuanmi", "yoyapai", "stairnode", "clashnode", "cfmem"],
                        help="目标站点（不指定时用 --all 处理所有）")
    parser.add_argument("--all", action="store_true", help="处理所有支持站点")
    parser.add_argument("--download", action="store_true", help="下载文件内容到本地")
    parser.add_argument("--merge", "--neokongbox", action="store_true", help="合并 nodes/ 目录下所有 .txt 文件为 NeoKongBox 兼容格式（去重 + Base64 转明文）")
    parser.add_argument("--outdir", default="./nodes", help="文件输出目录（默认: ./nodes）")
    parser.add_argument("--no-verify-ssl", action="store_true", help="跳过 SSL 证书验证（用于 stairnode.com 等站点）")
    args = parser.parse_args()

    # ── 无参数：一键全自动（下载+合并+清理） ──
    if not args.site and not args.merge:
        print("=== 一键全自动模式 ===")
        results = process_all_sites(True, args.outdir, not args.no_verify_ssl)
        print(f"\n{'='*60}")
        print("  汇总")
        print(f"{'='*60}")
        for site_key, result in results.items():
            if site_key == "cfmem":
                site_name = "CFMem"
            elif site_key == "github_raw":
                site_name = "GitHub Raw"
            elif site_key == "direct_sub":
                site_name = "直连订阅源"
            else:
                site_name = SITES[site_key]["name"]
            if result.get("status") == "OK" or (isinstance(result, dict) and result.get("status") == "OK"):
                file_count = len(result.get("files", []))
                if args.download:
                    print(f"  [OK] {site_name}: {file_count} 个文件已保存")
            else:
                print(f"  [!] {site_name}: {result.get('status', 'UNKNOWN')}")

        # 合并 + 清理
        print(f"\n{'='*60}")
        print("  开始合并为 NeoKongBox 格式")
        print(f"{'='*60}")
        output_file = os.path.join(args.outdir, "neokongbox.txt")
        merge_nodes(args.outdir, output_file)
        cleanup_txt_files(args.outdir)
        return

    # ── 合并模式 ──
    if args.merge:
        nodes_dir = args.outdir
        output_file = os.path.join(nodes_dir, "neokongbox.txt")
        merge_nodes(nodes_dir, output_file)
        cleanup_txt_files(args.outdir)
        return

    if not args.all and not args.site:
        parser.print_help()
        sys.exit(1)

    # ── 下载模式 ──
    if args.all:
        results = process_all_sites(args.download, args.outdir, not args.no_verify_ssl)
        print(f"\n{'='*60}")
        print("  汇总")
        print(f"{'='*60}")
        for site_key, result in results.items():
            if site_key == "cfmem":
                site_name = "CFMem"
            elif site_key == "github_raw":
                site_name = "GitHub Raw"
            elif site_key == "direct_sub":
                site_name = "直连订阅源"
            else:
                site_name = SITES[site_key]["name"]

            if result.get("status") == "OK" or (isinstance(result, dict) and result.get("status") == "OK"):
                file_count = len(result.get("files", []))
                link_count = len(result.get("links", []))
                if args.download:
                    print(f"  [OK] {site_name}: {file_count} 个文件已保存")
                else:
                    print(f"  [OK] {site_name}: 完成")
            else:
                print(f"  [!] {site_name}: {result.get('status', 'UNKNOWN')}")

        # 如果同时请求了 --merge，下载后自动合并
        if args.merge:
            print(f"\n{'='*60}")
            print("  开始合并为 NeoKongBox 格式")
            print(f"{'='*60}")
            output_file = os.path.join(args.outdir, "neokongbox.txt")
            merge_nodes(args.outdir, output_file)
            cleanup_txt_files(args.outdir)
    else:
        if args.site == "cfmem":
            result = process_cfmem(args.download, args.outdir)
            site_name = "CFMem"
        else:
            result = process_site(args.site, args.download, args.outdir, not args.no_verify_ssl)
            site_name = SITES[args.site]["name"]

        if result["status"] == "OK":
            print(f"\n[OK] {site_name} 处理完成: {len(result['links'])} 个链接")
            if result.get("files"):
                print(f"    已保存 {len(result['files'])} 个文件")
        else:
            print(f"\n[!] {site_name}: {result['status']}")
            sys.exit(1)


if __name__ == "__main__":
    main()
