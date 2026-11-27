# -*- coding: utf-8 -*-
"""通用工具函数。"""
import hashlib
import re
import secrets
import subprocess
import sys
import time
import uuid


def logf(fmt, *args):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), fmt % args), file=sys.stderr, flush=True)


def short_hash(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]


def new_uuid():
    return str(uuid.uuid4())


def random_hex(n):
    return secrets.token_hex(n)


def random_digits(n):
    return "".join(str(secrets.randbelow(10)) for _ in range(n))


def truncate(s, n):
    return s if len(s) <= n else s[:n] + "..."


def parse_dur(s, default):
    """解析 Go 风格时长（60s / 10m / 2m / 1h / 300ms），返回秒。"""
    if not s:
        return default
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(ns|us|µs|ms|s|m|h)", s.strip())
    if not m:
        raise ValueError("非法时长: %r" % s)
    val = float(m.group(1))
    mult = {"ns": 1e-9, "us": 1e-6, "µs": 1e-6, "ms": 1e-3, "s": 1, "m": 60, "h": 3600}[m.group(2)]
    secs = val * mult
    return secs if secs > 0 else default


def parse_retry_after(v):
    v = (v or "").strip()
    if not v:
        return 0.0
    try:
        return parse_dur(v + "s", 0.0)
    except ValueError:
        return 0.0


def num_of(v):
    """兼容 JSON 数字与带千分位/货币符号的数字字符串。"""
    if isinstance(v, bool):
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        clean = v.replace(",", "").replace(" ", "").replace("¥", "").replace("$", "").strip()
        try:
            return float(clean)
        except ValueError:
            return 0.0
    return 0.0


def run_sign(cmd, target):
    try:
        p = subprocess.run(["sh", "-c", cmd], input=target.encode("utf-8"),
                           capture_output=True, timeout=10)
        return p.stdout.decode("utf-8", "ignore").strip()
    except Exception:
        return ""


def atoi_safe(s):
    n = 0
    for ch in s:
        if not ch.isdigit():
            break
        n = n * 10 + int(ch)
    return n


def home_dir(workspace):
    if not workspace:
        return ""
    i = workspace.find(r"\Doubao")
    if i > 0:
        return workspace[:i]
    return ""


def format_reward(n):
    n = num_of(n)
    return "%d 积分" % int(n) if n > 0 else ""


def mask_key(k):
    if len(k) <= 10:
        return "****"
    return k[:6] + "…" + k[-4:]


def local_midnight_epoch():
    lt = time.localtime()
    return time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, 0, 0, -1))
