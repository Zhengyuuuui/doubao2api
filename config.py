# -*- coding: utf-8 -*-
"""配置加载（等价 internal/config/config.go）。"""
import json
import os

from util import parse_dur

DEFAULT_USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36 SamanthaDoubao/2.30.1")

DEFAULT_UPSTREAM_QUERY = {
    "aid": "582478", "real_aid": "582478", "channel": "win",
    "client_platform": "pc_client", "device_platform": "desktop",
    "doubao_device_platform": "desktop", "doubao_pc_version": "2.30.1",
    "pc_version": "2.30.1", "pkg_type": "release_version", "language": "zh",
    "region": "CN", "sys_region": "CN", "runtime": "web",
    "runtime_version": "3.37.4", "samantha_web": "1",
    "use-olympus-account": "1", "version_code": "20800",
    "web_platform": "desktop", "chromium_version": "147.0.7727.149",
}

DEFAULT_QR_QUERY = {
    "aid": "582478", "language": "zh", "next": "https://www.doubao.com/",
    "is_from_ttaccountsdk": "1", "passport_jssdk_version": "4.4.6",
    "passport_jssdk_type": "normal", "sdk_version": "2.2.11-doubao.0",
    "pkg_type": "release_version", "version_code": "20800", "device_platform": "desktop",
}


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        c = json.load(f)

    c.setdefault("listen", "127.0.0.1:8800")
    c.setdefault("admin_token", os.environ.get("DOUBAO2API_ADMIN_TOKEN", ""))
    c.setdefault("log_level", "info")
    c.setdefault("api_keys", [])
    c.setdefault("accounts", [])
    c.setdefault("models", [])
    c.setdefault("model_aliases", {})
    c.setdefault("model_params", {})
    c.setdefault("default_model", "doubao")

    up = c.setdefault("upstream", {})
    up.setdefault("chat_url", "https://www.doubao.com/chat/completion")
    up.setdefault("user_agent", DEFAULT_USER_AGENT)
    up.setdefault("timeout_seconds", 300)
    up.setdefault("heartbeat_seconds", 15)
    up.setdefault("flatten_history", True)
    up.setdefault("a_bogus", "")
    up.setdefault("sign_command", "")
    up.setdefault("query", {})
    for k, v in DEFAULT_UPSTREAM_QUERY.items():
        up["query"].setdefault(k, v)

    wm = c.setdefault("work_mode", {})
    wm.setdefault("enabled", True)
    wm.setdefault("bot_id", "7338286299411103781")
    wm.setdefault("mode_id", "3")
    wm.setdefault("reasoning_effort", "4")
    wm.setdefault("memory_profile", "300k_230")
    wm.setdefault("total_window_size", "300000")
    wm.setdefault("enable_sandbox", False)
    wm.setdefault("workspace", "")

    qr = c.setdefault("qr", {})
    qr.setdefault("enabled", True)
    qr.setdefault("get_url", "https://accounts.doubao.com/passport/web/get_qrcode/")
    qr.setdefault("check_url", "https://accounts.doubao.com/passport/web/check_qrconnect/")
    qr.setdefault("query", {})
    for k, v in DEFAULT_QR_QUERY.items():
        qr["query"].setdefault(k, v)

    pool = c.setdefault("pool", {})
    pool.setdefault("rate_limit_cooldown", "60s")
    pool.setdefault("overload_cooldown", "15s")
    pool.setdefault("sticky_ttl", "10m")
    pool.setdefault("large_request_bytes", 262144)
    pool.setdefault("large_budget", 6)
    pool.setdefault("large_window", "2m")
    pool.setdefault("deterministic", False)

    ck = c.setdefault("checkin", {})
    ck.setdefault("enabled", True)
    ck.setdefault("hour", 9)
    ck.setdefault("scene", "pc_incentive_task_mvp_v3")

    # 校验账号
    for i, a in enumerate(c["accounts"]):
        if not a.get("name"):
            a["name"] = "acct-%d" % (i + 1)
        if not a.get("cookie"):
            raise ValueError("accounts[%d] 缺少 cookie" % i)
        a.setdefault("local_app_id", "582478")

    # 解析时长 → 秒
    try:
        c["_pool_dur"] = {
            "rate_limit_cooldown": parse_dur(pool["rate_limit_cooldown"], 60),
            "overload_cooldown": parse_dur(pool["overload_cooldown"], 15),
            "sticky_ttl": parse_dur(pool["sticky_ttl"], 600),
            "large_window": parse_dur(pool["large_window"], 120),
        }
    except ValueError as e:
        raise ValueError(str(e))

    return c


def resolve_model(cfg, name):
    if not name:
        name = cfg["default_model"]
    aliases = cfg.get("model_aliases", {})
    if name in aliases and aliases[name]:
        return aliases[name]
    return name


def model_list(cfg):
    if cfg.get("models"):
        return list(cfg["models"])
    seen = set()
    out = []

    def add(m):
        if m and m not in seen:
            seen.add(m)
            out.append(m)

    add(cfg["default_model"])
    for k in cfg.get("model_aliases", {}):
        add(k)
    return out
