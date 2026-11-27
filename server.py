# -*- coding: utf-8 -*-
"""HTTP 层：协议转换、调度、密钥、额度、签到、模型、扫码 + 控制台后端（等价 internal/server/*.go）。"""
import hashlib
import hmac
import ipaddress
import json
import os
import queue
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from config import model_list, resolve_model
from doubao import AccumSink, QueueSink, build_body, conversation_key, pump
from pool import Pool
from util import (atoi_safe, format_reward, local_midnight_epoch, logf, mask_key,
                  new_uuid, num_of, parse_retry_after, random_hex, run_sign,
                  short_hash, truncate)

MAX_BODY_BYTES = 32 << 20


def _chunk_sse(id_, created, delta, finish):
    chunk = {"id": id_, "object": "chat.completion.chunk", "created": created,
             "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
    return "data: " + json.dumps(chunk, ensure_ascii=False) + "\n\n"


def _error_sse(msg):
    return "data: " + json.dumps({"error": {"message": msg}}, ensure_ascii=False) + "\n\n"


def _collect_quota(v, out):
    if isinstance(v, dict):
        limits = v.get("window_limits")
        if isinstance(limits, list):
            gname = v.get("feature_group_name", "")
            for l in limits:
                if not isinstance(l, dict):
                    continue
                total = num_of(l.get("total_amount"))
                used = num_of(l.get("used_amount"))
                if total <= 0 and used <= 0:
                    continue
                out["total"] += total
                out["used"] += used
                out["groups"].append({
                    "name": gname, "total": total, "used": used,
                    "percent": num_of(l.get("used_percent")),
                    "end_time": int(num_of(l.get("end_time"))),
                })
        for child in v.values():
            _collect_quota(child, out)
    elif isinstance(v, list):
        for child in v:
            _collect_quota(child, out)


def _collect_cookies(resp):
    out = []
    try:
        set_cookies = resp.headers.get_all("Set-Cookie") or []
    except Exception:
        set_cookies = []
    for sc in set_cookies:
        out.append(sc.split(";", 1)[0])
    return "; ".join(out)


def _map_qr_status(s):
    s = (s or "").strip().lower()
    if s in ("new", "1", "0", "waiting"):
        return "waiting"
    if s in ("scanning", "scanned", "2", "5"):
        return "scanned"
    if s in ("confirmed", "success", "3", "6"):
        return "confirmed"
    if s in ("expired", "4", "7"):
        return "expired"
    if s in ("canceled", "cancelled", "8"):
        return "canceled"
    return ""


class Server:
    def __init__(self, cfg):
        self.cfg = cfg
        self.pool = Pool(cfg["_pool_dur"])
        self.lock = threading.Lock()
        self.accs = {}
        self.convs = {}
        self.keys = []
        self.keys_lock = threading.Lock()
        self.usage_lock = threading.Lock()
        self.qr_lock = threading.Lock()
        self.qr_sess = {}
        self.last_checkin = {}
        self.checkin_lock = threading.Lock()
        self.up_at = time.time()
        self.console_html = self._load_console_html()

        self._resync_accounts()
        self._load_keys()
        if cfg["checkin"].get("enabled"):
            threading.Thread(target=self._checkin_loop, daemon=True).start()

    # ---------- 路径 ----------
    def _base_dir(self):
        return os.path.dirname(os.path.abspath(__file__))

    def accounts_path(self):
        return os.environ.get("DOUBAO2API_ACCOUNTS", os.path.join(self._base_dir(), "accounts.json"))

    def keys_path(self):
        return os.environ.get("DOUBAO2API_KEYS", os.path.join(self._base_dir(), "keys.json"))

    def usage_path(self):
        return os.environ.get("DOUBAO2API_USAGE", os.path.join(self._base_dir(), "usage.jsonl"))

    def _load_console_html(self):
        path = os.environ.get("DOUBAO2API_CONSOLE", os.path.join(self._base_dir(), "console.html"))
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        except OSError:
            return "<!doctype html><meta charset=utf-8><h1>console.html 未找到</h1>"

    # ---------- 日志 ----------
    def _log(self, fmt, *args):
        logf(fmt, *args)

    # ---------- 账号持久化（accounts.json 单一数据源） ----------
    def _load_accounts_file(self):
        try:
            with open(self.accounts_path(), "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("accounts") or []
        except OSError:
            return []
        except ValueError:
            return []

    def _save_accounts_file(self, accounts):
        path = self.accounts_path()
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"accounts": accounts}, f, ensure_ascii=False, indent=2)

    def _resync_accounts(self):
        """从 accounts.json 重建内存账号与号池（enabled 才入池）。"""
        accounts = self._load_accounts_file()
        if not accounts and self.cfg.get("accounts"):
            accounts = [dict(a) for a in self.cfg["accounts"]]
            for a in accounts:
                a.setdefault("enabled", True)
            self._save_accounts_file(accounts)

        enabled = {}
        for a in accounts:
            a = dict(a)
            name = (a.get("name") or "").strip()
            if not name or not a.get("cookie"):
                continue
            a.setdefault("local_app_id", "582478")
            a.setdefault("enabled", True)
            if a["enabled"]:
                enabled[name] = a

        with self.lock:
            for name in list(self.accs.keys()):
                if name not in enabled:
                    self.pool.remove(name)
            self.accs = enabled
            for name in enabled:
                self.pool.add(name)
        return {"total": self.pool.length(), "healthy": self.pool.healthy()}

    # ---------- 密钥 ----------
    def _load_keys(self):
        try:
            with open(self.keys_path(), "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list) and data:
                self.keys = data
                logf("keys loaded=%d", len(data))
                return
        except OSError:
            pass
        except ValueError:
            pass
        self.keys = []
        for i, k in enumerate(self.cfg.get("api_keys") or []):
            self.keys.append({"id": "k%d" % (i + 1), "name": "default-%d" % (i + 1),
                              "key": k, "created": int(time.time()), "enabled": True})
        if self.keys:
            self._save_keys()
            logf("keys seeded from config=%d", len(self.keys))

    def _save_keys(self):
        with self.keys_lock:
            out = json.dumps(self.keys, indent=2)
        with open(self.keys_path(), "w", encoding="utf-8") as f:
            f.write(out)

    def _lookup_key(self, token):
        if not token:
            return None
        th = hashlib.sha256(token.encode("utf-8")).digest()
        with self.keys_lock:
            for k in self.keys:
                if not k.get("enabled") or not k.get("key"):
                    continue
                kh = hashlib.sha256(k["key"].encode("utf-8")).digest()
                if hmac.compare_digest(th, kh):
                    return k
        return None

    # ---------- 用量 ----------
    def record_usage(self, key_id, model, chars, status, latency_ms, conv="", account="", error=""):
        rec = {"ts": int(time.time()), "key": key_id, "model": model, "chars": chars,
               "tokens": int(chars / 1.7), "status": status, "latency_ms": latency_ms,
               "conv": conv, "account": account, "error": error}
        with self.usage_lock:
            try:
                with open(self.usage_path(), "a", encoding="utf-8") as f:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            except OSError:
                pass

    def _scan_usage(self):
        recs = []
        try:
            with open(self.usage_path(), "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        recs.append(json.loads(line))
                    except ValueError:
                        continue
        except OSError:
            pass
        return recs

    def _aggregate_usage(self, days=7):
        since = time.time() - days * 86400
        today_start = local_midnight_epoch()
        by_key = {}
        for rec in self._scan_usage():
            ts = rec.get("ts", 0)
            if ts < since:
                continue
            k = rec.get("key", "")
            a = by_key.get(k)
            if a is None:
                a = {"key_id": k, "name": k, "requests": 0, "chars": 0, "tokens": 0,
                     "today_requests": 0, "today_tokens": 0, "last_ts": 0}
                by_key[k] = a
            a["requests"] += 1
            a["chars"] += rec.get("chars", 0)
            a["tokens"] += rec.get("tokens", 0)
            if ts >= today_start:
                a["today_requests"] += 1
                a["today_tokens"] += rec.get("tokens", 0)
            if ts > a["last_ts"]:
                a["last_ts"] = ts
        with self.keys_lock:
            for k in self.keys:
                if k["id"] in by_key:
                    by_key[k["id"]]["name"] = k.get("name") or k["id"]
        return sorted(by_key.values(), key=lambda x: x["tokens"], reverse=True)

    def _today_stats(self):
        today_start = local_midnight_epoch()
        reqs = 0
        chars = 0
        lat_sum = 0
        lat_n = 0
        for rec in self._scan_usage():
            if rec.get("ts", 0) < today_start:
                continue
            reqs += 1
            chars += rec.get("chars", 0)
            lat_sum += rec.get("latency_ms", 0)
            lat_n += 1
        return {"requests": reqs, "chars": chars,
                "avg_latency_ms": int(lat_sum / lat_n) if lat_n else 0}

    def _read_logs(self, limit=50):
        recs = self._scan_usage()
        out = []
        for rec in recs[-limit:]:
            out.append({
                "ts": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(rec.get("ts", 0))),
                "model": rec.get("model", ""),
                "status": rec.get("status", 0),
                "latency_ms": rec.get("latency_ms", 0),
                "chars": rec.get("chars", 0),
                "conv": rec.get("conv", ""),
                "account": rec.get("account", ""),
                "error": rec.get("error", ""),
            })
        return out

    # ---------- 会话状态 ----------
    def _get_conv(self, k):
        with self.lock:
            return dict(self.convs.get(k, {}))

    def _set_conv(self, k, conv, section):
        if not k or not conv:
            return
        with self.lock:
            self.convs[k] = {"conversation_id": conv, "section_id": section, "at": time.time()}
            if len(self.convs) > 5000:
                now = time.time()
                for key in [x for x, v in self.convs.items() if now - v["at"] > 3600]:
                    self.convs.pop(key, None)

    # ---------- 模型参数 ----------
    def _model_param_for(self, model_name, model_key):
        mp = self.cfg.get("model_params") or {}
        if model_name in mp:
            return mp[model_name]
        if model_key in mp:
            return mp[model_key]
        return None

    def _status_payload(self):
        return {"accounts": self.pool.stats(), "healthy": self.pool.healthy(),
                "total": self.pool.length()}

    # ---------- 上游转发 ----------
    def _build_url(self, acct):
        u = urllib.parse.urlsplit(self.cfg["upstream"]["chat_url"])
        q = dict(urllib.parse.parse_qsl(u.query))
        for k, v in self.cfg["upstream"]["query"].items():
            if k not in q:
                q[k] = v
        dev = acct.get("device_id") or short_hash(acct["cookie"])
        q["device_id"] = dev
        q["fp"] = "verify_" + dev
        q["tea_uuid"] = dev
        if acct.get("web_id"):
            q["web_id"] = acct["web_id"]
        q["web_tab_id"] = new_uuid()

        def enc(qq):
            return urllib.parse.urlunsplit(
                (u.scheme, u.netloc, u.path, urllib.parse.urlencode(qq), u.fragment))

        url = enc(q)
        if self.cfg["upstream"].get("sign_command"):
            sig = run_sign(self.cfg["upstream"]["sign_command"], url)
            if sig:
                q["a_bogus"] = sig
                url = enc(q)
        elif self.cfg["upstream"].get("a_bogus"):
            q["a_bogus"] = self.cfg["upstream"]["a_bogus"]
            url = enc(q)
        return url

    def _forward(self, req, body, acct):
        try:
            target = self._build_url(acct)
        except Exception as e:
            self._log("build_url_error acct=%s err=%s", acct.get("name"), e)
            return 0, True, None, str(e)
        headers = {
            "Content-Type": "application/json",
            "Accept": "*/*",
            "agw-js-conv": "str, str",
            "last-event-id": "undefined",
            "x-flow-trace": "04-0000000000000000000000000000000-0000000000000000-01",
            "User-Agent": self.cfg["upstream"]["user_agent"],
            "Referer": "https://www.doubao.com/",
            "Cookie": acct["cookie"],
        }
        r = urllib.request.Request(target, data=body, headers=headers, method="POST")
        try:
            resp = urllib.request.urlopen(r, timeout=self.cfg["upstream"]["timeout_seconds"])
        except urllib.error.HTTPError as e:
            raw = e.read(4096).decode("utf-8", "ignore")
            msg = raw.strip()
            self._log("upstream_error acct=%s status=%d body=%s", acct.get("name"), e.code,
                      truncate(msg, 200))
            if e.code in (401, 403):
                self.pool.note_auth_failure(acct["name"], msg)
                return 0, True, None, msg
            if e.code == 429:
                self.pool.note_rate_limit(acct["name"], parse_retry_after(e.headers.get("Retry-After", "")))
                return 0, True, None, msg
            if e.code >= 500:
                self.pool.note_overload(acct["name"])
                return 0, True, None, msg
            return e.code, False, None, msg or ("upstream %d" % e.code)
        except Exception as e:
            self._log("net_error acct=%s err=%s", acct.get("name"), e)
            self.pool.note_overload(acct["name"])
            return 0, True, None, str(e)
        return 200, False, resp, ""

    def _pick_key(self, conv, large, tried):
        sticky_name = self.pool.resolve_sticky(conv)
        if sticky_name and sticky_name not in tried:
            h = self.pool.pick_by_sticky(sticky_name, large)
            if h is not None:
                return h
            self.pool.unbind_sticky(conv)
        return self.pool.pick(tried, large)

    def _pick_and_forward(self, req, body, conv_key, large):
        max_attempts = min(self.pool.length(), 3)
        tried = set()
        for _ in range(max_attempts):
            h = self._pick_key(conv_key, large, tried)
            if h is None:
                break
            name = h.name_str()
            tried.add(name)
            if large:
                self.pool.note_large(name)
            acct = self.accs.get(name)
            if not acct:
                self.pool.note_auth_failure(name, "account missing")
                continue
            status, retry, resp, msg = self._forward(req, body, acct)
            if retry:
                continue
            if status == 200 and resp is not None:
                self.pool.note_success(name)
                self.pool.bind_sticky(conv_key, name)
                return resp, name, None
            return None, None, {"status": status, "message": msg}
        return None, None, {"status": 503, "message": "所有豆包账号均不可用"}

    # ---------- 输出 ----------
    def chat_stream_gen(self, req, raw, key_id):
        """同步 generator：号池流式 chat，yield SSE 字符串（供 FastAPI StreamingResponse）。"""
        started = time.time()
        try:
            resp, name, err = self.run_chat(req, raw, key_id)
        except ValueError as e:
            yield _error_sse(str(e))
            yield "data: [DONE]\n\n"
            return
        if err:
            yield _error_sse(err.get("message", ""))
            yield "data: [DONE]\n\n"
            return

        sink = QueueSink()

        def worker():
            try:
                pump(resp, sink)
            except Exception as e:
                self._log("pump_error acct=%s err=%s", name, e)
            finally:
                sink.close()

        threading.Thread(target=worker, daemon=True).start()

        conv_key = conversation_key(req, raw)
        id_ = "chatcmpl-" + short_hash(conv_key)
        created = int(time.time())
        hb = self.cfg["upstream"]["heartbeat_seconds"]
        wrote = False
        finish_reason = "stop"
        last_hb = time.monotonic()

        while True:
            try:
                ev = sink.q.get(timeout=0.2)
            except queue.Empty:
                ev = None

            if time.monotonic() - last_hb >= hb:
                last_hb = time.monotonic()
                yield ": ping\n\n"

            if ev is None:
                continue
            if ev is sink.CLOSED:
                break

            kind = ev[0]
            if kind == "text":
                wrote = True
                yield _chunk_sse(id_, created, {"content": ev[1]}, None)
            elif kind == "reason":
                yield _chunk_sse(id_, created, {"reasoning_content": ev[1]}, None)
            elif kind == "conv":
                self._set_conv(conv_key, ev[1], ev[2])
            elif kind == "finish":
                if ev[1] != 1 and not sink.final_text and ev[2]:
                    wrote = True
                    yield _chunk_sse(id_, created, {"content": ev[2]}, None)

        if not wrote and not sink.final_text:
            yield _chunk_sse(id_, created, {"content": "(空响应)"}, None)
        yield _chunk_sse(id_, created, {}, finish_reason)
        yield "data: [DONE]\n\n"
        latency = int((time.time() - started) * 1000)
        self._log("ok acct=%s stream=true latency=%dms chars=%d", name, latency, len(sink.final_text))
        self.record_usage(key_id, req.get("model", ""), len(sink.final_text), 200, latency, conv_key, name, "")

    def chat_whole(self, req, raw, key_id):
        """非流式号池 chat，返回 (payload_dict, err)。"""
        started = time.time()
        try:
            resp, name, err = self.run_chat(req, raw, key_id)
        except ValueError as e:
            return None, str(e)
        if err:
            return None, err.get("message", "")
        return self._whole_body(resp, None, name, req, started, key_id)

    def _whole_body(self, resp, conv_key, name, req, started, key_id):
        sink = AccumSink()
        try:
            pump(resp, sink)
        except Exception as e:
            return None, "上游流读取失败: %s" % e
        if sink.conv[0]:
            self._set_conv(conv_key, sink.conv[0], sink.conv[1])
        text = sink.final_text
        if not text:
            return None, "上游没有返回内容"
        payload = {
            "id": "chatcmpl-" + short_hash(conv_key),
            "object": "chat.completion",
            "created": int(time.time()),
            "model": req.get("model", ""),
            "choices": [{"index": 0,
                         "message": {"role": "assistant", "content": text},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        }
        latency = int((time.time() - started) * 1000)
        self._log("ok acct=%s stream=false latency=%dms chars=%d", name, latency, len(text))
        self.record_usage(key_id, req.get("model", ""), len(text), 200, latency, conv_key, name, "")
        return payload, None

    def run_chat(self, req, raw, key_id):
        """返回 (resp, acct_name, err)。"""
        model_key = resolve_model(self.cfg, req.get("model"))
        effort = (req.get("reasoning_effort") or "").strip()
        wm = self.cfg["work_mode"]
        mem_profile, win_size = wm["memory_profile"], wm["total_window_size"]
        mp = self._model_param_for(req.get("model", ""), model_key)
        if mp:
            if mp.get("memory_profile"):
                mem_profile = mp["memory_profile"]
            if mp.get("total_window_size"):
                win_size = mp["total_window_size"]
        conv_key = conversation_key(req, raw)
        st = self._get_conv(conv_key)

        body = build_body(req, model_key, {
            "bot_id": wm["bot_id"], "mode_id": wm["mode_id"],
            "reasoning_effort": effort or wm["reasoning_effort"],
            "memory_profile": mem_profile, "total_window": win_size,
            "enable_sandbox": wm["enable_sandbox"], "workspace": wm["workspace"],
            "device_name": "USER", "local_app_id": "582478",
            "local_device_id": "", "work_mode": wm["enabled"],
            "conversation_id": st.get("conversation_id", ""),
            "flatten": self.cfg["upstream"]["flatten_history"],
        })
        body_bytes = body.encode("utf-8")
        large = (self.cfg["pool"]["large_request_bytes"] > 0
                 and len(body_bytes) >= self.cfg["pool"]["large_request_bytes"])

        resp, name, err = self._pick_and_forward(req, body_bytes, conv_key, large)
        if err:
            return None, name, err
        return resp, name, None

    # ---------- 管理数据 ----------
    def admin_models(self):
        out = []
        for name in model_list(self.cfg):
            p = self.cfg.get("model_params", {}).get(name) or {}
            win = p.get("total_window_size") or self.cfg["work_mode"]["total_window_size"]
            item = {
                "id": name,
                "model_item_key": resolve_model(self.cfg, name),
                "item_id": p.get("item_id", ""),
                "memory_profile": p.get("memory_profile", ""),
                "context_length": atoi_safe(win),
                "ratio": p.get("ratio", ""),
                "display": p.get("display") or name,
                "enabled": True,
                "is_default": name == self.cfg["default_model"],
            }
            out.append(item)
        return {"models": out, "total": len(out), "default_model": self.cfg["default_model"]}

    def admin_accounts(self):
        stats = {s["name"]: s for s in self.pool.stats()}
        out = []
        for name, s in stats.items():
            item = {"name": s["name"], "cooling": s["cooling"], "disabled": s["disabled"],
                    "fails": s["fails"], "reason": s["reason"]}
            a = self.accs.get(name)
            if a:
                item["device_id"] = a.get("device_id", "")
                item["note"] = a.get("note", "")
            out.append(item)
        return {"accounts": out, "healthy": self.pool.healthy(), "total": self.pool.length()}

    def quota(self, want_name=""):
        name, acct = "", None
        with self.lock:
            if want_name:
                acct = self.accs.get(want_name)
                if acct:
                    name = want_name
            else:
                for n, a in self.accs.items():
                    name, acct = n, a
                    break
        if not acct:
            return {"account": "", "error": "没有可用账号"}
        dev = acct.get("device_id") or short_hash(acct["cookie"])
        u = urllib.parse.urlsplit("https://www.doubao.com/alice/commerce/sale/subscription/quota/summary/")
        q = dict(urllib.parse.parse_qsl(u.query))
        for k, v in self.cfg["upstream"]["query"].items():
            q[k] = v
        q["device_id"] = dev
        url = urllib.parse.urlunsplit((u.scheme, u.netloc, u.path, urllib.parse.urlencode(q), u.fragment))
        headers = {"Content-Type": "application/json", "Accept": "application/json",
                   "Referer": "https://www.doubao.com/",
                   "User-Agent": self.cfg["upstream"]["user_agent"], "Cookie": acct["cookie"]}
        req = urllib.request.Request(url, data=b"{}", headers=headers, method="POST")
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            raw = resp.read(1 << 20).decode("utf-8", "ignore")
        except urllib.error.HTTPError as e:
            raw = e.read(1 << 20).decode("utf-8", "ignore")
            return {"account": name, "error": "上游 %d: %s" % (e.code, truncate(raw, 120))}
        except Exception as e:
            return {"account": name, "error": str(e)}

        out = {"account": name, "total": 0.0, "used": 0.0, "remaining": 0.0,
               "percent": 0.0, "groups": []}
        try:
            payload = json.loads(raw)
            data = payload.get("data")
            if isinstance(data, dict):
                _collect_quota(data, out)
        except ValueError:
            out["error"] = "解析失败"
            return out
        out["remaining"] = out["total"] - out["used"]
        if out["remaining"] < 0:
            out["remaining"] = 0.0
        if out["total"] > 0:
            out["percent"] = round(out["used"] / out["total"] * 100, 1)
        return out

    # ---------- 签到 ----------
    def _do_checkin(self, name, cookie, dev):
        res = {"name": name, "status": "error", "message": "", "ts": int(time.time())}
        if not cookie:
            res["message"] = "无 Cookie"
            return res
        if not dev:
            dev = short_hash(cookie)
        u = urllib.parse.urlsplit("https://www.doubao.com/alice/ug/campaign/landing/checkin")
        q = dict(urllib.parse.parse_qsl(u.query))
        for k, v in self.cfg["upstream"]["query"].items():
            q[k] = v
        q["device_id"] = dev
        url = urllib.parse.urlunsplit((u.scheme, u.netloc, u.path, urllib.parse.urlencode(q), u.fragment))
        scene = self.cfg["checkin"].get("scene") or "pc_incentive_task_mvp_v3"
        body = json.dumps({"scene": scene}).encode("utf-8")
        headers = {"Content-Type": "application/json", "Accept": "application/json",
                   "Referer": "https://www.doubao.com/",
                   "User-Agent": self.cfg["upstream"]["user_agent"], "Cookie": cookie}
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            raw = resp.read(1 << 20).decode("utf-8", "ignore")
            status = resp.status
        except urllib.error.HTTPError as e:
            raw = e.read(1 << 20).decode("utf-8", "ignore")
            status = e.code
        except Exception as e:
            res["message"] = str(e)
            return res

        try:
            payload = json.loads(raw)
        except ValueError:
            payload = {}
        code = payload.get("code", 0)
        msg = payload.get("msg", "")
        message = payload.get("message", "")
        data = payload.get("data") or {}

        if status == 200 and code == 0 and data.get("idempotent"):
            res["status"] = "already"
            res["message"] = "今日已签到（幂等命中）"
            res["reward"] = format_reward(data.get("reward_amount"))
            res["days"] = data.get("continuous_days", 0)
        elif status == 200 and code == 0 and data.get("checked_in"):
            res["status"] = "ok"
            res["message"] = "签到成功"
            res["reward"] = format_reward(data.get("reward_amount"))
            res["days"] = data.get("continuous_days", 0)
        elif "already" in message or "已签到" in msg:
            res["status"] = "already"
            res["message"] = "今日已签到"
        elif code == 710010702:
            res["status"] = "no_campaign"
            res["message"] = "当前无可用签到活动"
        else:
            res["status"] = "error"
            res["message"] = truncate((message + msg).strip(), 120) or truncate(raw, 120)
        return res

    def _run_checkin_all(self, only=""):
        with self.lock:
            targets = [(n, a) for n, a in self.accs.items() if not only or n == only]
        out = []
        for n, a in targets:
            r = self._do_checkin(n, a.get("cookie"), a.get("device_id"))
            with self.checkin_lock:
                self.last_checkin[n] = r
            self._log("checkin acct=%s status=%s msg=%s", r["name"], r["status"], r["message"])
            out.append(r)
        return out

    def _checkin_loop(self):
        hour = self.cfg["checkin"].get("hour") or 9
        if hour < 0 or hour > 23:
            hour = 9
        while True:
            now = time.localtime()
            nxt = time.mktime((now.tm_year, now.tm_mon, now.tm_mday, hour, 0, 0, 0, 0, -1))
            if nxt <= time.time():
                nxt += 86400
            time.sleep(max(1, nxt - time.time()))
            res = self._run_checkin_all()
            self._log("checkin_daily done accounts=%d", len(res))

    # ---------- 扫码 ----------
    def qr_start(self):
        cfg_qr = self.cfg["qr"]
        if not cfg_qr.get("enabled") or not cfg_qr.get("get_url"):
            return {"error": {"code": "qr_disabled", "type": "not_implemented",
                              "message": "扫码登录未启用"}}, 501
        u = urllib.parse.urlsplit(cfg_qr["get_url"])
        q = dict(urllib.parse.parse_qsl(u.query))
        for k, v in cfg_qr["query"].items():
            q[k] = v
        url = urllib.parse.urlunsplit((u.scheme, u.netloc, u.path, urllib.parse.urlencode(q), u.fragment))
        headers = {"User-Agent": self.cfg["upstream"]["user_agent"], "Referer": "https://www.doubao.com/"}
        req = urllib.request.Request(url, headers=headers)
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            raw = resp.read(1 << 20).decode("utf-8", "ignore")
        except Exception as e:
            return {"error": {"code": "qr_upstream", "type": "api_error",
                              "message": "请求二维码失败: %s" % e}}, 502

        try:
            payload = json.loads(raw)
        except ValueError:
            payload = {}
        data = payload.get("data") or {}
        if data.get("error_code") or (not data.get("token") and not data.get("qrcode")):
            msg = data.get("description") or payload.get("message") or truncate(raw, 200)
            return {"error": {"code": "qr_rejected", "type": "api_error",
                              "message": "上游拒绝生成二维码: %s" % msg}}, 502

        token = data.get("token") or data.get("qrcode")
        image = data.get("base64_image") or data.get("qrcode")
        sess = {"id": random_hex(8), "token": token, "status": "waiting",
                "image": image, "qrcode_url": data.get("qrcode_index_url", ""),
                "created": time.time(), "message": "", "account": ""}
        with self.qr_lock:
            self.qr_sess[sess["id"]] = sess
            for sid in [x for x, v in self.qr_sess.items() if time.time() - v["created"] > 600]:
                self.qr_sess.pop(sid, None)
        return {"id": sess["id"], "status": sess["status"], "image": sess["image"],
                "qrcode_url": sess["qrcode_url"]}, 200

    def qr_poll(self, qid):
        with self.qr_lock:
            sess = self.qr_sess.get(qid)
        if not sess:
            return {"error": {"code": "qr_not_found", "type": "invalid_request_error",
                              "message": "会话不存在或已过期"}}, 404
        cfg_qr = self.cfg["qr"]
        u = urllib.parse.urlsplit(cfg_qr["check_url"])
        q = dict(urllib.parse.parse_qsl(u.query))
        for k, v in cfg_qr["query"].items():
            q[k] = v
        q["token"] = sess["token"]
        url = urllib.parse.urlunsplit((u.scheme, u.netloc, u.path, urllib.parse.urlencode(q), u.fragment))
        headers = {"User-Agent": self.cfg["upstream"]["user_agent"], "Referer": "https://www.doubao.com/"}
        req = urllib.request.Request(url, headers=headers)
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            raw = resp.read(1 << 20).decode("utf-8", "ignore")
            cookies = _collect_cookies(resp)
        except Exception as e:
            return {"id": qid, "status": sess["status"], "message": "轮询失败: %s" % e}, 200

        try:
            payload = json.loads(raw)
        except ValueError:
            payload = {}
        data = payload.get("data") or {}
        status = _map_qr_status(data.get("status", ""))
        if not status:
            status = sess["status"]
        if data.get("redirect_url"):
            sess["message"] = data["redirect_url"]

        if status == "confirmed" and cookies:
            name = "qr-" + random_hex(4)
            self._upsert_account_file(name, cookies, "", "", "扫码新增", True)
            sess["account"] = name
            self._log("qr_confirmed id=%s account=%s", qid, name)

        with self.qr_lock:
            sess["status"] = status
        return {"id": qid, "status": status, "message": sess["message"],
                "account": sess["account"]}, 200

    # ---------- 控制台账号 CRUD（accounts.json） ----------
    def _upsert_account_file(self, name, cookie, device_id, web_id, note, enabled):
        accounts = self._load_accounts_file()
        found = False
        for a in accounts:
            if a.get("name") == name:
                a.update({"cookie": cookie, "device_id": device_id or "", "web_id": web_id or "",
                          "note": note or "", "enabled": enabled})
                a.setdefault("local_app_id", "582478")
                found = True
                break
        if not found:
            accounts.append({"name": name, "cookie": cookie, "device_id": device_id or "",
                             "web_id": web_id or "", "note": note or "",
                             "local_app_id": "582478", "enabled": enabled})
        self._save_accounts_file(accounts)
        return self._resync_accounts()

    def _delete_account_file(self, name):
        accounts = [a for a in self._load_accounts_file() if a.get("name") != name]
        self._save_accounts_file(accounts)
        return self._resync_accounts()

    def _toggle_account_file(self, name):
        accounts = self._load_accounts_file()
        new_val = None
        for a in accounts:
            if a.get("name") == name:
                a["enabled"] = not a.get("enabled", True)
                new_val = a["enabled"]
        self._save_accounts_file(accounts)
        return self._resync_accounts(), new_val

