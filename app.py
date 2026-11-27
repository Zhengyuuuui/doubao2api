# -*- coding: utf-8 -*-
"""整合层：laoin 多模态后端 + 我们的号池/密钥/签到/控制台。

- laoin 的 create_app 提供多模态/生成/文件/工具/千问/浏览器QR 端点（/v1/*、/admin、/auth）
- 我们追加号池 chat（/v1/pool/*）与号池管理/控制台端点（/api/*、/、/healthz）
"""
import asyncio
import hmac
import ipaddress
import json
import os
import time

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

from laoin.unified_server import create_app
from config import load_config, model_list
from server import Server
from util import logf, mask_key, random_hex


def build_app():
    """构造 FastAPI app 并返回 (app, srv)。"""
    laoin_api_key = os.environ.get("DOUBAO_API_KEY", "") or None
    laoin_rpm = float(os.environ.get("DOUBAO_RPM_LIMIT", "20"))

    # laoin 多模态后端（自带 lifespan 启动浏览器）
    app = create_app(api_key=laoin_api_key, rpm_limit=laoin_rpm)

    cfg = load_config(os.environ.get("DOUBAO2API_CONFIG", "config.json"))
    srv = Server(cfg)

    # ---------- 认证辅助 ----------
    def _is_loopback(request: Request) -> bool:
        ip = request.client.host if request.client else ""
        try:
            return ipaddress.ip_address(ip).is_loopback
        except ValueError:
            return ip in ("127.0.0.1", "::1")

    def _bearer(request: Request) -> str:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            return auth[7:].strip()
        return request.headers.get("x-api-key", "").strip()

    def _pool_auth(request: Request) -> str:
        tok = _bearer(request)
        k = srv._lookup_key(tok)
        if k:
            return k["id"]
        with srv.keys_lock:
            empty = not srv.keys
        detail = "尚未创建任何密钥，请在控制台「密钥」页生成" if empty else "invalid api key"
        raise HTTPException(status_code=401, detail=detail)

    def _admin_auth(request: Request) -> None:
        if _is_loopback(request):
            return
        tok = _bearer(request)
        admin_token = cfg.get("admin_token") or ""
        if admin_token and hmac.compare_digest(tok, admin_token):
            return
        raise HTTPException(status_code=403, detail="管理接口仅限本机访问")

    # ---------- 控制台 / 健康 ----------
    @app.get("/")
    def index():
        return HTMLResponse(srv.console_html)

    @app.get("/healthz")
    def healthz():
        return {"ok": True, "accounts": srv.pool.length(),
                "healthy": srv.pool.healthy(),
                "uptime": int(time.time() - srv.up_at)}

    # ---------- 号池 chat（/v1/pool/*）----------
    @app.post("/v1/pool/chat/completions")
    async def pool_chat(request: Request):
        key_id = _pool_auth(request)
        body = await request.json()
        raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
        if body.get("stream"):
            return StreamingResponse(srv.chat_stream_gen(body, raw, key_id),
                                     media_type="text/event-stream")
        payload, err = srv.chat_whole(body, raw, key_id)
        if err:
            raise HTTPException(status_code=502, detail=err)
        return JSONResponse(payload)

    @app.get("/v1/pool/models")
    async def pool_models(request: Request):
        _pool_auth(request)
        data = []
        for m in model_list(cfg):
            win = cfg["work_mode"]["total_window_size"]
            mp = cfg.get("model_params", {}).get(m)
            if mp and mp.get("total_window_size"):
                win = mp["total_window_size"]
            item = {"id": m, "object": "model", "created": int(srv.up_at), "owned_by": "doubao"}
            n = int(win) if str(win).isdigit() else 0
            if n > 0:
                item["context_length"] = n
                item["max_tokens"] = n
            data.append(item)
        return {"object": "list", "data": data}

    # ---------- 管理接口（GET）----------
    @app.get("/api/status")
    def api_status(request: Request):
        _admin_auth(request)
        return srv._status_payload()

    @app.get("/api/pool")
    def api_pool(request: Request):
        _admin_auth(request)
        return srv.admin_accounts()

    @app.get("/api/models/admin")
    def api_models_admin(request: Request):
        _admin_auth(request)
        return srv.admin_models()

    @app.get("/api/quota")
    def api_quota(request: Request, name: str = ""):
        _admin_auth(request)
        return srv.quota(name)

    @app.get("/api/keys")
    def api_keys(request: Request):
        _admin_auth(request)
        with srv.keys_lock:
            out = [{"id": k["id"], "name": k["name"], "key_masked": mask_key(k.get("key", "")),
                    "key": k.get("key", ""), "created": k.get("created", 0),
                    "enabled": k.get("enabled", True)} for k in srv.keys]
        return {"keys": out, "total": len(out)}

    @app.get("/api/usage")
    def api_usage(request: Request, days: int = 7):
        _admin_auth(request)
        return {"days": days, "usage": srv._aggregate_usage(days)}

    @app.get("/api/checkin")
    def api_checkin(request: Request):
        _admin_auth(request)
        with srv.checkin_lock:
            results = list(srv.last_checkin.values())
        return {"results": results,
                "enabled": cfg["checkin"].get("enabled", True),
                "hour": cfg["checkin"].get("hour", 9),
                "scene": cfg["checkin"].get("scene", ""),
                "server_time": time.strftime("%Y-%m-%d %H:%M:%S")}

    @app.get("/api/overview")
    def api_overview(request: Request):
        _admin_auth(request)
        st = srv._status_payload()
        quota = srv.quota()
        usage7 = srv._aggregate_usage(7)
        today = srv._today_stats()
        return {
            "gateway": st,
            "accounts_enabled": st["total"],
            "quota": quota,
            "today_chars": today["chars"],
            "today_tokens_est": sum(u["today_tokens"] for u in usage7),
            "usage_today_requests": sum(u["today_requests"] for u in usage7),
            "usage_tokens_7d": sum(u["tokens"] for u in usage7),
            "requests_today": today["requests"],
            "avg_latency_ms": today["avg_latency_ms"],
        }

    @app.get("/api/accounts")
    def api_accounts(request: Request):
        _admin_auth(request)
        accounts = srv._load_accounts_file()
        items = [{"name": a.get("name", ""), "device_id": a.get("device_id", ""),
                  "web_id": a.get("web_id", ""), "note": a.get("note", ""),
                  "enabled": a.get("enabled", True)} for a in accounts]
        return {"accounts": items}

    @app.get("/api/logs")
    def api_logs(request: Request, limit: int = 50):
        _admin_auth(request)
        return {"logs": srv._read_logs(min(max(limit, 1), 500))}

    @app.get("/api/settings")
    def api_settings(request: Request):
        _admin_auth(request)
        return {
            "listen": cfg["listen"],
            "gw_base": cfg["listen"],
            "gw_key_set": bool(srv.keys),
            "accounts_file": srv.accounts_path(),
            "keys_file": srv.keys_path(),
            "usage_file": srv.usage_path(),
            "default_model": cfg["default_model"],
            "checkin_enabled": cfg["checkin"].get("enabled", True),
            "work_mode_enabled": cfg["work_mode"].get("enabled", True),
            "multimodal": True,
        }

    @app.get("/api/models")
    def api_models(request: Request):
        _admin_auth(request)
        items = [{"id": m} for m in model_list(cfg)]
        return {"models": items, "items": items}

    # ---------- 管理接口（POST）----------
    @app.post("/api/accounts")
    async def api_upsert_account(request: Request):
        _admin_auth(request)
        b = await request.json()
        name = (b.get("name") or "").strip()
        cookie = (b.get("cookie") or "").strip()
        if not name or not cookie:
            raise HTTPException(status_code=400, detail="name/cookie 必填")
        reload_res = srv._upsert_account_file(name, cookie, b.get("device_id", ""),
                                              b.get("web_id", ""), b.get("note", ""), True)
        return {"ok": True, "reload": reload_res}

    @app.post("/api/accounts/{name}/toggle")
    def api_toggle_account(name: str, request: Request):
        _admin_auth(request)
        reload_res, val = srv._toggle_account_file(name)
        return {"ok": True, "enabled": val, "reload": reload_res}

    @app.post("/api/keys")
    async def api_create_key(request: Request):
        _admin_auth(request)
        b = await request.json()
        name = (b.get("name") or "").strip() or ("key-" + random_hex(3))
        k = {"id": "k" + random_hex(4), "name": name, "key": "sk-" + random_hex(16),
             "created": int(time.time()), "enabled": True}
        with srv.keys_lock:
            srv.keys.append(k)
        srv._save_keys()
        return {"key": k}

    @app.post("/api/checkin")
    def api_run_checkin(request: Request, name: str = ""):
        _admin_auth(request)
        res = srv._run_checkin_all(name)
        return {"results": res, "total": len(res)}

    @app.post("/api/models/test")
    async def api_model_test(request: Request):
        _admin_auth(request)
        b = await request.json()
        started = time.time()
        model = b.get("model", "")
        req = {"model": model or cfg["default_model"],
               "metadata": {"conversation_id": "model-test-" + (model or "x")},
               "messages": [{"role": "user", "content": "只回一个字：好"}]}
        raw = json.dumps(req, ensure_ascii=False).encode("utf-8")
        payload, err = await asyncio.to_thread(srv.chat_whole, req, raw, "")
        if err:
            return {"ok": False, "error": err[:200],
                    "latency_ms": int((time.time() - started) * 1000)}
        text = ((payload.get("choices") or [{}])[0].get("message") or {}).get("content", "")
        bad = any(k in (text or "") for k in ("出了点问题", "稍后重试", "系统繁忙"))
        return {"ok": not bad, "reply": text,
                "latency_ms": int((time.time() - started) * 1000),
                "error": "上游返回错误话术" if bad else ""}

    @app.post("/api/test")
    async def api_test(request: Request):
        """控制台测试台：号池流式 chat（SSE 透传）。"""
        _admin_auth(request)
        b = await request.json()
        req = {"model": b.get("model") or cfg["default_model"],
               "stream": True,
               "metadata": {"conversation_id": b.get("conversation_id") or "panel-test"},
               "messages": [{"role": "user", "content": b.get("message") or "你好"}]}
        if b.get("reasoning_effort"):
            req["reasoning_effort"] = b["reasoning_effort"]
        raw = json.dumps(req, ensure_ascii=False).encode("utf-8")
        return StreamingResponse(srv.chat_stream_gen(req, raw, ""),
                                 media_type="text/event-stream")

    # ---------- 号池扫码（我们自己的 QR 逻辑）----------
    @app.post("/api/qr")
    async def api_qr_start(request: Request):
        _admin_auth(request)
        data, code = srv.qr_start()
        return JSONResponse(data, status_code=code)

    @app.get("/api/qr/{qid}")
    def api_qr_poll(qid: str, request: Request):
        _admin_auth(request)
        data, code = srv.qr_poll(qid)
        return JSONResponse(data, status_code=code)

    # ---------- 管理接口（DELETE）----------
    @app.delete("/api/accounts/{name}")
    def api_delete_account(name: str, request: Request):
        _admin_auth(request)
        srv._delete_account_file(name)
        return {"ok": True, "reload": {"total": srv.pool.length()}}

    @app.delete("/api/keys/{kid}")
    def api_delete_key(kid: str, request: Request):
        _admin_auth(request)
        with srv.keys_lock:
            srv.keys = [k for k in srv.keys if k["id"] != kid]
        srv._save_keys()
        return {"ok": True, "id": kid}

    return app, srv
