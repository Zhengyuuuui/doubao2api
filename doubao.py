# -*- coding: utf-8 -*-
"""豆包协议：请求构造 + SSE 解析（等价 internal/doubao/doubao.go）。"""
import hashlib
import json
import queue
import time

from util import atoi_safe, home_dir, new_uuid, random_digits


def msg_text(m):
    content = m.get("content")
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for p in content:
            if isinstance(p, dict) and p.get("text"):
                parts.append(p["text"])
        return "".join(parts)
    return ""


def build_body(req, model_key, o):
    msgs = req.get("messages") or []
    if not msgs:
        raise ValueError("messages 不能为空")
    last_user = ""
    for m in reversed(msgs):
        if m.get("role") == "user":
            last_user = msg_text(m)
            break
    if not last_user:
        raise ValueError("缺少 user 消息")

    text = last_user
    if not o.get("conversation_id") and o.get("flatten"):
        last_idx = len(msgs) - 1
        parts = []
        for i, m in enumerate(msgs):
            t = msg_text(m).strip()
            if not t:
                continue
            if i == last_idx:
                break
            role = m.get("role", "")
            if role == "user":
                prefix = "用户："
            elif role == "assistant":
                prefix = "助手："
            else:
                prefix = role + "："
            parts.append(prefix + t)
        if parts:
            text = "\n".join(parts) + "\n用户：" + last_user

    msg_id = new_uuid()
    local_conv = "local_" + random_digits(16)
    now_ms = int(time.time() * 1000)

    block = {
        "block_type": 10000,
        "content": {
            "text_block": {"text": text, "icon_url": "", "icon_url_dark": "", "summary": ""},
            "pc_event_block": "",
        },
        "block_id": new_uuid(),
        "parent_id": "",
        "meta_info": [],
        "append_fields": [],
    }

    body = {
        "client_meta": {
            "local_conversation_id": local_conv,
            "conversation_id": o.get("conversation_id", ""),
            "bot_id": o.get("bot_id", ""),
            "last_section_id": "",
            "last_message_index": None,
            "local_permissions": [
                {"permission_name": "ACCESS_COARSE_LOCATION", "status": 1},
                {"permission_name": "ACCESS_FINE_LOCATION", "status": 1},
                {"permission_name": "ACCESS_BACKGROUND_LOCATION", "status": 1},
            ],
        },
        "messages": [{
            "local_message_id": msg_id,
            "content_block": [block],
            "message_status": 0,
        }],
    }

    work = bool(o.get("work_mode"))
    mode = 2 if work else 0
    opt = {
        "create_time_ms": now_ms,
        "answer_with_suggest": False,
        "agent_mode": 1 if work else 0,
        "need_deep_think": 10001,
        "unique_key": new_uuid(),
        "start_seq": 0,
        "need_create_conversation": not o.get("conversation_id"),
        "conversation_init_option": {"need_ack_conversation": True},
        "sse_recv_event_options": {"support_chunk_delta": True},
        "support_lazy_fetch_stream": True,
        "is_old_user": True,
        "message_storage_type": 0,
        "conversation_mode": mode,
    }

    task_param = {
        "runtime_type": 2,
        "sandbox_auth_type": 1,
        "device_name": o.get("device_name", ""),
        "folder_name": "",
        "local_app_id": o.get("local_app_id", ""),
        "local_device_id": o.get("local_device_id", ""),
        "workspace": o.get("workspace", ""),
    }
    init_ext = {
        "model_item_key": model_key,
        "reasoning_effort": o.get("reasoning_effort", ""),
        "mode_id": o.get("mode_id", ""),
    }
    init_ext["agent_task_param"] = json.dumps(task_param, ensure_ascii=False)
    opt["conversation_init_ext"] = init_ext

    opt["model_config"] = {
        "model_item_key": model_key,
        "model_extra_params": {
            "memory_profile": o.get("memory_profile", ""),
            "total_window_size": o.get("total_window", ""),
        },
        "reasoning_effort": atoi_safe(o.get("reasoning_effort", "")),
    }
    opt["aggregate_params"] = {
        "mention_skill_list": "[]",
        "mention_plugin_list": "[]",
        "mention_ext": "[{}]",
        "conversation_mode": str(mode),
        "mode_id": o.get("mode_id", ""),
        "model_item_key": model_key,
        "agent_mode": "1" if work else "0",
        "reasoning_effort": o.get("reasoning_effort", ""),
        "provider_id": "",
    }

    if work:
        shared = [o["workspace"]] if o.get("workspace") else []
        client_opt = {
            "enable_sandbox": o.get("enable_sandbox", False),
            "os": "Windows",
            "shared_folder_path": shared,
            "agent_workspace": {
                "agent_workspace": o.get("workspace", ""),
                "local_skill_paths": [],
            },
            "client_env_id": new_uuid(),
            "sandbox_id": "sandbox-" + new_uuid(),
            "sandbox_auth_type": 1,
        }
        gtp = {
            "action": 0,
            "thread_local_message_id": [msg_id],
            "client_option": client_opt,
            "runtime_type": 2,
            "agent_task_param": task_param,
            "agent_task_param_change": {
                "runtime_changed": False, "device_changed": False,
                "sandbox_auth_type_changed": False,
            },
            "need_modify_conversation": False,
            "task_input_json": json.dumps({
                "agents_md": {"files": [], "state": 2},
                "schema_version": 1,
                "home_dir": home_dir(o.get("workspace", "")),
                "project_context": {},
                "localConnectors": [],
            }, ensure_ascii=False),
        }
        opt["general_task_param"] = gtp
        opt["ext"] = {
            "general_task_param": json.dumps(gtp, ensure_ascii=False),
            "use_deep_think": "10001",
            "agent_mode": "1",
            "is_finish": "1",
            "conversation_init_option": '{"need_ack_conversation":true}',
            "commerce_credit_config_enable": "0",
        }

    body["option"] = opt
    body["user_context"] = []
    if "ext" not in opt:
        body["ext"] = {"use_deep_think": "10001", "is_finish": "1"}
    return json.dumps(body, ensure_ascii=False)


class Sink:
    def on_text(self, t):
        pass

    def on_reasoning(self, t):
        pass

    def on_conversation(self, conv, section):
        pass

    def on_finish(self, end_type, brief):
        pass


def pump(lines, sink):
    """解析上游 SSE。lines 为字节行或文本行的可迭代对象。"""
    event = ""
    data = []
    saw_chunk_delta = [False]

    def dispatch():
        nonlocal event, data
        if not event and not data:
            return
        payload = "\n".join(data)
        ev, event, data = event, "", []
        if not payload:
            return
        try:
            d = json.loads(payload)
        except ValueError:
            d = None

        if ev == "CHUNK_DELTA":
            if d and d.get("text"):
                saw_chunk_delta[0] = True
                sink.on_text(d["text"])
        elif ev == "STREAM_CHUNK":
            if not d:
                return
            for op in (d.get("patch_op") or []):
                pv = op.get("patch_value") or {}
                for b in (pv.get("content_block") or []):
                    bt = b.get("block_type")
                    content = b.get("content") or {}
                    if bt == 10000:
                        tb = content.get("text_block") or {}
                        if not saw_chunk_delta[0] and tb.get("text"):
                            sink.on_text(tb["text"])
                    elif bt == 10040:
                        tb = content.get("thinking_block") or {}
                        t = tb.get("streaming_title") or tb.get("finish_title") or ""
                        if t:
                            sink.on_reasoning(t)
        elif ev == "SSE_ACK":
            if d:
                ack = d.get("ack_client_meta") or {}
                sink.on_conversation(ack.get("conversation_id", ""), ack.get("section_id", ""))
        elif ev == "SSE_REPLY_END":
            if d:
                mfa = d.get("msg_finish_attr") or {}
                sink.on_finish(d.get("end_type", 0), mfa.get("brief", ""))

    for raw in lines:
        if isinstance(raw, bytes):
            line = raw.decode("utf-8", "ignore")
        else:
            line = raw
        line = line.rstrip("\r\n")
        if line == "":
            dispatch()
            continue
        if line.startswith("event:"):
            event = line[len("event:"):].strip()
        elif line.startswith("data:"):
            data.append(line[len("data:"):].strip())
        # 以 ':' 开头的注释帧忽略
    dispatch()


class QueueSink(Sink):
    CLOSED = object()

    def __init__(self):
        self.q = queue.Queue(maxsize=1024)
        self.final = []
        self.reason = []

    def _put(self, item):
        try:
            self.q.put_nowait(item)
        except queue.Full:
            pass

    def on_text(self, t):
        self.final.append(t)
        self._put(("text", t))

    def on_reasoning(self, t):
        self.reason.append(t)
        self._put(("reason", t))

    def on_conversation(self, conv, section):
        self._put(("conv", conv, section))

    def on_finish(self, end_type, brief):
        self._put(("finish", end_type, brief))

    def close(self):
        self.q.put(self.CLOSED)

    @property
    def final_text(self):
        return "".join(self.final)


class AccumSink(Sink):
    def __init__(self):
        self.final = []
        self.reason = []
        self.conv = ("", "")

    def on_text(self, t):
        self.final.append(t)

    def on_reasoning(self, t):
        self.reason.append(t)

    def on_conversation(self, conv, section):
        self.conv = (conv, section)

    def on_finish(self, end_type, brief):
        pass

    @property
    def final_text(self):
        return "".join(self.final)


def conversation_key(req, raw):
    meta = req.get("metadata")
    if isinstance(meta, dict):
        for k in ("conversation_id", "conversationId", "session_id"):
            v = meta.get(k)
            if isinstance(v, str) and v:
                return "c:" + v
    if req.get("user"):
        return "u:" + req["user"]
    for m in req.get("messages") or []:
        if m.get("role") == "user":
            return "h:" + hashlib.sha256(msg_text(m).encode("utf-8")).hexdigest()[:24]
    return "r:" + hashlib.sha256(raw).hexdigest()[:24]
