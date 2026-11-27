# -*- coding: utf-8 -*-
"""号池：冷却、熔断、会话粘性与大请求预算（等价 internal/pool/pool.go）。"""
import secrets
import threading
import time


class _Handle:
    def __init__(self, name):
        self.name = name
        self.cool_until = 0.0
        self.last_used = 0.0
        self.fails = 0
        self.disabled = False
        self.reason = ""
        self.in_flight = 0
        self.large_use = []

    def name_str(self):
        return self.name


class Pool:
    def __init__(self, opts):
        self.lock = threading.Lock()
        self.opt = opts
        self.items = []
        self.by_name = {}
        self.sticky = {}  # conv -> (name, at)

    def add(self, name):
        with self.lock:
            if name in self.by_name:
                return
            e = _Handle(name)
            self.items.append(e)
            self.by_name[name] = e

    def remove(self, name):
        with self.lock:
            self.by_name.pop(name, None)
            self.items = [e for e in self.items if e.name != name]
            for c in [c for c, v in self.sticky.items() if v[0] == name]:
                self.sticky.pop(c, None)

    def length(self):
        with self.lock:
            return len(self.items)

    def _budget_full(self, e, now):
        if self.opt["large_budget"] <= 0 or self.opt["large_window"] <= 0:
            return False
        e.large_use = [t for t in e.large_use if now - t < self.opt["large_window"]]
        return len(e.large_use) >= self.opt["large_budget"]

    def pick(self, tried, large):
        with self.lock:
            now = time.time()
            cands = []
            for e in self.items:
                if e.disabled or (tried and e.name in tried):
                    continue
                if now < e.cool_until:
                    continue
                if large and self._budget_full(e, now):
                    continue
                cands.append((now - e.last_used, e))
            if not cands:
                return None
            cands.sort(key=lambda x: x[0], reverse=True)
            cands = cands[:5]
            e = cands[0][1] if self.opt["deterministic"] else secrets.choice(cands)[1]
            e.last_used = now
            return e

    def note_large(self, name):
        with self.lock:
            e = self.by_name.get(name)
            if e:
                e.large_use.append(time.time())

    def note_success(self, name):
        with self.lock:
            e = self.by_name.get(name)
            if e:
                e.fails = 0

    def note_rate_limit(self, name, retry_after):
        with self.lock:
            e = self.by_name.get(name)
            if not e:
                return
            e.fails += 1
            d = self.opt["rate_limit_cooldown"]
            if retry_after > d:
                d = retry_after
            e.cool_until = time.time() + d

    def note_overload(self, name):
        with self.lock:
            e = self.by_name.get(name)
            if not e:
                return
            e.fails += 1
            e.cool_until = time.time() + self.opt["overload_cooldown"]

    def note_auth_failure(self, name, reason):
        with self.lock:
            e = self.by_name.get(name)
            if e:
                e.disabled = True
                e.reason = reason

    def resolve_sticky(self, conv):
        if not conv:
            return None
        with self.lock:
            v = self.sticky.get(conv)
            if not v or time.time() - v[1] > self.opt["sticky_ttl"]:
                self.sticky.pop(conv, None)
                return None
            e = self.by_name.get(v[0])
            if e and e.disabled:
                self.sticky.pop(conv, None)
                return None
            return v[0]

    def bind_sticky(self, conv, name):
        if not conv or not name:
            return
        with self.lock:
            self.sticky[conv] = (name, time.time())

    def unbind_sticky(self, conv):
        with self.lock:
            self.sticky.pop(conv, None)

    def pick_by_sticky(self, name, large):
        with self.lock:
            e = self.by_name.get(name)
            if not e or e.disabled:
                return None
            now = time.time()
            if now < e.cool_until:
                return None
            if large and self._budget_full(e, now):
                return None
            e.last_used = now
            return e

    def stats(self):
        with self.lock:
            now = time.time()
            out = []
            for e in self.items:
                s = {"name": e.name, "cooling": now < e.cool_until,
                     "disabled": e.disabled, "reason": e.reason,
                     "fails": e.fails, "in_flight": e.in_flight}
                if s["cooling"]:
                    s["cool_left_ms"] = int((e.cool_until - now) * 1000)
                out.append(s)
            return out

    def healthy(self):
        with self.lock:
            now = time.time()
            return sum(1 for e in self.items if not e.disabled and now >= e.cool_until)
