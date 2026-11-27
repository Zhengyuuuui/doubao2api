# -*- coding: utf-8 -*-
"""启动入口：doubao2api bridge（号池网关 + laoin 多模态后端），单进程、单端口。"""
import argparse
import os
import sys

import uvicorn


def parse_listen(s):
    host, _, port = s.rpartition(":")
    if not port:
        host, port = "", host
    return host or "127.0.0.1", int(port)


def main():
    ap = argparse.ArgumentParser(description="doubao2api bridge（号池 + 多模态）")
    ap.add_argument("-config", default="config.json", help="号池配置文件路径")
    ap.add_argument("--host", default=None, help="监听地址（覆盖 config.listen）")
    ap.add_argument("--port", type=int, default=None, help="监听端口（覆盖 config.listen）")
    args = ap.parse_args()

    if args.config:
        os.environ["DOUBAO2API_CONFIG"] = args.config

    # 端口解析：优先命令行，其次 config.json 的 listen
    from config import load_config
    try:
        cfg = load_config(args.config)
        listen = cfg.get("listen", "127.0.0.1:8800")
    except (OSError, ValueError) as e:
        print("加载配置失败: %s" % e, file=sys.stderr)
        listen = "127.0.0.1:8800"

    host, port = parse_listen(listen)
    if args.host:
        host = args.host
    if args.port:
        port = args.port

    from app import build_app
    app, srv = build_app()

    from util import logf
    logf("doubao2api bridge 监听 %s:%d（号池账号 %d，多模态由 laoin 浏览器后端提供）",
         host, port, len(srv.accs))

    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
