#!/usr/bin/env python3
"""会话 seq 高水位修复 —— ① SequenceAllocator floor 自愈的运维等价物。

背景
    seq 分配以 Redis 计数器为高水位。Redis 被 flush / 实例重启 / key 丢失后，缺失 key 的
    INCR 从 1 重新计数，而持久化历史(Postgres)仍保留旧的更高 seq —— 新消息拿到低于历史的
    seq，客户端按 seq 升序渲染时被插到历史中间(见 flare-im-seq 的 floor 自愈)。

    代码侧修复(①)在 ingest 首次触达会话时以存储权威 max_seq 作 floor。本脚本用于**运行中、
    尚未部署 ① 的后端**：把每个会话的 Redis seq key 抬到 Postgres 的 MAX(seq)，只升不降，
    绝不比现状更差；下一条消息即从正确高水位继续。

用法
    # 扫描并报告偏低的 seq key(不修改)
    python3 repair_seq_highwater.py --report
    # 修复某会话
    python3 repair_seq_highwater.py --conversation 1AJH6C586VGF7KD0TX
    # 修复全部偏低会话
    python3 repair_seq_highwater.py --all --apply

环境(默认对齐 config/base.toml 的本地 dev)
    PG_DSN   默认 host=localhost port=25432 dbname=flare2 user=flare password=flare123
    REDIS_HOST/REDIS_PORT/REDIS_SEQ_DB  默认 localhost / 26379 / 4
    TENANT   默认 "0"
仅依赖标准库(内置极简 RESP 客户端 + psql 子进程)，无需 pip。
"""
import argparse
import os
import socket
import subprocess
import sys


def psql(sql: str) -> str:
    env = dict(os.environ, PGPASSWORD=os.environ.get("PG_PASSWORD", "flare123"))
    out = subprocess.run(
        [
            "psql",
            "-h", os.environ.get("PG_HOST", "localhost"),
            "-p", os.environ.get("PG_PORT", "25432"),
            "-U", os.environ.get("PG_USER", "flare"),
            "-d", os.environ.get("PG_DB", "flare2"),
            "-tA", "-F", "|", "-c", sql,
        ],
        env=env, capture_output=True, text=True,
    )
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip())
    return out.stdout.strip()


class Redis:
    def __init__(self, host, port, db):
        self.s = socket.create_connection((host, port))
        self._cmd("SELECT", str(db))

    def _read(self):
        buf = b""
        while not buf.endswith(b"\r\n"):
            buf += self.s.recv(1)
        t, line = buf[:1], buf[1:-2]
        if t in (b"+", b":"):
            return line.decode()
        if t == b"-":
            raise RuntimeError(line.decode())
        if t == b"$":
            n = int(line)
            if n == -1:
                return None
            data = b""
            while len(data) < n + 2:
                data += self.s.recv(n + 2 - len(data))
            return data[:-2].decode()
        return line.decode()

    def _cmd(self, *args):
        c = b"*%d\r\n" % len(args)
        for a in args:
            a = a.encode() if isinstance(a, str) else a
            c += b"$%d\r\n%s\r\n" % (len(a), a)
        self.s.sendall(c)
        return self._read()

    def get_int(self, key):
        v = self._cmd("GET", key)
        return int(v) if v is not None else 0

    def set(self, key, value):
        return self._cmd("SET", key, str(value))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conversation", help="仅修复该会话")
    ap.add_argument("--all", action="store_true", help="扫描全部会话")
    ap.add_argument("--apply", action="store_true", help="实际写入(默认仅报告)")
    ap.add_argument("--report", action="store_true", help="等价于不带 --apply")
    args = ap.parse_args()

    tenant = os.environ.get("TENANT", "0")
    redis = Redis(
        os.environ.get("REDIS_HOST", "localhost"),
        int(os.environ.get("REDIS_PORT", "26379")),
        int(os.environ.get("REDIS_SEQ_DB", "4")),
    )

    # 消息与事件共用同一会话 seq 计数器：高水位必须取两表 GREATEST，
    # 只看 messages 会在事件占据高位时把 key 抬得不够高，照样撞号。
    if args.conversation:
        conv = args.conversation.replace("'", "''")  # psql -c 无参数绑定，转义防呆
        rows = [(args.conversation, int(psql(
            "SELECT GREATEST("
            f" COALESCE((SELECT MAX(seq) FROM messages WHERE conversation_id='{conv}'),0),"
            f" COALESCE((SELECT MAX(seq) FROM events WHERE conversation_id='{conv}'),0))"
        ) or 0))]
    else:
        rows = [
            (cid, int(mx))
            for line in psql(
                "SELECT conversation_id, MAX(seq) FROM ("
                " SELECT conversation_id, seq FROM messages"
                " UNION ALL SELECT conversation_id, seq FROM events"
                ") t GROUP BY conversation_id"
            ).splitlines()
            if line
            for cid, mx in [line.split("|")]
        ]

    fixed = scanned = below = 0
    for conv, pg_max in rows:
        scanned += 1
        key = f"seq:{tenant}:{conv}"
        cur = redis.get_int(key)
        if cur < pg_max:
            below += 1
            print(f"[low] {key}: redis={cur} < pg_max={pg_max}")
            if args.apply:
                redis.set(key, pg_max)
                fixed += 1
    print(
        f"\nscanned={scanned} below_highwater={below} fixed={fixed} "
        f"({'APPLIED' if args.apply else 'report-only'})"
    )
    if below and not args.apply:
        print("重新运行加 --apply 实际抬高这些 key。")


if __name__ == "__main__":
    sys.exit(main())
