"""机房装机 IPv4 单地址池服务.

运维在网页上设置一段连续 IPv4 地址(最多 32 个); 设备通过接口完成
申请(预留 30 秒 + 确认码) -> 确认(转为 5 分钟租约) -> 续租 -> 释放.

一致性设计:
- 所有写操作在 BEGIN IMMEDIATE 事务中完成, SQLite 串行化写者,
  并发申请/确认/改范围不会重复占址;
- 部分唯一索引兜底: 同一地址、同一设备最多各有一条有效分配;
- 截止时间以绝对时间戳落库, 重启或写入失败后仍按原截止时间判定,
  过期记录由写事务顺带清理(sweep), 不留幽灵占用.
"""
from __future__ import annotations

import os
import secrets
import sqlite3
import time
from contextlib import asynccontextmanager, contextmanager
from ipaddress import IPv4Address
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

RESERVATION_TTL = 30.0   # 确认码有效期: 30 秒
LEASE_TTL = 300.0        # 确认后的租约: 5 分钟
MAX_POOL_SIZE = 32       # 地址池上限

ACTIVE_STATES = ("reserved", "leased")

SCHEMA = """
CREATE TABLE IF NOT EXISTS pool_config (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    base_ip    TEXT NOT NULL,
    count      INTEGER NOT NULL CHECK (count BETWEEN 1 AND 32),
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS allocations (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    address        TEXT NOT NULL,
    device_id      TEXT NOT NULL,
    state          TEXT NOT NULL
                   CHECK (state IN ('reserved', 'leased', 'released', 'expired')),
    code           TEXT,
    reserved_until REAL,
    lease_until    REAL,
    created_at     REAL NOT NULL,
    updated_at     REAL NOT NULL
);

-- 兜底约束: 同一地址最多一条有效分配, 同一设备最多一条有效分配
CREATE UNIQUE INDEX IF NOT EXISTS idx_active_address
    ON allocations(address) WHERE state IN ('reserved', 'leased');
CREATE UNIQUE INDEX IF NOT EXISTS idx_active_device
    ON allocations(device_id) WHERE state IN ('reserved', 'leased');
"""

STATIC_FILE = Path(__file__).parent / "static" / "index.html"


# ---------------------------------------------------------------- 工具函数

def ip_to_int(ip: str) -> int:
    return int(IPv4Address(ip.strip()))


def int_to_ip(value: int) -> str:
    return str(IPv4Address(value))


def new_confirm_code() -> str:
    return secrets.token_hex(4)  # 8 位十六进制确认码


# ---------------------------------------------------------------- 请求模型

class PoolIn(BaseModel):
    base_ip: str = Field(min_length=7, max_length=15)
    count: int = Field(ge=1, le=MAX_POOL_SIZE)


class ApplyIn(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)


class ConfirmIn(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    address: str = Field(min_length=7, max_length=15)
    code: str = Field(min_length=1, max_length=64)


class DeviceAddressIn(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    address: str = Field(min_length=7, max_length=15)


# ---------------------------------------------------------------- 应用工厂

def create_app(db_path: str | None = None) -> FastAPI:
    db_path = db_path or os.environ.get("POOL_DB", "pool.db")

    def connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    @contextmanager
    def write_tx():
        """串行写事务: 任何异常都会整体回滚, 不留下半截写入."""
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def sweep_expired(conn: sqlite3.Connection, now: float) -> None:
        """把已过期的预留/租约标记为 expired, 释放其地址(在同一事务内)."""
        conn.execute(
            "UPDATE allocations SET state='expired', code=NULL, updated_at=? "
            "WHERE state='reserved' AND reserved_until <= ?", (now, now))
        conn.execute(
            "UPDATE allocations SET state='expired', code=NULL, updated_at=? "
            "WHERE state='leased' AND lease_until <= ?", (now, now))

    def get_config(conn: sqlite3.Connection) -> sqlite3.Row | None:
        return conn.execute("SELECT * FROM pool_config WHERE id=1").fetchone()

    def active_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
        return conn.execute(
            "SELECT * FROM allocations WHERE state IN ('reserved','leased')").fetchall()

    def allocation_payload(row: sqlite3.Row, now: float, reused: bool) -> dict:
        payload = {
            "address": row["address"],
            "device_id": row["device_id"],
            "state": row["state"],
            "reused_existing": reused,
        }
        if row["state"] == "reserved":
            payload["code"] = row["code"]
            payload["reserved_until"] = row["reserved_until"]
            payload["confirm_remaining"] = max(0.0, row["reserved_until"] - now)
        elif row["state"] == "leased":
            payload["lease_until"] = row["lease_until"]
            payload["lease_remaining"] = max(0.0, row["lease_until"] - now)
        return payload

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = connect()
        try:
            conn.executescript(SCHEMA)
        finally:
            conn.close()
        yield

    app = FastAPI(title="机房 IPv4 地址池", lifespan=lifespan)

    # ------------------------------------------------------------ 页面

    @app.get("/", response_class=FileResponse, include_in_schema=False)
    def index():
        return FileResponse(STATIC_FILE)

    # ------------------------------------------------------------ 地址池配置

    @app.get("/api/pool")
    def get_pool():
        now = time.time()
        conn = connect()
        try:
            cfg = get_config(conn)
            rows = conn.execute(
                "SELECT * FROM allocations WHERE state IN ('reserved','leased')"
            ).fetchall()
            recent = conn.execute(
                "SELECT * FROM allocations ORDER BY id DESC LIMIT 30").fetchall()
        finally:
            conn.close()

        # 按原截止时间判定有效状态(只读请求不写库, 过期记录留给写事务清理)
        by_addr: dict[str, dict] = {}
        for r in rows:
            if r["state"] == "reserved" and r["reserved_until"] > now:
                by_addr[r["address"]] = {
                    "status": "reserved", "device_id": r["device_id"],
                    "remaining": r["reserved_until"] - now,
                }
            elif r["state"] == "leased" and r["lease_until"] > now:
                by_addr[r["address"]] = {
                    "status": "leased", "device_id": r["device_id"],
                    "remaining": r["lease_until"] - now,
                }

        addresses = []
        config = None
        if cfg:
            base = ip_to_int(cfg["base_ip"])
            config = {
                "base_ip": cfg["base_ip"],
                "count": cfg["count"],
                "last_ip": int_to_ip(base + cfg["count"] - 1),
            }
            for i in range(cfg["count"]):
                ip = int_to_ip(base + i)
                entry = {"address": ip, "status": "free"}
                entry.update(by_addr.get(ip, {}))
                addresses.append(entry)

        return {
            "configured": cfg is not None,
            "config": config,
            "addresses": addresses,
            "server_now": now,
            "recent": [
                {
                    "id": r["id"], "address": r["address"], "device_id": r["device_id"],
                    "state": r["state"], "reserved_until": r["reserved_until"],
                    "lease_until": r["lease_until"], "updated_at": r["updated_at"],
                }
                for r in recent
            ],
        }

    @app.post("/api/pool")
    def set_pool(body: PoolIn):
        try:
            base = ip_to_int(body.base_ip)
        except Exception:
            raise HTTPException(400, "无效的 IPv4 地址")
        if base + body.count - 1 > 0xFFFFFFFF:
            raise HTTPException(400, "地址范围超出 IPv4 空间")
        new_range = {int_to_ip(base + i) for i in range(body.count)}

        now = time.time()
        with write_tx() as conn:
            sweep_expired(conn, now)
            outside = sorted(
                r["address"] for r in active_rows(conn)
                if r["address"] not in new_range
            )
            if outside:
                # 缩小/移动范围会排除有效预留或租约 -> 拒绝, 原配置与分配继续有效
                raise HTTPException(
                    409, f"新范围排除了这些有效分配: {', '.join(outside)}")
            conn.execute(
                "INSERT INTO pool_config(id, base_ip, count, updated_at) VALUES(1,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET base_ip=excluded.base_ip, "
                "count=excluded.count, updated_at=excluded.updated_at",
                (str(IPv4Address(base)), body.count, now))
        return {"ok": True, "base_ip": str(IPv4Address(base)), "count": body.count,
                "range": f"{int_to_ip(base)} - {int_to_ip(base + body.count - 1)}"}

    # ------------------------------------------------------------ 设备操作

    @app.post("/api/apply")
    def apply(body: ApplyIn):
        device_id = body.device_id.strip()
        if not device_id:
            raise HTTPException(400, "device_id 不能为空")
        now = time.time()
        with write_tx() as conn:
            sweep_expired(conn, now)
            cfg = get_config(conn)
            if cfg is None:
                raise HTTPException(409, "地址池尚未配置, 请先在页面设置地址范围")

            # 同一设备重试: 返回其当前分配(预留或租约)
            existing = conn.execute(
                "SELECT * FROM allocations WHERE device_id=? "
                "AND state IN ('reserved','leased') ORDER BY id DESC",
                (device_id,)).fetchone()
            if existing is not None:
                return allocation_payload(existing, now, reused=True)

            used = {r["address"] for r in active_rows(conn)}
            base = ip_to_int(cfg["base_ip"])
            free = next(
                (int_to_ip(base + i) for i in range(cfg["count"])
                 if int_to_ip(base + i) not in used), None)
            if free is None:
                raise HTTPException(409, "地址池已耗尽, 没有空闲地址")

            conn.execute(
                "INSERT INTO allocations(address, device_id, state, code, "
                "reserved_until, lease_until, created_at, updated_at) "
                "VALUES(?,?, 'reserved', ?, ?, NULL, ?, ?)",
                (free, device_id, new_confirm_code(),
                 now + RESERVATION_TTL, now, now))
            row = conn.execute(
                "SELECT * FROM allocations WHERE address=? AND state='reserved'",
                (free,)).fetchone()
            return allocation_payload(row, now, reused=False)

    @app.post("/api/confirm")
    def confirm(body: ConfirmIn):
        now = time.time()
        with write_tx() as conn:
            row = conn.execute(
                "SELECT * FROM allocations WHERE address=? AND state='reserved'",
                (body.address,)).fetchone()
            if row is None:
                raise HTTPException(409, "该地址当前没有待确认的预留")
            if row["reserved_until"] <= now:
                # 预留已过期: 顺手清理, 旧确认码作废
                sweep_expired(conn, now)
                raise HTTPException(410, "确认码已过期, 预留已被回收")
            if row["code"] != body.code or row["device_id"] != body.device_id:
                # 确认码与分配记录绑定, 旧码/错码无法操作新分配
                raise HTTPException(403, "确认码或设备身份不匹配")
            conn.execute(
                "UPDATE allocations SET state='leased', code=NULL, "
                "reserved_until=NULL, lease_until=?, updated_at=? WHERE id=?",
                (now + LEASE_TTL, now, row["id"]))
            row = conn.execute(
                "SELECT * FROM allocations WHERE id=?", (row["id"],)).fetchone()
            return allocation_payload(row, now, reused=False)

    @app.post("/api/renew")
    def renew(body: DeviceAddressIn):
        now = time.time()
        with write_tx() as conn:
            row = conn.execute(
                "SELECT * FROM allocations WHERE address=? AND device_id=? "
                "AND state='leased'", (body.address, body.device_id)).fetchone()
            if row is None:
                raise HTTPException(404, "没有找到该设备在此地址上的有效租约")
            if row["lease_until"] <= now:
                sweep_expired(conn, now)
                raise HTTPException(410, "租约已过期, 地址已被回收")
            conn.execute(
                "UPDATE allocations SET lease_until=?, updated_at=? WHERE id=?",
                (now + LEASE_TTL, now, row["id"]))
            row = conn.execute(
                "SELECT * FROM allocations WHERE id=?", (row["id"],)).fetchone()
            return allocation_payload(row, now, reused=False)

    @app.post("/api/release")
    def release(body: DeviceAddressIn):
        now = time.time()
        with write_tx() as conn:
            cur = conn.execute(
                "UPDATE allocations SET state='released', code=NULL, updated_at=? "
                "WHERE address=? AND device_id=? AND state IN ('reserved','leased')",
                (now, body.address, body.device_id))
            if cur.rowcount == 0:
                raise HTTPException(404, "没有找到该设备在此地址上的有效分配")
        return {"ok": True, "address": body.address, "released": True}

    return app


app = create_app()
