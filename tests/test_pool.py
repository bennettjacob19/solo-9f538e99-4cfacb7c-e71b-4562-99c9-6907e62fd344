"""地址池服务测试: 覆盖申请/确认/续租/释放、幂等重试、过期重用、
旧码失效、缩容拒绝、并发不重复占址、重启后按原截止时间判定."""
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import main as pool_main  # noqa: E402


@pytest.fixture()
def env(tmp_path):
    db = tmp_path / "pool.db"
    app = pool_main.create_app(str(db))
    with TestClient(app) as client:
        yield client, app, db


def set_pool(client, base="10.0.0.10", count=4):
    r = client.post("/api/pool", json={"base_ip": base, "count": count})
    assert r.status_code == 200, r.text
    return r


def apply(client, device):
    return client.post("/api/apply", json={"device_id": device})


def confirm(client, device, address, code):
    return client.post("/api/confirm",
                       json={"device_id": device, "address": address, "code": code})


def db_exec(db, sql, args=()):
    conn = sqlite3.connect(db)
    conn.execute(sql, args)
    conn.commit()
    conn.close()


def db_rows(db, sql, args=()):
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, args).fetchall()
    conn.close()
    return rows


def force_expire(db, address):
    """把该地址的有效分配截止时间改到过去, 模拟时间流逝."""
    db_exec(db,
            "UPDATE allocations SET reserved_until=:t WHERE address=:a AND state='reserved'",
            {"t": time.time() - 1, "a": address})
    db_exec(db,
            "UPDATE allocations SET lease_until=:t WHERE address=:a AND state='leased'",
            {"t": time.time() - 1, "a": address})


# ------------------------------------------------------------ 基本流程

def test_full_lifecycle(env):
    client, _, _ = env
    set_pool(client, count=2)

    r = apply(client, "dev-a")
    assert r.status_code == 200
    body = r.json()
    assert body["state"] == "reserved"
    assert body["address"] == "10.0.0.10"
    assert 0 < body["confirm_remaining"] <= 30
    code = body["code"]

    r = confirm(client, "dev-a", body["address"], code)
    assert r.status_code == 200
    leased = r.json()
    assert leased["state"] == "leased"
    assert 0 < leased["lease_remaining"] <= 300

    # 续租
    r = client.post("/api/renew",
                    json={"device_id": "dev-a", "address": leased["address"]})
    assert r.status_code == 200
    assert r.json()["lease_remaining"] > 290

    # 释放后地址空闲
    r = client.post("/api/release",
                    json={"device_id": "dev-a", "address": leased["address"]})
    assert r.status_code == 200
    pool = client.get("/api/pool").json()
    assert all(a["status"] == "free" for a in pool["addresses"])


def test_apply_requires_configured_pool(env):
    client, _, db = env
    r = apply(client, "dev-a")
    assert r.status_code == 409
    # 写入失败不留任何记录
    assert db_rows(db, "SELECT * FROM allocations") == []


def test_same_device_retry_returns_current_allocation(env):
    client, _, _ = env
    set_pool(client, count=2)
    first = apply(client, "dev-a").json()
    second = apply(client, "dev-a").json()
    assert second["reused_existing"] is True
    assert second["address"] == first["address"]
    assert second["code"] == first["code"]  # 重试不换新码

    confirm(client, "dev-a", first["address"], first["code"])
    third = apply(client, "dev-a").json()
    assert third["state"] == "leased"
    assert third["address"] == first["address"]
    assert "code" not in third


def test_distinct_devices_get_distinct_addresses(env):
    client, _, _ = env
    set_pool(client, count=2)
    a = apply(client, "dev-a").json()
    b = apply(client, "dev-b").json()
    assert a["address"] != b["address"]


def test_pool_exhausted(env):
    client, _, _ = env
    set_pool(client, count=1)
    assert apply(client, "dev-a").status_code == 200
    r = apply(client, "dev-b")
    assert r.status_code == 409


# ------------------------------------------------------------ 过期与旧码

def test_expired_reservation_reusable_and_old_code_dead(env):
    client, _, db = env
    set_pool(client, count=1)
    old = apply(client, "dev-a").json()
    force_expire(db, old["address"])

    # 过期地址可被另一台设备重用
    new = apply(client, "dev-b").json()
    assert new["address"] == old["address"]
    assert new["code"] != old["code"]

    # 旧码不能操作新分配
    r = confirm(client, "dev-a", old["address"], old["code"])
    assert r.status_code in (403, 409, 410)

    # 新码正常确认
    r = confirm(client, "dev-b", new["address"], new["code"])
    assert r.status_code == 200


def test_expired_code_rejected(env):
    client, _, db = env
    set_pool(client, count=1)
    a = apply(client, "dev-a").json()
    force_expire(db, a["address"])
    r = confirm(client, "dev-a", a["address"], a["code"])
    assert r.status_code == 410


def test_expired_lease_reusable(env):
    client, _, db = env
    set_pool(client, count=1)
    a = apply(client, "dev-a").json()
    confirm(client, "dev-a", a["address"], a["code"])
    force_expire(db, a["address"])

    b = apply(client, "dev-b").json()
    assert b["address"] == a["address"]

    # 过期租约不能续租
    r = client.post("/api/renew",
                    json={"device_id": "dev-a", "address": a["address"]})
    assert r.status_code in (404, 410)


def test_wrong_code_or_device_rejected_and_reservation_kept(env):
    client, _, _ = env
    set_pool(client, count=1)
    a = apply(client, "dev-a").json()
    assert confirm(client, "dev-a", a["address"], "deadbeef").status_code == 403
    assert confirm(client, "dev-b", a["address"], a["code"]).status_code == 403
    # 失败的确认不消耗预留
    assert confirm(client, "dev-a", a["address"], a["code"]).status_code == 200


# ------------------------------------------------------------ 范围调整

def test_shrink_excluding_active_allocations_rejected(env):
    client, _, _ = env
    set_pool(client, count=4)
    a = apply(client, "dev-a").json()
    confirm(client, "dev-a", a["address"], a["code"])
    others = [apply(client, f"dev-{c}").json() for c in "bcd"]  # 占满 .11-.13

    # 缩到 2 个会排除 .12/.13 上的有效预留
    r = client.post("/api/pool", json={"base_ip": "10.0.0.10", "count": 2})
    assert r.status_code == 409

    # 原配置与分配继续有效
    pool = client.get("/api/pool").json()
    assert pool["config"]["count"] == 4
    statuses = {x["address"]: x["status"] for x in pool["addresses"]}
    assert statuses[a["address"]] == "leased"
    for o in others:
        assert statuses[o["address"]] == "reserved"


def test_shrink_allowed_when_only_free_addresses_excluded(env):
    client, _, _ = env
    set_pool(client, count=4)
    a = apply(client, "dev-a").json()  # 占用 .10
    confirm(client, "dev-a", a["address"], a["code"])
    r = client.post("/api/pool", json={"base_ip": "10.0.0.10", "count": 1})
    assert r.status_code == 200
    pool = client.get("/api/pool").json()
    assert pool["config"]["count"] == 1
    assert pool["addresses"][0]["status"] == "leased"


def test_expired_allocations_do_not_block_shrink(env):
    client, _, db = env
    set_pool(client, count=4)
    a = apply(client, "dev-a").json()
    force_expire(db, a["address"])
    r = client.post("/api/pool", json={"base_ip": "10.0.0.20", "count": 2})
    assert r.status_code == 200


def test_invalid_pool_config_rejected(env):
    client, _, _ = env
    assert client.post("/api/pool",
                       json={"base_ip": "999.1.1.1", "count": 4}).status_code == 400
    assert client.post("/api/pool",
                       json={"base_ip": "10.0.0.1", "count": 33}).status_code == 422
    assert client.post("/api/pool",
                       json={"base_ip": "10.0.0.1", "count": 0}).status_code == 422
    assert client.post("/api/pool",
                       json={"base_ip": "255.255.255.255", "count": 2}).status_code == 400


# ------------------------------------------------------------ 并发

def test_concurrent_apply_never_duplicates(env):
    client, app, db = env
    set_pool(client, count=8)

    def one(i):
        with TestClient(app) as c:
            return c.post("/api/apply", json={"device_id": f"dev-{i}"})

    with ThreadPoolExecutor(max_workers=16) as ex:
        results = list(ex.map(one, range(16)))

    oks = [r for r in results if r.status_code == 200]
    assert len(oks) == 8
    assert len({r.json()["address"] for r in oks}) == 8  # 地址互不重复
    assert sum(1 for r in results if r.status_code == 409) == 8

    # 数据库层面同样无重复占址
    dup = db_rows(db,
                  "SELECT address, COUNT(*) n FROM allocations "
                  "WHERE state IN ('reserved','leased') GROUP BY address HAVING n > 1")
    assert dup == []


def test_concurrent_confirm_single_winner(env):
    client, app, _ = env
    set_pool(client, count=1)
    a = apply(client, "dev-a").json()

    def one(_):
        with TestClient(app) as c:
            return c.post("/api/confirm", json={
                "device_id": "dev-a", "address": a["address"], "code": a["code"]})

    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(one, range(8)))
    assert sum(1 for r in results if r.status_code == 200) == 1
    assert sum(1 for r in results if r.status_code == 409) == 7


def test_concurrent_apply_and_shrink_keeps_invariant(env):
    client, app, db = env
    set_pool(client, count=8)

    def apply_one(i):
        with TestClient(app) as c:
            return c.post("/api/apply", json={"device_id": f"dev-{i}"})

    def shrink(_):
        with TestClient(app) as c:
            return c.post("/api/pool", json={"base_ip": "10.0.0.10", "count": 4})

    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = [ex.submit(apply_one, i) for i in range(8)]
        futures += [ex.submit(shrink, i) for i in range(4)]
        for f in futures:
            f.result()

    # 不变量: 任何时刻同一地址最多一条有效分配
    dup = db_rows(db,
                  "SELECT address, COUNT(*) n FROM allocations "
                  "WHERE state IN ('reserved','leased') GROUP BY address HAVING n > 1")
    assert dup == []
    # 不变量: 若缩容成功, 所有有效分配都落在新范围内
    cfg = db_rows(db, "SELECT * FROM pool_config WHERE id=1")[0]
    base = int(pool_main.IPv4Address(cfg["base_ip"]))
    allowed = {str(pool_main.IPv4Address(base + i)) for i in range(cfg["count"])}
    active = db_rows(db, "SELECT address FROM allocations "
                         "WHERE state IN ('reserved','leased')")
    assert all(r["address"] in allowed for r in active)


# ------------------------------------------------------------ 持久化 / 重启

def test_restart_keeps_original_deadlines(tmp_path):
    db = str(tmp_path / "pool.db")
    with TestClient(pool_main.create_app(db)) as c1:
        set_pool(c1, count=2)
        a = apply(c1, "dev-a").json()
        confirm(c1, "dev-a", a["address"], a["code"])

    # 模拟重启: 同一数据库文件起一个新应用实例
    with TestClient(pool_main.create_app(db)) as c2:
        pool = c2.get("/api/pool").json()
        entry = next(x for x in pool["addresses"] if x["address"] == a["address"])
        assert entry["status"] == "leased"          # 未过期租约仍然有效
        assert 0 < entry["remaining"] <= 300
        # 同一设备重试仍返回原分配
        again = c2.post("/api/apply", json={"device_id": "dev-a"}).json()
        assert again["address"] == a["address"]

        # 把租约截止时间改到过去 -> 立即按过期处理, 不留幽灵占用
        db_exec(db, "UPDATE allocations SET lease_until=? WHERE state='leased'",
                (time.time() - 1,))
        pool = c2.get("/api/pool").json()
        assert all(x["status"] == "free" for x in pool["addresses"])
        b = c2.post("/api/apply", json={"device_id": "dev-b"}).json()
        assert b["address"] == a["address"]  # 地址可重用
