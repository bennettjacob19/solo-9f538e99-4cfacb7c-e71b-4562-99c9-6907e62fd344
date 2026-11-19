# 机房 IPv4 地址池

机房装机时为设备分配 IPv4 的单地址池服务: FastAPI + SQLite, 同一地址不会同时交给两台设备。

## 功能

- 运维在网页设置一段**连续 IPv4 地址**(起始地址 + 数量, 最多 32 个)
- 设备流程: **申请 → 确认 → 续租 → 释放**
  - 申请: 预留一个空闲地址, 返回 **30 秒**内有效的确认码
  - 确认: 凭确认码转为 **5 分钟**租约
  - 续租: 租约顺延 5 分钟; 释放: 立即归还地址
- 同一设备重复申请返回其当前分配(幂等重试)
- 过期预留/租约的地址可被重用; 确认码与分配记录绑定, **旧码无法操作新分配**
- 缩小/移动地址范围若排除有效预留或租约 → **拒绝**, 原配置与分配继续有效
- 并发申请、确认、改范围由 SQLite `BEGIN IMMEDIATE` 事务串行化 + 部分唯一索引兜底, 不会重复占址
- 截止时间以绝对时间戳落库: 写入失败整体回滚, 重启后仍按原截止时间判定, 不留幽灵占用
- 网页实时显示每个地址的占用状态与剩余时间

## 启动

### Docker Compose(推荐)

```bash
docker compose up --build
```

访问 http://localhost:8000 。SQLite 数据保存在 `pool-data` 卷中, 容器重启不丢状态。

### 本地运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --port 8000
```

数据库路径默认 `./pool.db`, 可用环境变量 `POOL_DB` 覆盖。

## 接口

| 方法 | 路径 | 请求体 | 说明 |
|---|---|---|---|
| GET | `/api/pool` | — | 地址范围、每个地址状态、最近记录 |
| POST | `/api/pool` | `{base_ip, count}` | 设置地址范围(1–32 个) |
| POST | `/api/apply` | `{device_id}` | 申请地址 → 预留 + 确认码(30s) |
| POST | `/api/confirm` | `{device_id, address, code}` | 确认 → 5 分钟租约 |
| POST | `/api/renew` | `{device_id, address}` | 续租 5 分钟 |
| POST | `/api/release` | `{device_id, address}` | 释放地址 |

错误码: `400` 参数无效 · `403` 确认码/设备不匹配 · `404` 无此有效分配 · `409` 冲突(池未配置/已耗尽/缩容排除有效分配) · `410` 预留或租约已过期

## 测试

```bash
pip install pytest httpx
python -m pytest tests/ -v
```

覆盖: 完整生命周期、幂等重试、过期重用、旧码失效、缩容拒绝、并发不重复占址、重启后按原截止时间判定。
