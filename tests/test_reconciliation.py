"""交接单处理情况对账快照 reconciliation 端到端集成测试：需要真实 Redis
（REDIS_URL，默认 redis://localhost:6399/15）。

运行：
    REDIS_URL=redis://localhost:6399/15 \
    HANDOFF_LEASE_S=1 \
    BARRIER_POLL_INTERVAL_S=0.05 python3 -m pytest -s tests/test_reconciliation.py
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("REDIS_URL", "redis://localhost:6399/15")
os.environ.setdefault("HANDOFF_LEASE_S", "1")
os.environ.setdefault("BARRIER_POLL_INTERVAL_S", "0.05")

from app.main import app  # noqa: E402

API = "http://test"


@pytest.fixture(autouse=True)
async def _lifespan():
    async with LifespanManager(app):
        # 批次标识是调用方给定的可读字符串（非随机 UUID），测试重复运行时会
        # 命中上一轮残留的批次键；用例均在专用测试库（默认 db15）上运行，
        # 每例开始前清空，保证可重复且互不影响。
        await app.state.recon.redis.flushdb()
        yield


async def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url=API)


_seq = 0


async def make_handoff(client, participants, complete=False, lease=False):
    """建屏障 -> 达标 -> 建交接单，返回 handoff 视图。

    complete=True：认领并完成（终态 completed）。
    lease=True：认领但不完成（leased）。
    """
    global _seq
    _seq += 1
    tag = f"t{_seq}-{id(client)}-"
    pids = [f"{tag}{p}" for p in participants]
    deadline = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()
    r = await client.post(
        "/barriers",
        json={"participants": pids, "threshold": 1, "deadline": deadline},
    )
    assert r.status_code == 201, r.text
    bid = r.json()["barrier_id"]
    ci = await client.post(f"/barriers/{bid}/checkin", json={"participant_id": pids[0]})
    assert ci.json()["status"] == "reached"
    h = await client.post("/handoffs", json={"barrier_id": bid})
    assert h.status_code == 201, h.text
    view = h.json()
    hid = view["handoff_id"]
    if complete or lease:
        owner = f"fin-{hid[:6]}"
        g = await client.post(f"/handoffs/{hid}/claim", json={"owner": owner})
        assert g.status_code == 200, g.text
        fence = g.json()["fence"]
        if complete:
            d = await client.post(
                f"/handoffs/{hid}/complete",
                json={"owner": owner, "fence": fence},
            )
            assert d.status_code == 200, d.text
            view = (await client.get(f"/handoffs/{hid}")).json()
        else:
            view = g.json()
    return view


def submit(client, batch_id, ids):
    return client.put(
        f"/reconciliation/batches/{batch_id}", json={"handoff_ids": ids}
    )


def get_batch(client, batch_id):
    return client.get(f"/reconciliation/batches/{batch_id}")


# ---------------------------------------------------------------------------
# 首次提交
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_first_submit_records_items_in_input_order_with_counts():
    """首次提交 201：按输入顺序逐单记录来源屏障 / 状态 / 完成时间，计数正确。"""
    async with await _client() as c:
        h1 = await make_handoff(c, ["a", "b"], complete=True)
        h2 = await make_handoff(c, ["c", "d"])  # idle 未完成
        h3 = await make_handoff(c, ["e", "f"], lease=True)  # leased 未完成

        bid = "batch-basic-1"
        ids = [h2["handoff_id"], h1["handoff_id"], h3["handoff_id"]]
        r = await submit(c, bid, ids)
        assert r.status_code == 201, r.text
        body = r.json()

        assert body["batch_id"] == bid
        assert body["replayed"] is False
        assert body["handoff_ids"] == ids
        assert body["completed_count"] == 1
        assert body["pending_count"] == 2
        assert len(body["items"]) == 3

        # 顺序与输入一致，逐单字段定格
        it0, it1, it2 = body["items"]
        assert it0["handoff_id"] == h2["handoff_id"]
        assert it0["barrier_id"] == h2["barrier_id"]
        assert it0["status"] == "idle"
        assert it0["completed_at"] is None

        assert it1["handoff_id"] == h1["handoff_id"]
        assert it1["barrier_id"] == h1["barrier_id"]
        assert it1["status"] == "completed"
        assert it1["completed_at"] == h1["completed_at"]

        assert it2["status"] == "leased"
        assert it2["completed_at"] is None

        # created_at 为带时区的 ISO 时间
        created = datetime.fromisoformat(body["created_at"])
        assert created.tzinfo is not None


@pytest.mark.asyncio
async def test_expired_lease_is_snapshotted_as_idle_pending():
    """保存瞬间租约已到期的交接单按 idle（未完成）入账，与 GET /handoffs 同口径。"""
    async with await _client() as c:
        h = await make_handoff(c, ["a"], lease=True)
        await asyncio.sleep(1.2)  # HANDOFF_LEASE_S=1，等待租约到期
        live = (await c.get(f"/handoffs/{h['handoff_id']}")).json()
        assert live["status"] == "idle"

        r = await submit(c, "batch-expired-lease", [h["handoff_id"]])
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["items"][0]["status"] == "idle"
        assert body["completed_count"] == 0
        assert body["pending_count"] == 1


# ---------------------------------------------------------------------------
# 重试回放 / 冲突
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_same_batch_same_list_replays_first_snapshot():
    """同批次标识 + 相同列表重试：200 + replayed=true，逐字段回放首次快照。"""
    async with await _client() as c:
        h = await make_handoff(c, ["a"])  # 首次快照时 idle
        bid = "batch-replay-1"
        ids = [h["handoff_id"]]

        r1 = await submit(c, bid, ids)
        assert r1.status_code == 201
        first = r1.json()
        assert first["items"][0]["status"] == "idle"

        # 之后把交接单完成：实时状态已变
        g = await c.post(f"/handoffs/{h['handoff_id']}/claim", json={"owner": "x"})
        await c.post(
            f"/handoffs/{h['handoff_id']}/complete",
            json={"owner": "x", "fence": g.json()["fence"]},
        )

        # 同批次同列表重试：回放首次快照，状态仍是 idle、计数不变、created_at 不变
        r2 = await submit(c, bid, ids)
        assert r2.status_code == 200, r2.text
        replay = r2.json()
        assert replay["replayed"] is True
        assert replay["created_at"] == first["created_at"]
        assert replay["completed_count"] == first["completed_count"] == 0
        assert replay["pending_count"] == first["pending_count"] == 1
        assert replay["items"][0]["status"] == "idle"
        assert replay["items"][0]["completed_at"] is None

        # GET 与重放内容一致
        r3 = await get_batch(c, bid)
        assert r3.status_code == 200
        got = r3.json()
        assert got["replayed"] is False
        assert got["items"] == replay["items"]
        assert got["created_at"] == first["created_at"]


@pytest.mark.asyncio
async def test_same_batch_different_list_conflicts_409():
    """同一批次标识提交不同列表：409，首次快照保持不变。"""
    async with await _client() as c:
        h1 = await make_handoff(c, ["a"])
        h2 = await make_handoff(c, ["b"])
        bid = "batch-conflict-1"

        r1 = await submit(c, bid, [h1["handoff_id"]])
        assert r1.status_code == 201

        # 不同成员
        r2 = await submit(c, bid, [h2["handoff_id"]])
        assert r2.status_code == 409
        # 追加 / 删除成员
        r3 = await submit(c, bid, [h1["handoff_id"], h2["handoff_id"]])
        assert r3.status_code == 409
        # 仅顺序不同也视为另一份列表（顺序敏感）
        h3 = await make_handoff(c, ["c"])
        r4 = await submit(c, bid, [h2["handoff_id"], h1["handoff_id"]])
        assert r4.status_code == 409
        _ = h3

        # 首次快照未被改写
        got = (await get_batch(c, bid)).json()
        assert got["handoff_ids"] == [h1["handoff_id"]]
        assert len(got["items"]) == 1


@pytest.mark.asyncio
async def test_concurrent_submits_only_first_wins_or_replays():
    """同一批次并发提交相同列表：只有一个 201，其余全部 200 回放同一快照。"""
    async with await _client() as c:
        h1 = await make_handoff(c, ["a"])
        h2 = await make_handoff(c, ["b"])
        bid = "batch-concurrent-1"
        ids = [h1["handoff_id"], h2["handoff_id"]]

        results = await asyncio.gather(*(submit(c, bid, ids) for _ in range(8)))
        created = [r for r in results if r.status_code == 201]
        replayed = [r for r in results if r.status_code == 200]
        assert len(created) == 1, [r.status_code for r in results]
        assert len(replayed) == 7
        first_body = created[0].json()
        for r in replayed:
            b = r.json()
            assert b["replayed"] is True
            assert b["created_at"] == first_body["created_at"]
            assert b["items"] == first_body["items"]


# ---------------------------------------------------------------------------
# 整批失败不占用批次标识
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_handoff_fails_whole_batch_without_taking_batch_id():
    """任一交接单不存在：404、零副作用；批次标识未被占用，可重试成功。"""
    async with await _client() as c:
        h = await make_handoff(c, ["a"])
        bid = "batch-missing-1"

        r = await submit(c, bid, [h["handoff_id"], "deadbeefdeadbeef"])
        assert r.status_code == 404
        assert "deadbeefdeadbeef" in r.json()["detail"]

        # 批次不存在：GET 404
        assert (await get_batch(c, bid)).status_code == 404
        # 物理键未写入（批次标识未被占用）
        assert await app.state.recon.redis.exists(f"recon:{bid}") == 0

        # 用同一批次标识提交有效列表：首次创建成功
        r2 = await submit(c, bid, [h["handoff_id"]])
        assert r2.status_code == 201
        assert r2.json()["handoff_ids"] == [h["handoff_id"]]


@pytest.mark.asyncio
async def test_unknown_handoff_any_position_fails_whole_batch():
    """不存在的交接单出现在任意位置都整批失败（存在的单也不产生快照）。"""
    async with await _client() as c:
        h1 = await make_handoff(c, ["a"])
        h2 = await make_handoff(c, ["b"])
        bid = "batch-missing-2"
        r = await submit(
            c, bid, [h1["handoff_id"], "nope000000000000", h2["handoff_id"]]
        )
        assert r.status_code == 404
        assert (await get_batch(c, bid)).status_code == 404


@pytest.mark.asyncio
async def test_concurrent_submits_when_handoff_missing_do_not_take_batch_id():
    """并发提交含不存在交接单的批次：全部 404，且后续同标识仍可首次创建。"""
    async with await _client() as c:
        h = await make_handoff(c, ["a"])
        bid = "batch-missing-concurrent"
        results = await asyncio.gather(
            *(
                submit(c, bid, [h["handoff_id"], f"missing{i:012d}xxxxxx"])
                for i in range(6)
            )
        )
        assert {r.status_code for r in results} == {404}
        assert await app.state.recon.redis.exists(f"recon:{bid}") == 0

        r = await submit(c, bid, [h["handoff_id"]])
        assert r.status_code == 201


# ---------------------------------------------------------------------------
# 快照不随后续变化改写
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_snapshot_immutable_after_handoffs_change():
    """保存后交接单完成 / 租约流转：GET 永远返回保存瞬间的状态与计数。"""
    async with await _client() as c:
        h1 = await make_handoff(c, ["a"], complete=True)
        h2 = await make_handoff(c, ["b"])  # idle
        h3 = await make_handoff(c, ["c"], lease=True)  # leased
        bid = "batch-immutable-1"
        ids = [h1["handoff_id"], h2["handoff_id"], h3["handoff_id"]]

        first = (await submit(c, bid, ids)).json()
        assert first["completed_count"] == 1
        assert first["pending_count"] == 2

        # h2 完成；h3 租约到期后被他人接管
        g2 = await c.post(f"/handoffs/{h2['handoff_id']}/claim", json={"owner": "y"})
        await c.post(
            f"/handoffs/{h2['handoff_id']}/complete",
            json={"owner": "y", "fence": g2.json()["fence"]},
        )
        await asyncio.sleep(1.2)
        await c.post(f"/handoffs/{h3['handoff_id']}/claim", json={"owner": "z"})

        got = (await get_batch(c, bid)).json()
        assert got["created_at"] == first["created_at"]
        assert got["completed_count"] == 1
        assert got["pending_count"] == 2
        statuses = {it["handoff_id"]: it["status"] for it in got["items"]}
        assert statuses[h1["handoff_id"]] == "completed"
        assert statuses[h2["handoff_id"]] == "idle"  # 保存瞬间未完成
        assert statuses[h3["handoff_id"]] == "leased"
        # h2 在快照中的 completed_at 仍为 null
        h2_item = next(it for it in got["items"] if it["handoff_id"] == h2["handoff_id"])
        assert h2_item["completed_at"] is None


@pytest.mark.asyncio
async def test_snapshot_survives_source_barrier_reclamation():
    """来源屏障到期回收后，对账快照与其引用的交接单信息仍可完整复核。"""
    async with await _client() as c:
        h = await make_handoff(c, ["a"], complete=True)
        bid = h["barrier_id"]
        batch = "batch-survive-1"
        r = await submit(c, batch, [h["handoff_id"]])
        assert r.status_code == 201

        # 手动回收屏障键（模拟 TTL 到期）
        async for key in app.state.store.redis.scan_iter(match=f"barrier:{bid}*"):
            await app.state.store.redis.delete(key)

        got = await get_batch(c, batch)
        assert got.status_code == 200
        body = got.json()
        assert body["items"][0]["barrier_id"] == bid
        assert body["items"][0]["status"] == "completed"
        assert body["completed_count"] == 1


@pytest.mark.asyncio
async def test_snapshot_atomic_against_concurrent_completions():
    """批次提交与多张交接单并发完成竞争：快照内逐单状态与计数严格自洽，
    不可能出现「item=completed 但未计入 completed_count」的跨时刻混态。"""
    async with await _client() as c:
        # 6 张已认领、租约有效的交接单（完成令牌预先取得，租约 1s 内全部并发执行）
        hs = []
        for i in range(6):
            h = await make_handoff(c, [f"m{i}"], lease=True)
            hs.append(h)
        ids = [h["handoff_id"] for h in hs]
        bid = "batch-atomic-concurrent"

        # 每张单的完成需要它各自的 owner/fence：lease=True 的返回即为认领响应。
        creds = [(h["owner"], h["fence"]) for h in hs]

        async def complete_one(hid, cred):
            owner, fence = cred
            return await c.post(
                f"/handoffs/{hid}/complete",
                json={"owner": owner, "fence": fence},
            )

        async def submit_batch():
            return await submit(c, bid, ids)

        results = await asyncio.gather(
            submit_batch(),
            *(complete_one(hid, cred) for hid, cred in zip(ids, creds)),
        )
        snap_resp = results[0]
        complete_resps = results[1:]
        # 全部完成请求本身都应成功（租约未到期）
        assert {r.status_code for r in complete_resps} == {200}, [
            r.status_code for r in complete_resps
        ]

        # 无论完成脚本与快照脚本的实际交错顺序如何：
        # 1) 快照中 completed 状态的条数必须等于 completed_count；
        # 2) 其余条数等于 pending_count；
        # 3) 每条 completed 都带有完成时刻；
        # 4) 计数之和等于总数。
        if snap_resp.status_code == 201:
            snap = snap_resp.json()
            marked = [it for it in snap["items"] if it["status"] == "completed"]
            pending = [it for it in snap["items"] if it["status"] != "completed"]
            assert len(marked) == snap["completed_count"]
            assert len(pending) == snap["pending_count"]
            assert all(it["completed_at"] is not None for it in marked)
            assert all(it["completed_at"] is None for it in pending)
            assert (
                snap["completed_count"] + snap["pending_count"] == len(ids) == 6
            )
            assert {it["status"] for it in pending} <= {"idle", "leased"}

            # 之后重放必须逐字段等于并发下定格的首次快照
            replay = (await submit(c, bid, ids)).json()
            assert replay["replayed"] is True
            assert replay["items"] == snap["items"]
            assert replay["completed_count"] == snap["completed_count"]
        else:  # 极端情况下（理论不会：批次首次提交不可能冲突）显式暴露
            pytest.fail(f"快照提交异常状态码 {snap_resp.status_code}: {snap_resp.text}")


@pytest.mark.asyncio
async def test_recon_keys_have_no_ttl():
    """对账快照键不设置 TTL（长期可复核）。"""
    async with await _client() as c:
        h = await make_handoff(c, ["a"])
        bid = "batch-ttl-1"
        assert (await submit(c, bid, [h["handoff_id"]])).status_code == 201
        assert await app.state.recon.redis.ttl(f"recon:{bid}") == -1


# ---------------------------------------------------------------------------
# 校验 / 错误路径 / OpenAPI
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_and_empty_handoff_ids_rejected_422():
    """非空列表、批内重复 ID、空标识一律 422（在服务端校验前拒绝）。"""
    async with await _client() as c:
        h = await make_handoff(c, ["a"])
        hid = h["handoff_id"]
        assert (await submit(c, "batch-val-1", [])).status_code == 422
        assert (await submit(c, "batch-val-2", [hid, hid])).status_code == 422
        assert (await submit(c, "batch-val-3", ["   "])).status_code == 422
        assert (
            await submit(c, "batch-val-4", [hid, f"  {hid}  "])
        ).status_code == 422  # 去空白后重复
        # 请求体缺字段
        r = await c.put("/reconciliation/batches/batch-val-5", json={})
        assert r.status_code == 422
        # 422 校验失败不得占用批次标识
        assert (await get_batch(c, "batch-val-2")).status_code == 404


@pytest.mark.asyncio
async def test_batch_id_validation_422():
    """路径批次标识为空 / 含非法字符 / 超长返回 422。"""
    async with await _client() as c:
        h = await make_handoff(c, ["a"])
        for bad in ("bad id/x", "id|x", "bad@id", "id*1"):
            r = await submit(c, bad, [h["handoff_id"]])
            assert r.status_code in (422, 404), (bad, r.status_code)
        long_id = "x" * 129
        assert (await submit(c, long_id, [h["handoff_id"]])).status_code == 422
        # 合法字符（含空格 / : / .）可用
        ok_id = "2026-09-29.payroll:team a"
        r = await submit(c, ok_id, [h["handoff_id"]])
        assert r.status_code == 201, r.text
        assert r.json()["batch_id"] == ok_id


@pytest.mark.asyncio
async def test_get_unknown_batch_404():
    async with await _client() as c:
        assert (await get_batch(c, "batch-does-not-exist")).status_code == 404


@pytest.mark.asyncio
async def test_reconciliation_endpoints_in_openapi():
    async with await _client() as c:
        schema = (await c.get("/openapi.json")).json()
        for path in (
            "/reconciliation/batches/{batch_id}",
        ):
            assert path in schema["paths"], path
        assert "put" in schema["paths"][path]
        assert "get" in schema["paths"][path]
        for name in (
            "ReconciliationRequest",
            "ReconciliationView",
            "SnapshotItemView",
        ):
            assert name in schema["components"]["schemas"], name
        root = (await c.get("/")).json()
        assert (
            root["endpoints"]["submit_reconciliation"]
            == "PUT /reconciliation/batches/{batch_id}"
        )
