"""交接单处理情况对账快照（reconciliation snapshot）端到端集成测试：
需要真实 Redis（REDIS_URL，默认 redis://localhost:6399/15）。

运行：
    REDIS_URL=redis://localhost:6399/15 \
    HANDOFF_LEASE_S=1 \
    BARRIER_POLL_INTERVAL_S=0.05 python3 -m pytest -s tests/test_recon.py
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

# 必须在导入 app 前固定配置（短租约，便于到期回落 idle 的测试）
os.environ.setdefault("REDIS_URL", "redis://localhost:6399/15")
os.environ.setdefault("HANDOFF_LEASE_S", "1")
os.environ.setdefault("BARRIER_POLL_INTERVAL_S", "0.05")

from app.main import app  # noqa: E402

API = "http://test"


@pytest.fixture(autouse=True)
async def _lifespan():
    async with LifespanManager(app):
        yield


@pytest.fixture(autouse=True)
async def _flushdb(_lifespan):
    # 本模块使用固定批次标识：每条测试前清空当前库，避免跨运行的 recon:*
    # 残留造成“已被另一份列表占用”的误判（本模块测试均自建所需交接单）。
    await app.state.recon.redis.flushdb()
    yield


async def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url=API)


async def create_barrier(client, participants, threshold=None, ttl_s=30):
    deadline = (datetime.now(timezone.utc) + timedelta(seconds=ttl_s)).isoformat()
    r = await client.post(
        "/barriers",
        json={
            "participants": participants,
            "threshold": threshold,
            "deadline": deadline,
        },
    )
    assert r.status_code == 201, r.text
    return r.json()


async def reach(client, participants, threshold=None, ttl_s=30):
    """建屏障并报到到达标，返回 (bid, quorum, frozen_at)。"""
    b = await create_barrier(client, participants, threshold=threshold, ttl_s=ttl_s)
    bid = b["barrier_id"]
    n = threshold if threshold is not None else len(participants)
    for pid in participants[: n - 1]:
        await client.post(f"/barriers/{bid}/checkin", json={"participant_id": pid})
    last = await client.post(
        f"/barriers/{bid}/checkin", json={"participant_id": participants[n - 1]}
    )
    assert last.json()["status"] == "reached", last.text
    return bid, last.json()["quorum"], last.json()["frozen_at"]


async def make_handoff(client, name):
    """建一个单参与者屏障 -> 达标 -> 建交接单，返回 (hid, bid, quorum, frozen_at)。"""
    bid, quorum, frozen_at = await reach(client, [name], threshold=1)
    r = await client.post("/handoffs", json={"barrier_id": bid})
    assert r.status_code == 201, r.text
    return r.json()["handoff_id"], bid, quorum, frozen_at


async def claim(client, hid, owner):
    return await client.post(f"/handoffs/{hid}/claim", json={"owner": owner})


async def complete(client, hid, owner, fence):
    return await client.post(
        f"/handoffs/{hid}/complete", json={"owner": owner, "fence": fence}
    )


async def put_snapshot(client, batch, handoff_ids):
    return await client.put(
        f"/reconciliation-snapshots/{batch}", json={"handoff_ids": handoff_ids}
    )


# ---------------------------------------------------------------------------
# 首次提交
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_first_submit_records_items_in_input_order_with_counts():
    async with await _client() as c:
        h1, b1, q1, f1 = await make_handoff(c, "a")
        h2, b2, q2, f2 = await make_handoff(c, "b")
        h3, b3, q3, f3 = await make_handoff(c, "c")

        # 完成中间一单
        await claim(c, h2, "fin")
        done = await complete(c, h2, "fin", 1)
        assert done.status_code == 200
        completed_at = done.json()["completed_at"]

        r = await put_snapshot(c, "batch-1", [h1, h2, h3])
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["batch_id"] == "batch-1"
        assert body["replayed"] is False
        assert body["completed_count"] == 1
        assert body["pending_count"] == 2

        # 严格按输入顺序
        assert [it["handoff_id"] for it in body["items"]] == [h1, h2, h3]
        by_id = {it["handoff_id"]: it for it in body["items"]}

        it1 = by_id[h1]
        assert it1["barrier_id"] == b1
        assert it1["frozen_members"] == q1
        assert it1["frozen_at"] == f1
        assert it1["status"] == "idle"
        assert it1["completed_at"] is None

        it2 = by_id[h2]
        assert it2["barrier_id"] == b2
        assert it2["frozen_members"] == q2
        assert it2["frozen_at"] == f2
        assert it2["status"] == "completed"
        assert it2["completed_at"] == completed_at

        it3 = by_id[h3]
        assert it3["barrier_id"] == b3
        assert it3["frozen_members"] == q3
        assert it3["frozen_at"] == f3
        assert it3["status"] == "idle"

        assert body["created_at"] is not None


@pytest.mark.asyncio
async def test_leased_status_recorded_while_lease_valid():
    async with await _client() as c:
        h, _, _, _ = await make_handoff(c, "a")
        await claim(c, h, "worker")
        r = await put_snapshot(c, "batch-lease", [h])
        assert r.status_code == 201
        assert r.json()["items"][0]["status"] == "leased"
        assert r.json()["items"][0]["completed_at"] is None
        assert r.json()["completed_count"] == 0
        assert r.json()["pending_count"] == 1


@pytest.mark.asyncio
async def test_expired_lease_recorded_as_idle_in_new_snapshot():
    async with await _client() as c:
        h, _, _, _ = await make_handoff(c, "a")
        await claim(c, h, "worker")
        await asyncio.sleep(1.15)  # HANDOFF_LEASE_S=1
        r = await put_snapshot(c, "batch-expired", [h])
        assert r.status_code == 201
        assert r.json()["items"][0]["status"] == "idle"


# ---------------------------------------------------------------------------
# 幂等回放
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_same_list_replays_first_snapshot_200():
    async with await _client() as c:
        h1, _, _, _ = await make_handoff(c, "a")
        h2, _, _, _ = await make_handoff(c, "b")

        first = await put_snapshot(c, "idem-1", [h1, h2])
        assert first.status_code == 201
        first_body = first.json()

        # 首次之后才完成：重试必须回放首次（全部未完成）快照
        await claim(c, h1, "fin")
        await complete(c, h1, "fin", 1)

        retry = await put_snapshot(c, "idem-1", [h1, h2])
        assert retry.status_code == 200
        body = retry.json()
        assert body["replayed"] is True
        assert body["created_at"] == first_body["created_at"]
        assert body["completed_count"] == 0
        assert body["pending_count"] == 2
        assert [it["status"] for it in body["items"]] == ["idle", "idle"]
        assert all(it["completed_at"] is None for it in body["items"])


@pytest.mark.asyncio
async def test_retry_reordered_same_set_replays_first_order():
    async with await _client() as c:
        h1, _, _, _ = await make_handoff(c, "a")
        h2, _, _, _ = await make_handoff(c, "b")
        h3, _, _, _ = await make_handoff(c, "c")

        first = await put_snapshot(c, "idem-reorder", [h1, h2, h3])
        assert first.status_code == 201

        # 重排顺序：仍是同一份列表，回放首次快照（含首次条目顺序）
        retry = await put_snapshot(c, "idem-reorder", [h3, h1, h2])
        assert retry.status_code == 200
        body = retry.json()
        assert body["replayed"] is True
        assert [it["handoff_id"] for it in body["items"]] == [h1, h2, h3]


@pytest.mark.asyncio
async def test_conflict_when_same_batch_uses_different_list():
    async with await _client() as c:
        h1, _, _, _ = await make_handoff(c, "a")
        h2, _, _, _ = await make_handoff(c, "b")

        first = await put_snapshot(c, "conflict-batch", [h1])
        assert first.status_code == 201
        first_body = first.json()

        conflict = await put_snapshot(c, "conflict-batch", [h1, h2])
        assert conflict.status_code == 409

        # 不同子集同样冲突
        conflict2 = await put_snapshot(c, "conflict-batch", [h2])
        assert conflict2.status_code == 409

        # 首次快照未被改写，且原列表仍可正常回放
        replay = await put_snapshot(c, "conflict-batch", [h1])
        assert replay.status_code == 200
        assert replay.json()["created_at"] == first_body["created_at"]
        assert [it["handoff_id"] for it in replay.json()["items"]] == [h1]


# ---------------------------------------------------------------------------
# 整批失败 / 不占用批次标识
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_handoff_fails_whole_batch_and_does_not_occupy_batch():
    async with await _client() as c:
        h1, _, _, _ = await make_handoff(c, "a")
        ghost = "0" * 32

        # 未知单夹在中间：整批 404
        r = await put_snapshot(c, "reusable-batch", [h1, ghost])
        assert r.status_code == 404, r.text
        assert ghost in r.json()["detail"]

        # 批次标识未被占用：同一批次标识携合法列表仍可首次提交
        ok = await put_snapshot(c, "reusable-batch", [h1])
        assert ok.status_code == 201
        assert ok.json()["replayed"] is False

        # Redis 中也不存在失败尝试残留
        keys = await app.state.recon.redis.keys("recon:reusable-batch")
        # 失败发生在写入之前；成功后只剩一个批次键
        assert keys == ["recon:reusable-batch"]


@pytest.mark.asyncio
async def test_request_validation_422():
    async with await _client() as c:
        assert (await put_snapshot(c, "b", [])).status_code == 422
        assert (await put_snapshot(c, "b", ["   "])).status_code == 422
        h = "a" * 32
        assert (await put_snapshot(c, "b", [h, h])).status_code == 422
        # 去空白 / 去重后的重复也拒绝
        assert (await put_snapshot(c, "b", [h, f"  {h}  "])).status_code == 422


# ---------------------------------------------------------------------------
# 已保存快照不改写 + GET
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_snapshot_is_frozen_against_later_changes():
    async with await _client() as c:
        h1, _, _, _ = await make_handoff(c, "a")
        h2, _, _, _ = await make_handoff(c, "b")
        first = (await put_snapshot(c, "freeze-me", [h1, h2])).json()

        # 之后认领并完成两单
        for h in (h1, h2):
            await claim(c, h, "fin")
            assert (await complete(c, h, "fin", 1)).status_code == 200

        got = await c.get("/reconciliation-snapshots/freeze-me")
        assert got.status_code == 200
        body = got.json()
        assert body["replayed"] is False
        assert body["created_at"] == first["created_at"]
        # 仍是首次固化的未完成视图
        assert body["completed_count"] == 0
        assert body["pending_count"] == 2
        assert [it["status"] for it in body["items"]] == ["idle", "idle"]
        assert all(it["completed_at"] is None for it in body["items"])


@pytest.mark.asyncio
async def test_get_unknown_batch_404():
    async with await _client() as c:
        r = await c.get("/reconciliation-snapshots/no-such-batch")
        assert r.status_code == 404


@pytest.mark.asyncio
async def test_snapshot_key_has_no_ttl():
    async with await _client() as c:
        h, _, _, _ = await make_handoff(c, "a")
        await put_snapshot(c, "persist-batch", [h])
        r = app.state.recon.redis
        assert await r.ttl("recon:persist-batch") == -1


# ---------------------------------------------------------------------------
# 并发：读取与保存在同一原子脚本内，批次内部不会跨时刻拼接
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_completion_cannot_mix_inconsistent_states():
    """批量读取与多个并发完成同时发生：快照是单一 Redis 瞬间的自洽视图。

    原子实现下，每个 completed 条目必带完成时刻且时刻 <= 快照时刻；每个
    未完成条目必无完成时刻；计数与条目一致。不会出现“某条目按完成后读取、
    另一条目按完成前读取”造成的计数错乱。多轮重复提高竞成交错覆盖率。
    """
    async with await _client() as c:
        for attempt in range(8):
            handoffs = []
            for i in range(6):
                h, _, _, _ = await make_handoff(c, f"n{attempt}-{i}")
                await claim(c, h, "fin")
                handoffs.append(h)

            batch = f"race-{attempt}"
            complete_reqs = [complete(c, h, "fin", 1) for h in handoffs]
            put_req = put_snapshot(c, batch, handoffs)
            results = await asyncio.gather(put_req, *complete_reqs)
            snap_resp = results[0]
            assert snap_resp.status_code == 201, snap_resp.text
            assert all(r.status_code == 200 for r in results[1:])
            snap = snap_resp.json()

            completed_items = [it for it in snap["items"] if it["status"] == "completed"]
            pending_items = [it for it in snap["items"] if it["status"] != "completed"]
            assert snap["completed_count"] == len(completed_items)
            assert snap["pending_count"] == len(pending_items)
            assert len(completed_items) + len(pending_items) == 6

            created = datetime.fromisoformat(snap["created_at"])
            for it in completed_items:
                assert it["completed_at"] is not None
                # 完成先于快照读取：完成时刻不得晚于快照时刻
                assert datetime.fromisoformat(it["completed_at"]) <= created
            for it in pending_items:
                assert it["status"] in ("idle", "leased")
                assert it["completed_at"] is None

            # 全部完成落定后：实时交接单全为 completed；已固化快照不再改写
            for h in handoffs:
                assert (await c.get(f"/handoffs/{h}")).json()["status"] == "completed"
            replay = (await put_snapshot(c, batch, handoffs)).json()
            assert replay["completed_count"] == snap["completed_count"]
            assert replay["items"] == snap["items"]


@pytest.mark.asyncio
async def test_concurrent_first_submits_same_batch_only_one_wins():
    """同批次并发首提（相同列表）：全部返回同一份首次快照。"""
    async with await _client() as c:
        h1, _, _, _ = await make_handoff(c, "a")
        rs = await asyncio.gather(
            *(put_snapshot(c, "race-first", [h1]) for _ in range(8))
        )
        assert {r.status_code for r in rs} <= {200, 201}
        created_at = {r.json()["created_at"] for r in rs}
        assert len(created_at) == 1
        # 恰好一个 201，其余回放 200
        assert sum(1 for r in rs if r.status_code == 201) == 1
        assert all(
            [it["handoff_id"] for it in r.json()["items"]] == [h1] for r in rs
        )


# ---------------------------------------------------------------------------
# OpenAPI / 根端点
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recon_endpoints_in_openapi():
    async with await _client() as c:
        schema = (await c.get("/openapi.json")).json()
        path = "/reconciliation-snapshots/{batch_id}"
        assert path in schema["paths"]
        assert "put" in schema["paths"][path]
        assert "get" in schema["paths"][path]
        for name in ("ReconSnapshotRequest", "ReconItemView", "ReconciliationSnapshotView"):
            assert name in schema["components"]["schemas"], name
        root = (await c.get("/")).json()
        assert root["endpoints"]["put_recon_snapshot"] == f"PUT {path}"
        assert root["endpoints"]["get_recon_snapshot"] == f"GET {path}"
