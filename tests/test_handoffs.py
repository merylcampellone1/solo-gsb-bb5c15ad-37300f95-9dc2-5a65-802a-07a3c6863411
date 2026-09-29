"""达标结果交接单 handoff 端到端集成测试：需要真实 Redis（REDIS_URL，默认
redis://localhost:6399/15）。租约时长由 HANDOFF_LEASE_S 控制，测试取短租约。

运行：
    REDIS_URL=redis://localhost:6399/15 \
    HANDOFF_LEASE_S=1 \
    BARRIER_POLL_INTERVAL_S=0.05 python3 -m pytest -s tests/test_handoffs.py
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timedelta, timezone

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

# 必须在导入 app 前固定配置（短租约，便于到期接管测试）
os.environ.setdefault("REDIS_URL", "redis://localhost:6399/15")
os.environ.setdefault("HANDOFF_LEASE_S", "1")
os.environ.setdefault("BARRIER_POLL_INTERVAL_S", "0.05")

from app.main import app  # noqa: E402

API = "http://test"


@pytest.fixture(autouse=True)
async def _lifespan():
    async with LifespanManager(app):
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


async def checkin(client, bid, pid):
    return await client.post(f"/barriers/{bid}/checkin", json={"participant_id": pid})


async def reach(client, participants, threshold=None, ttl_s=30):
    """建屏障并报到到达标，返回 (bid, quorum, frozen_at)。"""
    b = await create_barrier(client, participants, threshold=threshold, ttl_s=ttl_s)
    bid = b["barrier_id"]
    n = threshold if threshold is not None else len(participants)
    for pid in participants[: n - 1]:
        await checkin(client, bid, pid)
    last = await checkin(client, bid, participants[n - 1])
    assert last.json()["status"] == "reached", last.text
    quorum = last.json()["quorum"]
    frozen_at = last.json()["frozen_at"]
    return bid, quorum, frozen_at


async def create_handoff(client, bid):
    return await client.post("/handoffs", json={"barrier_id": bid})


async def claim(client, hid, owner):
    return await client.post(f"/handoffs/{hid}/claim", json={"owner": owner})


async def renew(client, hid, owner, fence):
    return await client.post(
        f"/handoffs/{hid}/renew", json={"owner": owner, "fence": fence}
    )


async def complete(client, hid, owner, fence):
    return await client.post(
        f"/handoffs/{hid}/complete", json={"owner": owner, "fence": fence}
    )


# ---------------------------------------------------------------------------
# 创建
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_handoff_copies_quorum_and_frozen_at():
    """已达标屏障创建交接单：201，快照为冻结的 quorum/frozen_at，初始 idle。"""
    async with await _client() as c:
        bid, quorum, frozen_at = await reach(c, ["a", "b", "c"], threshold=2)
        r = await create_handoff(c, bid)
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["barrier_id"] == bid
        assert body["status"] == "idle"
        assert body["quorum"] == quorum
        assert body["frozen_at"] == frozen_at
        assert body["owner"] is None and body["fence"] is None
        assert body["expires_at"] is None and body["completed_at"] is None
        assert body["leased_at"] is None
        assert len(body["handoff_id"]) == 32


@pytest.mark.asyncio
async def test_duplicate_create_returns_same_handoff_200():
    """同一屏障重复创建返回原单（200）；并发重复也只有一单。"""
    async with await _client() as c:
        bid, quorum, _ = await reach(c, ["a", "b"], threshold=1)
        r1 = await create_handoff(c, bid)
        assert r1.status_code == 201
        hid = r1.json()["handoff_id"]

        r2 = await create_handoff(c, bid)
        assert r2.status_code == 200
        assert r2.json()["handoff_id"] == hid

        # 并发重复创建：全部返回同一个 id，绝不产生两单
        rs = await asyncio.gather(*(create_handoff(c, bid) for _ in range(6)))
        assert {r.status_code for r in rs} <= {200}
        assert {r.json()["handoff_id"] for r in rs} == {hid}
        assert all(r.json()["quorum"] == quorum for r in rs)


@pytest.mark.asyncio
async def test_create_handoff_404_for_missing_barrier():
    async with await _client() as c:
        r = await create_handoff(c, "deadbeefdeadbeef")
        assert r.status_code == 404


@pytest.mark.asyncio
async def test_create_handoff_409_when_not_reached():
    """waiting / expired / cancelled 三种未达标情形均 409，且不产生交接单。"""
    async with await _client() as c:
        # waiting
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=30)
        await checkin(c, b["barrier_id"], "a")
        r = await create_handoff(c, b["barrier_id"])
        assert r.status_code == 409, r.text

        # cancelled
        b2 = await create_barrier(c, ["x", "y"], threshold=2, ttl_s=30)
        rc = await c.post(
            f"/barriers/{b2['barrier_id']}/cancel", json={"reason": "停"}
        )
        assert rc.status_code == 200
        r = await create_handoff(c, b2["barrier_id"])
        assert r.status_code == 409

        # expired
        b3 = await create_barrier(c, ["p", "q"], threshold=2, ttl_s=1)
        await asyncio.sleep(1.2)
        r = await create_handoff(c, b3["barrier_id"])
        assert r.status_code == 409


@pytest.mark.asyncio
async def test_create_handoff_request_validation_422():
    async with await _client() as c:
        assert (await c.post("/handoffs", json={"barrier_id": "  "})).status_code == 422
        assert (await c.post("/handoffs", json={})).status_code == 422
        # 首尾空白会被去除
        r = await c.post("/handoffs", json={"barrier_id": " deadbeefdeadbeef "})
        assert r.status_code == 404  # 清洗后查不到屏障


# ---------------------------------------------------------------------------
# 认领 / 续租 / 完成基本流程
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_claim_returns_fence_and_expiry_then_get_reflects_lease():
    async with await _client() as c:
        bid, quorum, frozen_at = await reach(c, ["a", "b"], threshold=1)
        hid = (await create_handoff(c, bid)).json()["handoff_id"]

        t0 = time.time()
        r = await claim(c, hid, "worker-1")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["status"] == "leased"
        assert body["owner"] == "worker-1"
        assert body["fence"] == 1  # 首次认领 fence 从 1 开始
        leased_at = datetime.fromisoformat(body["leased_at"])
        expires_at = datetime.fromisoformat(body["expires_at"])
        assert leased_at.tzinfo is not None
        # HANDOFF_LEASE_S=1：到期时间约为起租后 1 秒
        assert 0.9 < (expires_at - leased_at).total_seconds() < 1.3
        assert expires_at > datetime.now(timezone.utc)
        assert time.time() - t0 < 2

        g = (await c.get(f"/handoffs/{hid}")).json()
        assert g["status"] == "leased"
        assert g["owner"] == "worker-1" and g["fence"] == 1
        assert g["quorum"] == quorum and g["frozen_at"] == frozen_at
        assert g["expires_at"] == body["expires_at"]
        assert g["completed_at"] is None


@pytest.mark.asyncio
async def test_renew_requires_matching_owner_and_fence():
    async with await _client() as c:
        bid, _, _ = await reach(c, ["a"], threshold=1)
        hid = (await create_handoff(c, bid)).json()["handoff_id"]
        grant = (await claim(c, hid, "w1")).json()
        fence = grant["fence"]

        # owner 不符
        r = await renew(c, hid, "w2", fence)
        assert r.status_code == 409
        # fence 错误
        r = await renew(c, hid, "w1", fence + 99)
        assert r.status_code == 409
        r = await renew(c, hid, "w1", 0)
        assert r.status_code == 422  # ge=1

        # 正确凭证续租成功，fence 不变，到期时间顺延
        old_expires = datetime.fromisoformat(grant["expires_at"])
        await asyncio.sleep(0.3)
        r = await renew(c, hid, "w1", fence)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["status"] == "leased" and body["owner"] == "w1"
        assert body["fence"] == fence
        assert datetime.fromisoformat(body["expires_at"]) > old_expires


@pytest.mark.asyncio
async def test_complete_requires_valid_credentials_and_is_terminal():
    async with await _client() as c:
        bid, _, _ = await reach(c, ["a"], threshold=1)
        hid = (await create_handoff(c, bid)).json()["handoff_id"]
        grant = (await claim(c, hid, "w1")).json()
        fence = grant["fence"]

        # 错误 owner / fence -> 409
        assert (await complete(c, hid, "w2", fence)).status_code == 409
        assert (await complete(c, hid, "w1", fence + 1)).status_code == 409

        r = await complete(c, hid, "w1", fence)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["status"] == "completed"
        assert body["owner"] == "w1" and body["fence"] == fence
        completed_at = body["completed_at"]
        assert datetime.fromisoformat(completed_at).tzinfo is not None

        g = (await c.get(f"/handoffs/{hid}")).json()
        assert g["status"] == "completed"
        assert g["completed_at"] == completed_at
        assert g["owner"] is None and g["fence"] is None  # 终态不暴露租约

        # 完成后永久拒绝再次认领 / 续租 / 完成
        assert (await claim(c, hid, "wX")).status_code == 409
        assert (await renew(c, hid, "w1", fence)).status_code == 409
        assert (await complete(c, hid, "w1", fence)).status_code == 409
        # 状态依然 completed，时间不变
        g2 = (await c.get(f"/handoffs/{hid}")).json()
        assert g2["status"] == "completed" and g2["completed_at"] == completed_at


@pytest.mark.asyncio
async def test_unknown_handoff_returns_404():
    async with await _client() as c:
        assert (await c.get("/handoffs/deadbeefdeadbeef")).status_code == 404
        assert (await claim(c, "deadbeefdeadbeef", "w")).status_code == 404
        assert (await renew(c, "deadbeefdeadbeef", "w", 1)).status_code == 404
        assert (await complete(c, "deadbeefdeadbeef", "w", 1)).status_code == 404


@pytest.mark.asyncio
async def test_claim_validation_422():
    async with await _client() as c:
        bid, _, _ = await reach(c, ["a"], threshold=1)
        hid = (await create_handoff(c, bid)).json()["handoff_id"]
        assert (await c.post(f"/handoffs/{hid}/claim", json={"owner": " "})).status_code == 422
        assert (await c.post(f"/handoffs/{hid}/claim", json={})).status_code == 422


# ---------------------------------------------------------------------------
# 竞争认领、到期接管、旧凭证拒绝
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_claim_only_one_wins():
    """空闲单并发认领：只有一个请求成为 fence=1 的持有者，其余 409 busy。"""
    async with await _client() as c:
        bid, _, _ = await reach(c, ["a"], threshold=1)
        hid = (await create_handoff(c, bid)).json()["handoff_id"]

        results = await asyncio.gather(
            *(claim(c, hid, f"w{i}") for i in range(8))
        )
        oks = [r for r in results if r.status_code == 200]
        conflicts = [r for r in results if r.status_code == 409]
        assert len(oks) == 1, [r.status_code for r in results]
        assert len(conflicts) == 7
        winner = oks[0].json()
        assert winner["fence"] == 1
        # 败者文案携带当前持有者
        assert winner["owner"] in conflicts[0].json()["detail"]

        g = (await c.get(f"/handoffs/{hid}")).json()
        assert g["owner"] == winner["owner"] and g["fence"] == 1


@pytest.mark.asyncio
async def test_takeover_after_expiry_increments_fence_and_rejects_old_credential():
    """租约到期后他人可接管：fence 单调递增；旧持有者的旧 fence 续租/完成均 409。"""
    async with await _client() as c:
        bid, _, _ = await reach(c, ["a"], threshold=1)
        hid = (await create_handoff(c, bid)).json()["handoff_id"]

        g1 = (await claim(c, hid, "old")).json()
        assert g1["fence"] == 1

        # 租约内他人认领 -> busy 409
        busy = await claim(c, hid, "new")
        assert busy.status_code == 409
        assert "old" in busy.json()["detail"]

        # 等到租约到期（HANDOFF_LEASE_S=1）
        await asyncio.sleep(1.15)

        # GET 已显示空闲（不泄露旧凭证），但 fence 在 Redis 内延续
        g = (await c.get(f"/handoffs/{hid}")).json()
        assert g["status"] == "idle"
        assert g["owner"] is None and g["fence"] is None

        # 旧持有者在到期后续租 / 完成：租约已失效
        assert (await renew(c, hid, "old", 1)).status_code == 409
        assert (await complete(c, hid, "old", 1)).status_code == 409

        # 到期接管成功，fence=2
        g2 = (await claim(c, hid, "new"))
        assert g2.status_code == 200, g2.text
        grant2 = g2.json()
        assert grant2["owner"] == "new" and grant2["fence"] == 2

        # 旧 fence（1）即使 owner 名字相同也被拒
        assert (await renew(c, hid, "new", 1)).status_code == 409
        # 旧 owner 拿新 fence 也被拒（owner 不符）
        assert (await renew(c, hid, "old", 2)).status_code == 409
        # 正确新凭证续租成功
        assert (await renew(c, hid, "new", 2)).status_code == 200


@pytest.mark.asyncio
async def test_renew_keeps_lease_alive_across_expiry_boundary():
    """及时续租可不断推迟到期；一旦停止续租仍会到期被接管，fence 继续递增。"""
    async with await _client() as c:
        bid, _, _ = await reach(c, ["a"], threshold=1)
        hid = (await create_handoff(c, bid)).json()["handoff_id"]
        fence = (await claim(c, hid, "keeper")).json()["fence"]

        # 在 1s 租约内续租两次，跨过原始到期点仍持有
        await asyncio.sleep(0.5)
        r1 = await renew(c, hid, "keeper", fence)
        assert r1.status_code == 200
        await asyncio.sleep(0.6)  # 已越过原 1s 到期点，但 0.5s 时续过租
        r2 = await renew(c, hid, "keeper", fence)
        assert r2.status_code == 200
        g = (await c.get(f"/handoffs/{hid}")).json()
        assert g["status"] == "leased" and g["fence"] == fence

        # 停止续租，到期后可被接管为 fence=2
        await asyncio.sleep(1.2)
        takeover = await claim(c, hid, "other")
        assert takeover.status_code == 200
        assert takeover.json()["fence"] == fence + 1


@pytest.mark.asyncio
async def test_concurrent_takeover_after_expiry_only_one_wins():
    """到期瞬间多个认领并发：只有一个接管成功，fence 唯一递增。"""
    async with await _client() as c:
        winners = set()
        for i in range(10):
            bid, _, _ = await reach(c, ["a"], threshold=1)
            hid = (await create_handoff(c, bid)).json()["handoff_id"]
            first = (await claim(c, hid, f"orig{i}")).json()
            assert first["fence"] == 1
            await asyncio.sleep(1.15)

            results = await asyncio.gather(
                *(claim(c, hid, f"t{i}-{j}") for j in range(5))
            )
            oks = [r for r in results if r.status_code == 200]
            assert len(oks) == 1, [r.status_code for r in results]
            grant = oks[0].json()
            assert grant["fence"] == 2
            winners.add(grant["owner"])

            # 原持有者旧凭证完成必败
            assert (await complete(c, hid, f"orig{i}", 1)).status_code == 409
        # 各轮赢家不同侧面证明接管确实随机落到竞争者
        assert len(winners) >= 2


@pytest.mark.asyncio
async def test_fence_increments_each_takeover_and_final_owner_completes():
    """多次到期接管：fence 1 -> 2 -> 3 单调递增；只有最新 fence 的持有者可完成。"""
    async with await _client() as c:
        bid, _, _ = await reach(c, ["a"], threshold=1)
        hid = (await create_handoff(c, bid)).json()["handoff_id"]

        f1 = (await claim(c, hid, "w1")).json()["fence"]
        assert f1 == 1
        await asyncio.sleep(1.15)
        f2 = (await claim(c, hid, "w2")).json()["fence"]
        assert f2 == 2
        await asyncio.sleep(1.15)
        f3 = (await claim(c, hid, "w3")).json()["fence"]
        assert f3 == 3

        # 只有 w3/f3 能完成
        assert (await complete(c, hid, "w2", f2)).status_code == 409
        r = await complete(c, hid, "w3", f3)
        assert r.status_code == 200
        assert r.json()["status"] == "completed"


# ---------------------------------------------------------------------------
# 独立持久保存
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handoff_survives_barrier_reclamation():
    """屏障键被回收（删除）后交接单仍完整可查、可走完全部租约流程；
    “屏障 -> 交接单”映射独立保留：重复创建仍返回原单，快照不回读屏障。"""
    async with await _client() as c:
        bid, quorum, frozen_at = await reach(c, ["a", "b"], threshold=2)
        created = await create_handoff(c, bid)
        hid = created.json()["handoff_id"]

        # 模拟屏障到期回收：删除全部屏障键（保留 handoff:* 键空间）
        r = app.state.store.redis
        barrier_keys = [
            f"barrier:{bid}",
            f"barrier:{bid}:roster",
            f"barrier:{bid}:checked",
            f"barrier:{bid}:frozen",
            f"barrier:{bid}:events",
            f"barrier:{bid}:cancel",
            f"barrier:{bid}:gmap",
            f"barrier:{bid}:gcheck",
        ]
        await r.delete(*barrier_keys)

        # 屏障本身已不存在：GET 屏障 404
        assert (await c.get(f"/barriers/{bid}")).status_code == 404

        # 交接单本体与快照仍在，不依赖任何屏障键
        g = (await c.get(f"/handoffs/{hid}")).json()
        assert g["barrier_id"] == bid
        assert g["status"] == "idle"
        assert g["quorum"] == quorum
        assert g["frozen_at"] == frozen_at

        # 独立保留的映射保证同一屏障重复创建仍返回原单（200，原 id 与快照）
        repost = await create_handoff(c, bid)
        assert repost.status_code == 200
        assert repost.json()["handoff_id"] == hid
        assert repost.json()["quorum"] == quorum

        # 认领 / 续租 / 完成全流程不受屏障回收影响
        grant = (await claim(c, hid, "finalizer")).json()
        assert grant["fence"] == 1
        assert (await renew(c, hid, "finalizer", 1)).status_code == 200
        done = await complete(c, hid, "finalizer", 1)
        assert done.status_code == 200
        assert (await c.get(f"/handoffs/{hid}")).json()["status"] == "completed"


@pytest.mark.asyncio
async def test_handoff_keys_have_no_ttl():
    """交接单本体与屏障映射键不设置 TTL（区别于屏障键）。"""
    async with await _client() as c:
        bid, _, _ = await reach(c, ["a"], threshold=1)
        hid = (await create_handoff(c, bid)).json()["handoff_id"]
        r = app.state.store.redis
        assert await r.ttl(f"handoff:{hid}") == -1
        assert await r.ttl(f"handoff:bybarrier:{bid}") == -1


# ---------------------------------------------------------------------------
# OpenAPI / 根端点
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handoff_endpoints_in_openapi():
    async with await _client() as c:
        schema = (await c.get("/openapi.json")).json()
        for path in (
            "/handoffs",
            "/handoffs/{handoff_id}",
            "/handoffs/{handoff_id}/claim",
            "/handoffs/{handoff_id}/renew",
            "/handoffs/{handoff_id}/complete",
        ):
            assert path in schema["paths"], path
        for name in (
            "CreateHandoffRequest",
            "ClaimRequest",
            "RenewRequest",
            "CompleteRequest",
            "HandoffView",
            "ClaimResponse",
            "RenewResponse",
            "CompleteHandoffResponse",
        ):
            assert name in schema["components"]["schemas"], name
        root = (await c.get("/")).json()
        assert root["endpoints"]["create_handoff"] == "POST /handoffs"
        assert root["endpoints"]["claim_handoff"] == "POST /handoffs/{handoff_id}/claim"
