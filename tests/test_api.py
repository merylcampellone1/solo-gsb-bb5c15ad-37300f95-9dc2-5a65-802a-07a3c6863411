"""端到端集成测试：需要真实 Redis（REDIS_URL，默认 redis://localhost:6399）。

运行：
    REDIS_URL=redis://localhost:6399/0 \
    BARRIER_POLL_INTERVAL_S=0.05 python3 -m pytest -s tests/
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

# 在导入 app 前固定配置
os.environ.setdefault("REDIS_URL", "redis://localhost:6399/15")
os.environ.setdefault("BARRIER_POLL_INTERVAL_S", "0.05")

from app.main import app  # noqa: E402

API = "http://test"


@pytest.fixture(autouse=True)
async def _lifespan():
    async with LifespanManager(app):
        yield


async def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url=API)


async def create_barrier(
    client, participants, threshold=None, ttl_s=5, groups=None, quotas=None
):
    deadline = (datetime.now(timezone.utc) + timedelta(seconds=ttl_s)).isoformat()
    body = {
        "participants": participants,
        "threshold": threshold,
        "deadline": deadline,
    }
    if groups is not None:
        body["participant_groups"] = groups
    if quotas is not None:
        body["group_thresholds"] = quotas
    r = await client.post("/barriers", json=body)
    assert r.status_code == 201, r.text
    return r.json()


async def checkin(client: AsyncClient, bid: str, pid: str):
    return await client.post(f"/barriers/{bid}/checkin", json={"participant_id": pid})


async def cancel(client: AsyncClient, bid: str, reason: str):
    return await client.post(f"/barriers/{bid}/cancel", json={"reason": reason})


async def replace(client: AsyncClient, bid: str, original: str, replacement: str):
    return await client.post(
        f"/barriers/{bid}/replace",
        json={"participant_id": original, "replacement_id": replacement},
    )


async def batch_checkin(client: AsyncClient, bid: str, participants, key: str):
    return await client.post(
        f"/barriers/{bid}/batch-checkins",
        json={"participants": participants, "idempotency_key": key},
    )


@pytest.mark.asyncio
async def test_health_and_root():
    async with await _client() as c:
        r = await c.get("/health")
        assert r.status_code == 200 and r.json()["status"] == "ok"
        r = await c.get("/")
        assert r.status_code == 200 and "endpoints" in r.json()


@pytest.mark.asyncio
async def test_create_validations():
    async with await _client() as c:
        # threshold 大于人数
        r = await c.post(
            "/barriers",
            json={
                "participants": ["a"],
                "threshold": 2,
                "deadline": "2030-01-01T00:00:00Z",
            },
        )
        assert r.status_code == 422
        # deadline 已过去
        r = await c.post(
            "/barriers",
            json={
                "participants": ["a"],
                "deadline": "2000-01-01T00:00:00Z",
            },
        )
        assert r.status_code == 422
        # 名单去重
        r = await c.post(
            "/barriers",
            json={
                "participants": ["a", "a", " b ", "b"],
                "deadline": "2030-01-01T00:00:00Z",
            },
        )
        assert r.status_code == 201
        body = r.json()
        assert body["participants"] == ["a", "b"]
        assert body["threshold"] == 2  # 默认全员


@pytest.mark.asyncio
async def test_idempotent_checkin_and_non_member():
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=10)
        bid = b["barrier_id"]

        r1 = await checkin(c, bid, "a")
        assert r1.status_code == 200 and r1.json()["status"] == "checked_in"
        r2 = await checkin(c, bid, "a")
        assert r2.status_code == 200
        assert r2.json()["status"] == "already_checked_in"

        g = await c.get(f"/barriers/{bid}")
        assert g.json()["checked_in"] == ["a"]
        assert g.json()["checked_count"] == 1

        r3 = await checkin(c, bid, "outsider")
        assert r3.status_code == 403
        r4 = await c.get("/barriers/deadbeefdeadbeef/wait")
        assert r4.status_code == 404


@pytest.mark.asyncio
async def test_concurrent_reach_freezes_snapshot_exactly_once():
    """6 人屏障，达标人数 4：6 个并发报到必须且只能有一个触发 reached，
    冻结 quorum 唯一确定，其余为 late。"""
    async with await _client() as c:
        members = [f"m{i}" for i in range(6)]
        b = await create_barrier(c, members, threshold=4, ttl_s=10)
        bid = b["barrier_id"]

        results = await asyncio.gather(*(checkin(c, bid, m) for m in members))
        assert all(r.status_code == 200 for r in results), results

        bodies = [r.json() for r in results]
        triggered = [x for x in bodies if x["triggered"]]
        assert len(triggered) == 1, f"应有且仅有一个 triggered，实际 {triggered}"

        reachers = [x for x in bodies if x["status"] == "reached"]
        assert len(reachers) == 1
        quorum = reachers[0]["quorum"]
        assert len(quorum) == 4
        frozen_at = reachers[0]["frozen_at"]

        # 冻结后任何视图、任何后续报到都不能改写 quorum / frozen_at
        for _ in range(3):
            r = await checkin(c, bid, "m5")
            assert r.json()["status"] == "late"
            assert r.json()["triggered"] is False

        # 达标后非名单成员依然被拒绝，且不泄露冻结快照
        r = await checkin(c, bid, "stranger")
        assert r.status_code == 403

        g = await c.get(f"/barriers/{bid}")
        view = g.json()
        assert view["status"] == "reached"
        assert view["quorum"] == quorum
        assert view["frozen_at"] == frozen_at
        assert set(view["checked_in"]) == set(members)
        late = set(view["late"])
        assert late == set(members) - set(quorum)


@pytest.mark.asyncio
async def test_multiple_waiters_all_woken_on_reach():
    """多个等待者并发挂起，达标时必须全部立即拿到同一份冻结快照。"""
    async with await _client() as c:
        members = ["w1", "w2", "w3", "w4"]
        b = await create_barrier(c, members, threshold=2, ttl_s=10)
        bid = b["barrier_id"]

        async def wait_view():
            async with await _client() as c2:
                started = time.time()
                r = await c2.get(f"/barriers/{bid}/wait")
                return time.time() - started, r

        waiters = [asyncio.create_task(wait_view()) for _ in range(4)]
        await asyncio.sleep(0.4)  # 确保都已进入订阅+等待
        await checkin(c, bid, "w1")
        await asyncio.sleep(0.2)
        await checkin(c, bid, "w2")  # 达标

        outcomes = await asyncio.gather(*waiters)
        for elapsed, r in outcomes:
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["status"] == "reached"
            assert sorted(body["quorum"]) == ["w1", "w2"]
            assert elapsed < 5, f"等待者未被及时唤醒: {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_wait_returns_immmediately_when_already_settled():
    async with await _client() as c:
        b = await create_barrier(c, ["x", "y"], threshold=1, ttl_s=10)
        bid = b["barrier_id"]
        await checkin(c, bid, "x")
        t0 = time.time()
        r = await c.get(f"/barriers/{bid}/wait")
        assert r.status_code == 200
        assert r.json()["status"] == "reached"
        assert time.time() - t0 < 2


@pytest.mark.asyncio
async def test_deadline_returns_checked_and_missing_and_rejects_checkin():
    async with await _client() as c:
        members = ["d1", "d2", "d3"]
        b = await create_barrier(c, members, threshold=3, ttl_s=1)
        bid = b["barrier_id"]
        await checkin(c, bid, "d1")

        t0 = time.time()
        r = await c.get(f"/barriers/{bid}/wait")
        elapsed = time.time() - t0
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "expired"
        assert body["checked_in"] == ["d1"]
        assert body["missing"] == ["d2", "d3"]
        assert 0.8 < elapsed < 5, f"应在截止时返回，实际 {elapsed:.2f}s"

        # 截止后报到被拒绝，结果不变
        r2 = await checkin(c, bid, "d2")
        assert r2.status_code == 409
        g = await c.get(f"/barriers/{bid}")
        assert g.json()["status"] == "expired"
        assert g.json()["checked_in"] == ["d1"]
        assert g.json()["missing"] == ["d2", "d3"]


@pytest.mark.asyncio
async def test_waiter_joining_after_reach_gets_snapshot():
    """晚加入的等待者：到达时屏障已达标，应立即得到快照（二次检查路径）。"""
    async with await _client() as c:
        b = await create_barrier(c, ["p", "q"], threshold=1, ttl_s=10)
        bid = b["barrier_id"]
        await checkin(c, bid, "p")
        await asyncio.sleep(0.1)
        r = await c.get(f"/barriers/{bid}/wait")
        assert r.json()["status"] == "reached"
        assert r.json()["quorum"] == ["p"]


async def get_events(client: AsyncClient, bid: str, cursor=0, limit=100):
    return await client.get(
        f"/barriers/{bid}/events", params={"cursor": cursor, "limit": limit}
    )


@pytest.mark.asyncio
async def test_checkin_events_recorded_in_order():
    """首次、重复、达标、迟到四种报到各产生一条有序事件，字段齐全。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=2, ttl_s=10)
        bid = b["barrier_id"]

        # 尚未报到时为空页
        r = await get_events(c, bid)
        assert r.status_code == 200
        assert r.json() == {"barrier_id": bid, "events": [], "next_cursor": 0}

        assert (await checkin(c, bid, "a")).json()["status"] == "checked_in"
        assert (await checkin(c, bid, "a")).json()["status"] == "already_checked_in"
        assert (await checkin(c, bid, "b")).json()["status"] == "reached"
        assert (await checkin(c, bid, "c")).json()["status"] == "late"

        body = (await get_events(c, bid)).json()
        assert body["next_cursor"] == 4
        events = body["events"]
        assert [e["seq"] for e in events] == [1, 2, 3, 4]
        assert [e["participant_id"] for e in events] == ["a", "a", "b", "c"]
        assert [e["result"] for e in events] == [
            "checked_in",
            "already_checked_in",
            "reached",
            "late",
        ]
        for e in events:
            ts = datetime.fromisoformat(e["ts"])
            assert ts.tzinfo is not None  # 带时区的合法时间


@pytest.mark.asyncio
async def test_events_pagination_cursor_limit():
    """cursor/limit 翻页不重不漏，读完后游标不再前进；非法参数 422。"""
    async with await _client() as c:
        members = [f"p{i}" for i in range(5)]
        b = await create_barrier(c, members, threshold=5, ttl_s=10)
        bid = b["barrier_id"]
        for m in members:
            await checkin(c, bid, m)

        # 每页 2 条，翻页读完全部 5 条
        seen, cursor = [], 0
        for _ in range(3):
            body = (await get_events(c, bid, cursor=cursor, limit=2)).json()
            assert len(body["events"]) <= 2
            seen.extend(body["events"])
            cursor = body["next_cursor"]
        assert [e["seq"] for e in seen] == [1, 2, 3, 4, 5]
        assert [e["participant_id"] for e in seen] == members
        assert cursor == 5

        # 读完后为空页，游标保持不动
        body = (await get_events(c, bid, cursor=cursor, limit=2)).json()
        assert body["events"] == []
        assert body["next_cursor"] == 5

        # 从中间游标开始只取右侧事件
        body = (await get_events(c, bid, cursor=3, limit=100)).json()
        assert [e["seq"] for e in body["events"]] == [4, 5]

        # 非法参数与不存在的屏障
        assert (await get_events(c, bid, cursor=-1)).status_code == 422
        assert (await get_events(c, bid, limit=0)).status_code == 422
        assert (await get_events(c, "deadbeefdeadbeef")).status_code == 404


@pytest.mark.asyncio
async def test_failed_checkins_produce_no_events():
    """非名单成员（403）、截止后报到（409）、屏障不存在（404）都不产生事件。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=1)
        bid = b["barrier_id"]

        r = await checkin(c, bid, "outsider")
        assert r.status_code == 403

        await asyncio.sleep(1.2)  # 等到过截止时间
        r = await checkin(c, bid, "a")
        assert r.status_code == 409

        r = await checkin(c, "deadbeefdeadbeef", "a")
        assert r.status_code == 404

        body = (await get_events(c, bid)).json()
        assert body["events"] == []
        assert body["next_cursor"] == 0


@pytest.mark.asyncio
async def test_concurrent_checkins_events_no_gap_no_dup():
    """并发报到：事件总数等于成功报到次数，seq 恰好为 1..N 连续不重不漏。"""
    async with await _client() as c:
        members = [f"m{i}" for i in range(8)]
        b = await create_barrier(c, members, threshold=4, ttl_s=10)
        bid = b["barrier_id"]

        # 8 个成员首报 + 2 个并发重复报到，共 10 个并发请求
        reqs = [checkin(c, bid, m) for m in members]
        reqs += [checkin(c, bid, "m0"), checkin(c, bid, "m1")]
        results = await asyncio.gather(*reqs)
        assert all(r.status_code == 200 for r in results)

        events = (await get_events(c, bid, limit=100)).json()["events"]
        assert len(events) == 10
        assert [e["seq"] for e in events] == list(range(1, 11))

        # m0、m1 各出现两次（首报 + 重复），其余成员各一次
        counts = Counter(e["participant_id"] for e in events)
        assert counts["m0"] == 2 and counts["m1"] == 2
        assert all(counts[f"m{i}"] == 1 for i in range(2, 8))

        # 事件结果与报到响应状态一一对应（同一原子操作的产物）
        assert {e["result"] for e in events} <= {
            "checked_in",
            "already_checked_in",
            "reached",
            "late",
        }
        assert sum(1 for e in events if e["result"] == "reached") == 1


@pytest.mark.asyncio
async def test_cancel_basic_then_checkin_conflicts_and_views_expose_it():
    """取消成功：状态 cancelled、保存 reason/cancelled_at；之后报到 409；
    GET 与 wait 都返回取消信息；取消不产生报到事件。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=10)
        bid = b["barrier_id"]
        await checkin(c, bid, "a")

        # 空 / 纯空白 reason 被 422 拒绝
        assert (await cancel(c, bid, "   ")).status_code == 422
        r = await cancel(c, bid, "  上游任务放弃等待  ")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["status"] == "cancelled"
        assert body["reason"] == "上游任务放弃等待"  # 首尾空白被去除
        cancelled_at = body["cancelled_at"]
        assert datetime.fromisoformat(cancelled_at).tzinfo is not None
        assert body["view"]["status"] == "cancelled"
        assert body["view"]["cancel_reason"] == "上游任务放弃等待"
        assert body["view"]["cancelled_at"] == cancelled_at
        # 取消时已有的报到作为快照保留
        assert body["view"]["checked_in"] == ["a"]
        assert body["view"]["checked_count"] == 1

        # 之后任何名单成员报到均 409，且不写入 checked
        r = await checkin(c, bid, "b")
        assert r.status_code == 409
        g = await c.get(f"/barriers/{bid}")
        v = g.json()
        assert v["status"] == "cancelled"
        assert v["cancel_reason"] == "上游任务放弃等待"
        assert v["cancelled_at"] == cancelled_at
        assert v["checked_in"] == ["a"]

        # wait 立即返回取消终态
        t0 = time.time()
        w = await c.get(f"/barriers/{bid}/wait")
        assert w.status_code == 200
        wv = w.json()
        assert wv["status"] == "cancelled"
        assert wv["cancel_reason"] == "上游任务放弃等待"
        assert wv["cancelled_at"] == cancelled_at
        assert time.time() - t0 < 2

        # 取消后的报到不产生审计事件
        events = (await get_events(c, bid)).json()["events"]
        assert [e["result"] for e in events] == ["checked_in"]


@pytest.mark.asyncio
async def test_repeated_cancel_is_idempotent_same_result():
    """重复取消幂等：返回首次保存的 reason/cancelled_at，即使本次 reason 不同。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=10)
        bid = b["barrier_id"]

        r1 = await cancel(c, bid, "first reason")
        assert r1.status_code == 200
        first = r1.json()
        assert first["status"] == "cancelled"

        r2 = await cancel(c, bid, "second reason")
        assert r2.status_code == 200
        second = r2.json()
        assert second["status"] == "already_cancelled"
        assert second["reason"] == "first reason"
        assert second["cancelled_at"] == first["cancelled_at"]
        assert second["view"]["cancelled_at"] == first["cancelled_at"]
        assert second["view"]["cancel_reason"] == "first reason"

        # 终态视图仍是第一次的结果
        g = await c.get(f"/barriers/{bid}")
        assert g.json()["cancel_reason"] == "first reason"
        assert g.json()["cancelled_at"] == first["cancelled_at"]


@pytest.mark.asyncio
async def test_cancel_conflicts_when_reached_or_expired():
    """已达标 → 取消 409 且冻结快照不变；已截止 → 取消 409。"""
    async with await _client() as c:
        # 已达标
        b = await create_barrier(c, ["a", "b"], threshold=1, ttl_s=10)
        bid = b["barrier_id"]
        cr = await checkin(c, bid, "a")
        assert cr.json()["status"] == "reached"
        frozen_at = cr.json()["frozen_at"]

        r = await cancel(c, bid, "too late")
        assert r.status_code == 409, r.text
        g = await c.get(f"/barriers/{bid}")
        assert g.json()["status"] == "reached"
        assert g.json()["frozen_at"] == frozen_at
        assert g.json()["cancel_reason"] is None

        # 已截止
        b2 = await create_barrier(c, ["x", "y"], threshold=2, ttl_s=1)
        bid2 = b2["barrier_id"]
        await asyncio.sleep(1.2)
        r = await cancel(c, bid2, "expired already")
        assert r.status_code == 409
        g = await c.get(f"/barriers/{bid2}")
        assert g.json()["status"] == "expired"
        assert g.json()["cancel_reason"] is None

        # 不存在的屏障 → 404
        assert (await cancel(c, "deadbeefdeadbeef", "nope")).status_code == 404


@pytest.mark.asyncio
async def test_multiple_waiters_all_woken_on_cancel():
    """多个等待者并发挂起，取消时必须全部立即拿到同一份取消结果。"""
    async with await _client() as c:
        members = ["c1", "c2", "c3", "c4"]
        b = await create_barrier(c, members, threshold=4, ttl_s=10)
        bid = b["barrier_id"]
        await checkin(c, bid, "c1")

        async def wait_view():
            async with await _client() as c2:
                started = time.time()
                r = await c2.get(f"/barriers/{bid}/wait")
                return time.time() - started, r

        waiters = [asyncio.create_task(wait_view()) for _ in range(4)]
        await asyncio.sleep(0.4)  # 确保都已进入订阅+等待
        rc = await cancel(c, bid, "stop all")
        assert rc.status_code == 200
        cancelled_at = rc.json()["cancelled_at"]

        outcomes = await asyncio.gather(*waiters)
        for elapsed, r in outcomes:
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["status"] == "cancelled"
            assert body["cancel_reason"] == "stop all"
            assert body["cancelled_at"] == cancelled_at
            assert body["checked_in"] == ["c1"]
            assert elapsed < 5, f"等待者未被及时唤醒: {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_waiter_joining_after_cancel_gets_snapshot():
    """晚加入的等待者：到达时屏障已取消，应立即得到取消快照（二次检查路径）。"""
    async with await _client() as c:
        b = await create_barrier(c, ["p", "q"], threshold=2, ttl_s=10)
        bid = b["barrier_id"]
        await cancel(c, bid, "called off")
        await asyncio.sleep(0.1)
        r = await c.get(f"/barriers/{bid}/wait")
        assert r.json()["status"] == "cancelled"
        assert r.json()["cancel_reason"] == "called off"


@pytest.mark.asyncio
async def test_cancel_and_trigger_checkin_race_exactly_one_terminal_state():
    """取消与达标报到的 Lua 脚本在 Redis 内原子竞争：交替两个脚本的入队顺序，
    两个方向各跑多轮，先执行者成为唯一终态，败者必为冲突，终态绝不被改写。"""
    from app.barrier_store import CHECKIN_SCRIPT, CANCEL_SCRIPT
    from app.main import app

    async with await _client() as c:
        winners = []
        r = app.state.store.redis

        for i in range(40):
            b = await create_barrier(c, ["a", "b"], threshold=1, ttl_s=10)
            bid = b["barrier_id"]
            keys = [
                f"barrier:{bid}",
                f"barrier:{bid}:roster",
                f"barrier:{bid}:checked",
                f"barrier:{bid}:frozen",
                f"barrier:{bid}:events",
                f"barrier:{bid}:cancel",
                f"barrier:{bid}:gmap",
                f"barrier:{bid}:gcheck",
            ]
            now = f"{time.time():.3f}"
            # 两个脚本放入同一个事务：EXEC 时 Redis 按入队顺序串行执行，
            # 交替顺序即可稳定覆盖“先达标”与“先取消”两个方向。
            pipe = r.pipeline(transaction=True)
            if i % 2 == 0:
                pipe.eval(CHECKIN_SCRIPT, 8, *keys, "a", now)
                pipe.eval(CANCEL_SCRIPT, 8, *keys, "race", now)
            else:
                pipe.eval(CANCEL_SCRIPT, 8, *keys, "race", now)
                pipe.eval(CHECKIN_SCRIPT, 8, *keys, "a", now)
            first, second = await pipe.execute()

            g = await c.get(f"/barriers/{bid}")
            status = g.json()["status"]
            assert status in ("reached", "cancelled")

            if i % 2 == 0:
                checkin_raw, cancel_raw = first, second
            else:
                cancel_raw, checkin_raw = first, second

            if status == "reached":
                # 先达标：报到脚本返回 reached；取消脚本返回 reached 冲突
                assert checkin_raw[0] == "reached"
                assert cancel_raw[0] == "reached"
                # 终态不可改写：再取消 409
                assert (await cancel(c, bid, "again")).status_code == 409
            else:
                # 先取消：取消脚本写入 cancelled；报到脚本返回 cancelled 冲突
                assert cancel_raw[0] == "cancelled"
                assert checkin_raw[0] == "cancelled"
                # 终态不可改写：再报到 409
                assert (await checkin(c, bid, "a")).status_code == 409
            winners.append(status)

        assert set(winners) == {"reached", "cancelled"}, Counter(winners)


# ---------------------------------------------------------------------------
# 分组配额（participant_groups / group_thresholds）
# ---------------------------------------------------------------------------


async def _create_grouped(client, **overrides):
    participants = ["a1", "a2", "b1", "b2", "c1"]
    groups = {p: ("A" if p.startswith("a") else "B" if p.startswith("b") else "C")
              for p in participants}
    payload = dict(
        participants=participants,
        threshold=4,
        ttl_s=15,
        groups=groups,
        quotas={"A": 2, "B": 1, "C": 1},
    )
    payload.update(overrides)
    r = await client.post(
        "/barriers",
        json={
            "participants": payload["participants"],
            "threshold": payload["threshold"],
            "deadline": (
                datetime.now(timezone.utc) + timedelta(seconds=payload["ttl_s"])
            ).isoformat(),
            "participant_groups": payload["groups"],
            "group_thresholds": payload["quotas"],
        },
    )
    return r


def _progress(view):
    assert view["group_progress"] is not None
    return {g["group"]: g for g in view["group_progress"]}


@pytest.mark.asyncio
async def test_group_quota_create_validation_422():
    """未知成员、漏分组、非正配额、配额超过组人数、总配额矛盾均返回 422。"""
    async with await _client() as c:
        participants = ["a1", "a2", "b1"]
        groups = {"a1": "A", "a2": "A", "b1": "B"}

        async def create(groups=groups, quotas=None, participants=participants,
                         threshold=3):
            return await c.post(
                "/barriers",
                json={
                    "participants": participants,
                    "threshold": threshold,
                    "deadline": "2030-01-01T00:00:00Z",
                    "participant_groups": groups,
                    "group_thresholds": quotas,
                },
            )

        # 未知成员出现在分组映射里
        r = await create(groups={**groups, "ghost": "A"}, quotas={"A": 2, "B": 1})
        assert r.status_code == 422
        # participant_groups 漏了某参与者
        r = await create(groups={"a1": "A", "a2": "A"}, quotas={"A": 2, "B": 1})
        assert r.status_code == 422
        # 只传其中一个字段
        r = await c.post(
            "/barriers",
            json={
                "participants": participants,
                "deadline": "2030-01-01T00:00:00Z",
                "participant_groups": groups,
            },
        )
        assert r.status_code == 422
        r = await c.post(
            "/barriers",
            json={
                "participants": participants,
                "deadline": "2030-01-01T00:00:00Z",
                "group_thresholds": {"A": 2, "B": 1},
            },
        )
        assert r.status_code == 422
        # 非正配额
        r = await create(quotas={"A": 0, "B": 1})
        assert r.status_code == 422
        r = await create(quotas={"A": -1, "B": 1})
        assert r.status_code == 422
        # 配额超过组人数（A 组只有 2 人）
        r = await create(quotas={"A": 3, "B": 1})
        assert r.status_code == 422
        # 配额引用不存在的分组 / 漏掉已存在的分组
        r = await create(quotas={"A": 2, "X": 1})
        assert r.status_code == 422
        r = await create(quotas={"A": 2})
        assert r.status_code == 422
        # 各组配额之和（2+2=4）超过总 threshold（3）—— 总配额矛盾
        r = await create(quotas={"A": 2, "B": 2}, threshold=3)
        assert r.status_code == 422
        # 空分组名
        r = await create(
            groups={"a1": " ", "a2": "A", "b1": "B"},
            quotas={"A": 1, "B": 1, " ": 1},
        )
        assert r.status_code == 422


@pytest.mark.asyncio
async def test_group_quota_not_reached_until_both_total_and_groups_satisfied():
    """总人数先达标但组配额未满 -> 继续 waiting；补齐短板组后唯一触发 reached。"""
    async with await _client() as c:
        b = await _create_grouped(c)
        assert b.status_code == 201, b.text
        body = b.json()
        bid = body["barrier_id"]
        assert body["participant_groups"] == {
            "a1": "A", "a2": "A", "b1": "B", "b2": "B", "c1": "C",
        }
        prog = _progress(body)
        assert set(prog) == {"A", "B", "C"}
        for name in ("A", "B", "C"):
            assert prog[name]["checked_count"] == 0
            assert prog[name]["satisfied"] is False

        # A 组 2 人 + B 组 1 人 + C 组 1 人 = 4，刚好总人数与各组配额同时满足
        for pid in ("a1", "b1", "c1"):
            r = await checkin(c, bid, pid)
            assert r.json()["status"] == "checked_in"
            assert r.json()["triggered"] is False
        # 第 4 人报到 b2：总数达标（4/4）但 A 组只有 1/2，仍须 waiting
        r = await checkin(c, bid, "b2")  # A=1,B=2,C=1，A 组缺 1
        assert r.status_code == 200
        v = r.json()["view"]
        assert v["status"] == "waiting"
        assert r.json()["triggered"] is False
        prog = _progress(v)
        assert (prog["A"]["checked_count"], prog["A"]["satisfied"]) == (1, False)
        assert (prog["B"]["checked_count"], prog["B"]["satisfied"]) == (2, True)
        assert (prog["C"]["checked_count"], prog["C"]["satisfied"]) == (1, True)
        assert v["checked_count"] == 4  # 总数已达标但仍不能触发

        # 补齐 A 组：本次唯一触发达标
        r = await checkin(c, bid, "a2")
        body = r.json()
        assert body["status"] == "reached"
        assert body["triggered"] is True
        assert sorted(body["quorum"]) == ["a1", "a2", "b1", "b2", "c1"]
        v = body["view"]
        prog = _progress(v)
        assert all(prog[g]["satisfied"] for g in prog)
        assert prog["A"]["checked_count"] == 2
        assert prog["B"]["checked_count"] == 2
        assert prog["C"]["checked_count"] == 1

        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["status"] == "reached"
        assert g["quorum"] == body["quorum"]
        assert all(x["satisfied"] for x in g["group_progress"])


@pytest.mark.asyncio
async def test_group_quota_duplicate_checkins_not_double_counted():
    """组配额未满时重复报到不重复累计；达标后报到为 late 且冻结进度不变。"""
    async with await _client() as c:
        b = (await _create_grouped(c)).json()
        bid = b["barrier_id"]
        await checkin(c, bid, "a1")
        for _ in range(3):
            r = await checkin(c, bid, "a1")
            assert r.json()["status"] == "already_checked_in"
            assert r.json()["triggered"] is False
        v = (await c.get(f"/barriers/{bid}")).json()
        assert v["checked_count"] == 1
        prog = _progress(v)
        assert prog["A"]["checked_count"] == 1  # 重复报到没有重复累计

        # 达标
        for pid in ("a2", "b1", "c1"):
            await checkin(c, bid, pid)
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["status"] == "reached"
        frozen_prog = {x["group"]: x["checked_count"] for x in g["group_progress"]}

        # 冻结后的报到（含已报到成员重复与新成员 b2）均不改写计数与快照
        for pid in ("b2", "a1", "b2"):
            r = await checkin(c, bid, pid)
            assert r.json()["status"] == "late"
            assert r.json()["triggered"] is False
            rv = r.json()["view"]
            late_counts = {x["group"]: x["checked_count"] for x in rv["group_progress"]}
            assert late_counts == frozen_prog
            assert rv["quorum"] == g["quorum"]
        g2 = (await c.get(f"/barriers/{bid}")).json()
        assert g2["quorum"] == g["quorum"]
        assert g2["frozen_at"] == g["frozen_at"]
        assert set(g2["late"]) == {"b2"}


@pytest.mark.asyncio
async def test_group_quota_concurrent_cross_group_single_trigger():
    """跨组并发报到：所有短板在同一波补齐时，仍只有一个请求 triggered=true。"""
    async with await _client() as c:
        participants = ["a1", "a2", "b1", "b2", "c1", "c2"]
        groups = {p: p[0].upper() for p in participants}
        b = await create_barrier(
            c,
            participants,
            threshold=6,
            ttl_s=15,
            groups=groups,
            quotas={"A": 2, "B": 2, "C": 2},
        )
        bid = b["barrier_id"]

        # 先报到 5 人：B 组差 1（b2 未到），总数也差 1
        first = ["a1", "a2", "b1", "c1", "c2"]
        for pid in first:
            await checkin(c, bid, pid)
        v = (await c.get(f"/barriers/{bid}")).json()
        assert v["status"] == "waiting"

        # 同一分组的剩余成员 + 一个重复报到并发：只有 b2 能触发
        reqs = [checkin(c, bid, "b2"), checkin(c, bid, "a1")]
        results = await asyncio.gather(*reqs)
        bodies = [r.json() for r in results]
        triggered = [x for x in bodies if x["triggered"]]
        assert len(triggered) == 1, bodies
        assert triggered[0]["status"] == "reached"
        assert sorted(triggered[0]["quorum"]) == sorted(participants)

        # 更大并发面：全新屏障，所有人同时首报到，只有一个 reached
        b2 = await create_barrier(
            c,
            participants,
            threshold=6,
            ttl_s=15,
            groups=groups,
            quotas={"A": 2, "B": 2, "C": 2},
        )
        bid2 = b2["barrier_id"]
        results = await asyncio.gather(*(checkin(c, bid2, p) for p in participants))
        bodies = [r.json() for r in results]
        assert len([x for x in bodies if x["triggered"]]) == 1
        assert len([x for x in bodies if x["status"] == "reached"]) == 1
        assert {x["status"] for x in bodies} <= {"checked_in", "reached"}


@pytest.mark.asyncio
async def test_group_quota_expired_and_cancelled_snapshot_progress():
    """expired / cancelled 终态也带 group_progress（当时快照计数）。"""
    async with await _client() as c:
        participants = ["a1", "a2", "b1", "b2"]
        groups = {"a1": "A", "a2": "A", "b1": "B", "b2": "B"}

        # 截止：A 组满、B 组缺，配额未全部满足
        b = await create_barrier(
            c, participants, threshold=4, ttl_s=1,
            groups=groups, quotas={"A": 2, "B": 2},
        )
        bid = b["barrier_id"]
        await checkin(c, bid, "a1")
        await checkin(c, bid, "a2")
        await asyncio.sleep(1.2)
        v = (await c.get(f"/barriers/{bid}")).json()
        assert v["status"] == "expired"
        prog = _progress(v)
        assert prog["A"] == {
            "group": "A", "threshold": 2, "checked_count": 2, "satisfied": True,
        }
        assert prog["B"]["checked_count"] == 0 and prog["B"]["satisfied"] is False

        # 取消：部分分组已满足，快照如实呈现
        b = await create_barrier(
            c, participants, threshold=4, ttl_s=15,
            groups=groups, quotas={"A": 2, "B": 1},
        )
        bid = b["barrier_id"]
        await checkin(c, bid, "b1")
        r = await cancel(c, bid, "停")
        assert r.status_code == 200
        prog = _progress(r.json()["view"])
        assert prog["A"]["checked_count"] == 0 and prog["A"]["satisfied"] is False
        assert prog["B"]["checked_count"] == 1 and prog["B"]["satisfied"] is True


@pytest.mark.asyncio
async def test_ungrouped_barrier_keeps_legacy_response_shape():
    """不传新字段时响应中不出现分组信息（group_progress 为 null 序列化省略/为空）。"""
    async with await _client() as c:
        b = await create_barrier(c, ["x", "y", "z"], threshold=2, ttl_s=15)
        bid = b["barrier_id"]
        assert b["group_progress"] is None
        assert b["participant_groups"] is None

        r = await checkin(c, bid, "x")
        assert r.json()["view"]["group_progress"] is None
        r = await checkin(c, bid, "y")
        assert r.json()["status"] == "reached"
        assert r.json()["view"]["group_progress"] is None

        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["group_progress"] is None and g["participant_groups"] is None
        rc = await cancel(
            c,
            (await create_barrier(c, ["x"], threshold=1, ttl_s=15))["barrier_id"],
            "n",
        )
        assert rc.json()["view"]["group_progress"] is None


# ---------------------------------------------------------------------------
# 运行中延期 PATCH /barriers/{id}/deadline
# ---------------------------------------------------------------------------


async def extend_deadline(client: AsyncClient, bid: str, deadline):
    body = deadline if isinstance(deadline, dict) else {"deadline": deadline}
    return await client.patch(f"/barriers/{bid}/deadline", json=body)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


@pytest.mark.asyncio
async def test_extend_deadline_success_returns_full_updated_view():
    """waiting 中延期到更晚时间：200，返回完整 BarrierView，deadline 已更新。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=5)
        bid = b["barrier_id"]
        old_deadline = datetime.fromisoformat(b["deadline"])
        await checkin(c, bid, "a")

        new_dt = datetime.now(timezone.utc) + timedelta(seconds=30)
        r = await extend_deadline(c, bid, _iso(new_dt))
        assert r.status_code == 200, r.text
        view = r.json()
        # 返回的是更新后的完整 BarrierView
        assert view["barrier_id"] == bid
        assert view["status"] == "waiting"
        assert view["participants"] == ["a", "b", "c"]
        assert view["threshold"] == 3
        assert view["checked_in"] == ["a"]
        assert view["checked_count"] == 1
        assert view["quorum"] is None and view["cancel_reason"] is None
        returned = datetime.fromisoformat(view["deadline"])
        assert abs((returned - new_dt).total_seconds()) < 1.0
        assert returned > old_deadline

        # GET 也能读到新 deadline
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["deadline"] == view["deadline"]
        assert g["status"] == "waiting"


@pytest.mark.asyncio
async def test_extend_rejects_earlier_equal_and_past_time_with_409_or_422():
    """新时间早于/等于当前 deadline -> 409；已是过去时间 -> 422。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=10)
        bid = b["barrier_id"]
        # deadline 在 Redis 中按毫秒存储；以 GET 读到的实际值为比较基准
        current = datetime.fromisoformat((await c.get(f"/barriers/{bid}")).json()["deadline"])

        # 更早
        r = await extend_deadline(
            c, bid, _iso(current - timedelta(seconds=5))
        )
        assert r.status_code == 409, r.text
        # 完全相同
        r = await extend_deadline(c, bid, _iso(current))
        assert r.status_code == 409, r.text
        # 过去时间（请求体层面 422）
        r = await extend_deadline(c, bid, "2000-01-01T00:00:00Z")
        assert r.status_code == 422, r.text
        # 被拒后 deadline 原封不动
        g = (await c.get(f"/barriers/{bid}")).json()
        assert datetime.fromisoformat(g["deadline"]) == current
        assert g["status"] == "waiting"


@pytest.mark.asyncio
async def test_extend_conflicts_on_reached_cancelled_and_expired():
    """终态一律拒绝延期且不被复活：reached / cancelled -> 409；expired -> 409。"""
    async with await _client() as c:
        later = _iso(datetime.now(timezone.utc) + timedelta(seconds=60))

        # reached
        b = await create_barrier(c, ["a", "b"], threshold=1, ttl_s=10)
        bid = b["barrier_id"]
        await checkin(c, bid, "a")
        r = await extend_deadline(c, bid, later)
        assert r.status_code == 409
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["status"] == "reached"
        assert g["quorum"] == ["a"]

        # cancelled
        b2 = await create_barrier(c, ["x", "y"], threshold=2, ttl_s=10)
        bid2 = b2["barrier_id"]
        await cancel(c, bid2, "停")
        r = await extend_deadline(c, bid2, later)
        assert r.status_code == 409
        g = (await c.get(f"/barriers/{bid2}")).json()
        assert g["status"] == "cancelled"
        assert g["cancel_reason"] == "停"

        # expired：先延期到一个很晚的值也必须被拒绝，终态不得复活
        b3 = await create_barrier(c, ["p", "q"], threshold=2, ttl_s=1)
        bid3 = b3["barrier_id"]
        await asyncio.sleep(1.2)
        g = (await c.get(f"/barriers/{bid3}")).json()
        assert g["status"] == "expired"
        r = await extend_deadline(c, bid3, later)
        assert r.status_code == 409, r.text
        g = (await c.get(f"/barriers/{bid3}")).json()
        assert g["status"] == "expired"

        # 不存在 -> 404
        r = await extend_deadline(c, "deadbeefdeadbeef", later)
        assert r.status_code == 404


@pytest.mark.asyncio
async def test_concurrent_extensions_only_largest_takes_effect():
    """并发延期以 Redis 实际生效顺序为准：

    - 每个新时间都严格晚于其执行时的当前值 -> 按 Redis 串行顺序全部可成功，
      最终 deadline 为最大者；
    - 一旦最大值已生效，与之相同或更早的并发请求返回 409；
    - 相同的新值重试必为 409，再晚于当前值则可继续延期。
    """
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=10)
        bid = b["barrier_id"]
        base = datetime.now(timezone.utc)
        # 严格递增的一组时间：不论实际执行顺序如何，只有最大者最终留下；
        # 比当前（执行瞬间）更早或相同的请求得到 409。
        offsets = [15, 25, 30, 40, 60]
        new_times = [_iso(base + timedelta(seconds=s)) for s in offsets]

        results = await asyncio.gather(
            *(extend_deadline(c, bid, t) for t in new_times)
        )
        statuses = [r.status_code for r in results]
        assert all(s in (200, 409) for s in statuses), statuses
        # 最大时间的那个请求一定成功（它晚于任何可能的当前值）
        max_body = max(
            results,
            key=lambda r: datetime.fromisoformat(r.json()["deadline"])
            if r.status_code == 200
            else datetime.min.replace(tzinfo=timezone.utc),
        )
        assert max_body.status_code == 200

        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["status"] == "waiting"
        final_deadline = datetime.fromisoformat(g["deadline"])
        expected = max(datetime.fromisoformat(t) for t in new_times)
        assert abs((final_deadline - expected).total_seconds()) < 1.0

        # 同一波并发中提交两个相同的新值（都晚于当前最大值）：
        # 仅先执行的一个成功，另一个必为 409。
        later = _iso(base + timedelta(seconds=120))
        dup = await asyncio.gather(
            extend_deadline(c, bid, later),
            extend_deadline(c, bid, later),
        )
        assert sorted(r.status_code for r in dup) == [200, 409], [
            r.status_code for r in dup
        ]
        assert datetime.fromisoformat((await c.get(f"/barriers/{bid}")).json()["deadline"]) > final_deadline

        # 再拿相同值延期仍是 409（必须严格晚于当前值）
        assert (await extend_deadline(c, bid, later)).status_code == 409
        # 晚于当前值则可继续延期
        r = await extend_deadline(c, bid, _iso(base + timedelta(seconds=180)))
        assert r.status_code == 200, r.text


@pytest.mark.asyncio
async def test_waiter_keeps_waiting_with_old_deadline_after_extension():
    """已挂起的 /wait 在旧截止点不得误报 expired，延期通知使其读到新 deadline
    继续等待；随后达标时仍被正常唤醒并返回 reached。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=2)
        bid = b["barrier_id"]

        async def wait_view():
            async with await _client() as c2:
                started = time.time()
                r = await c2.get(f"/barriers/{bid}/wait")
                return time.time() - started, r

        waiter = asyncio.create_task(wait_view())
        await asyncio.sleep(0.4)  # 确保已订阅并挂起

        # 在旧截止（2s）之前延期到 8s
        new_dt = datetime.now(timezone.utc) + timedelta(seconds=8)
        r = await extend_deadline(c, bid, _iso(new_dt))
        assert r.status_code == 200

        # 越过旧截止点：等待者必须仍在等待（不能误报 expired）
        await asyncio.sleep(2.4)
        assert not waiter.done(), "等待者在旧截止点被错误唤醒"

        # 达标后正常唤醒
        await checkin(c, bid, "a")
        await checkin(c, bid, "b")
        elapsed, r = await asyncio.wait_for(waiter, timeout=5)
        assert r.status_code == 200
        assert r.json()["status"] == "reached"
        assert sorted(r.json()["quorum"]) == ["a", "b"]
        assert elapsed > 2.5  # 确实跨过了旧截止点仍在等


@pytest.mark.asyncio
async def test_waiter_woken_by_extension_and_then_expires_at_new_deadline():
    """延期通知立即唤醒等待者回读新 deadline；若无报到，则在新截止点 expired。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=2)
        bid = b["barrier_id"]

        async def wait_view():
            async with await _client() as c2:
                started = time.time()
                r = await c2.get(f"/barriers/{bid}/wait")
                return time.time() - started, r

        waiter = asyncio.create_task(wait_view())
        await asyncio.sleep(0.4)

        new_dt = datetime.now(timezone.utc) + timedelta(seconds=3)
        r = await extend_deadline(c, bid, _iso(new_dt))
        assert r.status_code == 200

        elapsed, r = await asyncio.wait_for(waiter, timeout=8)
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "expired"
        assert body["missing"] == ["a", "b"]
        # 在旧截止（约 2s）之后才结束，证明用的是新 deadline
        assert elapsed > 2.6, f"过早按旧截止返回: {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_extend_refreshes_ttl_of_all_barrier_keys():
    """延期后所有相关键的存活期都按新 deadline 顺延，不会提前被 Redis 回收。"""
    async with await _client() as c:
        # 用分组屏障：gmap / gcheck 也在建屏障时落键，可一并校验 TTL 顺延。
        # 终态键 frozen / cancel 在 waiting 时本就不存在，不在校验之列。
        participants = ["a1", "a2", "b1"]
        groups = {"a1": "A", "a2": "A", "b1": "B"}
        b = await create_barrier(
            c, participants, threshold=3, ttl_s=1,
            groups=groups, quotas={"A": 1, "B": 1},
        )
        bid = b["barrier_id"]
        await checkin(c, bid, "a1")
        r = app.state.store.redis
        keys = [
            f"barrier:{bid}",
            f"barrier:{bid}:roster",
            f"barrier:{bid}:checked",
            f"barrier:{bid}:events",
            f"barrier:{bid}:gmap",
            f"barrier:{bid}:gcheck",
        ]
        # 六个键均应存在
        for k in keys:
            assert await r.exists(k) == 1
        before = await r.pttl(keys[0])

        new_dt = datetime.now(timezone.utc) + timedelta(seconds=30)
        resp = await extend_deadline(c, bid, _iso(new_dt))
        assert resp.status_code == 200

        for key in keys:
            ttl = await r.pttl(key)
            assert ttl > 29000, f"{key} TTL 未随延期顺延: {ttl}ms"
        after = await r.pttl(keys[0])
        assert after > before

        # 跨过旧截止时间后屏障仍可查询，且仍为 waiting
        await asyncio.sleep(1.3)
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["status"] == "waiting"


@pytest.mark.asyncio
async def test_extend_and_cancel_checkin_races_have_unique_outcome():
    """延期与取消 / 达标报到两两原子竞争：用同一事务固定脚本入队顺序交替执行，
    终态后延期必为 409；先延期后取消则取消生效，终态唯一一致。"""
    from app.barrier_store import (
        CANCEL_SCRIPT,
        CHECKIN_SCRIPT,
        EXTEND_SCRIPT,
    )

    async with await _client() as c:
        r = app.state.store.redis
        winners = []

        for i in range(40):
            b = await create_barrier(c, ["a", "b"], threshold=1, ttl_s=60)
            bid = b["barrier_id"]
            keys = [
                f"barrier:{bid}",
                f"barrier:{bid}:roster",
                f"barrier:{bid}:checked",
                f"barrier:{bid}:frozen",
                f"barrier:{bid}:events",
                f"barrier:{bid}:cancel",
                f"barrier:{bid}:gmap",
                f"barrier:{bid}:gcheck",
            ]
            now = f"{time.time():.3f}"
            new_deadline = f"{time.time() + 120:.3f}"
            pipe = r.pipeline(transaction=True)
            mode = i % 3
            if mode == 0:
                # 先延期后报到：延期成功，随后报到达标
                pipe.eval(EXTEND_SCRIPT, 8, *keys, new_deadline, now, "86400000")
                pipe.eval(CHECKIN_SCRIPT, 8, *keys, "a", now)
            elif mode == 1:
                # 先报达标后延期：延期必须报 reached 冲突，快照不变
                pipe.eval(CHECKIN_SCRIPT, 8, *keys, "a", now)
                pipe.eval(EXTEND_SCRIPT, 8, *keys, new_deadline, now, "86400000")
            else:
                # 先延期后取消：延期成功，取消随后生效（waiting 中允许取消）
                pipe.eval(EXTEND_SCRIPT, 8, *keys, new_deadline, now, "86400000")
                pipe.eval(CANCEL_SCRIPT, 8, *keys, "race", now)
            first_raw, second_raw = await pipe.execute()

            g = await c.get(f"/barriers/{bid}")
            status = g.json()["status"]

            if mode == 0:
                assert first_raw[0] == "extended", first_raw
                assert second_raw[0] == "reached", second_raw
                assert status == "reached"
                assert (await extend_deadline(c, bid, new_deadline)).status_code == 409
                winners.append("reached")
            elif mode == 1:
                assert first_raw[0] == "reached", first_raw
                assert second_raw[0] == "reached", second_raw  # 延期冲突
                assert status == "reached"
                assert (await extend_deadline(c, bid, new_deadline)).status_code == 409
                winners.append("reached")
            else:
                assert first_raw[0] == "extended", first_raw
                assert second_raw[0] == "cancelled", second_raw
                assert status == "cancelled"
                # 取消后再延期必为 409，终态不复活；报到也被拒绝
                assert (await extend_deadline(c, bid, new_deadline)).status_code == 409
                assert (await checkin(c, bid, "a")).status_code == 409
                winners.append("cancelled")

        assert set(winners) == {"reached", "cancelled"}


# ---------------------------------------------------------------------------
# 运行中参与者替补 POST /barriers/{id}/replace
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replace_basic_swaps_roster_and_view_reflects_it():
    """未报到成员被名单外新标识原子替换：200，名单更新，原成员不再可报到。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=15)
        bid = b["barrier_id"]
        await checkin(c, bid, "a")

        r = await replace(c, bid, "b", "b-backup")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["status"] == "replaced"
        assert body["participant_id"] == "b"
        assert body["replacement_id"] == "b-backup"
        assert body["group"] is None
        assert datetime.fromisoformat(body["replaced_at"]).tzinfo is not None

        view = body["view"]
        assert view["status"] == "waiting"
        assert view["participants"] == ["a", "b-backup", "c"]
        assert view["checked_in"] == ["a"]
        assert view["checked_count"] == 1

        # GET 同样反映替换后的名单
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["participants"] == ["a", "b-backup", "c"]

        # 原成员已不在名单：报到 403（NotAParticipant）；新成员可正常报到
        assert (await checkin(c, bid, "b")).status_code == 403
        r = await checkin(c, bid, "b-backup")
        assert r.status_code == 200 and r.json()["status"] == "checked_in"

        # 替补本身不是报到：checked 计数不因其变化
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["checked_in"] == ["a", "b-backup"]


@pytest.mark.asyncio
async def test_replace_inherits_group_and_quota_uses_new_member():
    """分组屏障下替补继承原分组：新成员以原组身份补齐配额并触发达标。"""
    async with await _client() as c:
        participants = ["a1", "a2", "b1"]
        groups = {"a1": "A", "a2": "A", "b1": "B"}
        b = await create_barrier(
            c, participants, threshold=3, ttl_s=15,
            groups=groups, quotas={"A": 2, "B": 1},
        )
        bid = b["barrier_id"]
        await checkin(c, bid, "a1")
        await checkin(c, bid, "b1")

        # a2（A 组，未报到）替换为 a2-new，继承 A 组
        r = await replace(c, bid, "a2", "a2-new")
        assert r.status_code == 200, r.text
        assert r.json()["group"] == "A"
        view = r.json()["view"]
        assert view["participant_groups"]["a2-new"] == "A"
        assert "a2" not in view["participant_groups"]
        prog = {x["group"]: x for x in view["group_progress"]}
        assert prog["A"]["checked_count"] == 1  # 换人不改变任何报到计数
        assert prog["B"]["checked_count"] == 1

        # 新成员以 A 组身份报到：A=2、B=1、总数 3，唯一触发达标
        r = await checkin(c, bid, "a2-new")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "reached" and body["triggered"] is True
        assert sorted(body["quorum"]) == ["a1", "a2-new", "b1"]

        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["participants"] == ["a1", "a2-new", "b1"]
        assert g["participant_groups"] == {"a1": "A", "a2-new": "A", "b1": "B"}


@pytest.mark.asyncio
async def test_replace_request_validation_422():
    """空标识、纯空白、两个标识相同均返回 422（空白会被去除）。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=15)
        bid = b["barrier_id"]

        async def post(body):
            return await c.post(f"/barriers/{bid}/replace", json=body)

        assert (await post({"participant_id": " ", "replacement_id": "x"})).status_code == 422
        assert (await post({"participant_id": "a", "replacement_id": ""})).status_code == 422
        assert (await post({"participant_id": "a", "replacement_id": "   "})).status_code == 422
        # 去除首尾空白后相同
        r = await post({"participant_id": "a", "replacement_id": " a "})
        assert r.status_code == 422
        # 缺字段
        assert (await post({"participant_id": "a"})).status_code == 422


@pytest.mark.asyncio
async def test_replace_rejects_unknown_checked_and_existing_replacement():
    """原成员不存在/已被换掉 -> 404；原成员已报到、新标识已在名单 -> 409。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=15)
        bid = b["barrier_id"]
        await checkin(c, bid, "a")

        # 屏障从未声明过的成员 -> 404
        r = await replace(c, bid, "ghost", "ghost-x")
        assert r.status_code == 404

        # 新标识已在名单内 -> 409
        r = await replace(c, bid, "b", "c")
        assert r.status_code == 409

        # 原成员已报到 -> 409
        r = await replace(c, bid, "a", "a-new")
        assert r.status_code == 409
        # 报到与名单均未被改写
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["participants"] == ["a", "b", "c"]
        assert g["checked_in"] == ["a"]

        # 成功把 b 换成 b2 后，再次以 b 为原成员替补 -> 404（已被换下）
        assert (await replace(c, bid, "b", "b2")).status_code == 200
        assert (await replace(c, bid, "b", "b3")).status_code == 404

        # 新标识已在（替换后的）名单内：b2 是现成员 -> 409
        assert (await replace(c, bid, "c", "b2")).status_code == 409


@pytest.mark.asyncio
async def test_replace_chained_and_revert_allowed():
    """同一席位可连续替补；被换下的旧标识作为名单外标识可再次被换入。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=15)
        bid = b["barrier_id"]

        assert (await replace(c, bid, "b", "b1")).status_code == 200
        assert (await replace(c, bid, "b1", "b2")).status_code == 200
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["participants"] == ["a", "b2"]

        # b 已在名单外，可以把 b2 再换回 b
        assert (await replace(c, bid, "b2", "b")).status_code == 200
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["participants"] == ["a", "b"]
        assert (await checkin(c, bid, "b")).status_code == 200


@pytest.mark.asyncio
async def test_replace_rejected_in_terminal_states():
    """达标 / 取消 / 截止后替补一律 409，终态绝不因替补改变。"""
    async with await _client() as c:
        # reached
        b = await create_barrier(c, ["a", "b"], threshold=1, ttl_s=15)
        bid = b["barrier_id"]
        await checkin(c, bid, "a")
        r = await replace(c, bid, "b", "b-new")
        assert r.status_code == 409
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["status"] == "reached"
        assert g["quorum"] == ["a"]
        assert g["participants"] == ["a", "b"]

        # cancelled
        b2 = await create_barrier(c, ["x", "y"], threshold=2, ttl_s=15)
        bid2 = b2["barrier_id"]
        await cancel(c, bid2, "停")
        r = await replace(c, bid2, "y", "y-new")
        assert r.status_code == 409
        g = (await c.get(f"/barriers/{bid2}")).json()
        assert g["status"] == "cancelled"
        assert g["participants"] == ["x", "y"]

        # expired
        b3 = await create_barrier(c, ["p", "q"], threshold=2, ttl_s=1)
        bid3 = b3["barrier_id"]
        await asyncio.sleep(1.2)
        g = (await c.get(f"/barriers/{bid3}")).json()
        assert g["status"] == "expired"
        r = await replace(c, bid3, "q", "q-new")
        assert r.status_code == 409, r.text
        g = (await c.get(f"/barriers/{bid3}")).json()
        assert g["status"] == "expired"
        assert g["participants"] == ["p", "q"]

        # 屏障不存在 -> 404
        assert (await replace(c, "deadbeefdeadbeef", "a", "b")).status_code == 404


@pytest.mark.asyncio
async def test_replace_records_paginated_audit_event():
    """成功替补在换人同一原子操作内追加 replaced 事件，与报到事件统一分页。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=15)
        bid = b["barrier_id"]
        assert (await checkin(c, bid, "a")).json()["status"] == "checked_in"
        assert (await replace(c, bid, "b", "b-new")).status_code == 200
        assert (await checkin(c, bid, "b-new")).json()["status"] == "checked_in"

        body = (await get_events(c, bid)).json()
        assert body["next_cursor"] == 3
        events = body["events"]
        assert [e["seq"] for e in events] == [1, 2, 3]
        rep = events[1]
        assert rep["result"] == "replaced"
        assert rep["participant_id"] == "b-new"
        assert rep["original_participant_id"] == "b"
        assert datetime.fromisoformat(rep["ts"]).tzinfo is not None
        # 报到事件的替补专属字段为 null（与替补事件区分）
        assert events[0]["original_participant_id"] is None
        assert events[2]["original_participant_id"] is None

        # 分页：每页 1 条游标前进，不重不漏
        page1 = (await get_events(c, bid, cursor=0, limit=1)).json()
        assert [e["seq"] for e in page1["events"]] == [1]
        page2 = (await get_events(c, bid, cursor=page1["next_cursor"], limit=1)).json()
        assert [e["seq"] for e in page2["events"]] == [2]
        page3 = (await get_events(c, bid, cursor=page2["next_cursor"], limit=1)).json()
        assert [e["seq"] for e in page3["events"]] == [3]


@pytest.mark.asyncio
async def test_failed_replacements_produce_no_events():
    """被拒绝的替补（未知成员、已报到、名单内新标识、终态）不产生任何事件。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=1, ttl_s=15)
        bid = b["barrier_id"]
        await checkin(c, bid, "a")  # 同时达标

        assert (await replace(c, bid, "ghost", "g1")).status_code == 404
        assert (await replace(c, bid, "a", "a2")).status_code == 409  # 已报到
        assert (await replace(c, bid, "b", "a")).status_code == 409  # 新标识在名单
        assert (await replace(c, bid, "b", "b2")).status_code == 409  # 已达标终态

        events = (await get_events(c, bid)).json()["events"]
        assert [e["result"] for e in events] == ["reached"]


@pytest.mark.asyncio
async def test_concurrent_replace_same_original_only_one_wins():
    """两个替补并发争抢同一未报到成员：Redis 顺序唯一，恰好一个成功。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=15)
        bid = b["barrier_id"]

        results = await asyncio.gather(
            replace(c, bid, "b", "b-x"),
            replace(c, bid, "b", "b-y"),
        )
        statuses = sorted(r.status_code for r in results)
        assert statuses == [200, 404], statuses

        ok = next(r for r in results if r.status_code == 200).json()
        winner = ok["replacement_id"]
        assert winner in ("b-x", "b-y")
        loser = "b-y" if winner == "b-x" else "b-x"

        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["participants"] == ["a", winner, "c"]
        # 只有一条 replaced 事件（在赢家报到之前检查，避免混入报到事件）
        events = (await get_events(c, bid)).json()["events"]
        assert len(events) == 1 and events[0]["result"] == "replaced"
        assert events[0]["participant_id"] == winner
        # 赢家可报到；输家的新标识仍在名单外
        assert (await checkin(c, bid, winner)).status_code == 200
        assert (await checkin(c, bid, loser)).status_code == 403


@pytest.mark.asyncio
async def test_concurrent_replace_distinct_originals_both_win():
    """并发替补不同成员：两个都成功，分组继承各自独立，名单唯一确定。"""
    async with await _client() as c:
        participants = ["a1", "a2", "b1", "b2"]
        groups = {p: ("A" if p.startswith("a") else "B") for p in participants}
        b = await create_barrier(
            c, participants, threshold=4, ttl_s=15,
            groups=groups, quotas={"A": 2, "B": 2},
        )
        bid = b["barrier_id"]

        results = await asyncio.gather(
            replace(c, bid, "a2", "a2-new"),
            replace(c, bid, "b2", "b2-new"),
        )
        assert sorted(r.status_code for r in results) == [200, 200], results
        bodies = [r.json() for r in results]
        assert {b_["participant_id"]: b_["group"] for b_ in bodies} == {
            "a2": "A",
            "b2": "B",
        }

        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["participants"] == ["a1", "a2-new", "b1", "b2-new"]
        assert g["participant_groups"] == {
            "a1": "A", "a2-new": "A", "b1": "B", "b2-new": "B",
        }
        events = (await get_events(c, bid)).json()["events"]
        assert [e["result"] for e in events] == ["replaced", "replaced"]


@pytest.mark.asyncio
async def test_replace_and_checkin_race_unique_outcome():
    """替补与原成员报到用同一事务固定入队顺序交替执行：先执行者唯一胜出。"""
    from app.barrier_store import CHECKIN_SCRIPT, SUBSTITUTE_SCRIPT
    from app.main import app

    async with await _client() as c:
        r = app.state.store.redis
        outcomes = []

        for i in range(40):
            b = await create_barrier(c, ["a", "b"], threshold=1, ttl_s=30)
            bid = b["barrier_id"]
            keys = [
                f"barrier:{bid}",
                f"barrier:{bid}:roster",
                f"barrier:{bid}:checked",
                f"barrier:{bid}:frozen",
                f"barrier:{bid}:events",
                f"barrier:{bid}:cancel",
                f"barrier:{bid}:gmap",
                f"barrier:{bid}:gcheck",
            ]
            now = f"{time.time():.3f}"
            pipe = r.pipeline(transaction=True)
            if i % 2 == 0:
                # 先替补后报到：替补成功；原成员 a 已不在名单，报到必为 not_participant
                pipe.eval(SUBSTITUTE_SCRIPT, 8, *keys, "a", "a-new", now)
                pipe.eval(CHECKIN_SCRIPT, 8, *keys, "a", now)
            else:
                # 先报到（达标）后替补：报到 reached；替补必为 reached 冲突
                pipe.eval(CHECKIN_SCRIPT, 8, *keys, "a", now)
                pipe.eval(SUBSTITUTE_SCRIPT, 8, *keys, "a", "a-new", now)
            first_raw, second_raw = await pipe.execute()

            g = await c.get(f"/barriers/{bid}")
            status = g.json()["status"]

            if i % 2 == 0:
                assert first_raw[0] == "replaced", first_raw
                assert second_raw[0] == "not_participant", second_raw
                # 换人后屏障仍 waiting：新成员可以报到达标
                assert status == "waiting"
                assert g.json()["participants"] == ["a-new", "b"]
                cr = await checkin(c, bid, "a-new")
                assert cr.json()["status"] == "reached"
                outcomes.append("replaced")
            else:
                # 先报到（达标）后替补：报到 reached；替补因原成员已报到被拒绝
                # （名单/报到校验先于终态校验，返回 already_checked_in，同为拒绝）
                assert first_raw[0] == "reached", first_raw
                assert second_raw[0] == "already_checked_in", second_raw
                assert status == "reached"
                # 终态不可复活：HTTP 替补同样 409，名单不变
                assert (await replace(c, bid, "b", "b-new")).status_code == 409
                assert g.json()["participants"] == ["a", "b"]
                outcomes.append("checkin")

        assert set(outcomes) == {"replaced", "checkin"}, Counter(outcomes)


@pytest.mark.asyncio
async def test_replace_races_with_cancel_and_extend():
    """替补与取消 / 延期两两原子竞争：终态后替补 409；先替补后取消/延期正常。"""
    from app.barrier_store import (
        CANCEL_SCRIPT,
        EXTEND_SCRIPT,
        SUBSTITUTE_SCRIPT,
    )
    from app.main import app

    async with await _client() as c:
        r = app.state.store.redis

        for i in range(30):
            b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=60)
            bid = b["barrier_id"]
            keys = [
                f"barrier:{bid}",
                f"barrier:{bid}:roster",
                f"barrier:{bid}:checked",
                f"barrier:{bid}:frozen",
                f"barrier:{bid}:events",
                f"barrier:{bid}:cancel",
                f"barrier:{bid}:gmap",
                f"barrier:{bid}:gcheck",
            ]
            now = f"{time.time():.3f}"
            new_deadline = f"{time.time() + 120:.3f}"
            pipe = r.pipeline(transaction=True)
            mode = i % 4
            if mode == 0:
                pipe.eval(SUBSTITUTE_SCRIPT, 8, *keys, "b", "b-new", now)
                pipe.eval(CANCEL_SCRIPT, 8, *keys, "race", now)
            elif mode == 1:
                pipe.eval(CANCEL_SCRIPT, 8, *keys, "race", now)
                pipe.eval(SUBSTITUTE_SCRIPT, 8, *keys, "b", "b-new", now)
            elif mode == 2:
                pipe.eval(SUBSTITUTE_SCRIPT, 8, *keys, "b", "b-new", now)
                pipe.eval(EXTEND_SCRIPT, 8, *keys, new_deadline, now, "86400000")
            else:
                pipe.eval(EXTEND_SCRIPT, 8, *keys, new_deadline, now, "86400000")
                pipe.eval(SUBSTITUTE_SCRIPT, 8, *keys, "b", "b-new", now)
            first_raw, second_raw = await pipe.execute()

            g = (await c.get(f"/barriers/{bid}")).json()

            if mode == 0:
                assert first_raw[0] == "replaced", first_raw
                assert second_raw[0] == "cancelled", second_raw
                assert g["status"] == "cancelled"
                assert g["participants"] == ["a", "b-new"]  # 换人快照保留
                assert (await replace(c, bid, "a", "a2")).status_code == 409
            elif mode == 1:
                assert first_raw[0] == "cancelled", first_raw
                assert second_raw[0] == "cancelled", second_raw  # 替补冲突
                assert g["status"] == "cancelled"
                assert g["participants"] == ["a", "b"]  # 名单未被改写
            elif mode == 2:
                assert first_raw[0] == "replaced", first_raw
                assert second_raw[0] == "extended", second_raw
                assert g["status"] == "waiting"
                assert g["participants"] == ["a", "b-new"]
                assert datetime.fromisoformat(g["deadline"]) > datetime.fromisoformat(b["deadline"])
            else:
                assert first_raw[0] == "extended", first_raw
                assert second_raw[0] == "replaced", second_raw
                assert g["status"] == "waiting"
                assert g["participants"] == ["a", "b-new"]


@pytest.mark.asyncio
async def test_waiter_woken_on_replace_and_sees_new_roster():
    """替补成功发布 participant_replaced：已挂起的 /wait 被唤醒回读新名单，
    屏障仍 waiting 时继续等待；随后达标时再次被唤醒返回 reached。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=10)
        bid = b["barrier_id"]
        await checkin(c, bid, "a")

        async def wait_view():
            async with await _client() as c2:
                started = time.time()
                r = await c2.get(f"/barriers/{bid}/wait")
                return time.time() - started, r

        waiter = asyncio.create_task(wait_view())
        await asyncio.sleep(0.4)  # 确保已订阅并挂起

        r = await replace(c, bid, "b", "b-new")
        assert r.status_code == 200

        # 唤醒回读发生后等待仍应继续（不能错误返回）；留一点时间处理消息
        await asyncio.sleep(0.4)
        assert not waiter.done(), "替补不应让 waiting 的屏障提前结束等待"

        # 新成员报到达标：等待者被唤醒拿到含新名单的终态
        await checkin(c, bid, "b-new")
        elapsed, r = await asyncio.wait_for(waiter, timeout=5)
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "reached"
        assert sorted(body["quorum"]) == ["a", "b-new"]
        assert body["participants"] == ["a", "b-new"]
        assert elapsed < 5


# ---------------------------------------------------------------------------
# 批量报到 POST /barriers/{id}/batch-checkins（幂等键 + 整批原子 + 唯一冻结）
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_checkin_basic_and_per_member_view():
    """批量报到成功：200，逐成员结果与报到集合、视图计数一致。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=15)
        bid = b["barrier_id"]

        r = await batch_checkin(c, bid, ["a", "b"], "batch-1")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["status"] == "checked_in"
        assert body["triggered"] is False
        assert body["replayed"] is False
        assert body["idempotency_key"] == "batch-1"
        assert body["accepted_count"] == 2
        assert [a["participant_id"] for a in body["accepted"]] == ["a", "b"]
        assert {a["result"] for a in body["accepted"]} == {"checked_in"}
        assert body["quorum"] is None and body["frozen_at"] is None
        assert body["view"]["status"] == "waiting"
        assert body["view"]["checked_in"] == ["a", "b"]
        assert body["view"]["checked_count"] == 2

        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["checked_in"] == ["a", "b"] and g["checked_count"] == 2


@pytest.mark.asyncio
async def test_batch_checkin_request_validation_422():
    """空列表、空/空白/超长幂等键、批内重复/空标识均返回 422；空白会被去除。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=15)
        bid = b["barrier_id"]
        url = f"/barriers/{bid}/batch-checkins"

        async def post(body):
            return await c.post(url, json=body)

        assert (await post({"participants": [], "idempotency_key": "k"})).status_code == 422
        assert (await post({"participants": ["a"]})).status_code == 422  # 缺幂等键
        assert (await post({"participants": ["a"], "idempotency_key": ""})).status_code == 422
        assert (await post({"participants": ["a"], "idempotency_key": "   "})).status_code == 422
        assert (
            await post({"participants": ["a"], "idempotency_key": "k" * 257})
        ).status_code == 422
        # 批内重复（含去除首尾空白后重复）
        assert (await post({"participants": ["a", "a"], "idempotency_key": "k"})).status_code == 422
        assert (
            await post({"participants": [" a ", "a"], "idempotency_key": "k"})
        ).status_code == 422
        # 空白标识
        assert (await post({"participants": ["  "], "idempotency_key": "k"})).status_code == 422

        # 空白被去除后仍是合法请求
        r = await post({"participants": [" b ", "a"], "idempotency_key": "  key-1  "})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["idempotency_key"] == "key-1"
        assert [x["participant_id"] for x in body["accepted"]] == ["b", "a"]

        # 屏障不存在 -> 404（先于请求体语义冲突）
        assert (
            await c.post(
                "/barriers/deadbeefdeadbeef/batch-checkins",
                json={"participants": ["a"], "idempotency_key": "k"},
            )
        ).status_code == 404


@pytest.mark.asyncio
async def test_batch_checkin_idempotent_replay_returns_first_result():
    """同一幂等键重试逐字段回放首次响应；顺序不同但集合相同视为同一请求。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=60)
        bid = b["barrier_id"]

        r1 = await batch_checkin(c, bid, ["a", "b"], "idem-1")
        first = r1.json()
        assert r1.status_code == 200 and first["replayed"] is False

        # 同键原样重试
        r2 = await batch_checkin(c, bid, ["a", "b"], "idem-1")
        second = r2.json()
        assert r2.status_code == 200
        assert second["replayed"] is True
        assert second["status"] == first["status"]
        assert second["triggered"] == first["triggered"]
        assert second["accepted"] == first["accepted"]
        assert second["view"]["checked_in"] == first["view"]["checked_in"]
        assert second["view"]["checked_count"] == first["view"]["checked_count"]

        # 同键以不同顺序重试仍是回放（按集合判定），且回放入首次顺序
        r3 = await batch_checkin(c, bid, ["b", "a"], "idem-1")
        third = r3.json()
        assert third["replayed"] is True
        assert [x["participant_id"] for x in third["accepted"]] == ["a", "b"]

        # 重放没有再次写报到/事件：报到集合仍是 {a,b}，事件仍是 2 条
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["checked_in"] == ["a", "b"]
        events = (await get_events(c, bid)).json()["events"]
        assert [e["participant_id"] for e in events] == ["a", "b"]
        assert len(events) == 2


@pytest.mark.asyncio
async def test_batch_replay_freezes_first_view_even_after_later_changes():
    """首次响应后屏障继续变化（他人报到达标），同键重试仍返回首次快照。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=60)
        bid = b["barrier_id"]

        first = (await batch_checkin(c, bid, ["a"], "freeze-snap")).json()
        assert first["view"]["status"] == "waiting"
        assert first["view"]["checked_count"] == 1

        # 其他人把屏障推到达标
        await checkin(c, bid, "b")
        reached = (await checkin(c, bid, "c")).json()
        assert reached["status"] == "reached"

        # 同键回放：仍是首次执行时刻的 waiting 视图快照，而不是当前 reached
        replay = (await batch_checkin(c, bid, ["a"], "freeze-snap")).json()
        assert replay["replayed"] is True
        assert replay["status"] == "checked_in"
        assert replay["triggered"] is False
        assert replay["view"]["status"] == "waiting"
        assert replay["view"]["checked_in"] == ["a"]
        assert replay["view"]["checked_count"] == 1
        assert replay["quorum"] is None and replay["frozen_at"] is None

        # 对比：当前实时视图确已 reached（回放快照与实时状态解耦）
        live = (await c.get(f"/barriers/{bid}")).json()
        assert live["status"] == "reached"


@pytest.mark.asyncio
async def test_batch_same_key_different_participants_conflicts_409():
    """同一幂等键用于另一份参与者集合 -> 409，首次结果不被改写，新名单零副作用。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=60)
        bid = b["barrier_id"]
        assert (await batch_checkin(c, bid, ["a"], "shared-key")).status_code == 200

        r = await batch_checkin(c, bid, ["a", "b"], "shared-key")
        assert r.status_code == 409, r.text

        # 首次结果仍可原样回放
        replay = (await batch_checkin(c, bid, ["a"], "shared-key")).json()
        assert replay["replayed"] is True
        assert [x["participant_id"] for x in replay["accepted"]] == ["a"]

        # 冲突请求未把 b 写入报到集合，也未产生事件
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["checked_in"] == ["a"]
        events = (await get_events(c, bid)).json()["events"]
        assert len(events) == 1


@pytest.mark.asyncio
async def test_batch_with_unknown_member_whole_batch_has_no_side_effect():
    """列表含未知成员：403，整批不写报到/分组计数/事件，也不占用幂等键。"""
    async with await _client() as c:
        participants = ["a1", "a2", "b1"]
        groups = {"a1": "A", "a2": "A", "b1": "B"}
        b = await create_barrier(
            c, participants, threshold=3, ttl_s=15,
            groups=groups, quotas={"A": 2, "B": 1},
        )
        bid = b["barrier_id"]

        # a1 是名单成员、ghost 不是：整批必须一起失败，a1 也不得报到
        r = await batch_checkin(c, bid, ["a1", "ghost"], "bad-batch")
        assert r.status_code == 403
        assert "ghost" in r.json()["detail"]

        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["checked_in"] == [] and g["checked_count"] == 0
        prog = _progress(g)
        assert prog["A"]["checked_count"] == 0 and prog["B"]["checked_count"] == 0
        events = (await get_events(c, bid)).json()["events"]
        assert events == []

        # 幂等键未被占用：用修正后的名单和同一个键重试可以成功
        r2 = await batch_checkin(c, bid, ["a1", "a2", "b1"], "bad-batch")
        assert r2.status_code == 200, r2.text
        body = r2.json()
        assert body["replayed"] is False and body["triggered"] is True
        assert sorted(body["quorum"]) == participants
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["status"] == "reached"


@pytest.mark.asyncio
async def test_batch_failure_leaves_no_idempotency_keys_in_redis():
    """整批失败（未知成员/截止/取消）不仅不写报到，连幂等物理键都不创建。"""
    from app.main import app

    async with await _client() as c:
        store = app.state.store
        redis = store.redis

        # 未知成员
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=60)
        bid = b["barrier_id"]
        idem_key = store.idempotency_key(bid, "no-leak-1")
        idx_key = f"barrier:{bid}:idem:idx"
        assert (await batch_checkin(c, bid, ["a", "ghost"], "no-leak-1")).status_code == 403
        assert await redis.exists(idem_key) == 0
        assert await redis.exists(idx_key) == 0
        # 成功后幂等键与索引才出现
        assert (await batch_checkin(c, bid, ["a"], "no-leak-1")).status_code == 200
        assert await redis.exists(idem_key) == 1
        assert await redis.sismember(idx_key, idem_key) == 1

        # 截止
        b2 = await create_barrier(c, ["x"], threshold=1, ttl_s=1)
        await asyncio.sleep(1.2)
        idem2 = store.idempotency_key(b2["barrier_id"], "no-leak-2")
        assert (
            await batch_checkin(c, b2["barrier_id"], ["x"], "no-leak-2")
        ).status_code == 409
        assert await redis.exists(idem2) == 0

        # 取消
        b3 = await create_barrier(c, ["y"], threshold=1, ttl_s=60)
        await cancel(c, b3["barrier_id"], "停")
        idem3 = store.idempotency_key(b3["barrier_id"], "no-leak-3")
        assert (
            await batch_checkin(c, b3["barrier_id"], ["y"], "no-leak-3")
        ).status_code == 409
        assert await redis.exists(idem3) == 0


@pytest.mark.asyncio
async def test_batch_after_reached_all_late_snapshot_unchanged():
    """达标后的批量报到全部记为 late：冻结快照不变；重试回放同一迟到结果。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c", "d"], threshold=2, ttl_s=60)
        bid = b["barrier_id"]
        r1 = await batch_checkin(c, bid, ["a"], "reach-key")
        assert r1.json()["status"] == "checked_in"
        r2 = await checkin(c, bid, "b")
        assert r2.json()["status"] == "reached"
        frozen_at = r2.json()["frozen_at"]
        quorum = r2.json()["quorum"]

        r = await batch_checkin(c, bid, ["c", "d", "a"], "late-key")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "late"
        assert body["triggered"] is False
        assert body["quorum"] == quorum
        assert body["frozen_at"] == frozen_at
        assert {x["result"] for x in body["accepted"]} == {"late"}
        view = body["view"]
        assert view["status"] == "reached"
        assert view["quorum"] == quorum
        assert view["frozen_at"] == frozen_at
        assert view["checked_count"] == 2  # 冻结人数，不含迟到者
        assert set(view["late"]) == {"c", "d"}
        assert set(view["checked_in"]) == {"a", "b", "c", "d"}

        replay = (await batch_checkin(c, bid, ["c", "d", "a"], "late-key")).json()
        assert replay["replayed"] is True
        assert replay["status"] == "late"
        assert replay["view"]["late"] == view["late"]
        assert replay["view"]["frozen_at"] == frozen_at

        # 逐成员 late 事件（重复的 a 也只记一条 late）
        events = (await get_events(c, bid, limit=100)).json()["events"]
        assert [e["result"] for e in events][-3:] == ["late", "late", "late"]


@pytest.mark.asyncio
async def test_batch_triggers_quorum_and_wakes_all_waiters_once():
    """本批使屏障达标：triggered=true、quorum 冻结，且多个 /wait 等待者全部
    被 Pub/Sub 立即唤醒，拿到同一份冻结快照。"""
    async with await _client() as c:
        members = ["w1", "w2", "w3", "w4"]
        b = await create_barrier(c, members, threshold=3, ttl_s=60)
        bid = b["barrier_id"]

        async def wait_view():
            async with await _client() as c2:
                started = time.time()
                r = await c2.get(f"/barriers/{bid}/wait")
                return time.time() - started, r

        waiters = [asyncio.create_task(wait_view()) for _ in range(3)]
        await asyncio.sleep(0.4)  # 确保都已订阅并挂起

        r = await batch_checkin(c, bid, ["w1", "w2", "w3"], "wake-batch")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "reached" and body["triggered"] is True
        assert sorted(body["quorum"]) == ["w1", "w2", "w3"]
        assert body["frozen_at"] is not None
        results = [x["result"] for x in body["accepted"]]
        assert results.count("reached") == 1
        assert results.count("checked_in") == 2

        outcomes = await asyncio.gather(*waiters)
        for elapsed, resp in outcomes:
            assert resp.status_code == 200, resp.text
            v = resp.json()
            assert v["status"] == "reached"
            assert sorted(v["quorum"]) == ["w1", "w2", "w3"]
            assert elapsed < 5, f"等待者未被及时唤醒: {elapsed:.2f}s"

        # 冻结后任何后续批量 / 单人报到都不能改写快照
        again = await batch_checkin(c, bid, ["w4"], "after-key")
        assert again.json()["status"] == "late"
        assert again.json()["triggered"] is False
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["quorum"] == body["quorum"] and g["frozen_at"] == body["frozen_at"]


@pytest.mark.asyncio
async def test_batch_concurrent_same_idempotency_key_single_execution():
    """同一幂等键的并发请求只有一次实际执行：全部 200 且至多一个 replayed=false，
    逐成员事件不重复，响应内容一致。"""
    async with await _client() as c:
        members = [f"c{i}" for i in range(5)]
        b = await create_barrier(c, members, threshold=5, ttl_s=60)
        bid = b["barrier_id"]

        results = await asyncio.gather(
            *(batch_checkin(c, bid, ["c0", "c1"], "race-key") for _ in range(8))
        )
        assert all(r.status_code == 200 for r in results), results
        bodies = [r.json() for r in results]
        first_runs = [x for x in bodies if x["replayed"] is False]
        assert len(first_runs) == 1, [x["replayed"] for x in bodies]
        for body in bodies:
            assert body["status"] == "checked_in"
            assert [a["participant_id"] for a in body["accepted"]] == ["c0", "c1"]

        # 只应有 c0/c1 各一条事件，而不是 8 批 16 条
        events = (await get_events(c, bid, limit=100)).json()["events"]
        assert [e["participant_id"] for e in events] == ["c0", "c1"]
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["checked_in"] == ["c0", "c1"]


@pytest.mark.asyncio
async def test_batch_concurrent_with_single_checkin_only_one_freezes():
    """批量与单人报到并发争抢最后一个达标席位：只有一个请求触发冻结。"""
    async with await _client() as c:
        winner_kinds = set()
        for i in range(20):
            members = ["a", "b", "c"]
            b = await create_barrier(c, members, threshold=3, ttl_s=60)
            bid = b["barrier_id"]
            # 预置 2 人，最后一席由单人 c 与两个不同幂等键的批量 c 并发争抢
            await checkin(c, bid, "a")
            await checkin(c, bid, "b")

            async def single_last():
                await asyncio.sleep((i % 3) * 0.001)  # 打乱固定调度顺序
                return ("single", await checkin(c, bid, "c"))

            async def batch_last(key, delay):
                await asyncio.sleep(delay)
                return ("batch", await batch_checkin(c, bid, ["c"], key))

            coros = [
                single_last(),
                batch_last("bk1", ((i + 1) % 3) * 0.001),
                batch_last("bk2", ((i + 2) % 3) * 0.001),
            ]
            outs = await asyncio.gather(*coros)
            triggered = []
            for tag, resp in outs:
                assert resp.status_code == 200, resp.text
                body = resp.json()
                if body.get("triggered"):
                    triggered.append(tag)
            assert len(triggered) == 1, triggered
            winner_kinds.add(triggered[0])

            # 屏障只冻结一次，quorum 固定为 a,b,c；后到的批量结果为 late/回放
            g = (await c.get(f"/barriers/{bid}")).json()
            assert g["status"] == "reached" and sorted(g["quorum"]) == ["a", "b", "c"]
            r1 = (await batch_checkin(c, bid, ["c"], "bk1")).json()
            r2 = (await batch_checkin(c, bid, ["c"], "bk2")).json()
            assert r1["replayed"] and r2["replayed"]
            if triggered[0] == "single":
                # 单人先触发：两个批量首次执行时均已冻结，都是 late
                assert r1["status"] == r2["status"] == "late"
            else:
                # 某个批量先触发：它回放 reached，另一个回放 late
                assert {r1["status"], r2["status"]} == {"reached", "late"}

        # 随机化调度后，两类赢家都应出现，证明冻结权跨接口公平竞争
        assert winner_kinds == {"single", "batch"}, winner_kinds


@pytest.mark.asyncio
async def test_batch_rejected_after_expiry_or_cancel_without_side_effect():
    """截止后 / 取消后的批量报到 409 且整批零副作用；失败不占用幂等键。"""
    async with await _client() as c:
        # 截止
        members = ["e1", "e2", "e3"]
        b = await create_barrier(c, members, threshold=3, ttl_s=1)
        bid = b["barrier_id"]
        await checkin(c, bid, "e1")
        await asyncio.sleep(1.2)

        r = await batch_checkin(c, bid, ["e2", "e3"], "exp-batch")
        assert r.status_code == 409
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["status"] == "expired"
        assert g["checked_in"] == ["e1"]
        assert g["missing"] == ["e2", "e3"]
        # 无批量事件
        events = (await get_events(c, bid)).json()["events"]
        assert [e["participant_id"] for e in events] == ["e1"]

        # 取消
        b2 = await create_barrier(c, ["x", "y"], threshold=2, ttl_s=30)
        bid2 = b2["barrier_id"]
        await cancel(c, bid2, "停")
        r = await batch_checkin(c, bid2, ["x", "y"], "can-batch")
        assert r.status_code == 409
        g = (await c.get(f"/barriers/{bid2}")).json()
        assert g["status"] == "cancelled" and g["checked_in"] == []
        assert (await get_events(c, bid2)).json()["events"] == []

        # 失败不占用幂等键：对一个仍 waiting 的新屏障可复用同一键
        b3 = await create_barrier(c, ["x", "y"], threshold=2, ttl_s=30)
        r = await batch_checkin(c, b3["barrier_id"], ["x"], "can-batch")
        assert r.status_code == 200 and r.json()["replayed"] is False


@pytest.mark.asyncio
async def test_batch_group_quota_freeze_and_replayed_snapshot():
    """分组配额下批量补齐短板：本批原子满足总数与各组配额才触发；
    回放视图携带首次冻结快照的 group_progress。"""
    async with await _client() as c:
        participants = ["a1", "a2", "b1", "b2", "c1"]
        groups = {p: ("A" if p.startswith("a") else "B" if p.startswith("b") else "C")
                  for p in participants}
        b = await create_barrier(
            c, participants, threshold=4, ttl_s=30,
            groups=groups, quotas={"A": 2, "B": 1, "C": 1},
        )
        bid = b["barrier_id"]
        for pid in ("a1", "b1", "c1", "b2"):
            await checkin(c, bid, pid)
        # 此时总数 4 已达标但 A 组只有 1/2，仍 waiting
        assert (await c.get(f"/barriers/{bid}")).json()["status"] == "waiting"

        # 批量补 a2 + 一个重复的 b1：a2 使 A=2，本批唯一触发达标
        r = await batch_checkin(c, bid, ["a2", "b1"], "group-batch")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "reached" and body["triggered"] is True
        assert sorted(body["quorum"]) == ["a1", "a2", "b1", "b2", "c1"]
        prog = _progress(body["view"])
        assert prog["A"]["checked_count"] == 2 and prog["A"]["satisfied"] is True
        assert prog["B"]["checked_count"] == 2 and prog["B"]["satisfied"] is True
        assert prog["C"]["checked_count"] == 1 and prog["C"]["satisfied"] is True
        # b1 是批内重复成员，结果为 already_checked_in
        results = {x["participant_id"]: x["result"] for x in body["accepted"]}
        assert results == {"a2": "reached", "b1": "already_checked_in"}

        # 回放同样带冻结快照
        replay = (await batch_checkin(c, bid, ["a2", "b1"], "group-batch")).json()
        assert replay["replayed"] is True
        rprog = _progress(replay["view"])
        assert {g_: (x["checked_count"], x["satisfied"]) for g_, x in rprog.items()} == {
            "A": (2, True), "B": (2, True), "C": (1, True),
        }


@pytest.mark.asyncio
async def test_batch_events_are_per_member_and_ordered_with_no_failed_noise():
    """批量成功按成员顺序逐追加事件（含 already_checked_in / reached / late），
    整批失败不产生任何事件。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c", "d"], threshold=3, ttl_s=60)
        bid = b["barrier_id"]

        assert (await batch_checkin(c, bid, ["a", "b"], "e1")).status_code == 200
        # 含一个已报到成员 a + 新成员 c：c 触发 reached
        assert (await batch_checkin(c, bid, ["a", "c"], "e2")).status_code == 200
        # 达标后迟到批次
        assert (await batch_checkin(c, bid, ["d"], "e3")).status_code == 200
        # 整批失败：b 已报到合法但 ghost 非法，必须零事件
        assert (await batch_checkin(c, bid, ["b", "ghost"], "e4")).status_code == 403

        events = (await get_events(c, bid, limit=100)).json()["events"]
        assert [e["seq"] for e in events] == [1, 2, 3, 4, 5]
        assert [(e["participant_id"], e["result"]) for e in events] == [
            ("a", "checked_in"),
            ("b", "checked_in"),
            ("a", "already_checked_in"),
            ("c", "reached"),
            ("d", "late"),
        ]


@pytest.mark.asyncio
async def test_batch_respects_current_roster_after_substitution():
    """替补改写名单后：原成员批量报到按未知成员整批拒绝；新成员可批量报到。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=30)
        bid = b["barrier_id"]
        assert (await replace(c, bid, "b", "b-new")).status_code == 200

        # 原成员 b 已不在名单：即使与合法成员 a 同批也整批拒绝
        r = await batch_checkin(c, bid, ["a", "b"], "post-replace-bad")
        assert r.status_code == 403
        g = (await c.get(f"/barriers/{bid}")).json()
        assert g["checked_in"] == []

        # 按替换后名单批量报到可达标
        r = await batch_checkin(c, bid, ["a", "b-new", "c"], "post-replace-ok")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "reached" and body["triggered"] is True
        assert sorted(body["quorum"]) == ["a", "b-new", "c"]


@pytest.mark.asyncio
async def test_batch_idempotency_records_extend_with_deadline():
    """批量报到后延期：幂等记录 TTL 随新 deadline 顺延，延期后仍能回放首次结果。"""
    from app.main import app

    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=2)
        bid = b["barrier_id"]
        await batch_checkin(c, bid, ["a"], "ttl-batch")
        r = app.state.store.redis
        idem_key = app.state.store.idempotency_key(bid, "ttl-batch")
        idx_key = f"barrier:{bid}:idem:idx"
        before = await r.pttl(idem_key)

        new_dt = _iso(datetime.now(timezone.utc) + timedelta(seconds=60))
        assert (await extend_deadline(c, bid, new_dt)).status_code == 200
        assert await r.pttl(idem_key) > before + 30000
        assert await r.pttl(idx_key) > 30000

        # 跨过原截止时间后屏障仍在，且批量首次结果可原样回放
        await asyncio.sleep(2.4)
        replay = (await batch_checkin(c, bid, ["a"], "ttl-batch")).json()
        assert replay["replayed"] is True
        assert replay["view"]["status"] == "waiting"
        assert replay["view"]["checked_in"] == ["a"]


@pytest.mark.asyncio
async def test_batch_endpoint_present_in_openapi():
    """新接口与字段出现在 OpenAPI schema 中（可生成客户端 / 文档）。"""
    async with await _client() as c:
        schema = (await c.get("/openapi.json")).json()
        path = "/barriers/{barrier_id}/batch-checkins"
        assert path in schema["paths"]
        assert "BatchCheckinRequest" in schema["components"]["schemas"]
        assert "BatchCheckinResponse" in schema["components"]["schemas"]
        # 根端点清单也包含批量接口
        root = (await c.get("/")).json()
        assert root["endpoints"]["batch_checkin"] == f"POST {path}"

