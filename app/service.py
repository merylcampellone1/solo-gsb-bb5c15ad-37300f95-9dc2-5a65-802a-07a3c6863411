"""屏障视图组装与阻塞等待逻辑。"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from .barrier_store import BarrierMeta, BarrierStore
from .handoff_store import HandoffRecord
from .models import BarrierView, GroupProgress, HandoffView

# 不可再变的终态：达标、截止、主动取消
SETTLED_STATUSES = frozenset({"reached", "expired", "cancelled"})


def _dt(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _group_progress(
    thresholds: dict[str, int], counts: dict[str, int]
) -> list[GroupProgress] | None:
    """组装分组配额进度；未启用分组配额（无配额表）时返回 None。

    reached 状态传入的 counts 是冻结快照，其余状态是实时计数或批量首次
    执行后的快照；satisfied 统一按 counts 与配额比较，终态视图因此不再
    回读可变数据。
    """
    if not thresholds:
        return None
    return [
        GroupProgress(
            group=group,
            threshold=thresholds[group],
            checked_count=int(counts.get(group, 0)),
            satisfied=int(counts.get(group, 0)) >= thresholds[group],
        )
        for group in sorted(thresholds)
    ]


async def build_view(store: BarrierStore, meta: BarrierMeta, now: float | None = None) -> BarrierView:
    """读取当前屏障状态并组装视图。

    状态机：
      reached    —— 存在冻结快照（达标成员已被原子固定）
      cancelled  —— 存在取消终态（reason/cancelled_at 已原子写入）
      expired    —— 无冻结/取消快照且 now >= deadline
      waiting    —— 其余情况

    reached 与 cancelled 由 Redis Lua 脚本原子竞争，互斥且都不可改写。
    """
    now = now if now is not None else time.time()
    frozen = await store.get_frozen(meta.barrier_id)
    cancelled = await store.get_cancelled(meta.barrier_id)
    checked = await store.checked_members(meta.barrier_id)
    # reached 使用冻结的分组计数快照；其余状态读实时计数（终态不会再变化）。
    group_counts = (
        frozen["group_counts"]
        if frozen is not None
        else await store.group_check_counts(meta.barrier_id)
    )
    groups = meta.participant_groups if meta.groups_enabled else None

    if frozen is not None:
        quorum = frozen["quorum"]
        quorum_set = set(quorum)
        late = sorted(checked - quorum_set)
        return BarrierView(
            barrier_id=meta.barrier_id,
            status="reached",
            participants=meta.participants,
            threshold=meta.threshold,
            deadline=_dt(meta.deadline),
            created_at=_dt(meta.created_at),
            checked_in=sorted(checked),
            checked_count=len(quorum),
            quorum=quorum,
            frozen_at=_dt(frozen["frozen_at"]),
            late=late or None,
            creator_id=meta.creator_id,
            participant_groups=groups,
            group_progress=_group_progress(meta.group_thresholds, group_counts),
        )

    if cancelled is not None:
        # 取消时已有的报到作为快照保留，终态信息不可再变。
        checked_sorted = sorted(checked)
        return BarrierView(
            barrier_id=meta.barrier_id,
            status="cancelled",
            participants=meta.participants,
            threshold=meta.threshold,
            deadline=_dt(meta.deadline),
            created_at=_dt(meta.created_at),
            checked_in=checked_sorted,
            checked_count=len(checked_sorted),
            cancel_reason=cancelled["reason"],
            cancelled_at=_dt(cancelled["cancelled_at"]),
            creator_id=meta.creator_id,
            participant_groups=groups,
            group_progress=_group_progress(meta.group_thresholds, group_counts),
        )

    if now >= meta.deadline:
        roster_set = set(meta.participants)
        checked_sorted = sorted(checked)
        missing = sorted(roster_set - checked)
        return BarrierView(
            barrier_id=meta.barrier_id,
            status="expired",
            participants=meta.participants,
            threshold=meta.threshold,
            deadline=_dt(meta.deadline),
            created_at=_dt(meta.created_at),
            checked_in=checked_sorted,
            checked_count=len(checked_sorted),
            missing=missing,
            creator_id=meta.creator_id,
            participant_groups=groups,
            group_progress=_group_progress(meta.group_thresholds, group_counts),
        )

    checked_sorted = sorted(checked)
    return BarrierView(
        barrier_id=meta.barrier_id,
        status="waiting",
        participants=meta.participants,
        threshold=meta.threshold,
        deadline=_dt(meta.deadline),
        created_at=_dt(meta.created_at),
        checked_in=checked_sorted,
        checked_count=len(checked_sorted),
        creator_id=meta.creator_id,
        participant_groups=groups,
        group_progress=_group_progress(meta.group_thresholds, group_counts),
    )


def build_view_from_snapshot(barrier_id: str, snap: dict) -> BarrierView:
    """从批量报到首次执行时冻结的快照重建完整 BarrierView。

    同一幂等键重试必须返回**首次结果**：因此回放视图不能回读任何可变状态
    （首次执行后可能又有其他报到 / 截止到达），而由 Lua 脚本在首次执行的
    同一原子操作内保存的快照逐字段还原。快照内名单与报到集合已排序。
    """
    quotas = snap.get("quotas") or {}
    groups = snap.get("gmap") if quotas else None
    group_counts = {g: int(n) for g, n in (snap.get("gcounts") or {}).items()}
    common = dict(
        barrier_id=barrier_id,
        participants=list(snap["participants"]),
        threshold=int(snap["threshold"]),
        deadline=_dt(float(snap["deadline"])),
        created_at=_dt(float(snap["created_at"])),
        checked_in=list(snap["checked_in"]),
        creator_id=snap.get("creator") or None,
        participant_groups=groups,
        group_progress=_group_progress(quotas, group_counts),
    )

    if snap["view_status"] == "reached":
        quorum = list(snap["quorum"])
        late = snap.get("late") or None
        return BarrierView(
            **common,
            status="reached",
            # reached 视图的报到人数取冻结人数，而非迟到后的集合大小。
            checked_count=len(quorum),
            quorum=quorum,
            frozen_at=_dt(float(snap["frozen_at"])),
            late=late,
        )

    # 批量脚本的成功路径只可能留下 waiting / reached 两种视图状态：
    # expired / cancelled 在写入前整批拒绝，不保存快照。
    return BarrierView(
        **common,
        status=snap["view_status"],
        checked_count=len(snap["checked_in"]),
    )


def build_handoff_view(record: HandoffRecord) -> HandoffView:
    """把交接单存储记录组装为对外视图。

    租约字段（owner/fence/leased_at/expires_at）仅在 ``leased`` 状态出现：
    空闲或租约到期后对外不暴露上一任持有者的失效凭证；完成单只暴露
    completed_at。快照 quorum/frozen_at 在创建时自屏障复制，任何状态下都不再
    回读屏障数据，因此屏障回收后视图依然完整。
    """
    leased = record.status == "leased"
    completed = record.status == "completed"
    return HandoffView(
        handoff_id=record.handoff_id,
        barrier_id=record.barrier_id,
        status=record.status,
        quorum=record.quorum,
        frozen_at=_dt(record.frozen_at),
        owner=record.owner if leased else None,
        fence=record.fence if leased else None,
        leased_at=_dt(record.leased_at) if leased and record.leased_at else None,
        expires_at=_dt(record.expires_at) if leased and record.expires_at else None,
        completed_at=_dt(record.completed_at) if completed and record.completed_at else None,
    )


async def wait_for_settle(
    store: BarrierStore,
    meta: BarrierMeta,
    poll_interval_s: float,
    max_wait_s: float,
) -> BarrierView:
    """阻塞直到屏障达标、截止或被取消。

    主通道为 Pub/Sub：先订阅，再检查一次状态（消除“先检查后订阅”的丢消息窗口）；
    轮询作为兜底，覆盖发布丢失 / 截止时间到达（截止不发布消息）两种情况。
    多个等待者各自订阅同一频道，达标（reached）、取消（cancelled）、运行中
    延期（deadline_extended）或运行中替补（participant_replaced）发布时全部
    被唤醒。

    每次被唤醒（以及每轮轮询）都重新读取 meta 再组装视图：延期只改 meta 中的
    deadline，等待者必须以最新 deadline 继续等待，绝不能按进入请求时的旧
    deadline 误报 expired；替补只改 roster / gmap，等待者回读 meta 后看到的
    即是替换后的名单与分组映射。
    """
    def _current_meta(latest: BarrierMeta | None) -> BarrierMeta:
        # 极端情况下 meta 恰好在 TTL 点被回收：沿用最后一份已知 meta，
        # 由 build_view 依据终态键/时间给出结果，而不是抛错中断等待。
        return latest if latest is not None else meta

    # 已经有结果则立即返回（达标、取消快照或截止结果）
    first = await build_view(store, meta)
    if first.status in SETTLED_STATUSES:
        return first

    pubsub = store.redis.pubsub()
    await pubsub.subscribe(store.channel(meta.barrier_id))
    try:
        # 订阅成功后再次检查：终态/延期发布可能发生在第一次检查与订阅之间
        latest = await store.get_meta(meta.barrier_id)
        second = await build_view(store, _current_meta(latest))
        if second.status in SETTLED_STATUSES:
            return second

        start = time.time()
        while True:
            now = time.time()
            remaining_budget = max_wait_s - (now - start)
            if remaining_budget <= 0:
                latest = await store.get_meta(meta.barrier_id)
                return await build_view(store, _current_meta(latest), now)

            # 用最新截止时间计算本轮等待时长（延期后自动顺延）。
            deadline_d = _current_meta(latest).deadline
            # get_message 阻塞至多 timeout；Pub/Sub 消息与轮询共用这一等待
            timeout = min(
                poll_interval_s,
                max(0.0, deadline_d - now),
                remaining_budget,
            )
            if timeout > 0:
                await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=timeout
                )

            # 无论被哪条消息唤醒还是轮询到期，都先回写最新 meta（deadline 可能
            # 已被 PATCH 延期更新），再回读真实状态：达标 / 取消立即返回终态
            # 快照；到达最新截止才返回 expired；仍是 waiting 则继续等待。
            latest = await store.get_meta(meta.barrier_id)
            view = await build_view(store, _current_meta(latest))
            if view.status in SETTLED_STATUSES:
                return view
    finally:
        try:
            await pubsub.unsubscribe(store.channel(meta.barrier_id))
        finally:
            await pubsub.aclose()
