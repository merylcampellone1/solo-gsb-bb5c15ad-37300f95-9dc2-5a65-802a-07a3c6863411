"""Pydantic 请求 / 响应模型。"""

from __future__ import annotations

import re
from collections import Counter
from datetime import datetime, timezone

from pydantic import BaseModel, Field, model_validator


class CreateBarrierRequest(BaseModel):
    participants: list[str] = Field(
        ..., min_length=1, description="屏障参与者标识名单（如主机名、任务 id）"
    )
    threshold: int | None = Field(
        default=None,
        ge=1,
        description="达标所需报到人数；缺省为全体参与者人数",
    )
    deadline: datetime = Field(
        ...,
        description="截止时间，ISO-8601；不带时区偏移时按 UTC 处理",
    )
    creator_id: str | None = Field(default=None, description="创建者标识，可选")
    participant_groups: dict[str, str] | None = Field(
        default=None,
        description=(
            "参与者 -> 分组名的映射，每个参与者恰属一组；"
            "必须与 group_thresholds 同时提供或同时省略"
        ),
    )
    group_thresholds: dict[str, int] | None = Field(
        default=None,
        description=(
            "各分组的达标配额；启用分组配额后，只有总报到数达到 threshold 且"
            "每个分组报到数均达到对应配额才进入 reached"
        ),
    )

    @model_validator(mode="after")
    def _validate(self) -> "CreateBarrierRequest":
        # 名单去重并保持顺序；空标识无意义
        cleaned: list[str] = []
        for p in self.participants:
            name = p.strip()
            if not name:
                raise ValueError("participants 不得包含空字符串")
            if name not in cleaned:
                cleaned.append(name)
        if not cleaned:
            raise ValueError("participants 至少需要一个成员")
        self.participants = cleaned

        if self.threshold is not None and self.threshold > len(cleaned):
            raise ValueError("threshold 不能大于参与者人数")

        groups_given = self.participant_groups is not None
        quotas_given = self.group_thresholds is not None
        if groups_given != quotas_given:
            raise ValueError(
                "participant_groups 与 group_thresholds 必须同时提供或同时省略"
            )
        if not groups_given:
            return self

        # 每个参与者恰属一组：dict 天然保证一人一组，这里校验覆盖完整、无未知成员。
        roster = set(cleaned)
        group_of: dict[str, str] = {}
        for pid, raw_group in self.participant_groups.items():
            if pid not in roster:
                raise ValueError(f"participant_groups 包含名单外的参与者 {pid!r}")
            group = raw_group.strip()
            if not group:
                raise ValueError(f"参与者 {pid!r} 的分组名为空")
            group_of[pid] = group
        for pid in cleaned:
            if pid not in group_of:
                raise ValueError(f"participant_groups 缺少参与者 {pid!r} 的分组")

        # 配额清洗（分组名去空白；去重后重名视为冲突）
        cleaned_quotas: dict[str, int] = {}
        for raw_group, quota in self.group_thresholds.items():
            group = raw_group.strip()
            if not group:
                raise ValueError("group_thresholds 不得包含空分组名")
            if group in cleaned_quotas:
                raise ValueError(f"分组 {group!r} 在 group_thresholds 中重复")
            if not isinstance(quota, int) or isinstance(quota, bool) or quota <= 0:
                raise ValueError(f"分组 {group!r} 的配额必须为正整数")
            cleaned_quotas[group] = quota

        group_sizes = Counter(group_of.values())
        for group in cleaned_quotas:
            if group not in group_sizes:
                raise ValueError(
                    f"group_thresholds 包含 participant_groups 中不存在的分组 {group!r}"
                )
        for group, size in group_sizes.items():
            if group not in cleaned_quotas:
                raise ValueError(f"group_thresholds 缺少分组 {group!r} 的配额")
            quota = cleaned_quotas[group]
            if quota > size:
                raise ValueError(
                    f"分组 {group!r} 的配额 {quota} 超过该组人数 {size}"
                )

        effective_threshold = (
            self.threshold if self.threshold is not None else len(cleaned)
        )
        quota_sum = sum(cleaned_quotas.values())
        if quota_sum > effective_threshold:
            raise ValueError(
                f"各组配额之和 {quota_sum} 不能大于总达标人数 {effective_threshold}"
            )

        self.participant_groups = group_of
        self.group_thresholds = cleaned_quotas
        return self

    def deadline_ts(self) -> float:
        d = self.deadline
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc).timestamp()


class ExtendDeadlineRequest(BaseModel):
    deadline: datetime = Field(
        ...,
        description=(
            "新的绝对截止时间，ISO-8601；不带时区偏移时按 UTC 处理。"
            "必须严格晚于屏障当前 deadline，否则返回 409"
        ),
    )

    def deadline_ts(self) -> float:
        d = self.deadline
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc).timestamp()


class GroupProgress(BaseModel):
    group: str = Field(description="分组名")
    threshold: int = Field(description="该组达标配额")
    checked_count: int = Field(
        description="该组去重后的报到人数；reached 状态为达标冻结瞬间的快照"
    )
    satisfied: bool = Field(description="该组报到数是否已达到配额")


class BarrierView(BaseModel):
    barrier_id: str
    status: str = Field(description="waiting | reached | expired | cancelled")
    participants: list[str]
    threshold: int
    deadline: datetime
    created_at: datetime
    checked_in: list[str]
    # waiting 阶段的当前报到人数；其余状态为快照人数
    checked_count: int
    # 达标后冻结的成员名单（仅 reached）
    quorum: list[str] | None = None
    frozen_at: datetime | None = None
    # 截止时仍未报到的成员（仅 expired）
    missing: list[str] | None = None
    # 达标冻结之后才报到的成员（reached 状态的补充信息，不改写结果）
    late: list[str] | None = None
    # 主动取消的原因与时间（仅 cancelled）
    cancel_reason: str | None = None
    cancelled_at: datetime | None = None
    creator_id: str | None = None
    # 创建时声明的参与者 -> 分组映射（仅启用分组配额时出现）
    participant_groups: dict[str, str] | None = None
    # 分组配额进度（仅启用分组配额时出现）；未启用时为 null，行为与旧版一致
    group_progress: list[GroupProgress] | None = None


class CheckinRequest(BaseModel):
    participant_id: str = Field(..., min_length=1, description="参与者标识")


class BatchCheckinRequest(BaseModel):
    participants: list[str] = Field(
        ...,
        min_length=1,
        description="批量报到的参与者标识列表（如主机名、任务 id）；自动去除首尾空白，不得含空标识或重复标识",
    )
    idempotency_key: str = Field(
        ...,
        min_length=1,
        max_length=256,
        description=(
            "客户端生成的幂等键（1-256 字符）；同一幂等键的重试返回首次结果，"
            "与不同参与者列表复用同一幂等键返回 409"
        ),
    )

    @model_validator(mode="after")
    def _validate(self) -> "BatchCheckinRequest":
        # 与创建名单一致地清洗：去首尾空白、拒绝空标识、拒绝批内重复。
        cleaned: list[str] = []
        for p in self.participants:
            name = p.strip()
            if not name:
                raise ValueError("participants 不得包含空字符串")
            if name in cleaned:
                raise ValueError(f"参与者 {name!r} 在同一次批量报到中重复")
            cleaned.append(name)
        if not cleaned:
            raise ValueError("participants 至少需要一个成员")
        self.participants = cleaned

        key = self.idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key 不得为空白字符串")
        if len(key) > 256:
            raise ValueError("idempotency_key 长度不得超过 256 字符")
        self.idempotency_key = key
        return self


class BatchParticipantResult(BaseModel):
    participant_id: str = Field(description="参与者标识")
    result: str = Field(
        description=(
            "该成员在本次（首次执行的）批量操作中的结果："
            "checked_in（新报到）| already_checked_in（此前已报到）"
            "| reached（本批中触发达标的成员，至多一个）| late（达标后报到）"
        )
    )


class BatchCheckinResponse(BaseModel):
    barrier_id: str
    status: str = Field(
        description=(
            "checked_in（本批已受理，屏障继续等待）| reached（本批使屏障达标）"
            "| late（屏障已达标，本批全部记为迟到）"
        )
    )
    triggered: bool = Field(description="本批是否首次使屏障达标并冻结 quorum；重试回放恒为首次值")
    replayed: bool = Field(
        description="true 表示本次响应由同一幂等键的首次结果回放，未再次执行报到"
    )
    idempotency_key: str = Field(description="请求携带的幂等键（去首尾空白后）")
    accepted_count: int = Field(description="本批参与者条数（等于 accepted 长度）")
    accepted: list[BatchParticipantResult] = Field(
        description="逐成员结果，顺序与首次请求的 participants 一致；重试回放首次列表"
    )
    quorum: list[str] | None = None
    frozen_at: datetime | None = None
    view: BarrierView


class CheckinResponse(BaseModel):
    barrier_id: str
    status: str = Field(
        description=(
            "checked_in（已报到，继续等待）| already_checked_in（幂等重复报到）"
            "| reached（本次报到触发达标）| late（达标后报到，快照未变）"
        )
    )
    triggered: bool
    quorum: list[str] | None = None
    frozen_at: datetime | None = None
    view: BarrierView


class CancelRequest(BaseModel):
    reason: str = Field(
        ...,
        min_length=1,
        description="取消原因，非空；去除首尾空白后不得为空字符串",
    )

    @model_validator(mode="after")
    def _validate(self) -> "CancelRequest":
        reason = self.reason.strip()
        if not reason:
            raise ValueError("reason 不得为空白字符串")
        self.reason = reason
        return self


class CancelResponse(BaseModel):
    barrier_id: str
    status: str = Field(
        description="cancelled（本次完成取消）| already_cancelled（重复取消，幂等返回同一结果）"
    )
    reason: str = Field(description="取消原因；重复取消时返回首次保存的原因")
    cancelled_at: datetime
    view: BarrierView


class ReplaceRequest(BaseModel):
    participant_id: str = Field(
        ..., min_length=1, description="被替换的原参与者标识（必须仍在名单内且尚未报到）"
    )
    replacement_id: str = Field(
        ..., min_length=1, description="换上的新参与者标识（必须是当前名单外的新标识）"
    )

    @model_validator(mode="after")
    def _validate(self) -> "ReplaceRequest":
        # 与创建名单一致地去除首尾空白；两个标识清洗后都不得为空、不得相同。
        original = self.participant_id.strip()
        replacement = self.replacement_id.strip()
        if not original:
            raise ValueError("participant_id 不得为空白字符串")
        if not replacement:
            raise ValueError("replacement_id 不得为空白字符串")
        if original == replacement:
            raise ValueError("replacement_id 必须是名单外的新标识，不能与原参与者相同")
        self.participant_id = original
        self.replacement_id = replacement
        return self


class ReplaceResponse(BaseModel):
    barrier_id: str
    status: str = Field(description="replaced（本次替补原子生效）")
    participant_id: str = Field(description="被换下的原参与者标识")
    replacement_id: str = Field(description="换上的新参与者标识（换人后即为现名单成员）")
    group: str | None = Field(
        default=None,
        description="新成员继承自原成员的分组名；未启用分组配额时为 null",
    )
    replaced_at: datetime = Field(description="替补生效时间（UTC）")
    view: BarrierView


class CheckinEvent(BaseModel):
    seq: int = Field(description="事件序号，从 1 开始单调递增，只追加不回改")
    participant_id: str = Field(
        description=(
            "事件相关的参与者标识：报到事件为报到者；替补事件为换上的新标识"
        )
    )
    result: str = Field(
        description=(
            "checked_in | already_checked_in | reached | late | replaced"
            "（replaced 为运行中参与者替补事件）"
        )
    )
    ts: datetime = Field(description="事件发生时间（UTC）")
    # 仅替补事件（result=replaced）出现：被换下的原参与者标识；
    # 替补事件中 participant_id 为换上的新标识。其余事件不返回该字段。
    original_participant_id: str | None = Field(
        default=None,
        description="仅替补事件出现：被换下的原参与者标识",
    )


class BarrierEventsPage(BaseModel):
    barrier_id: str
    events: list[CheckinEvent]
    next_cursor: int = Field(
        description="下一页游标（本页最大 seq）；以其为 cursor 继续查询，无新事件时与入参 cursor 相同"
    )


# ---------------------------------------------------------------------------
# 达标结果交接单 handoff
# ---------------------------------------------------------------------------


class CreateHandoffRequest(BaseModel):
    barrier_id: str = Field(
        ..., min_length=1, description="已达标屏障的 id；仅 reached 屏障可建立交接单"
    )

    @model_validator(mode="after")
    def _validate(self) -> "CreateHandoffRequest":
        bid = self.barrier_id.strip()
        if not bid:
            raise ValueError("barrier_id 不得为空白字符串")
        self.barrier_id = bid
        return self


class ClaimRequest(BaseModel):
    owner: str = Field(
        ...,
        min_length=1,
        description="认领者标识（去首尾空白后非空）；租约期内凭 owner + fence 操作",
    )

    @model_validator(mode="after")
    def _validate(self) -> "ClaimRequest":
        owner = self.owner.strip()
        if not owner:
            raise ValueError("owner 不得为空白字符串")
        self.owner = owner
        return self


class RenewRequest(BaseModel):
    owner: str = Field(..., min_length=1, description="当前租约持有者标识")
    fence: int = Field(
        ...,
        ge=1,
        description="认领时获得的单调递增栅栏令牌；被到期接管后旧 fence 立即失效",
    )

    @model_validator(mode="after")
    def _validate(self) -> "RenewRequest":
        owner = self.owner.strip()
        if not owner:
            raise ValueError("owner 不得为空白字符串")
        self.owner = owner
        return self


class CompleteRequest(BaseModel):
    owner: str = Field(..., min_length=1, description="当前租约持有者标识")
    fence: int = Field(..., ge=1, description="认领时获得的栅栏令牌")

    @model_validator(mode="after")
    def _validate(self) -> "CompleteRequest":
        owner = self.owner.strip()
        if not owner:
            raise ValueError("owner 不得为空白字符串")
        self.owner = owner
        return self


class HandoffView(BaseModel):
    handoff_id: str
    barrier_id: str = Field(description="来源屏障 id；屏障回收后本单仍独立保存")
    status: str = Field(
        description="idle（空闲可认领：从未认领或租约已到期）| leased（租约有效）| completed（已完成终态）"
    )
    # 自屏障达标冻结快照独立复制的结果，交接单生命周期内永不改写
    quorum: list[str] = Field(description="达标成员快照（创建时自屏障 frozen 复制）")
    frozen_at: datetime = Field(description="屏障达标冻结时刻（UTC，自 frozen 复制）")
    # 租约信息：仅 leased 状态出现；idle 不返回上一任持有者的失效凭证
    owner: str | None = Field(default=None, description="当前租约持有者；idle/completed 为 null")
    fence: int | None = Field(
        default=None,
        description="当前租约的单调递增栅栏令牌；每次到期接管 +1；idle/completed 为 null",
    )
    leased_at: datetime | None = None
    expires_at: datetime | None = Field(
        default=None, description="租约到期时刻（UTC，以 Redis 时间裁决）"
    )
    completed_at: datetime | None = Field(
        default=None, description="完成时刻（UTC，仅 completed）"
    )


class ClaimResponse(BaseModel):
    handoff_id: str
    status: str = Field(description="leased（认领 / 到期接管成功）")
    owner: str
    fence: int = Field(description="本次认领的栅栏令牌；到期接管时严格大于上一任")
    leased_at: datetime
    expires_at: datetime


class RenewResponse(BaseModel):
    handoff_id: str
    status: str = Field(description="leased（续租成功）")
    owner: str
    fence: int = Field(description="续租不更换持有者，fence 保持不变")
    leased_at: datetime
    expires_at: datetime = Field(description="续租后新的到期时刻（UTC）")


class CompleteHandoffResponse(BaseModel):
    handoff_id: str
    status: str = Field(description="completed（本次完成；终态永久拒绝再次认领）")
    owner: str
    fence: int
    completed_at: datetime


# ---------------------------------------------------------------------------
# 交接单处理情况对账快照 reconciliation
# ---------------------------------------------------------------------------

# 批次标识允许字母 / 数字 / - _ . : 与空白，防止任意内容进入 Redis 键空间。
_BATCH_ID_RE = re.compile(r"^[A-Za-z0-9\-_.: ]+$")


class ReconciliationRequest(BaseModel):
    handoff_ids: list[str] = Field(
        ...,
        min_length=1,
        description=(
            "非空交接单 ID 列表，按输入顺序逐单记录；不得包含空标识或重复标识"
        ),
    )

    @model_validator(mode="after")
    def _validate(self) -> "ReconciliationRequest":
        cleaned: list[str] = []
        for raw in self.handoff_ids:
            hid = raw.strip()
            if not hid:
                raise ValueError("handoff_ids 不得包含空字符串")
            if hid in cleaned:
                raise ValueError(f"交接单 {hid!r} 在同一批次中重复")
            cleaned.append(hid)
        if not cleaned:
            raise ValueError("handoff_ids 至少需要一个交接单")
        self.handoff_ids = cleaned
        return self


def validate_batch_id(batch_id: str) -> str:
    """校验路径中的批次标识：去首尾空白后非空、长度受限且字符安全。"""
    bid = batch_id.strip()
    if not bid:
        raise ValueError("batch_id 不得为空白字符串")
    if len(bid) > 128:
        raise ValueError("batch_id 长度不得超过 128 字符")
    if not _BATCH_ID_RE.fullmatch(bid):
        raise ValueError("batch_id 只能包含字母、数字、空格与 - _ . : 字符")
    return bid


class SnapshotItemView(BaseModel):
    handoff_id: str
    barrier_id: str = Field(description="来源屏障 id（自交接单保存快照瞬间读取）")
    status: str = Field(
        description=(
            "快照保存瞬间交接单的有效状态：completed（已完成）"
            "| leased（租约有效，未完成）| idle（空闲或租约已到期，未完成）"
        )
    )
    completed_at: datetime | None = Field(
        default=None, description="完成时刻（UTC，仅 status=completed）"
    )


class ReconciliationView(BaseModel):
    batch_id: str
    created_at: datetime = Field(
        description="首次快照保存时刻（UTC，以 Redis 时间裁决）；重试回放首次值"
    )
    handoff_ids: list[str] = Field(
        description="首次提交的交接单列表（保序，用于复核与冲突比对）"
    )
    items: list[SnapshotItemView] = Field(
        description="逐单处理情况，顺序与 handoff_ids 一致"
    )
    completed_count: int = Field(
        description="已完成数量：保存瞬间 status=completed 的交接单数"
    )
    pending_count: int = Field(
        description="未完成数量：保存瞬间 status 为 leased/idle 的交接单数"
    )
    replayed: bool = Field(
        default=False,
        description="true 表示同批次标识 + 相同列表的重试，响应为首次快照回放",
    )
