"""Redis 持久层：交接单处理情况的对账快照（reconciliation snapshot）。

收尾人员提交一个**批次标识**与一份**非空交接单 id 列表**，系统在首次提交时
按输入顺序把每单当时的「来源屏障 / 冻结成员 / 状态 / 完成时间」固化为一份
**可复核**的快照，并给出已完成与未完成数量。

``recon:{batch_id}``  hash  对账快照本体（无 TTL，独立持久保存）：
                      fingerprint 首次提交的交接单有序列表（JSON 字符串），
                                  同批次重试据此判定回放 / 冲突
                      payload     首次执行时刻固化的完整快照（JSON，见下）
                      created_at  快照时刻（Redis 时间，秒）

payload 结构（JSON）：
``{created_at, completed_count, pending_count,
   items:[{handoff_id, barrier_id, quorum, frozen_at, status, completed_at}]}``

「查重 / 指纹比较 / 逐单存在性校验 / 逐单读取 / 批次落盘」在**同一段 Lua
脚本**内一次原子完成：

- 同批次 + **相同列表**（成员集合相同，顺序可不同）重试 → 不再次读取交接单，
  逐字段回放**首次快照**（replayed，含首次的条目顺序），即使各单之后被
  认领 / 完成也不改变；
- 同批次 + **不同列表** → ``conflict``（409），首次快照不可改写；
- 列表中**任一交接单不存在** → ``unknown``（404），整批中止于任何写入之前，
  **不占用批次标识**，可用同一批次标识携带修正后的列表安全重试；
- 全部读取发生在脚本内同一次 ``redis.call('TIME')`` 的同一瞬间，与交接单的
  完成脚本（另一段原子 Lua）在 Redis 单线程上串行：并发完成绝不会让同一批次
  混入不同时刻的状态（不会出现部分成员记为完成前、部分记为完成后的拼接视图）。

快照键 ``recon:*`` 与交接单 ``handoff:*`` 一样**从不设置 TTL**：快照一经
保存即独立持久，交接单后续被认领、到期接管、完成都不会回写到快照。
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from redis.asyncio import Redis

# 提交（首建 / 回放）脚本。KEYS = [批次键, handoff:{id1}, ..., handoff:{idN}]；
# ARGV = [指纹 JSON, N, id1, ..., idN]。返回：
#   {'created', payload}   首次提交完成，payload 为本次固化的快照 JSON
#   {'replayed', payload}  同批次相同列表（集合相同，顺序可不同）重试：
#                          回放首次快照（不再读交接单）
#   {'conflict'}           同批次已被另一份列表占用（409）
#   {'unknown', handoff_id} 某个交接单不存在（404），整批零写入、不占批次标识
#
# 查重先于存在性校验：已保存批次的重试永远直接回放 / 冲突，不会因其中某单
# 事后被删除而改变（快照自包含，不依赖交接单继续存在）。
RECON_PUT_SCRIPT = """
local batch_key = KEYS[1]
local fingerprint = ARGV[1]
local n = tonumber(ARGV[2])

local existing = redis.call('HGET', batch_key, 'fingerprint')
if existing then
    if existing == fingerprint then
        return {'replayed', redis.call('HGET', batch_key, 'payload')}
    end
    return {'conflict'}
end

-- 首次提交：先做整批存在性校验，任一交接单缺失立即返回。
-- 此刻尚未写入任何键，批次标识保持未占用，可随后原样重试。
for i = 1, n do
    if redis.call('HEXISTS', KEYS[i + 1], 'status') == 0 then
        return {'unknown', ARGV[2 + i]}
    end
end

-- 全部交接单在同一脚本、同一次 TIME 读取：并发的完成脚本要么整体先于本次
-- 读取（全部看到完成后状态），要么整体后于本次读取，不会在批次内部跨时刻拼接。
local t = redis.call('TIME')
local now = t[1] + t[2] / 1000000

local items = {}
local completed = 0
for i = 1, n do
    local vals = redis.call('HMGET', KEYS[i + 1],
        'barrier_id', 'quorum', 'frozen_at', 'status', 'expires_at',
        'completed_at')
    local stored_status = vals[4]
    local eff_status = stored_status
    -- 与交接单读取脚本同一套对外状态裁决：leased 仅当租约按 Redis TIME
    -- 仍有效；租约已到期对外回落为 idle。
    if stored_status ~= 'completed' then
        local expires = tonumber(vals[5]) or 0
        if expires > now then
            eff_status = 'leased'
        else
            eff_status = 'idle'
        end
    end

    local item = {
        handoff_id = ARGV[2 + i],
        barrier_id = vals[1],
        quorum = cjson.decode(vals[2]),
        frozen_at = tonumber(vals[3]),
        status = eff_status
    }
    if eff_status == 'completed' then
        completed = completed + 1
        item.completed_at = tonumber(vals[6])
    end
    items[i] = item
end

local payload = cjson.encode({
    created_at = now,
    completed_count = completed,
    pending_count = n - completed,
    items = items
})
redis.call('HSET', batch_key,
    'fingerprint', fingerprint,
    'payload', payload,
    'created_at', tostring(now))
return {'created', payload}
"""


class ReconciliationConflict(Exception):
    """同一批次标识已被另一份交接单列表占用；首次快照不可改写。"""


class ReconBatchNotFound(Exception):
    """对账快照批次不存在。"""


@dataclass(frozen=True)
class ReconItem:
    """批次中一张交接单在快照瞬间的处理情况。"""

    handoff_id: str
    # 来源屏障（handoff 创建时记录，快照自交接单复制）
    barrier_id: str
    # 达标冻结成员（创建交接单时自屏障 frozen 复制的那份名单）
    frozen_members: list[str]
    # 屏障达标冻结时刻（Unix 秒）
    frozen_at: float
    # idle（空闲可认领）| leased（租约有效）| completed（已完成）
    status: str
    # 完成时刻（Unix 秒）；仅 status=completed 出现
    completed_at: float | None

    @property
    def is_completed(self) -> bool:
        return self.status == "completed"


@dataclass(frozen=True)
class ReconSnapshot:
    """一份已固化的对账快照：内容不随交接单后续变化而改写。"""

    batch_id: str
    created_at: float  # 快照时刻（Unix 秒，Redis 时间）
    items: tuple[ReconItem, ...]
    completed_count: int
    pending_count: int


def _parse_payload(batch_id: str, payload: str) -> ReconSnapshot:
    data = json.loads(payload)
    items = tuple(
        ReconItem(
            handoff_id=item["handoff_id"],
            barrier_id=item["barrier_id"],
            frozen_members=list(item["quorum"]),
            frozen_at=float(item["frozen_at"]),
            status=item["status"],
            completed_at=(
                float(item["completed_at"]) if item.get("completed_at") is not None else None
            ),
        )
        for item in data["items"]
    )
    return ReconSnapshot(
        batch_id=batch_id,
        created_at=float(data["created_at"]),
        items=items,
        completed_count=int(data["completed_count"]),
        pending_count=int(data["pending_count"]),
    )


class ReconStore:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self._put = redis.register_script(RECON_PUT_SCRIPT)

    @staticmethod
    def batch_key(batch_id: str) -> str:
        return f"recon:{batch_id}"

    @staticmethod
    def fingerprint(handoff_ids: list[str]) -> str:
        """列表指纹：以**成员集合**判定「同一份列表」（顺序可不同）。

        首次提交按输入顺序固化逐单结果；重试即使重排了 id 顺序，也回放首次
        快照（含首次的条目顺序），与批量报到接口「集合相同即可回放」一致。
        调用方已完成去空白、非空与批内去重校验。
        """
        return json.dumps(sorted(handoff_ids), ensure_ascii=False)

    async def put(
        self, batch_id: str, handoff_ids: list[str]
    ) -> tuple[ReconSnapshot, bool]:
        """首建或回放对账快照。

        返回 ``(snapshot, created)``：首次提交 ``created=True``，同批次相同
        有序列表重试 ``created=False``（回放首次快照）。同批次不同列表抛
        :class:`ReconciliationConflict`；任一交接单不存在抛
        :class:`HandoffNotFound`（整批零写入、不占用批次标识）。
        """
        from .handoff_store import HandoffNotFound, HandoffStore

        keys = [self.batch_key(batch_id)] + [
            HandoffStore.handoff_key(hid) for hid in handoff_ids
        ]
        args = [self.fingerprint(handoff_ids), str(len(handoff_ids)), *handoff_ids]
        raw = await self._put(keys=keys, args=args)

        if raw[0] == "conflict":
            raise ReconciliationConflict(batch_id)
        if raw[0] == "unknown":
            raise HandoffNotFound(raw[1])
        snapshot = _parse_payload(batch_id, raw[1])
        return snapshot, raw[0] == "created"

    async def get(self, batch_id: str) -> ReconSnapshot | None:
        """读取已固化的对账快照（只读 recon 键，绝不回读交接单当前状态）。"""
        payload = await self.redis.hget(self.batch_key(batch_id), "payload")
        if payload is None:
            return None
        return _parse_payload(batch_id, payload)
