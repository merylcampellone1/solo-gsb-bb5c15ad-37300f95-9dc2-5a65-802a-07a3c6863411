"""Redis 持久层：交接单处理情况对账快照（reconciliation snapshot）。

收尾人员按**批次**把一批交接单在某一时刻的处理情况固定为可复核快照。
批次标识由调用方给定（如 ``2026-09-29-payroll``），同一批次标识 + 同一份
交接单列表的重试逐字段回放首次快照；批次标识相同但列表不同返回冲突；任一
交接单不存在时整批失败且**不占用批次标识**（随后可用同一批次标识重试）。

键布局（无 TTL：对账快照需长期可复核，与交接单本体一样独立持久保存）：

``recon:{batch_id}``       hash  批次快照本体：
                           batch_id        批次标识
                           created_at      首次快照时刻（Redis TIME，秒）
                           fingerprint     首次提交的交接单有序列表指纹
                           list_json       首次提交的交接单列表（JSON，保序）
                           items_json      逐单快照（JSON，顺序与列表一致）
                           completed_count 已完成数量
                           pending_count   未完成数量

读取每一张交接单（HGET 多字段）、判定有效状态、统计完成 / 未完成数量与
整批快照落盘在**同一段 Lua 脚本**内一次原子完成：快照在脚本执行的同一个
Redis 瞬间读取全部交接单，并发的完成 / 认领 / 到期接管不可能让同一批次混入
不同时刻的状态。交接单本体在快照保存后仍可继续流转（认领 / 续租 / 完成），
但快照只读，之后任何变化都不改写它。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis


# 提交批次脚本。一次原子完成：
#   1) 批次已存在：指纹相同 -> 回放首次快照；指纹不同 -> conflict（整批不写）；
#   2) 逐张校验交接单存在性：任一不存在 -> not_found，整批零副作用，
#      不写批次键（不占用批次标识），可随后用同一批次标识安全重试；
#   3) 在同一脚本内读取全部交接单的 barrier_id / status / owner / fence /
#      expires_at / completed_at，以 Redis TIME 裁决每张单在**本瞬间**的
#      有效状态（completed | leased | idle），统计完成 / 未完成数量；
#   4) 批次快照（逐单快照 + 计数 + 指纹 + 列表）原子落盘。
#
# 返回数组（定长，位置取值，避免 Lua 空表在 RESP 中被编码为 nil 带来的歧义）：
#   {'unknown', hid}
#       第 N 张交接单不存在；整批零副作用，批次标识未被占用
#   {'conflict'}
#       该批次标识已保存过另一份交接单列表（409）
#   {'ok', created_flag, batch_id, created_at, items_json,
#    completed_count, pending_count, list_json}
#       created_flag='1' 首次创建；'0' 同批次同列表重放首次快照
SUBMIT_SNAPSHOT_SCRIPT = """
local batch_key   = KEYS[1]
local list_json   = ARGV[1]
local fingerprint = ARGV[2]

-- 既有批次：永远以首次快照为准。指纹相同原样回放；不同则冲突，绝不改写。
if redis.call('HEXISTS', batch_key, 'batch_id') == 1 then
    if redis.call('HGET', batch_key, 'fingerprint') ~= fingerprint then
        return {'conflict'}
    end
    return {'ok', '0',
            redis.call('HGET', batch_key, 'batch_id'),
            redis.call('HGET', batch_key, 'created_at'),
            redis.call('HGET', batch_key, 'items_json'),
            redis.call('HGET', batch_key, 'completed_count'),
            redis.call('HGET', batch_key, 'pending_count'),
            redis.call('HGET', batch_key, 'list_json')}
end

local ids = cjson.decode(list_json)

-- 先校验全部交接单存在，再读取 / 写入任何快照内容：任一不存在即整批失败，
-- 批次键从未被创建，因此批次标识未被占用。
for _, hid in ipairs(ids) do
    if redis.call('HEXISTS', 'handoff:' .. hid, 'status') == 0 then
        return {'unknown', hid}
    end
end

-- 租约是否到期以 Redis 自身时间裁决（与认领 / 续租 / 完成脚本同一时钟）。
local t = redis.call('TIME')
local now = t[1] + t[2] / 1000000

local items = {}
local completed_count = 0
local pending_count = 0
for i, hid in ipairs(ids) do
    local key = 'handoff:' .. hid
    local vals = redis.call('HMGET', key,
        'barrier_id', 'status', 'owner', 'fence', 'expires_at', 'completed_at')
    local stored_status = vals[2]
    local effective
    local completed_at = false
    if stored_status == 'completed' then
        effective = 'completed'
        completed_count = completed_count + 1
        completed_at = tonumber(vals[6])
    else
        pending_count = pending_count + 1
        local expires = tonumber(vals[5]) or 0
        if stored_status == 'leased' and expires > now then
            effective = 'leased'
        else
            -- idle（从未认领）或租约已到期：与 GET /handoffs 一致，
            -- 对账口径统一记为 idle，不暴露上一任的失效凭证。
            effective = 'idle'
        end
    end
    items[i] = {
        handoff_id = hid,
        barrier_id = vals[1],
        status = effective,
        completed_at = completed_at
    }
end

redis.call('HSET', batch_key,
    'batch_id', ARGV[3],
    'created_at', tostring(now),
    'fingerprint', fingerprint,
    'list_json', list_json,
    'items_json', cjson.encode(items),
    'completed_count', tostring(completed_count),
    'pending_count', tostring(pending_count))
-- 对账快照不设 TTL：已保存快照不随交接单后续变化改写，长期可复核。

return {'ok', '1', ARGV[3], tostring(now), cjson.encode(items),
        tostring(completed_count), tostring(pending_count), list_json}
"""

# 读取脚本：只读取批次快照本身，绝不回读任何交接单（快照是保存时刻的定格）。
# 返回 {0} 表示批次不存在；否则返回
# {1, batch_id, created_at, items_json, completed_count,
#  pending_count, list_json, fingerprint}。
READ_SNAPSHOT_SCRIPT = """
local key = KEYS[1]
if redis.call('HEXISTS', key, 'batch_id') == 0 then
    return {0}
end
return {1,
    redis.call('HGET', key, 'batch_id'),
    redis.call('HGET', key, 'created_at'),
    redis.call('HGET', key, 'items_json'),
    redis.call('HGET', key, 'completed_count'),
    redis.call('HGET', key, 'pending_count'),
    redis.call('HGET', key, 'list_json'),
    redis.call('HGET', key, 'fingerprint')}
"""


class SnapshotBatchConflict(Exception):
    """批次标识已用于另一份交接单列表；首次快照不可改写。"""


class SnapshotHandoffNotFound(Exception):
    """批次中存在不存在的交接单；整批失败且不占用批次标识。

    参数携带第一个被发现的不存在交接单 id，用于错误文案。
    """


@dataclass(frozen=True)
class SnapshotItem:
    """快照中一张交接单在批次保存瞬间的处理情况。"""

    handoff_id: str
    # 来源屏障
    barrier_id: str
    # 保存瞬间的有效状态：completed（已完成）| leased（租约有效，未完成）
    # | idle（空闲或租约已到期，未完成）
    status: str
    # 完成时刻（Unix 秒）；仅 status=completed 时有值
    completed_at: float | None

    @property
    def completed(self) -> bool:
        return self.status == "completed"


@dataclass(frozen=True)
class ReconciliationSnapshot:
    batch_id: str
    created_at: float
    items: tuple[SnapshotItem, ...]
    completed_count: int
    pending_count: int
    # 首次提交的交接单列表（保序）
    handoff_ids: tuple[str, ...]


class ReconciliationStore:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self._submit = redis.register_script(SUBMIT_SNAPSHOT_SCRIPT)
        self._read = redis.register_script(READ_SNAPSHOT_SCRIPT)

    @staticmethod
    def batch_key(batch_id: str) -> str:
        return f"recon:{batch_id}"

    @staticmethod
    def fingerprint(handoff_ids: list[str]) -> str:
        """有序列表指纹：同顺序同一组 id 才算同一份列表的重试。

        指纹同时纳入**顺序与成员**：与“按输入顺序记录”的语义保持一致，
        换序提交视为另一份列表（409），不静默回放顺序不同的首次快照。
        """
        return hashlib.sha256(
            json.dumps(handoff_ids, ensure_ascii=False).encode("utf-8")
        ).hexdigest()

    async def submit(
        self, batch_id: str, handoff_ids: list[str]
    ) -> tuple[ReconciliationSnapshot, bool]:
        """提交 / 重试批次对账快照。

        - 首次提交：原子读取全部交接单并落盘，返回 ``(snapshot, created=True)``；
        - 同批次标识 + 完全相同（顺序敏感）的列表：回放首次快照，
          ``created=False``；
        - 同批次标识但列表不同 → :class:`SnapshotBatchConflict`；
        - 任一交接单不存在 → :class:`SnapshotHandoffNotFound`，整批失败且
          不写入批次键（不占用批次标识）。
        """
        raw = await self._submit(
            keys=[self.batch_key(batch_id)],
            args=[
                json.dumps(handoff_ids, ensure_ascii=False),
                self.fingerprint(handoff_ids),
                batch_id,
            ],
        )
        status = raw[0]
        if status == "conflict":
            raise SnapshotBatchConflict(batch_id)
        if status == "unknown":
            raise SnapshotHandoffNotFound(raw[1])

        created = raw[1] == "1"
        snapshot = self._decode(
            batch_id=raw[2],
            created_at=raw[3],
            items_json=raw[4],
            completed_count=raw[5],
            pending_count=raw[6],
            list_json=raw[7],
        )
        return snapshot, created

    async def get(self, batch_id: str) -> ReconciliationSnapshot | None:
        """读取已保存的批次快照；批次不存在返回 None。绝不回读交接单实时数据。"""
        raw = await self._read(keys=[self.batch_key(batch_id)])
        if not raw or int(raw[0]) == 0:
            return None
        return self._decode(
            batch_id=raw[1],
            created_at=raw[2],
            items_json=raw[3],
            completed_count=raw[4],
            pending_count=raw[5],
            list_json=raw[6],
        )

    @staticmethod
    def _decode(
        *,
        batch_id: str,
        created_at: str,
        items_json: str,
        completed_count: str,
        pending_count: str,
        list_json: str,
    ) -> ReconciliationSnapshot:
        items_raw: list[dict[str, Any]] = json.loads(items_json)
        items = tuple(
            SnapshotItem(
                handoff_id=item["handoff_id"],
                barrier_id=item["barrier_id"],
                status=item["status"],
                completed_at=(
                    float(item["completed_at"])
                    if item.get("completed_at") not in (None, False)
                    else None
                ),
            )
            for item in items_raw
        )
        return ReconciliationSnapshot(
            batch_id=batch_id,
            created_at=float(created_at),
            items=items,
            completed_count=int(completed_count),
            pending_count=int(pending_count),
            handoff_ids=tuple(json.loads(list_json)),
        )
