"""Redis 持久层：达标结果交接单（handoff）及其认领 / 续租 / 完成租约状态机。

交接单在某屏障**达标冻结之后**创建，独立复制一份达标快照（quorum、frozen_at），
此后与屏障的生命周期完全解耦：

``handoff:{hid}``            hash  交接单本体（无 TTL，独立持久保存）：
                             barrier_id  来源屏障
                             quorum      达标成员快照（创建时自 frozen 复制）
                             frozen_at   达标冻结时刻（自 frozen 复制）
                             status      idle（空闲可认领）| leased（租约有效）
                                         | completed（已完成，终态）
                             owner/fence 当前持有者与其单调递增的栅栏令牌
                             leased_at   当前租约的起租时刻
                             expires_at  当前租约的到期时刻（Redis 时间）
                             completed_at 完成时刻（仅 completed）
``handoff:bybarrier:{bid}``  string 来源屏障 -> 交接单 id 的映射（无 TTL）。
                             同一屏障重复创建据此返回原单；屏障被回收不影响交接单。

认领 / 续租 / 完成三段 Lua 脚本全部以 ``redis.call('TIME')`` 取 **Redis 自身
时间**裁决租约是否到期，竞争认领（空闲抢占 / 到期接管）、旧 owner 与旧 fence
凭证拒绝在同一段脚本内一次原子完成，Redis 单线程串行化保证并发唯一结果。
交接单键**从不设置 TTL**：屏障键到期回收（或被删除）后交接单与其索引仍在。
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis

from .barrier_store import BarrierNotFound


# 创建交接单脚本。返回数组：
#   {'unknown'}                  屏障不存在（meta 已过期回收或从未存在）
#   {'not_reached'}              屏障尚未达标冻结（waiting/expired/cancelled）
#   {'exists', hid}              同一屏障已有交接单：原样返回其 id（幂等）
#   {'created', hid, quorum, frozen_at, now}  本次创建完成，快照自 frozen 复制
#
# 「查索引 -> 校验屏障终态 -> 写交接单 -> 建索引」在同一脚本内原子完成：
# 并发的两个创建请求由 Redis 串行执行，先到者写索引与交接单，后到者只能看到
# 既有索引并返回原单，绝不可能为同一屏障产生两份交接单。
CREATE_HANDOFF_SCRIPT = """
local meta_key   = KEYS[1]
local frozen_key = KEYS[2]
local idx_key    = KEYS[3]
local hid        = ARGV[1]
local bid        = ARGV[2]

-- 同一屏障重复创建：索引先于一切检查，既有交接单永远原样返回。
local existing = redis.call('GET', idx_key)
if existing then
    return {'exists', existing}
end

-- 屏障必须存在（meta 含 threshold）且已达标冻结（frozen 含 quorum）：
-- cancelled 与 expired 不写 frozen，因此在此统一归入 not_reached（409）。
if redis.call('HEXISTS', meta_key, 'threshold') == 0 then
    return {'unknown'}
end
local quorum = redis.call('HGET', frozen_key, 'quorum')
if not quorum then
    return {'not_reached'}
end

local frozen_at = redis.call('HGET', frozen_key, 'frozen_at')
local handoff_key = 'handoff:' .. hid
-- status=idle 表示从未被认领；owner/fence/时间字段留空串，由认领脚本首次写入。
-- 交接单独立持久保存：不设置任何 TTL，屏障回收后该键与索引依然存在。
redis.call('HSET', handoff_key,
    'barrier_id', bid,
    'quorum', quorum,
    'frozen_at', frozen_at,
    'status', 'idle',
    'owner', '',
    'fence', '',
    'leased_at', '',
    'expires_at', '',
    'completed_at', '')
redis.call('SET', idx_key, hid)

local t = redis.call('TIME')
local now = t[1] + t[2] / 1000000
return {'created', hid, quorum, frozen_at, tostring(now)}
"""


# 认领脚本（空闲抢占 / 到期接管）。返回数组：
#   {'unknown'}                                       交接单不存在
#   {'completed'}                                     已完成，永久拒绝认领
#   {'busy', owner, fence, expires_at}                他人租约仍有效，认领被拒
#   {'claimed', owner, fence, leased_at, expires_at}  认领/接管成功
#
# 「读 status -> 读 expires_at -> 与 Redis TIME 比较 -> fence+1 -> 写回新租约」
# 整体原子：并发认领只有一个请求能越过 expires_at 判定并写回；接管时 fence
# 严格 +1，旧持有者的旧 fence 因此在续租 / 完成时必然被拒。
CLAIM_SCRIPT = """
local key   = KEYS[1]
local owner = ARGV[1]
local lease = tonumber(ARGV[2])

if redis.call('HEXISTS', key, 'status') == 0 then
    return {'unknown'}
end
if redis.call('HGET', key, 'status') == 'completed' then
    return {'completed'}
end

local t = redis.call('TIME')
local now = t[1] + t[2] / 1000000
-- HGET 缺失字段返回 Lua false，空串字段返回 ''（在 Lua 中为真值）：
-- 两者都必须回落到 0，否则 tonumber('') 为 nil，数值比较会抛错。
local expires = tonumber(redis.call('HGET', key, 'expires_at')) or 0
if expires > now then
    return {'busy', redis.call('HGET', key, 'owner'),
            redis.call('HGET', key, 'fence'), tostring(expires)}
end

-- 空闲（fence 初始为 0）或租约已到期：认领 / 接管，fence 单调递增。
local fence = tonumber(redis.call('HGET', key, 'fence')) or 0
fence = fence + 1
local new_expires = now + lease
redis.call('HSET', key,
    'status', 'leased',
    'owner', owner,
    'fence', tostring(fence),
    'leased_at', tostring(now),
    'expires_at', tostring(new_expires))
return {'claimed', owner, tostring(fence), tostring(now), tostring(new_expires)}
"""


# 续租脚本。返回数组：
#   {'unknown'}                                  交接单不存在
#   {'completed'}                                已完成，终态拒绝一切租约操作
#   {'expired', owner, fence}                    租约已到期（须重新认领）
#   {'bad_owner'}                                owner 与当前持有者不符（旧凭证）
#   {'bad_fence'}                                fence 与当前栅栏不符（被接管后的旧凭证）
#   {'renewed', owner, fence, leased_at, expires_at} 续租成功，到期时间向后顺延
#
# 凭证（owner + fence）与租约有效性的校验、到期时间写回在同一脚本内原子完成：
# 到期接管一旦发生，fence 已前进，旧持有者的续租必然得到 bad_fence。
RENEW_SCRIPT = """
local key   = KEYS[1]
local owner = ARGV[1]
local fence = tonumber(ARGV[2])
local lease = tonumber(ARGV[3])

if redis.call('HEXISTS', key, 'status') == 0 then
    return {'unknown'}
end
if redis.call('HGET', key, 'status') == 'completed' then
    return {'completed'}
end

local t = redis.call('TIME')
local now = t[1] + t[2] / 1000000
local expires = tonumber(redis.call('HGET', key, 'expires_at')) or 0
if expires <= now then
    return {'expired', redis.call('HGET', key, 'owner'),
            redis.call('HGET', key, 'fence')}
end
if redis.call('HGET', key, 'owner') ~= owner then
    return {'bad_owner'}
end
if (tonumber(redis.call('HGET', key, 'fence')) or 0) ~= fence then
    return {'bad_fence'}
end

local new_expires = now + lease
redis.call('HSET', key, 'expires_at', tostring(new_expires))
return {'renewed', owner, tostring(fence),
        redis.call('HGET', key, 'leased_at'), tostring(new_expires)}
"""


# 完成脚本。返回数组：
#   {'unknown'}                         交接单不存在
#   {'completed'}                       已完成过：终态不可再次完成（永久拒绝）
#   {'expired', owner, fence}           租约已到期（须重新认领）
#   {'bad_owner'} / {'bad_fence'}       凭证不符（旧持有者 / 被接管后的旧 fence）
#   {'done', owner, fence, completed_at} 本次完成，终态写定
#
# 与续租同一套原子凭证裁决；完成写入 status=completed 后，认领脚本永久拒绝
# 新认领，本脚本也拒绝再次完成。
COMPLETE_SCRIPT = """
local key   = KEYS[1]
local owner = ARGV[1]
local fence = tonumber(ARGV[2])

if redis.call('HEXISTS', key, 'status') == 0 then
    return {'unknown'}
end
if redis.call('HGET', key, 'status') == 'completed' then
    return {'completed'}
end

local t = redis.call('TIME')
local now = t[1] + t[2] / 1000000
local expires = tonumber(redis.call('HGET', key, 'expires_at')) or 0
if expires <= now then
    return {'expired', redis.call('HGET', key, 'owner'),
            redis.call('HGET', key, 'fence')}
end
if redis.call('HGET', key, 'owner') ~= owner then
    return {'bad_owner'}
end
if (tonumber(redis.call('HGET', key, 'fence')) or 0) ~= fence then
    return {'bad_fence'}
end

redis.call('HSET', key, 'status', 'completed', 'completed_at', tostring(now))
return {'done', owner, tostring(fence), tostring(now)}
"""


# 读取脚本：连同 Redis TIME 一起返回，状态判定（租约是否仍有效）以 Redis
# 时间为准，而不是应用进程时钟。返回 {0} 表示交接单不存在；否则为
# {1, now, barrier_id, quorum, frozen_at, status, owner, fence,
#  leased_at, expires_at, completed_at}（空值为空串）。
READ_SCRIPT = """
local key = KEYS[1]
if redis.call('HEXISTS', key, 'status') == 0 then
    return {0}
end
local vals = redis.call('HMGET', key,
    'barrier_id', 'quorum', 'frozen_at', 'status', 'owner', 'fence',
    'leased_at', 'expires_at', 'completed_at')
local t = redis.call('TIME')
local now = tostring(t[1] + t[2] / 1000000)
return {1, now, vals[1], vals[2], vals[3], vals[4], vals[5], vals[6],
        vals[7], vals[8], vals[9]}
"""


class BarrierNotReached(Exception):
    """源屏障尚未达标冻结（waiting/expired/cancelled），不能建立交接单。"""


class HandoffNotFound(Exception):
    """交接单不存在。"""


class HandoffCompleted(Exception):
    """交接单已完成：终态永久拒绝再次认领与一切租约操作。"""


class HandoffBusy(Exception):
    """认领被拒：当前持有者的租约仍有效。携带 owner / fence / expires_at。"""


class LeaseExpired(Exception):
    """续租 / 完成被拒：租约已到期，须重新认领（fence 会前进）。"""


class BadLeaseCredential(Exception):
    """续租 / 完成被拒：owner 或 fence 与当前有效凭证不符（旧凭证）。

    参数携带 ``owner`` / ``fence`` 区分两种拒绝原因。
    """


@dataclass(frozen=True)
class HandoffRecord:
    handoff_id: str
    barrier_id: str
    quorum: list[str]
    frozen_at: float
    # idle（空闲可认领）| leased（租约有效）| completed（已完成终态）
    status: str
    owner: str | None
    fence: int | None
    leased_at: float | None
    expires_at: float | None
    completed_at: float | None


@dataclass(frozen=True)
class LeaseGrant:
    """一次成功的认领 / 续租后持有的租约凭证与到期时间。"""

    owner: str
    fence: int
    leased_at: float
    expires_at: float


class HandoffStore:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self._create = redis.register_script(CREATE_HANDOFF_SCRIPT)
        self._claim = redis.register_script(CLAIM_SCRIPT)
        self._renew = redis.register_script(RENEW_SCRIPT)
        self._complete = redis.register_script(COMPLETE_SCRIPT)
        self._read = redis.register_script(READ_SCRIPT)

    @staticmethod
    def handoff_key(handoff_id: str) -> str:
        return f"handoff:{handoff_id}"

    @staticmethod
    def barrier_index_key(barrier_id: str) -> str:
        return f"handoff:bybarrier:{barrier_id}"

    async def create(self, barrier_id: str) -> tuple[bool, str]:
        """为已达标屏障建立交接单；同一屏障重复创建返回原单。

        返回 ``(created, handoff_id)``：首次创建 ``created=True``，重复创建
        ``created=False``。屏障不存在抛 :class:`BarrierNotFound`，未达标
        （waiting / expired / cancelled）抛 :class:`BarrierNotReached`。
        """
        handoff_id = uuid.uuid4().hex
        raw = await self._create(
            keys=[
                f"barrier:{barrier_id}",
                f"barrier:{barrier_id}:frozen",
                self.barrier_index_key(barrier_id),
            ],
            args=[handoff_id, barrier_id],
        )
        status = raw[0]
        if status == "unknown":
            raise BarrierNotFound(barrier_id)
        if status == "not_reached":
            raise BarrierNotReached(barrier_id)
        # created / exists 两种情况都带回交接单 id（并发下 exists 指向先到者）。
        return status == "created", raw[1]

    async def claim(self, handoff_id: str, owner: str, lease_s: float) -> LeaseGrant:
        """认领交接单：空闲或租约到期才可认领，成功返回新 fence 与到期时间。

        已完成抛 :class:`HandoffCompleted`（永久拒绝）；他人租约仍有效抛
        :class:`HandoffBusy`；交接单不存在抛 :class:`HandoffNotFound`。
        """
        raw = await self._claim(
            keys=[self.handoff_key(handoff_id)],
            args=[owner, repr(float(lease_s))],
        )
        status = raw[0]
        if status == "unknown":
            raise HandoffNotFound(handoff_id)
        if status == "completed":
            raise HandoffCompleted(handoff_id)
        if status == "busy":
            raise HandoffBusy(raw[1], int(raw[2]), float(raw[3]))
        return LeaseGrant(
            owner=raw[1],
            fence=int(raw[2]),
            leased_at=float(raw[3]),
            expires_at=float(raw[4]),
        )

    async def renew(
        self, handoff_id: str, owner: str, fence: int, lease_s: float
    ) -> LeaseGrant:
        """续租：owner + fence 必须与当前有效凭证一致且租约仍有效。

        租约已到期抛 :class:`LeaseExpired`；owner 不符抛
        ``BadLeaseCredential('owner')``；fence 过期（已被接管）抛
        ``BadLeaseCredential('fence')``；已完成抛 :class:`HandoffCompleted`。
        """
        raw = await self._renew(
            keys=[self.handoff_key(handoff_id)],
            args=[owner, int(fence), repr(float(lease_s))],
        )
        status = raw[0]
        if status == "unknown":
            raise HandoffNotFound(handoff_id)
        if status == "completed":
            raise HandoffCompleted(handoff_id)
        if status == "expired":
            raise LeaseExpired(raw[1], int(raw[2]))
        if status in ("bad_owner", "bad_fence"):
            raise BadLeaseCredential(status.removeprefix("bad_"))
        return LeaseGrant(
            owner=raw[1],
            fence=int(raw[2]),
            leased_at=float(raw[3]),
            expires_at=float(raw[4]),
        )

    async def complete(self, handoff_id: str, owner: str, fence: int) -> float:
        """完成交接单：凭证与租约有效性同续租裁决；成功返回完成时刻。

        完成后终态写定，再次认领 / 续租 / 完成均被拒绝。
        """
        raw = await self._complete(
            keys=[self.handoff_key(handoff_id)],
            args=[owner, int(fence)],
        )
        status = raw[0]
        if status == "unknown":
            raise HandoffNotFound(handoff_id)
        if status == "completed":
            raise HandoffCompleted(handoff_id)
        if status == "expired":
            raise LeaseExpired(raw[1], int(raw[2]))
        if status in ("bad_owner", "bad_fence"):
            raise BadLeaseCredential(status.removeprefix("bad_"))
        return float(raw[3])

    async def get(self, handoff_id: str) -> HandoffRecord | None:
        """读取交接单快照 / 状态 / 租约；租约有效性按同次返回的 Redis 时间裁决。"""
        raw = await self._read(keys=[self.handoff_key(handoff_id)])
        if not raw or int(raw[0]) == 0:
            return None
        now = float(raw[1])
        (
            barrier_id,
            quorum_json,
            frozen_at,
            stored_status,
            owner,
            fence,
            leased_at,
            expires_at,
            completed_at,
        ) = raw[2:11]

        if stored_status == "completed":
            status = "completed"
        elif stored_status == "leased" and expires_at and float(expires_at) > now:
            status = "leased"
        else:
            # 从未认领（idle）或租约已到期：对外都是“空闲可认领”，不暴露
            # 上一任持有者的失效凭证；fence 计数仍由认领脚本在 Redis 内延续。
            status = "idle"

        leased = status == "leased"
        completed = status == "completed"
        return HandoffRecord(
            handoff_id=handoff_id,
            barrier_id=barrier_id,
            quorum=json.loads(quorum_json),
            frozen_at=float(frozen_at),
            status=status,
            owner=owner if leased else None,
            fence=int(fence) if leased else None,
            leased_at=float(leased_at) if leased else None,
            expires_at=float(expires_at) if leased else None,
            completed_at=float(completed_at) if completed else None,
        )
