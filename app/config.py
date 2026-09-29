"""运行期配置，全部来自环境变量（容器中通过 compose 注入）。"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    redis_url: str
    # 屏障键在截止时间之后保留多久才由 Redis 自动回收（毫秒）
    ttl_buffer_ms: int
    # 等待接口内部的轮询兜底间隔（秒），Pub/Sub 是主要唤醒通道
    poll_interval_s: float
    # 单次等待请求的最长挂起时间（秒），防止极端情况下连接被无限占用
    max_wait_s: float
    # 达标结果交接单的认领租约时长（秒）；租约到期可被他人接管，fence 递增
    handoff_lease_s: float


def load_settings() -> Settings:
    lease_s = float(os.environ.get("HANDOFF_LEASE_S", "30"))
    if lease_s <= 0:
        raise ValueError("HANDOFF_LEASE_S 必须为正数")
    return Settings(
        redis_url=os.environ.get("REDIS_URL", "redis://redis:6379/0"),
        ttl_buffer_ms=int(os.environ.get("BARRIER_TTL_BUFFER_MS", str(24 * 3600 * 1000))),
        poll_interval_s=float(os.environ.get("BARRIER_POLL_INTERVAL_S", "1.0")),
        max_wait_s=float(os.environ.get("BARRIER_MAX_WAIT_S", str(3600))),
        handoff_lease_s=lease_s,
    )
