"""pytest 全局配置：在任何测试模块导入 app 之前固定运行期环境变量。

必须放在 conftest 中而不是各测试模块顶部：pytest 按文件名字母序收集，
test_api.py 先于 test_handoffs.py 导入；若交接单租约时长 HANDOFF_LEASE_S
在 app 首次导入后才设置，load_settings() 已用默认 30s 固化，到期接管测试
将无法在 1s 租约下推进。conftest.py 由 pytest 保证最先导入。
"""

import os

os.environ.setdefault("REDIS_URL", "redis://localhost:6399/15")
os.environ.setdefault("HANDOFF_LEASE_S", "1")
os.environ.setdefault("BARRIER_POLL_INTERVAL_S", "0.05")
