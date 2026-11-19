"""无业务主键集市的差量共享接收。

两个独立持久化仓：
- provider_store：提供方，按共享契约把原始快照解释为行多重集视图，生成基线/差量/完整替换；
- receiver_store：接收方县仓，核验并原子入仓，保存自己的进度、冲突与对账结果。

两仓各自落真实 Parquet，均可由 DuckDB 查询；进程内变量不作为“已收到”的凭证。
"""

__all__ = ["hashing", "contract", "store", "provider", "receiver", "handoff", "cli", "errors"]
