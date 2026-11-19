"""共享协议的错误类型。"""


class MarketError(Exception):
    """业务错误基类。"""


class ContractMismatch(MarketError):
    """传递所属共享契约与接收仓接纳的契约不一致。"""


class ContentConflict(MarketError):
    """同一传递身份重收但内容不同：停止并留冲突，不得覆盖。"""


class MissingBaseline(MarketError):
    """接收仓不持有差量要求的基线，需要补传或完整重建基线。"""


class StaleGeneration(MarketError):
    """新基线代际建立后，旧代际差量/旧快照不得再接进来。"""


class VerificationFailed(MarketError):
    """内容校验（文件摘要、重数、目标视图指纹）未通过。"""


class UnknownDelivery(MarketError):
    """确认或对账的传递身份在本地账本中不存在。"""
