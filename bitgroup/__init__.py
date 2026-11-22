"""bitgroup — 可重开的磁盘位图取群引擎。

纯标准库实现。核心抽象：

* :class:`bitgroup.pages.BitmapStore` —— 父子两个编号域的落盘分页位图存储，
  按需 pread 读取压缩页、LRU 驻留、可观察页读取与临时位图峰值；
* :class:`bitgroup.importer.Importer` —— 主表/子表 JSONL 导入，生成只读索引代号；
* :class:`bitgroup.engine.Engine` —— 绑定索引代号，反复执行三态集合条件、
  分页、分面与逐节点解释。
"""

from .errors import (
    BitgroupError,
    InputDataError,
    SchemaError,
    IndexError as BgIndexError,
    QueryError,
    TokenError,
    LedgerError,
    ReviewError,
)
from .types import ScalarType, validate_scalar, json_type_name
from .engine import Engine, QueryResult
from .importer import Importer, IndexHandle
from .ledger import Ledger, RegisterResult, parse_business_date
from .compare import CompareRun, compare_ledgers
from .review import ReviewExport, export_review_package

__all__ = [
    "BitgroupError",
    "InputDataError",
    "SchemaError",
    "BgIndexError",
    "QueryError",
    "TokenError",
    "LedgerError",
    "ReviewError",
    "ScalarType",
    "validate_scalar",
    "json_type_name",
    "Engine",
    "QueryResult",
    "Importer",
    "IndexHandle",
    "Ledger",
    "RegisterResult",
    "parse_business_date",
    "CompareRun",
    "compare_ledgers",
    "ReviewExport",
    "export_review_package",
]
