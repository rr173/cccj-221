"""引擎统一异常体系。"""


class BitgroupError(Exception):
    """所有 bitgroup 异常的基类。"""


class SchemaError(BitgroupError):
    """主/子表 schema 声明非法。"""


class InputDataError(BitgroupError):
    """输入数据错误：悬空 entity_id、重复身份、类型不符等。"""


class IndexError(BitgroupError):  # noqa: A001 - 领域内含义明确
    """索引代号/索引目录不存在、损坏或已被更换。"""


class QueryError(BitgroupError):
    """查询 AST 结构、类型或阈值非法。"""


class TokenError(QueryError):
    """分页令牌与当前索引代号或查询 AST 不匹配。"""


class LedgerError(BitgroupError):
    """历史状态台账：配置不一致、日期非法、事务/一致性错误。"""


class ReviewError(BitgroupError):
    """复核包导出：参数非法、输出路径已存在、发布失败或索引不一致。"""
