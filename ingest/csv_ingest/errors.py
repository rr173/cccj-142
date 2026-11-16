"""异常类型与隔离原因码。

退出码约定：
  0  成功
  2  清单错误（清单本身不合法）
  3  数据包结构性错误（CSV 无法完整、正确地解析）
  1  其他未预期错误
"""

EXIT_OK = 0
EXIT_GENERIC = 1
EXIT_MANIFEST = 2
EXIT_STRUCTURE = 3


class IngestError(Exception):
    """所有可预期工具错误的基类。"""

    exit_code = EXIT_GENERIC


class ManifestError(IngestError):
    """清单不合法：重复表名、列定义问题、非法关系、路径越界等。"""

    exit_code = EXIT_MANIFEST


class StructureError(IngestError):
    """数据包结构性失败：编码、引号、表头、字段数等问题，整包拒绝。"""

    exit_code = EXIT_STRUCTURE


# ---------------------------------------------------------------------------
# 隔离原因码。排序顺序即报告与隔离文件中的稳定排序顺序。
# ---------------------------------------------------------------------------
DUP_KEY = "DUP_KEY"                     # 表内规范化主键重复
MISSING_REQUIRED = "MISSING_REQUIRED"   # 必填列为空
MISSING_TARGET = "MISSING_TARGET"       # 外键在目标表中不存在
REF_QUARANTINED = "REF_QUARANTINED"     # 外键指向已被隔离的记录

REASON_ORDER = (DUP_KEY, MISSING_REQUIRED, MISSING_TARGET, REF_QUARANTINED)
REASON_RANK = {code: i for i, code in enumerate(REASON_ORDER)}

REASON_DESCRIPTIONS = {
    DUP_KEY: "重复主键（按该表声明的规范化规则相等）",
    MISSING_REQUIRED: "必填值缺失（原始字段为空字符串）",
    MISSING_TARGET: "缺失引用目标（目标表中不存在匹配主键）",
    REF_QUARANTINED: "引用到已隔离记录（目标键存在但其记录被隔离）",
}
