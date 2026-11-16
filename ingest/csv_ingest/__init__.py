"""csv_ingest：多表 CSV 数据包校验入库工具（标准库 + SQLite）。"""

from .errors import (
    DUP_KEY,
    MISSING_REQUIRED,
    MISSING_TARGET,
    REF_QUARANTINED,
    IngestError,
    ManifestError,
    StructureError,
)

__all__ = [
    "DUP_KEY",
    "MISSING_REQUIRED",
    "MISSING_TARGET",
    "REF_QUARANTINED",
    "IngestError",
    "ManifestError",
    "StructureError",
]
