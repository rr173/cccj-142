"""JSON 清单的加载与严格校验。

清单**只能声明规则**，本模块绝不 eval/exec 清单中的任何内容：
规范化仅支持两个布尔开关 ``trim``（去除首尾空白）与 ``casefold``（大小写折叠，
使用 Python 的 :py:meth:`str.casefold`），其余键一律拒绝。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from .errors import ManifestError

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ALLOWED_TOP_KEYS = {"version", "tables"}
_ALLOWED_TABLE_KEYS = {
    "name", "file", "columns", "primary_key", "required", "foreign_keys",
}
_ALLOWED_COLUMN_KEYS = {"name", "trim", "casefold"}
_ALLOWED_FK_KEYS = {"name", "columns", "target_table", "references"}


@dataclass(frozen=True)
class Normalization:
    """单列允许的全部规范化规则；默认两个开关都关闭。"""

    trim: bool = False
    casefold: bool = False

    def apply(self, value: str) -> str:
        if self.trim:
            value = value.strip()
        if self.casefold:
            value = value.casefold()
        return value


@dataclass(frozen=True)
class ForeignKey:
    name: str
    columns: tuple[str, ...]
    target_table: str
    references: tuple[str, ...]


@dataclass(frozen=True)
class Table:
    name: str
    file: str
    columns: tuple[str, ...]
    primary_key: tuple[str, ...]
    required: tuple[str, ...]
    foreign_keys: tuple[ForeignKey, ...]
    normalization: dict[str, Normalization]

    def norm_of(self, column: str) -> Normalization:
        return self.normalization[column]


@dataclass(frozen=True)
class Manifest:
    version: str | None
    tables: tuple[Table, ...]
    table_by_name: dict[str, Table] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# 词法/结构小工具
# ---------------------------------------------------------------------------

def _is_safe_relative_path(raw: str) -> bool:
    """词法检查：相对路径、无 ``..`` 越界、无绝对路径、无反斜杠/NUL。

    真正的符号链接逃逸在打开文件时再用 ``resolve()`` 复核一次。
    """
    if not isinstance(raw, str) or raw == "":
        return False
    if "\x00" in raw or "\\" in raw:
        return False
    if raw.startswith("/"):
        return False
    # Windows 盘符（例如 C:）也拒绝，避免跨平台歧义。
    if len(raw) >= 2 and raw[1] == ":":
        return False
    parts = raw.split("/")
    for part in parts:
        if part in ("", ".", ".."):
            return False
    return True


def _require_name(name, what: str, errors: list[str]) -> bool:
    if isinstance(name, str) and _NAME_RE.fullmatch(name):
        return True
    errors.append(f"{what} 的名称必须匹配 [A-Za-z_][A-Za-z0-9_]*，实际为 {name!r}")
    return False


def _as_nonempty_str(value, what: str, errors: list[str]) -> str | None:
    if isinstance(value, str) and value != "":
        return value
    errors.append(f"{what} 必须是非空字符串，实际为 {value!r}")
    return None


def _as_string_list(value, what: str, errors: list[str]) -> list[str] | None:
    if not isinstance(value, list) or value == []:
        errors.append(f"{what} 必须是非空数组")
        return None
    out: list[str] = []
    for item in value:
        if not isinstance(item, str) or item == "":
            errors.append(f"{what} 中的每一项都必须是非空字符串，发现 {item!r}")
            return None
        out.append(item)
    return out


def _as_bool(value, what: str, errors: list[str]) -> bool | None:
    if isinstance(value, bool):
        return value
    errors.append(f"{what} 必须是布尔值 true/false，实际为 {value!r}")
    return None


# ---------------------------------------------------------------------------
# 清单解析
# ---------------------------------------------------------------------------

def _parse_column(spec, errors: list[str]) -> tuple[str, Normalization] | None:
    if not isinstance(spec, dict):
        errors.append(f"列定义必须是对象，实际为 {spec!r}")
        return None
    extra = set(spec) - _ALLOWED_COLUMN_KEYS
    if extra:
        errors.append(f"列定义含不允许的键 {sorted(extra)}；只允许声明 name/trim/casefold")
        return None
    name = _as_nonempty_str(spec.get("name"), "列 name", errors)
    if name is None or not _require_name(name, "列", errors):
        return None
    trim = False
    casefold = False
    if "trim" in spec:
        trim = _as_bool(spec["trim"], f"列 {name} 的 trim", errors)
        if trim is None:
            return None
    if "casefold" in spec:
        casefold = _as_bool(spec["casefold"], f"列 {name} 的 casefold", errors)
        if casefold is None:
            return None
    return name, Normalization(trim=trim, casefold=casefold)


def _parse_table(spec, errors: list[str]) -> Table | None:
    if not isinstance(spec, dict):
        errors.append(f"表定义必须是对象，实际为 {spec!r}")
        return None
    extra = set(spec) - _ALLOWED_TABLE_KEYS
    if extra:
        errors.append(f"表定义含不允许的键 {sorted(extra)}")
        return None

    name = _as_nonempty_str(spec.get("name"), "表 name", errors)
    if name is not None:
        _require_name(name, "表", errors)
    file_name = _as_nonempty_str(spec.get("file"), f"表 {name} 的 file", errors)
    if file_name is not None and not _is_safe_relative_path(file_name):
        errors.append(f"表 {name} 的文件路径 {file_name!r} 非法或越出输入目录")
        file_name = None

    raw_columns = spec.get("columns")
    parsed_columns: list[tuple[str, Normalization]] = []
    seen_columns: set[str] = set()
    if not isinstance(raw_columns, list) or raw_columns == []:
        errors.append(f"表 {name} 的 columns 必须是非空数组")
    else:
        for col_spec in raw_columns:
            parsed = _parse_column(col_spec, errors)
            if parsed is None:
                continue
            col_name, norm = parsed
            if col_name in seen_columns:
                errors.append(f"表 {name} 存在重复列定义 {col_name!r}")
                continue
            seen_columns.add(col_name)
            parsed_columns.append(parsed)

    def _check_name_list(value, key: str) -> list[str] | None:
        items = _as_string_list(value, f"表 {name} 的 {key}", errors)
        if items is None:
            return None
        local_seen: set[str] = set()
        for item in items:
            if item not in seen_columns:
                errors.append(f"表 {name} 的 {key} 引用了未定义列 {item!r}")
            if item in local_seen:
                errors.append(f"表 {name} 的 {key} 中 {item!r} 重复")
            local_seen.add(item)
        return items

    pk = _check_name_list(spec.get("primary_key"), "primary_key")
    required: list[str] | None = []
    if "required" in spec:
        if not isinstance(spec["required"], list):
            errors.append(f"表 {name} 的 required 必须是数组（可为空数组）")
            required = None
        elif spec["required"]:
            required = _check_name_list(spec["required"], "required")

    # 外键
    fks: list[ForeignKey] = []
    seen_fk_names: set[str] = set()
    raw_fks = spec.get("foreign_keys", [])
    if not isinstance(raw_fks, list):
        errors.append(f"表 {name} 的 foreign_keys 必须是数组")
        raw_fks = []
    for fk_spec in raw_fks:
        fk = _parse_foreign_key(fk_spec, name, seen_columns, seen_fk_names, errors)
        if fk is not None:
            fks.append(fk)

    if errors and (name is None or not parsed_columns or pk is None):
        return None

    normalization = {col_name: norm for col_name, norm in parsed_columns}
    return Table(
        name=name,
        file=file_name,  # type: ignore[arg-type]
        columns=tuple(col_name for col_name, _ in parsed_columns),
        primary_key=tuple(pk or ()),
        required=tuple(required or ()),
        foreign_keys=tuple(fks),
        normalization=normalization,
    )


def _parse_foreign_key(spec, table_name: str, local_columns: set[str],
                       seen_fk_names: set[str], errors: list[str]) -> ForeignKey | None:
    if not isinstance(spec, dict):
        errors.append(f"表 {table_name} 的外键定义必须是对象，实际为 {spec!r}")
        return None
    extra = set(spec) - _ALLOWED_FK_KEYS
    if extra:
        errors.append(f"表 {table_name} 的外键含不允许的键 {sorted(extra)}")
        return None

    columns = _as_string_list(spec.get("columns"),
                              f"表 {table_name} 外键的 columns", errors)
    target = _as_nonempty_str(spec.get("target_table"),
                              f"表 {table_name} 外键的 target_table", errors)
    if target is not None:
        _require_name(target, f"表 {table_name} 外键 target_table", errors)
    references = _as_string_list(spec.get("references"),
                                 f"表 {table_name} 外键的 references", errors)

    fk_name = spec.get("name")
    if fk_name is None:
        # 名称可省略，按列名生成；显式重复在跨表引用检查阶段再处理。
        fk_name = "fk_" + "_".join(columns or ("anon",))
    elif not _require_name(fk_name, f"表 {table_name} 外键 name", errors):
        fk_name = None

    if columns is not None:
        local_seen: set[str] = set()
        for col in columns:
            if col not in local_columns:
                errors.append(
                    f"表 {table_name} 外键 {fk_name} 引用了未定义的本地列 {col!r}")
            if col in local_seen:
                errors.append(f"表 {table_name} 外键 {fk_name} 本地列 {col!r} 重复")
            local_seen.add(col)
    if references is not None and len(set(references)) != len(references):
        errors.append(f"表 {table_name} 外键 {fk_name} 的 references 存在重复列")
    if columns is not None and references is not None and len(columns) != len(references):
        errors.append(
            f"表 {table_name} 外键 {fk_name} 两端字段数不一致："
            f"{len(columns)} != {len(references)}")
    if fk_name is not None:
        if fk_name in seen_fk_names:
            errors.append(f"表 {table_name} 外键名称 {fk_name!r} 重复")
            fk_name = None
        else:
            seen_fk_names.add(fk_name)

    if errors and (columns is None or references is None or target is None
                   or fk_name is None):
        return None
    return ForeignKey(
        name=fk_name,  # type: ignore[arg-type]
        columns=tuple(columns or ()),
        target_table=target,  # type: ignore[arg-type]
        references=tuple(references or ()),
    )


def _cross_check(tables: list[Table], errors: list[str]) -> None:
    by_name: dict[str, Table] = {}
    for table in tables:
        if table.name in by_name:
            errors.append(f"重复表名 {table.name!r}")
            continue
        by_name[table.name] = table

    seen_files: dict[str, str] = {}
    for table in tables:
        if table.file in seen_files:
            errors.append(
                f"文件 {table.file} 同时被表 {seen_files[table.file]} 和 "
                f"{table.name} 声明，不允许复用")
        else:
            seen_files[table.file] = table.name
        # 主键列必须在列集合里（解析阶段已保证），这里复核规范化一致性。
        for fk in table.foreign_keys:
            target = by_name.get(fk.target_table)
            if target is None:
                errors.append(
                    f"表 {table.name} 外键 {fk.name} 指向不存在的表 "
                    f"{fk.target_table!r}")
                continue
            for ref_col in fk.references:
                if ref_col not in target.columns:
                    errors.append(
                        f"表 {table.name} 外键 {fk.name} 指向目标表 "
                        f"{target.name} 中不存在的列 {ref_col!r}")
            if len(fk.columns) != len(fk.references):
                continue  # 解析阶段已报错
            for local_col, ref_col in zip(fk.columns, fk.references):
                if local_col not in table.normalization or ref_col not in target.normalization:
                    continue
                local_norm = table.normalization[local_col]
                ref_norm = target.normalization[ref_col]
                if local_norm != ref_norm:
                    errors.append(
                        f"表 {table.name} 外键 {fk.name} 的本地列 {local_col} 与目标列 "
                        f"{target.name}.{ref_col} 规范化规则（trim/casefold）不一致")
            # 目标引用列必须构成目标表的主键（外键只能引用唯一主键）。
            if tuple(fk.references) != target.primary_key:
                errors.append(
                    f"表 {table.name} 外键 {fk.name} 必须引用目标表 "
                    f"{target.name} 的完整主键 {target.primary_key}，"
                    f"实际引用 {fk.references}")


def load_manifest(manifest_path: str | Path) -> Manifest:
    """读取并校验清单文件；任何不合法情况都抛 :class:`ManifestError`。"""
    path = Path(manifest_path)
    try:
        raw_text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ManifestError(f"清单文件不存在：{path.name}") from exc
    except OSError as exc:
        raise ManifestError(f"清单文件无法读取：{exc}") from exc
    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ManifestError(f"清单不是合法 JSON：{exc}") from exc

    errors: list[str] = []
    if not isinstance(raw, dict):
        raise ManifestError("清单顶层必须是 JSON 对象")
    extra = set(raw) - _ALLOWED_TOP_KEYS
    if extra:
        errors.append(f"清单顶层含不允许的键 {sorted(extra)}")
    version = raw.get("version")
    if version is not None and not isinstance(version, str):
        errors.append("清单 version 必须是字符串")
    raw_tables = raw.get("tables")
    if not isinstance(raw_tables, list) or raw_tables == []:
        raise ManifestError("清单 tables 必须是非空数组；未声明任何表无法入库")

    tables: list[Table] = []
    for spec in raw_tables:
        table = _parse_table(spec, errors)
        if table is not None:
            tables.append(table)

    if tables:
        _cross_check(tables, errors)

    if errors:
        raise ManifestError("清单校验失败：\n  - " + "\n  - ".join(errors))
    if not tables:
        raise ManifestError("清单未声明任何有效表")

    return Manifest(
        version=version,
        tables=tuple(tables),
        table_by_name={t.name: t for t in tables},
    )
