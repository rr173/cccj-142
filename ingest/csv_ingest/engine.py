"""入库引擎：SQLite 暂存 → 结构校验 → 逐表判定 → 外键传播不动点 → 流式交付。

有界内存说明
============
所有数据行先进入 SQLite 暂存表（每批 ``batch_size`` 行提交一次事务），
判定全部用 SQL 在磁盘索引上完成；输出阶段使用服务端游标
（``fetchmany``）逐批写出。进程内任何时刻只保留一批数据，
暂存表与索引的规模随磁盘而不是内存增长。

“等待判断”的数据如何参与判定
============================
1. 结构解析阶段，所有行（含将来会被隔离的行）都写入暂存表，
   同时存原始文本与按列规范化后的键列文本；
2. 重复键判定对暂存表整表 ``GROUP BY`` 规范化主键，组内每一行都隔离，
   与输入顺序无关；
3. 外键匹配也以暂存表中的**规范化主键**为目标，因此先到/后到不影响结果；
4. 外键隔离用“每轮新增”不动点迭代传播，直到某轮不再产生新隔离行为止；
   互相引用且键唯一、无其他错误的记录在迭代开始前都是有效状态，因此可以保留。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .errors import (
    DUP_KEY,
    MISSING_REQUIRED,
    MISSING_TARGET,
    REF_QUARANTINED,
    REASON_DESCRIPTIONS,
    REASON_RANK,
    StructureError,
)
from .manifest import Manifest, Table
from .scanner import logical_rows, check_header

QUARANTINE_HEADER = [
    "table_name",
    "source_file",
    "logical_record_no",
    "reason_count",
    "reason_codes",
    "reason_details",
    "raw_row_json",
]


def quote_ident(name: str) -> str:
    """标识符已在清单校验阶段通过 ``[A-Za-z_][A-Za-z0-9_]*`` 检查。"""
    return '"' + name.replace('"', '""') + '"'


def quote_norm_ident(name: str) -> str:
    """规范化副本列的引用形式：``"n_<列名>"``（前缀也在引号内，避免词法拆分）。"""
    return quote_ident("n_" + name)


def sql_str(text: str) -> str:
    """生成 SQL 字符串字面量；名称均来自已校验清单，这里仍统一转义单引号。"""
    return "'" + text.replace("'", "''") + "'"


@dataclass
class TableStats:
    name: str
    source_file: str
    read: int = 0
    valid: int = 0
    quarantined: int = 0
    reasons: dict[str, int] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.reasons is None:
            self.reasons = {}


class IngestionEngine:
    def __init__(self, manifest: Manifest, input_dir: Path, work_dir: Path,
                 batch_size: int = 1000):
        self.manifest = manifest
        self.input_dir = input_dir
        self.work_dir = work_dir
        self.batch_size = batch_size
        self.db_path = work_dir / "staging.sqlite3"
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode = DELETE")
        self.conn.execute("PRAGMA synchronous = OFF")
        self.conn.execute("PRAGMA temp_store = FILE")
        self.conn.execute("PRAGMA cache_size = -2000")  # 约 2 MiB 页缓存
        self.stats: dict[str, TableStats] = {
            t.name: TableStats(t.name, t.file) for t in manifest.tables
        }
        self._create_schema()

    # ------------------------------------------------------------------ schema
    def _create_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "CREATE TABLE _reasons ("
            "table_name TEXT NOT NULL, row_id INTEGER NOT NULL, "
            "reason_code TEXT NOT NULL, detail TEXT NOT NULL)")
        cur.execute(
            "CREATE UNIQUE INDEX _reasons_uq "
            "ON _reasons(table_name, row_id, reason_code, detail)")
        cur.execute(
            "CREATE TABLE _meta (table_name TEXT PRIMARY KEY, "
            "source_file TEXT NOT NULL, read_rows INTEGER NOT NULL)")
        for table in self.manifest.tables:
            cols = ['row_id INTEGER PRIMARY KEY', 'lno INTEGER NOT NULL']
            for col in table.columns:
                q = quote_ident(col)
                cols.append(f"{q} TEXT NOT NULL")
                cols.append(f"{quote_norm_ident(col)} TEXT NOT NULL")
            cur.execute(
                f"CREATE TABLE {quote_ident(table.name)} ({', '.join(cols)})")
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    # ---------------------------------------------------------------- staging
    def stage_all(self) -> None:
        """解析全部 CSV。任何结构性错误都抛出，且此前只写了暂存库（交付目录尚未生成）。"""
        for table in self.manifest.tables:
            self._stage_table(table)

    def _resolve_input(self, table: Table) -> Path:
        # 清单已做词法检查，这里再用 resolve 复核符号链接等逃逸。
        base = self.input_dir.resolve()
        candidate = (self.input_dir / table.file).resolve()
        try:
            candidate.relative_to(base)
        except ValueError as exc:
            raise StructureError(
                f"表 {table.name} 的文件 {table.file} 越出输入目录") from exc
        if not candidate.is_file():
            raise StructureError(
                f"表 {table.name} 声明的文件 {table.file} 不存在或不是普通文件")
        return candidate

    def _stage_table(self, table: Table) -> None:
        path = self._resolve_input(table)
        label = table.file  # 错误信息只带清单声明的相对路径，不带绝对路径
        check_header(path, label, table.columns)

        cur = self.conn.cursor()
        col_sql = ", ".join(
            [quote_ident(c) for c in table.columns]
            + [quote_norm_ident(c) for c in table.columns])
        placeholders = ", ".join(["?"] * (1 + 2 * len(table.columns)))
        insert_sql = (
            f"INSERT INTO {quote_ident(table.name)} "
            f"(lno, {col_sql}) VALUES ({placeholders})")

        batch: list[tuple] = []
        read_rows = 0
        for lno, row in logical_rows(path, label):
            if lno == 1:
                continue  # 表头已由 check_header 严格核对
            if len(row) != len(table.columns):
                raise StructureError(
                    f"文件 {label} 逻辑记录 {lno} 字段数错误："
                    f"期望 {len(table.columns)}，实际 {len(row)}")
            normalized = [table.norm_of(c).apply(v) for c, v in zip(table.columns, row)]
            batch.append(tuple([lno, *row, *normalized]))
            read_rows += 1
            if len(batch) >= self.batch_size:
                cur.executemany(insert_sql, batch)
                batch.clear()
        if batch:
            cur.executemany(insert_sql, batch)
        cur.execute(
            "INSERT INTO _meta(table_name, source_file, read_rows) VALUES (?, ?, ?)",
            (table.name, table.file, read_rows))
        self.stats[table.name].read = read_rows
        self.conn.commit()

    def create_indexes(self) -> None:
        """全部数据落盘后建索引：重复键 GROUP BY 与外键连接都走索引而非全表扫描。

        索引建在**规范化后**的键列上，与判定使用的比较口径一致。
        """
        cur = self.conn.cursor()
        for table in self.manifest.tables:
            t = quote_ident(table.name)
            pk_norm = ", ".join(quote_norm_ident(c) for c in table.primary_key)
            cur.execute(
                f"CREATE INDEX {quote_ident('idx_' + table.name + '_pk')} "
                f"ON {t} ({pk_norm})")
            # 外键本地列也建索引，便于反向传播与排查。
            seen: set[str] = set()
            for fk in table.foreign_keys:
                for col in fk.columns:
                    if col in seen or col in table.primary_key:
                        continue
                    seen.add(col)
                    cur.execute(
                        f"CREATE INDEX {quote_ident('idx_' + table.name + '_' + col)} "
                        f"ON {t} ({quote_norm_ident(col)})")
        self.conn.commit()

    # ------------------------------------------------------------- row-level
    def mark_duplicate_keys(self) -> None:
        """规范化主键相等即重复；重复组内**每一行**都隔离（不由输入顺序决定幸存者）。"""
        cur = self.conn.cursor()
        for table in self.manifest.tables:
            t = quote_ident(table.name)
            # 行值元组比较，避免字符串拼接在键值含分隔字节时发生碰撞。
            pk_tuple = "(" + ", ".join(
                quote_norm_ident(c) for c in table.primary_key) + ")"
            detail = " || X'1f' || ".join(
                [sql_str("pk=")]
                + [f"COALESCE({quote_norm_ident(c)}, '')"
                   for c in table.primary_key])
            sql = (
                f"INSERT OR IGNORE INTO _reasons(table_name, row_id, reason_code, detail) "
                f"SELECT ?, row_id, ?, {detail} FROM {t} "
                f"WHERE {pk_tuple} IN ("
                f"  SELECT {', '.join(quote_norm_ident(c) for c in table.primary_key)} "
                f"  FROM {t} GROUP BY {pk_tuple[1:-1]} HAVING COUNT(*) > 1)"
            )
            cur.execute(sql, (table.name, DUP_KEY))
        self.conn.commit()

    def mark_missing_required(self) -> None:
        """必填列的**原始文本**为空字符串即缺失；主键列隐式必填。"""
        cur = self.conn.cursor()
        for table in self.manifest.tables:
            required = list(dict.fromkeys([*table.primary_key, *table.required]))
            t = quote_ident(table.name)
            for col in required:
                sql = (
                    f"INSERT OR IGNORE INTO _reasons(table_name, row_id, reason_code, detail) "
                    f"SELECT ?, row_id, ?, ? FROM {t} WHERE {quote_ident(col)} = ''")
                cur.execute(sql, (
                    table.name, MISSING_REQUIRED, f"column={col} 原始值为空"))
        self.conn.commit()

    # ------------------------------------------------------------- FK cascade
    def mark_foreign_keys(self) -> None:
        """外键不动点传播。

        每轮对每条外键做两件事：
          * 目标表不存在匹配的规范化主键 -> MISSING_TARGET（一次性，不会重复）；
          * 目标键存在但承载该键的所有行都已隔离 -> REF_QUARANTINED（每轮新增）。
        合法互相引用的两条记录在迭代前均未隔离，因此不会被标记。
        """
        cur = self.conn.cursor()
        round_no = 0
        while True:
            round_no += 1
            changed = False
            for table in self.manifest.tables:
                for fk in table.foreign_keys:
                    target = self.manifest.table_by_name[fk.target_table]
                    if self._mark_fk_missing(cur, table, fk, target):
                        changed = True
                    if self._mark_fk_to_quarantined(cur, table, fk, target):
                        changed = True
            self.conn.commit()
            if not changed:
                break
            if round_no > self._max_fanout_rounds():
                # 理论上每轮至少新增一行，轮数不可能超过总行数；这是防御性断言。
                raise RuntimeError("外键传播未收敛（内部错误）")

    def _max_fanout_rounds(self) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT COALESCE(SUM(read_rows), 0) FROM _meta")
        return max(cur.fetchone()[0], 1)

    def _fk_nonempty_predicate(self, table: Table, fk) -> str:
        # 任一本地键列为空（原始文本为空）视为“未填写引用”，不参与外键判定。
        return " AND ".join(
            f"s.{quote_ident(col)} <> ''" for col in fk.columns) or "1=1"

    def _fk_join(self, table: Table, fk, target: Table, alias_t: str = "t") -> str:
        return " AND ".join(
            f"s.{quote_norm_ident(lc)} = {alias_t}.{quote_norm_ident(rc)}"
            for lc, rc in zip(fk.columns, fk.references))

    def _fk_detail_prefix(self, fk, target: Table) -> str:
        return sql_str(f"fk={fk.name} target={target.name} key=")

    def _fk_key_literal(self, table: Table, fk) -> str:
        return " || X'1f' || ".join(
            f"COALESCE(s.{quote_norm_ident(c)}, '')" for c in fk.columns)

    def _mark_fk_missing(self, cur, table: Table, fk, target: Table) -> bool:
        s = quote_ident(table.name)
        t = quote_ident(target.name)
        detail = (self._fk_detail_prefix(fk, target)
                  + " || X'1f' || " + self._fk_key_literal(table, fk))
        sql = (
            f"INSERT OR IGNORE INTO _reasons(table_name, row_id, reason_code, detail) "
            f"SELECT ?, s.row_id, ?, {detail} FROM {s} s "
            f"WHERE {self._fk_nonempty_predicate(table, fk)} "
            f"AND NOT EXISTS ("
            f"  SELECT 1 FROM {t} t WHERE {self._fk_join(table, fk, target)})"
        )
        before = self.conn.total_changes
        cur.execute(sql, (table.name, MISSING_TARGET))
        return self.conn.total_changes > before

    def _mark_fk_to_quarantined(self, cur, table: Table, fk, target: Table) -> bool:
        s = quote_ident(table.name)
        t = quote_ident(target.name)
        detail = (self._fk_detail_prefix(fk, target)
                  + " || X'1f' || " + self._fk_key_literal(table, fk))
        # 目标键存在，但不存在“同键且未隔离”的目标行 -> 引用到已隔离记录。
        # 两层相关 EXISTS 都能命中索引（目标主键索引 + _reasons 前缀索引）。
        sql = (
            f"INSERT OR IGNORE INTO _reasons(table_name, row_id, reason_code, detail) "
            f"SELECT ?, s.row_id, ?, {detail} FROM {s} s "
            f"WHERE {self._fk_nonempty_predicate(table, fk)} "
            f"AND EXISTS (SELECT 1 FROM {t} t WHERE {self._fk_join(table, fk, target)}) "
            f"AND NOT EXISTS ("
            f"  SELECT 1 FROM {t} t2 "
            f"  WHERE {self._fk_join(table, fk, target, 't2')} "
            f"  AND NOT EXISTS ("
            f"    SELECT 1 FROM _reasons r "
            f"    WHERE r.table_name = ? AND r.row_id = t2.row_id))"
        )
        before = self.conn.total_changes
        cur.execute(sql, (table.name, REF_QUARANTINED, target.name))
        return self.conn.total_changes > before

    # ------------------------------------------------------------- output
    def write_outputs(self, out_dir: Path) -> dict:
        valid_dir = out_dir / "valid"
        valid_dir.mkdir(parents=True, exist_ok=True)
        quarantine_path = out_dir / "quarantine.csv"
        report_path = out_dir / "report.json"
        summary_path = out_dir / "summary.txt"

        self._write_valid_tables(valid_dir)
        self._write_quarantine(quarantine_path)
        report = self._build_report()
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n",
            encoding="utf-8")
        summary_path.write_text(self._render_summary(report), encoding="utf-8")
        return report

    def _write_valid_tables(self, valid_dir: Path) -> None:
        for table in self.manifest.tables:
            t = quote_ident(table.name)
            sql = (
                f"SELECT {', '.join(quote_ident(c) for c in table.columns)} "
                f"FROM {t} WHERE row_id NOT IN ("
                f"  SELECT row_id FROM _reasons WHERE table_name = ?) "
                f"ORDER BY row_id")
            cur = self.conn.cursor()
            cur.execute(sql, (table.name,))
            out_path = valid_dir / f"{table.name}.csv"
            with out_path.open("w", encoding="utf-8", newline="") as fh:
                import csv as _csv
                writer = _csv.writer(fh)
                writer.writerow(list(table.columns))
                while True:
                    batch = cur.fetchmany(self.batch_size)
                    if not batch:
                        break
                    writer.writerows(batch)
                    self.stats[table.name].valid += len(batch)

    def _write_quarantine(self, quarantine_path: Path) -> None:
        import csv as _csv
        with quarantine_path.open("w", encoding="utf-8", newline="") as fh:
            writer = _csv.writer(fh)
            writer.writerow(QUARANTINE_HEADER)
            for table in self.manifest.tables:
                self._write_quarantine_for_table(writer, table)

    def _write_quarantine_for_table(self, writer, table: Table) -> None:
        t = quote_ident(table.name)
        cur = self.conn.cursor()
        cur.execute(
            f"SELECT row_id, lno FROM {t} WHERE row_id IN ("
            f"  SELECT row_id FROM _reasons WHERE table_name = ?) ORDER BY row_id",
            (table.name,))
        rows = cur.fetchall()
        self.stats[table.name].quarantined = len(rows)
        reason_cur = self.conn.cursor()
        data_cur = self.conn.cursor()
        for row_id, lno in rows:
            reason_cur.execute(
                "SELECT reason_code, detail FROM _reasons "
                "WHERE table_name = ? AND row_id = ? "
                "ORDER BY CASE reason_code "
                + " ".join(f"WHEN '{code}' THEN {rank}"
                           for code, rank in REASON_RANK.items())
                + " END, detail",
                (table.name, row_id))
            reasons = reason_cur.fetchall()
            for code, _ in reasons:
                self.stats[table.name].reasons[code] = \
                    self.stats[table.name].reasons.get(code, 0) + 1
            data_cur.execute(
                f"SELECT {', '.join(quote_ident(c) for c in table.columns)} "
                f"FROM {t} WHERE row_id = ?", (row_id,))
            raw = list(data_cur.fetchone())
            writer.writerow([
                table.name,
                table.file,
                lno,
                len(reasons),
                "|".join(code for code, _ in reasons),
                " | ".join(f"{code}: {detail}" for code, detail in reasons),
                json.dumps(raw, ensure_ascii=False),
            ])

    # ------------------------------------------------------------- report
    def _build_report(self) -> dict:
        tables_report = []
        totals = {"read": 0, "valid": 0, "quarantined": 0,
                  "reasons": {code: 0 for code in REASON_RANK}}
        for table in self.manifest.tables:
            stat = self.stats[table.name]
            reason_counts = {
                code: stat.reasons.get(code, 0) for code in REASON_RANK}
            tables_report.append({
                "table": table.name,
                "source_file": table.file,
                "read": stat.read,
                "valid": stat.valid,
                "quarantined": stat.quarantined,
                "reason_records": reason_counts,
                "reason_descriptions": dict(REASON_DESCRIPTIONS),
            })
            totals["read"] += stat.read
            totals["valid"] += stat.valid
            totals["quarantined"] += stat.quarantined
            for code, count in reason_counts.items():
                totals["reasons"][code] += count
        return {
            "tables": tables_report,
            "totals": totals,
            "reason_order": list(REASON_RANK),
        }

    def _render_summary(self, report: dict) -> str:
        lines = [
            "CSV 入库汇总报告",
            "=" * 60,
            f"{'表':<20}{'读取':>8}{'有效':>8}{'隔离':>8}  原因计数（按稳定顺序）",
            "-" * 60,
        ]
        for item in report["tables"]:
            rc = item["reason_records"]
            reason_text = ", ".join(
                f"{code}={rc[code]}" for code in report["reason_order"])
            lines.append(
                f"{item['table']:<20}{item['read']:>8}{item['valid']:>8}"
                f"{item['quarantined']:>8}  {reason_text}")
        t = report["totals"]
        lines.append("-" * 60)
        lines.append(
            f"{'合计':<20}{t['read']:>8}{t['valid']:>8}{t['quarantined']:>8}")
        lines.append("")
        lines.append("原因码含义：")
        for code in report["reason_order"]:
            lines.append(f"  {code:<18}{REASON_DESCRIPTIONS[code]}")
        lines.append("")
        return "\n".join(lines)
