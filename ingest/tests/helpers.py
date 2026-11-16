"""测试辅助：夹具写入、CLI 调用与隔离文件解析。

测试中的预期结果全部由人工在测试里手算写死，不调用正式校验代码来生成期望值。
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "bin" / "csv-ingest"


def write_package(base: Path, files: dict[str, str], manifest: dict) -> Path:
    """在 *base* 下写一个数据包，返回清单路径。值原样写入（调用方自行控制换行）。"""
    base.mkdir(parents=True, exist_ok=True)
    for rel, content in files.items():
        path = base / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8", newline="")
    manifest_path = base / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path


def run_cli(input_dir: Path, manifest: Path, output_dir: Path,
            extra: list[str] | None = None) -> subprocess.CompletedProcess:
    cmd = [sys.executable, "-I", str(CLI),
           "--input-dir", str(input_dir),
           "--manifest", str(manifest),
           "--output-dir", str(output_dir)]
    if extra:
        cmd.extend(extra)
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def read_csv(path: Path) -> list[list[str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.reader(fh))


def read_quarantine(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def index_quarantine(rows: list[dict]) -> dict[tuple[str, str], dict]:
    """以 (表名, 逻辑记录序号) 为键。"""
    return {(r["table_name"], r["logical_record_no"]): r for r in rows}


def valid_rows(output_dir: Path, table: str) -> list[list[str]]:
    return read_csv(output_dir / "valid" / f"{table}.csv")
