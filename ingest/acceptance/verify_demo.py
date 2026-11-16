#!/usr/bin/env python3
"""验收断言脚本：期望值全部在本文件中手算写死（来源 expected.md）。

不 import csv_ingest 的任何判定代码，只用标准库解析 CLI 产物并逐条比对。
用法：
  python3 -I acceptance/verify_demo.py <数据根目录> <输出根目录>
"""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CLI = HERE.parent / "bin" / "csv-ingest"

# ---- 手算预期（与 expected.md 一一对应；修改数据必须先手改这里） -----------------
EXPECTED_VALID = {
    "customers": [
        ["cust_id", "cust_name", "city"],
        ["C1", "Alice", "Beijing"],
        ["C5", "Dave", "Chengdu"],
    ],
    "orders": [
        ["order_id", "cust_id", "amount"],
        ["O1", "C1", "100"],
        ["O4", "C5", "400"],
    ],
    "invoices": [
        ["inv_id", "order_id", "total", "note"],
        ["I1", "O1", "10", "ok"],
        ["I4", "O4", "40", "ok"],
        ["I6", "O1", "60", "line one\nline two"],
    ],
    "partners": [
        ["pid", "peer"],
        ["P1", "P2"],
        ["P2", "P1"],
    ],
}

# (表, 逻辑记录序号) -> 有序原因码列表
EXPECTED_QUARANTINE = {
    ("customers", "3"): ["DUP_KEY"],
    ("customers", "4"): ["DUP_KEY"],
    ("customers", "5"): ["MISSING_REQUIRED"],
    ("orders", "3"): ["REF_QUARANTINED"],
    ("orders", "4"): ["MISSING_REQUIRED", "MISSING_TARGET"],
    ("orders", "6"): ["REF_QUARANTINED"],
    ("invoices", "3"): ["REF_QUARANTINED"],
    ("invoices", "4"): ["REF_QUARANTINED"],
    ("invoices", "6"): ["MISSING_TARGET"],
    ("partners", "4"): ["MISSING_TARGET"],
}

EXPECTED_STATS = {
    "customers": {"read": 5, "valid": 2, "quarantined": 3,
                  "DUP_KEY": 2, "MISSING_REQUIRED": 1,
                  "MISSING_TARGET": 0, "REF_QUARANTINED": 0},
    "orders": {"read": 5, "valid": 2, "quarantined": 3,
               "DUP_KEY": 0, "MISSING_REQUIRED": 1,
               "MISSING_TARGET": 1, "REF_QUARANTINED": 2},
    "invoices": {"read": 6, "valid": 3, "quarantined": 3,
                 "DUP_KEY": 0, "MISSING_REQUIRED": 0,
                 "MISSING_TARGET": 1, "REF_QUARANTINED": 2},
    "partners": {"read": 3, "valid": 2, "quarantined": 1,
                 "DUP_KEY": 0, "MISSING_REQUIRED": 0,
                 "MISSING_TARGET": 1, "REF_QUARANTINED": 0},
}
EXPECTED_TOTALS = {"read": 19, "valid": 9, "quarantined": 10}


def _read_csv(path: Path) -> list[list[str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.reader(fh))


def _read_dict(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def run_cli(pkg: Path, out: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-I", str(CLI),
         "--input-dir", str(pkg), "--manifest", str(pkg / "manifest.json"),
         "--output-dir", str(out), "--batch-size", "13", "--force"],
        capture_output=True, text=True, check=False)


def check_good(pkg: Path, out: Path) -> list[str]:
    failures: list[str] = []
    proc = run_cli(pkg, out)
    if proc.returncode != 0:
        return [f"好包运行失败（退出码 {proc.returncode}）：{proc.stderr.strip()}"]

    # 1) 有效表逐行比对（含顺序）。
    for table, expected_rows in EXPECTED_VALID.items():
        actual = _read_csv(out / "valid" / f"{table}.csv")
        if actual != expected_rows:
            failures.append(f"[{table}] 有效行不符\n  期望 {expected_rows}\n  实际 {actual}")

    # 2) 隔离记录逐条比对原因码；并核对定位字段与原始字段保留。
    qrows = _read_dict(out / "quarantine.csv")
    actual_map = {
        (r["table_name"], r["logical_record_no"]):
            r["reason_codes"].split("|") for r in qrows}
    if actual_map != EXPECTED_QUARANTINE:
        failures.append(f"隔离判定不符\n  期望 {EXPECTED_QUARANTINE}\n  实际 {actual_map}")
    for row in qrows:
        if row["source_file"] != f"csv/{row['table_name']}.csv":
            failures.append(f"source_file 异常：{row['source_file']}")
        if not row["raw_row_json"].startswith("["):
            failures.append("raw_row_json 未保留原始字段")
        # 原因排序必须稳定：与全局原因序一致。
        codes = row["reason_codes"].split("|")
        if codes != sorted(codes, key=lambda c:
                           ["DUP_KEY", "MISSING_REQUIRED", "MISSING_TARGET",
                            "REF_QUARANTINED"].index(c)):
            failures.append(f"原因码未按稳定顺序排列：{codes}")
    # I6 嵌入换行的原始文本必须完整保留。
    inv6 = next(r for r in qrows if r["table_name"] == "invoices"
                and r["logical_record_no"] == "6")
    if json.loads(inv6["raw_row_json"]) != ["I5", "O9", "50", "ok"]:
        failures.append(f"I5 原始字段不符：{inv6['raw_row_json']}")
    i6_valid = next(r for r in _read_csv(out / "valid" / "invoices.csv")
                    if r and r[0] == "I6")
    if i6_valid[3] != "line one\nline two":
        failures.append("I6 嵌入换行字段未完整保留")

    # 3) 报告计数。
    report = json.loads((out / "report.json").read_text(encoding="utf-8"))
    for item in report["tables"]:
        exp = EXPECTED_STATS[item["table"]]
        for key in ("read", "valid", "quarantined"):
            if item[key] != exp[key]:
                failures.append(
                    f"[{item['table']}] {key} 期望 {exp[key]} 实际 {item[key]}")
        for code, value in exp.items():
            if code in ("read", "valid", "quarantined"):
                continue
            if item["reason_records"].get(code) != value:
                failures.append(
                    f"[{item['table']}] 原因 {code} 期望 {value} "
                    f"实际 {item['reason_records'].get(code)}")
    if report["totals"] != {
            **EXPECTED_TOTALS,
            "reasons": {"DUP_KEY": 2, "MISSING_REQUIRED": 2,
                        "MISSING_TARGET": 3, "REF_QUARANTINED": 4}}:
        failures.append(f"合计不符：{report['totals']}")

    # 4) 交付物中不得出现输入绝对路径。
    abs_prefix = str(pkg.resolve())
    for path in out.rglob("*"):
        if path.is_file():
            if abs_prefix in path.read_text(encoding="utf-8", errors="replace"):
                failures.append(f"{path.name} 中泄露了输入绝对路径")
    return failures


def check_bad(pkg: Path, out: Path) -> list[str]:
    failures: list[str] = []
    proc = run_cli(pkg, out)
    if proc.returncode != 3:
        failures.append(f"损坏包退出码期望 3，实际 {proc.returncode}：{proc.stderr}")
    if out.exists() and any(out.iterdir()):
        failures.append(f"失败后留下部分输出：{sorted(p.name for p in out.iterdir())}")
    if "未闭合" not in proc.stderr:
        failures.append(f"错误信息未指出未闭合引号：{proc.stderr.strip()}")
    return failures


def main() -> int:
    data_root = Path(sys.argv[1])
    out_root = Path(sys.argv[2])
    out_root.mkdir(parents=True, exist_ok=True)
    all_failures: list[str] = []

    print("== 好包：运行命令 ==")
    good_out = out_root / "good"
    cmd = (f"{sys.executable} -I {CLI} --input-dir {data_root / 'package_good'} "
           f"--manifest {data_root / 'package_good' / 'manifest.json'} "
           f"--output-dir {good_out} --batch-size 13 --force")
    print(cmd)
    all_failures += [f"[好包] {f}" for f in
                     check_good(data_root / "package_good", good_out)]

    print("== 损坏包：运行命令 ==")
    bad_out = out_root / "bad"
    print(cmd.replace("package_good", "package_bad").replace(str(good_out), str(bad_out)))
    all_failures += [f"[损坏包] {f}" for f in
                     check_bad(data_root / "package_bad", bad_out)]

    if all_failures:
        print("\n验收失败：")
        for f in all_failures:
            print("  - " + f)
        return 1
    print("\n全部验收断言通过（有效行、隔离原因、逻辑序号、传播、互引、计数、原子失败）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
