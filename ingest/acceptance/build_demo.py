"""构建验收用的手算小数据包。

数据与预期由人工设计（见同目录 expected.md），本脚本只负责把**固定的文本**
原样写入磁盘，不调用 csv_ingest 的任何校验逻辑，避免“用被测代码生成预期”。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

MANIFEST = {
    "version": "acceptance-1",
    "tables": [
        {
            "name": "customers",
            "file": "csv/customers.csv",
            "columns": [
                {"name": "cust_id", "trim": True, "casefold": True},
                {"name": "cust_name"},
                {"name": "city"},
            ],
            "primary_key": ["cust_id"],
            "required": ["cust_name"],
        },
        {
            "name": "orders",
            "file": "csv/orders.csv",
            "columns": [
                {"name": "order_id"},
                # 外键端规范化规则必须与目标主键一致。
                {"name": "cust_id", "trim": True, "casefold": True},
                {"name": "amount"},
            ],
            "primary_key": ["order_id"],
            "required": ["amount"],
            "foreign_keys": [
                {"name": "fk_order_customer", "columns": ["cust_id"],
                 "target_table": "customers", "references": ["cust_id"]},
            ],
        },
        {
            "name": "invoices",
            "file": "csv/invoices.csv",
            "columns": [
                {"name": "inv_id"},
                {"name": "order_id"},
                {"name": "total"},
                {"name": "note"},
            ],
            "primary_key": ["inv_id"],
            "required": ["total"],
            "foreign_keys": [
                {"name": "fk_invoice_order", "columns": ["order_id"],
                 "target_table": "orders", "references": ["order_id"]},
            ],
        },
        {
            "name": "partners",
            "file": "csv/partners.csv",
            "columns": [{"name": "pid"}, {"name": "peer"}],
            "primary_key": ["pid"],
            "foreign_keys": [
                {"name": "fk_partner_self", "columns": ["peer"],
                 "target_table": "partners", "references": ["pid"]},
            ],
        },
    ],
}

# 每个文件都是手工排列的固定文本；引号与嵌入换行刻意保留。
FILES = {
    "csv/customers.csv": (
        "cust_id,cust_name,city\n"
        "C1,Alice,Beijing\n"
        " C2 ,Bob,Shanghai\n"
        "c2,Carol,Shenzhen\n"
        "C4,,Hangzhou\n"
        "C5,Dave,Chengdu\n"
    ),
    "csv/orders.csv": (
        "order_id,cust_id,amount\n"
        "O1,C1,100\n"
        "O2,  c2  ,200\n"
        "O3,C9,\n"
        "O4,C5,400\n"
        "O5,C4,500\n"
    ),
    # I6 的 note 含真实换行，验证逻辑记录序号不被物理换行干扰。
    "csv/invoices.csv": (
        "inv_id,order_id,total,note\n"
        "I1,O1,10,ok\n"
        "I2,O2,20,ok\n"
        "I3,O3,30,ok\n"
        "I4,O4,40,ok\n"
        "I5,O9,50,ok\n"
        "I6,O1,60,\"line one\nline two\"\n"
    ),
    "csv/partners.csv": (
        "pid,peer\n"
        "P1,P2\n"
        "P2,P1\n"
        "P3,P8\n"
    ),
}

# 一个结构性损坏的对照包：引号未闭合，必须整包拒绝。
BAD_FILES = {
    "csv/customers.csv": "cust_id,cust_name,city\nC1,Alice,Beijing\n",
    "csv/orders.csv": "order_id,cust_id,amount\nO1,C1,100\nO2,\"oops,200\n",
    "csv/invoices.csv": "inv_id,order_id,total,note\nI1,O1,10,ok\n",
    "csv/partners.csv": "pid,peer\nP1,P2\nP2,P1\n",
}


def build(root: Path) -> None:
    good = root / "package_good"
    bad = root / "package_bad"
    for base, files in ((good, FILES), (bad, BAD_FILES)):
        (base / "csv").mkdir(parents=True, exist_ok=True)
        for rel, content in files.items():
            (base / rel).write_text(content, encoding="utf-8", newline="")
        (base / "manifest.json").write_text(
            json.dumps(MANIFEST, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"已写出验收数据包：{good}")
    print(f"已写出损坏对照包：{bad}")


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "data"
    build(target)
