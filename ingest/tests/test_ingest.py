"""端到端验收测试。

每个用例的预期（哪些行有效、哪些行隔离、原因码、逻辑记录序号、计数）
均由人工在本文件中直接写死，不调用正式校验代码生成预期。
"""

from __future__ import annotations

import json
import shutil
import sys
import unittest
from pathlib import Path

# -I 模式忽略 PYTHONPATH，这里显式登记测试目录（helpers）与包所在目录。
_TEST_DIR = Path(__file__).resolve().parent
for _p in (str(_TEST_DIR), str(_TEST_DIR.parent)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from helpers import (
    index_quarantine,
    read_csv,
    read_quarantine,
    run_cli,
    valid_rows,
    write_package,
)

from csv_ingest.errors import (
    DUP_KEY,
    MISSING_REQUIRED,
    MISSING_TARGET,
    REF_QUARANTINED,
)
from csv_ingest.manifest import load_manifest, ManifestError
from csv_ingest.scanner import logical_rows, ensure_quotes_closed, StructureError

WORK = Path(__file__).resolve().parent / "_work"


def _two_table_manifest() -> dict:
    return {
        "version": "1",
        "tables": [
            {
                "name": "customers",
                "file": "data/customers.csv",
                "columns": [
                    {"name": "cust_id"},
                    {"name": "cust_name"},
                    {"name": "city", "trim": True, "casefold": True},
                ],
                "primary_key": ["cust_id"],
                "required": ["cust_name"],
            },
            {
                "name": "orders",
                "file": "data/orders.csv",
                "columns": [
                    {"name": "order_id"},
                    {"name": "cust_id"},
                    {"name": "amount"},
                ],
                "primary_key": ["order_id"],
                "required": ["amount"],
                "foreign_keys": [
                    {"name": "fk_orders_customer",
                     "columns": ["cust_id"],
                     "target_table": "customers",
                     "references": ["cust_id"]},
                ],
            },
        ],
    }


class TwoTableHandCheck(unittest.TestCase):
    """手算：两个表之间的有效引用、缺失引用与记录顺序。"""

    def setUp(self) -> None:
        self.pkg = WORK / "two_table"
        if self.pkg.exists():
            shutil.rmtree(self.pkg)
        self.files = {
            "data/customers.csv": (
                "cust_id,cust_name,city\r\n"
                "C1,Alice,Beijing\r\n"
                "C2,Bob, Shanghai \r\n"
                "C3,,Beijing\r\n"          # 必填 cust_name 缺失
            ),
            "data/orders.csv": (
                "order_id,cust_id,amount\r\n"
                "O1,C1,10\r\n"              # 有效引用
                "O2,C2,20\r\n"              # 有效引用
                "O3,C9,30\r\n"              # 目标不存在
                "O4,C1,40\r\n"              # 有效引用，验证顺序保留
            ),
        }
        self.manifest = write_package(
            self.pkg, self.files, _two_table_manifest())
        self.out = self.pkg / "out"

    def test_row_level_results(self) -> None:
        proc = run_cli(self.pkg, self.manifest, self.out)
        self.assertEqual(proc.returncode, 0, proc.stderr)

        # 客户表：C1、C2 有效；C3 因必填缺失隔离。顺序保持输入顺序。
        self.assertEqual(valid_rows(self.out, "customers"), [
            ["cust_id", "cust_name", "city"],
            ["C1", "Alice", "Beijing"],
            ["C2", "Bob", " Shanghai "],   # 原始文本保留，规范化只用于键判定
        ])
        # 订单表：O1/O2/O4 有效且顺序不变；O3 缺失目标。
        self.assertEqual(valid_rows(self.out, "orders"), [
            ["order_id", "cust_id", "amount"],
            ["O1", "C1", "10"],
            ["O2", "C2", "20"],
            ["O4", "C1", "40"],
        ])

        q = index_quarantine(read_quarantine(self.out / "quarantine.csv"))
        self.assertEqual(set(q), {("customers", "4"), ("orders", "4")})
        self.assertEqual(q[("customers", "4")]["reason_codes"], MISSING_REQUIRED)
        self.assertEqual(q[("orders", "4")]["reason_codes"], MISSING_TARGET)
        # 隔离行保留原文件、逻辑记录号与原始字段 JSON。
        self.assertEqual(q[("orders", "4")]["source_file"], "data/orders.csv")
        self.assertEqual(json.loads(q[("orders", "4")]["raw_row_json"]),
                         ["O3", "C9", "30"])

        report = json.loads((self.out / "report.json").read_text(encoding="utf-8"))
        by_table = {t["table"]: t for t in report["tables"]}
        self.assertEqual(
            (by_table["customers"]["read"],
             by_table["customers"]["valid"],
             by_table["customers"]["quarantined"]), (3, 2, 1))
        self.assertEqual(
            (by_table["orders"]["read"],
             by_table["orders"]["valid"],
             by_table["orders"]["quarantined"]), (4, 3, 1))
        self.assertEqual(by_table["orders"]["reason_records"][MISSING_TARGET], 1)
        self.assertEqual(by_table["customers"]["reason_records"][MISSING_REQUIRED], 1)


class DuplicateKeyFoldTest(unittest.TestCase):
    """两个主键在 trim/casefold 规范化后相等：整组隔离，不由顺序决定幸存者。"""

    def setUp(self) -> None:
        self.pkg = WORK / "dup_fold"
        if self.pkg.exists():
            shutil.rmtree(self.pkg)
        manifest = {
            "tables": [{
                "name": "items",
                "file": "items.csv",
                "columns": [
                    {"name": "sku", "trim": True, "casefold": True},
                    {"name": "label"},
                ],
                "primary_key": ["sku"],
                "required": ["label"],
            }],
        }
        files = {
            # 物理行 2/3/4 规范化后都是 "a1"；另有 " b2 " 唯一有效。
            "items.csv": (
                "sku,label\n"
                "A1,first\n"
                " a1 ,second\n"
                "A1,third\n"
                " b2 ,fourth\n"
            ),
        }
        self.manifest = write_package(self.pkg, files, manifest)
        self.out = self.pkg / "out"

    def test_whole_duplicate_group_quarantined(self) -> None:
        proc = run_cli(self.pkg, self.manifest, self.out)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(valid_rows(self.out, "items"),
                         [["sku", "label"], [" b2 ", "fourth"]])
        q = index_quarantine(read_quarantine(self.out / "quarantine.csv"))
        # 表头是逻辑记录 1；三条重复数据行是逻辑记录 2、3、4，整组隔离。
        self.assertEqual(set(q), {("items", "2"), ("items", "3"), ("items", "4")})
        for key in (("items", "2"), ("items", "3"), ("items", "4")):
            self.assertEqual(q[key]["reason_codes"], DUP_KEY)
            self.assertEqual(json.loads(q[key]["raw_row_json"])[1] is not None, True)
        # 原始文本保留，未被规范化改写。
        self.assertEqual(json.loads(q[("items", "3")]["raw_row_json"]),
                         [" a1 ", "second"])


class CascadePropagationTest(unittest.TestCase):
    """隔离沿外键逐层传播：父 → 子 → 孙；无关记录保留。"""

    def setUp(self) -> None:
        self.pkg = WORK / "cascade"
        if self.pkg.exists():
            shutil.rmtree(self.pkg)
        manifest = {
            "tables": [
                {"name": "parents", "file": "parents.csv",
                 "columns": [{"name": "pid"}, {"name": "v"}],
                 "primary_key": ["pid"], "required": ["v"]},
                {"name": "children", "file": "children.csv",
                 "columns": [{"name": "cid"}, {"name": "pid"}, {"name": "v"}],
                 "primary_key": ["cid"], "required": ["v"],
                 "foreign_keys": [
                     {"name": "fk_c_p", "columns": ["pid"],
                      "target_table": "parents", "references": ["pid"]}]},
                {"name": "grandchildren", "file": "grandchildren.csv",
                 "columns": [{"name": "gid"}, {"name": "cid"}, {"name": "v"}],
                 "primary_key": ["gid"], "required": ["v"],
                 "foreign_keys": [
                     {"name": "fk_g_c", "columns": ["cid"],
                      "target_table": "children", "references": ["cid"]}]},
            ],
        }
        files = {
            # P1 必填 v 缺失 -> 自身隔离；P2 有效；P3 有效（无关记录）。
            "parents.csv": "pid,v\nP1,\nP2,x\nP3,y\n",
            # C1->P1（传播隔离）；C2->P2 有效；C3->P9 目标缺失。
            "children.csv": "cid,pid,v\nC1,P1,a\nC2,P2,b\nC3,P9,c\n",
            # G1->C1（第二层传播）；G2->C2 有效；G3->C3（目标存在但已隔离）。
            "grandchildren.csv": "gid,cid,v\nG1,C1,m\nG2,C2,n\nG3,C3,o\n",
        }
        self.manifest = write_package(self.pkg, files, manifest)
        self.out = self.pkg / "out"

    def test_propagation_layer_by_layer(self) -> None:
        proc = run_cli(self.pkg, self.manifest, self.out)
        self.assertEqual(proc.returncode, 0, proc.stderr)

        self.assertEqual(valid_rows(self.out, "parents"),
                         [["pid", "v"], ["P2", "x"], ["P3", "y"]])
        self.assertEqual(valid_rows(self.out, "children"),
                         [["cid", "pid", "v"], ["C2", "P2", "b"]])
        self.assertEqual(valid_rows(self.out, "grandchildren"),
                         [["gid", "cid", "v"], ["G2", "C2", "n"]])

        q = index_quarantine(read_quarantine(self.out / "quarantine.csv"))
        self.assertEqual(q[("parents", "2")]["reason_codes"], MISSING_REQUIRED)
        self.assertEqual(q[("children", "2")]["reason_codes"], REF_QUARANTINED)
        self.assertEqual(q[("children", "4")]["reason_codes"], MISSING_TARGET)
        self.assertEqual(q[("grandchildren", "2")]["reason_codes"], REF_QUARANTINED)
        self.assertEqual(q[("grandchildren", "4")]["reason_codes"], REF_QUARANTINED)
        # 无关记录 P3/C2/G2 不在隔离文件。
        self.assertNotIn(("parents", "4"), q)
        self.assertNotIn(("children", "3"), q)
        self.assertNotIn(("grandchildren", "3"), q)

        report = json.loads((self.out / "report.json").read_text(encoding="utf-8"))
        totals = report["totals"]
        self.assertEqual(totals["read"], 9)
        self.assertEqual(totals["valid"], 4)
        self.assertEqual(totals["quarantined"], 5)
        self.assertEqual(totals["reasons"][REF_QUARANTINED], 3)
        self.assertEqual(totals["reasons"][MISSING_TARGET], 1)
        self.assertEqual(totals["reasons"][MISSING_REQUIRED], 1)


class MutualReferenceTest(unittest.TestCase):
    """互相引用、键唯一且无其他错误：两条记录都保留。"""

    def setUp(self) -> None:
        self.pkg = WORK / "mutual"
        if self.pkg.exists():
            shutil.rmtree(self.pkg)
        manifest = {
            "tables": [{
                "name": "nodes", "file": "nodes.csv",
                "columns": [{"name": "nid"}, {"name": "peer"}],
                "primary_key": ["nid"],
                "foreign_keys": [
                    {"name": "fk_peer", "columns": ["peer"],
                     "target_table": "nodes", "references": ["nid"]}]},
            ],
        }
        files = {"nodes.csv": "nid,peer\nN1,N2\nN2,N1\nN3,N9\n"}
        self.manifest = write_package(self.pkg, files, manifest)
        self.out = self.pkg / "out"

    def test_cycle_survives(self) -> None:
        proc = run_cli(self.pkg, self.manifest, self.out)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(valid_rows(self.out, "nodes"),
                         [["nid", "peer"], ["N1", "N2"], ["N2", "N1"]])
        q = index_quarantine(read_quarantine(self.out / "quarantine.csv"))
        self.assertEqual(set(q), {("nodes", "4")})
        self.assertEqual(q[("nodes", "4")]["reason_codes"], MISSING_TARGET)


class MultipleReasonsTest(unittest.TestCase):
    """同一行同时缺必填值且引用不存在：两个原因都保留并按稳定顺序排列。"""

    def setUp(self) -> None:
        self.pkg = WORK / "multi_reason"
        if self.pkg.exists():
            shutil.rmtree(self.pkg)
        manifest = {
            "tables": [
                {"name": "p", "file": "p.csv",
                 "columns": [{"name": "pid"}, {"name": "v"}],
                 "primary_key": ["pid"], "required": ["v"]},
                {"name": "c", "file": "c.csv",
                 "columns": [{"name": "cid"}, {"name": "pid"}, {"name": "v"}],
                 "primary_key": ["cid"], "required": ["v"],
                 "foreign_keys": [
                     {"name": "fk", "columns": ["pid"],
                      "target_table": "p", "references": ["pid"]}]},
            ],
        }
        files = {
            "p.csv": "pid,v\nP1,x\n",
            # C1：v 缺失 + 引用 P9 不存在；C2：仅 v 缺失。
            "c.csv": "cid,pid,v\nC1,P9,\nC2,P1,\n",
        }
        self.manifest = write_package(self.pkg, files, manifest)
        self.out = self.pkg / "out"

    def test_all_reasons_kept_sorted(self) -> None:
        proc = run_cli(self.pkg, self.manifest, self.out)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        q = index_quarantine(read_quarantine(self.out / "quarantine.csv"))
        row = q[("c", "2")]
        self.assertEqual(row["reason_count"], "2")
        self.assertEqual(
            row["reason_codes"],
            f"{MISSING_REQUIRED}|{MISSING_TARGET}")  # 稳定排序：rank 先于文字
        self.assertIn("column=v", row["reason_details"])
        self.assertIn("fk=fk", row["reason_details"])
        # 只有一个问题的行不附带多余原因。
        self.assertEqual(q[("c", "3")]["reason_codes"], MISSING_REQUIRED)


class EmbeddedNewlineTest(unittest.TestCase):
    """嵌入换行的引号字段不改变逻辑记录序号；重排表后定位仍稳定。"""

    def setUp(self) -> None:
        self.pkg = WORK / "multiline"
        if self.pkg.exists():
            shutil.rmtree(self.pkg)
        manifest = {
            "tables": [{
                "name": "notes", "file": "notes.csv",
                "columns": [{"name": "nid"}, {"name": "body"}],
                "primary_key": ["nid"], "required": ["body"]},
            ],
        }
        # 数据行 2 的 body 含真实换行与转义引号，物理上占 3 行但只是逻辑记录 2。
        files = {
            "notes.csv": (
                'nid,body\n'
                'N1,"line1\nline2"\n'
                'N2,"say ""hi""\nagain"\n'
                'N3,\n'
            ),
        }
        self.manifest = write_package(self.pkg, files, manifest)
        self.out = self.pkg / "out"

    def test_logical_record_numbers(self) -> None:
        proc = run_cli(self.pkg, self.manifest, self.out)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        rows = valid_rows(self.out, "notes")
        self.assertEqual(rows, [
            ["nid", "body"],
            ["N1", "line1\nline2"],
            ["N2", 'say "hi"\nagain'],
        ])
        q = read_quarantine(self.out / "quarantine.csv")
        # N3 是逻辑记录 4（表头=1），尽管它在文件中跨过多个物理行。
        self.assertEqual(len(q), 1)
        self.assertEqual(q[0]["logical_record_no"], "4")  # 表头=1，N1=2，N2=3，N3=4
        self.assertEqual(q[0]["reason_codes"], MISSING_REQUIRED)


class DeterminismTest(unittest.TestCase):
    """同内容在不同文件遍历顺序下产物逐字节相同（清单顺序驱动一切）。"""

    def test_byte_identical_outputs(self) -> None:
        pkg_a = WORK / "determ_a"
        pkg_b = WORK / "determ_b"
        for p in (pkg_a, pkg_b):
            if p.exists():
                shutil.rmtree(p)
        for pkg in (pkg_a, pkg_b):
            write_package(pkg, {
                "data/customers.csv": "cust_id,cust_name,city\nC1,A,x\nC2,B,y\n",
                "data/orders.csv": (
                    "order_id,cust_id,amount\nO1,C1,1\nO2,C2,2\nO3,C9,3\n"),
            }, _two_table_manifest())

        out_a = pkg_a / "out"
        out_b = pkg_b / "out"
        self.assertEqual(run_cli(pkg_a, pkg_a / "manifest.json", out_a).returncode, 0)
        self.assertEqual(run_cli(pkg_b, pkg_b / "manifest.json", out_b).returncode, 0)

        names = ["report.json", "summary.txt", "quarantine.csv",
                 "valid/customers.csv", "valid/orders.csv"]
        for name in names:
            self.assertEqual(
                (out_a / name).read_bytes(), (out_b / name).read_bytes(),
                msg=f"{name} 在两次运行间不一致")
        # 交付内容中不得出现输入绝对路径。
        for name in names:
            text = (out_a / name).read_text(encoding="utf-8")
            self.assertNotIn(str(pkg_a.resolve()), text)

    def test_table_declaration_order_does_not_change_verdicts(self) -> None:
        # 同一份数据，清单中两张表的声明顺序对调：判定结果必须完全一致，
        # 因为重复识别与外键判定都以磁盘暂存表为依据，与遍历顺序无关。
        pkg = WORK / "determ_reorder"
        if pkg.exists():
            shutil.rmtree(pkg)
        write_package(pkg, {
            "data/customers.csv": "cust_id,cust_name,city\nC1,A,x\nC2,,y\n",
            "data/orders.csv": "order_id,cust_id,amount\nO1,C1,1\nO2,C2,2\nO3,C9,3\n",
        }, _two_table_manifest())
        reordered = _two_table_manifest()
        reordered["tables"] = list(reversed(reordered["tables"]))
        (pkg / "manifest_reversed.json").write_text(
            json.dumps(reordered, ensure_ascii=False, indent=2), encoding="utf-8")

        out1, out2 = pkg / "out1", pkg / "out2"
        self.assertEqual(run_cli(pkg, pkg / "manifest.json", out1).returncode, 0)
        self.assertEqual(
            run_cli(pkg, pkg / "manifest_reversed.json", out2).returncode, 0)

        # 各表有效行与隔离原因集合与顺序无关。
        for table in ("customers", "orders"):
            self.assertEqual(read_csv(out1 / "valid" / f"{table}.csv"),
                             read_csv(out2 / "valid" / f"{table}.csv"))
        q1 = sorted((r["table_name"], r["logical_record_no"], r["reason_codes"])
                    for r in read_quarantine(out1 / "quarantine.csv"))
        q2 = sorted((r["table_name"], r["logical_record_no"], r["reason_codes"])
                    for r in read_quarantine(out2 / "quarantine.csv"))
        self.assertEqual(q1, q2)
        r1 = json.loads((out1 / "report.json").read_text(encoding="utf-8"))
        r2 = json.loads((out2 / "report.json").read_text(encoding="utf-8"))
        self.assertEqual(r1["totals"], r2["totals"])


class StructuralFailureTest(unittest.TestCase):
    """损坏文件尾、错误字段数、越界路径、坏编码：整包失败且没有部分输出。"""

    def _base_manifest(self) -> dict:
        return {
            "tables": [{
                "name": "t", "file": "t.csv",
                "columns": [{"name": "a"}, {"name": "b"}],
                "primary_key": ["a"], "required": ["b"]}],
        }

    def _assert_failed_atomically(self, pkg: Path, manifest: Path,
                                  expected_code: int = 3) -> None:
        out = pkg / "out"
        proc = run_cli(pkg, manifest, out)
        self.assertEqual(proc.returncode, expected_code, proc.stdout)
        # 输出目录要么不存在，要么为空——不得留下看似完整的部分结果。
        if out.exists():
            self.assertEqual(list(out.iterdir()), [])

    def test_unclosed_quote_at_tail(self) -> None:
        pkg = WORK / "fail_quote"
        if pkg.exists():
            shutil.rmtree(pkg)
        files = {"t.csv": "a,b\nX1,y\nX2,\"unterminated\n"}
        manifest = write_package(pkg, files, self._base_manifest())
        self._assert_failed_atomically(pkg, manifest)
        with self.assertRaises(StructureError):
            ensure_quotes_closed(pkg / "t.csv")

    def test_wrong_field_count(self) -> None:
        pkg = WORK / "fail_fields"
        if pkg.exists():
            shutil.rmtree(pkg)
        files = {"t.csv": "a,b\nX1,ok\nX2,only_one,X3\n"}
        manifest = write_package(pkg, files, self._base_manifest())
        self._assert_failed_atomically(pkg, manifest)

    def test_bad_header(self) -> None:
        pkg = WORK / "fail_header"
        if pkg.exists():
            shutil.rmtree(pkg)
        files = {"t.csv": "a,wrong\nX1,ok\n"}
        manifest = write_package(pkg, files, self._base_manifest())
        self._assert_failed_atomically(pkg, manifest)

    def test_bad_encoding(self) -> None:
        pkg = WORK / "fail_encoding"
        if pkg.exists():
            shutil.rmtree(pkg)
        (pkg).mkdir(parents=True)
        (pkg / "t.csv").write_bytes("a,b\nX1,\xff\xfe bad\n".encode("utf-8", "ignore")
                                    if False else b"a,b\nX1,\xff\xfebad\n")
        manifest = write_package(pkg, {}, self._base_manifest())
        self._assert_failed_atomically(pkg, manifest)

    def test_path_escape_rejected_by_manifest(self) -> None:
        pkg = WORK / "fail_escape"
        if pkg.exists():
            shutil.rmtree(pkg)
        bad_manifest = self._base_manifest()
        bad_manifest["tables"][0]["file"] = "../outside.csv"
        manifest = write_package(pkg, {"t.csv": "a,b\nX1,y\n"}, bad_manifest)
        out = pkg / "out"
        proc = run_cli(pkg, manifest, out)
        self.assertEqual(proc.returncode, 2)
        if out.exists():
            self.assertEqual(list(out.iterdir()), [])

    def test_symlink_escape_rejected_at_open(self) -> None:
        pkg = WORK / "fail_symlink"
        if pkg.exists():
            shutil.rmtree(pkg)
        outside = WORK / "outside_target.csv"
        outside.write_text("a,b\nX1,y\n", encoding="utf-8")
        manifest = write_package(pkg, {}, self._base_manifest())
        (pkg / "t.csv").symlink_to(outside)  # 词法合法，解析后指向包外
        self._assert_failed_atomically(pkg, manifest)

    def test_empty_file(self) -> None:
        pkg = WORK / "fail_empty"
        if pkg.exists():
            shutil.rmtree(pkg)
        manifest = write_package(pkg, {"t.csv": ""}, self._base_manifest())
        self._assert_failed_atomically(pkg, manifest)


class ManifestValidationTest(unittest.TestCase):
    """清单规则：只允许声明，不允许代码；各类非法声明必须拒绝。"""

    def _load(self, payload: dict):
        pkg = WORK / "manifest_bad"
        if pkg.exists():
            shutil.rmtree(pkg)
        pkg.mkdir(parents=True)
        path = pkg / "manifest.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return load_manifest(path)

    def test_duplicate_table_name(self) -> None:
        payload = {"tables": [
            {"name": "t", "file": "a.csv",
             "columns": [{"name": "id"}], "primary_key": ["id"]},
            {"name": "t", "file": "b.csv",
             "columns": [{"name": "id"}], "primary_key": ["id"]}]}
        with self.assertRaises(ManifestError) as ctx:
            self._load(payload)
        self.assertIn("重复表名", str(ctx.exception))

    def test_duplicate_column(self) -> None:
        payload = {"tables": [{"name": "t", "file": "a.csv",
                               "columns": [{"name": "id"}, {"name": "id"}],
                               "primary_key": ["id"]}]}
        with self.assertRaises(ManifestError):
            self._load(payload)

    def test_unknown_key_and_non_boolean_rule(self) -> None:
        payload = {"tables": [{"name": "t", "file": "a.csv",
                               "columns": [{"name": "id", "trim": "yes()"}],
                               "primary_key": ["id"]}]}
        with self.assertRaises(ManifestError):
            self._load(payload)

    def test_code_like_normalization_rejected(self) -> None:
        # 任何清单里夹带的“可执行内容”都没有入口：未知键直接拒绝。
        payload = {"tables": [{"name": "t", "file": "a.csv",
                               "columns": [{"name": "id", "expr": "__import__('os')"}],
                               "primary_key": ["id"]}]}
        with self.assertRaises(ManifestError) as ctx:
            self._load(payload)
        self.assertIn("不允许的键", str(ctx.exception))

    def test_fk_unknown_table(self) -> None:
        payload = {"tables": [{"name": "t", "file": "a.csv",
                               "columns": [{"name": "id"}, {"name": "ref"}],
                               "primary_key": ["id"],
                               "foreign_keys": [
                                   {"columns": ["ref"], "target_table": "ghost",
                                    "references": ["id"]}]}]}
        with self.assertRaises(ManifestError):
            self._load(payload)

    def test_fk_field_count_mismatch(self) -> None:
        payload = {"tables": [
            {"name": "p", "file": "p.csv",
             "columns": [{"name": "k1"}, {"name": "k2"}],
             "primary_key": ["k1", "k2"]},
            {"name": "c", "file": "c.csv",
             "columns": [{"name": "id"}, {"name": "ref"}],
             "primary_key": ["id"],
             "foreign_keys": [
                 {"columns": ["ref"], "target_table": "p",
                  "references": ["k1", "k2"]}]}]}
        with self.assertRaises(ManifestError) as ctx:
            self._load(payload)
        self.assertIn("字段数不一致", str(ctx.exception))

    def test_fk_normalization_mismatch(self) -> None:
        payload = {"tables": [
            {"name": "p", "file": "p.csv",
             "columns": [{"name": "k", "casefold": True}],
             "primary_key": ["k"]},
            {"name": "c", "file": "c.csv",
             "columns": [{"name": "id"}, {"name": "ref"}],
             "primary_key": ["id"],
             "foreign_keys": [
                 {"columns": ["ref"], "target_table": "p",
                  "references": ["k"]}]}]}
        with self.assertRaises(ManifestError) as ctx:
            self._load(payload)
        self.assertIn("规范化规则", str(ctx.exception))

    def test_absolute_and_dotdot_paths(self) -> None:
        for bad in ("/etc/passwd", "sub/../../x.csv", "C:/x.csv"):
            payload = {"tables": [{"name": "t", "file": bad,
                                   "columns": [{"name": "id"}],
                                   "primary_key": ["id"]}]}
            with self.assertRaises(ManifestError):
                self._load(payload)


class CompositeKeyAndFoldFkTest(unittest.TestCase):
    """复合主键 + 外键两端折叠规则一致时按规范化值匹配。"""

    def test_composite_matching(self) -> None:
        pkg = WORK / "composite"
        if pkg.exists():
            shutil.rmtree(pkg)
        norm = {"trim": True, "casefold": True}
        manifest = {"tables": [
            {"name": "p", "file": "p.csv",
             "columns": [{"name": "k1", **norm}, {"name": "k2", **norm},
                         {"name": "v"}],
             "primary_key": ["k1", "k2"], "required": ["v"]},
            {"name": "c", "file": "c.csv",
             "columns": [{"name": "id"}, {"name": "a", **norm},
                         {"name": "b", **norm}],
             "primary_key": ["id"],
             "foreign_keys": [
                 {"columns": ["a", "b"], "target_table": "p",
                  "references": ["k1", "k2"]}]},
        ]}
        files = {
            "p.csv": "k1,k2,v\n A , B ,x\n",
            # 本地键折叠后等于 (a,b) -> 命中；第二行复合键不存在。
            "c.csv": "id,a,b\nC1,a,b\nC2,z,z\n",
        }
        manifest_path = write_package(pkg, files, manifest)
        out = pkg / "out"
        proc = run_cli(pkg, manifest_path, out)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(valid_rows(out, "c"),
                         [["id", "a", "b"], ["C1", "a", "b"]])
        q = index_quarantine(read_quarantine(out / "quarantine.csv"))
        self.assertEqual(q[("c", "3")]["reason_codes"], MISSING_TARGET)


class BoundedMemorySmokeTest(unittest.TestCase):
    """小批大小 + 大文件：功能正确且全程不把整表读进内存（逐批/游标路径）。"""

    def test_large_table_small_batch(self) -> None:
        pkg = WORK / "bounded"
        if pkg.exists():
            shutil.rmtree(pkg)
        n_parents = 2000
        manifest = {
            "tables": [
                {"name": "p", "file": "p.csv",
                 "columns": [{"name": "pid"}, {"name": "v"}],
                 "primary_key": ["pid"], "required": ["v"]},
                {"name": "c", "file": "c.csv",
                 "columns": [{"name": "cid"}, {"name": "pid"}],
                 "primary_key": ["cid"],
                 "foreign_keys": [
                     {"columns": ["pid"], "target_table": "p",
                      "references": ["pid"]}]},
            ],
        }
        pkg.mkdir(parents=True)
        with (pkg / "p.csv").open("w", encoding="utf-8", newline="") as fh:
            fh.write("pid,v\n")
            for i in range(n_parents):
                fh.write(f"P{i},v{i}\n")
        with (pkg / "c.csv").open("w", encoding="utf-8", newline="") as fh:
            fh.write("cid,pid\n")
            for i in range(n_parents):
                # 偶数行引用存在的父；奇数行引用不存在的父。
                pid = f"P{i}" if i % 2 == 0 else f"Q{i}"
                fh.write(f"C{i},{pid}\n")
        manifest_path = write_package(pkg, {}, manifest)
        out = pkg / "out"
        proc = run_cli(pkg, manifest_path, out,
                       extra=["--batch-size", "37"])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        report = json.loads((out / "report.json").read_text(encoding="utf-8"))
        by_table = {t["table"]: t for t in report["tables"]}
        self.assertEqual(by_table["p"]["valid"], n_parents)
        self.assertEqual(by_table["c"]["valid"], n_parents // 2)
        self.assertEqual(
            by_table["c"]["reason_records"][MISSING_TARGET], n_parents // 2)
        # 有效表行数 = 数据行 + 表头。
        self.assertEqual(len(read_csv(out / "valid" / "c.csv")),
                         n_parents // 2 + 1)


class ThreeReasonsOnOneRowTest(unittest.TestCase):
    """同一行同时触发重复键、必填缺失与坏引用：三个原因全部保留且稳定排序。"""

    def test_all_three_reasons(self) -> None:
        pkg = WORK / "three_reasons"
        if pkg.exists():
            shutil.rmtree(pkg)
        manifest = {
            "tables": [
                {"name": "p", "file": "p.csv",
                 "columns": [{"name": "pid"}, {"name": "v"}],
                 "primary_key": ["pid"], "required": ["v"]},
                {"name": "c", "file": "c.csv",
                 "columns": [{"name": "cid"}, {"name": "pid"}, {"name": "v"}],
                 "primary_key": ["cid"], "required": ["v"],
                 "foreign_keys": [
                     {"columns": ["pid"], "target_table": "p",
                      "references": ["pid"]}]},
            ],
        }
        files = {
            "p.csv": "pid,v\nP1,x\n",
            # cid=C1 出现两次（DUP_KEY）；两条都缺 v；第一条还引用不存在的 P9。
            "c.csv": "cid,pid,v\nC1,P9,\nC1,P1,\n",
        }
        manifest_path = write_package(pkg, files, manifest)
        out = pkg / "out"
        proc = run_cli(pkg, manifest_path, out)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        q = index_quarantine(read_quarantine(out / "quarantine.csv"))
        self.assertEqual(set(q), {("c", "2"), ("c", "3")})
        self.assertEqual(q[("c", "2")]["reason_codes"],
                         f"{DUP_KEY}|{MISSING_REQUIRED}|{MISSING_TARGET}")
        self.assertEqual(q[("c", "2")]["reason_count"], "3")
        # 第二条只有重复键与必填缺失两个原因，不附带坏引用。
        self.assertEqual(q[("c", "3")]["reason_codes"],
                         f"{DUP_KEY}|{MISSING_REQUIRED}")
        # 没有任何有效行。
        self.assertEqual(valid_rows(out, "c"), [["cid", "pid", "v"]])


class ScannerUnitTest(unittest.TestCase):
    """直接对扫描器做小型手算单元测试。"""

    def test_empty_fk_not_treated_as_missing_target(self) -> None:
        # 外键列原始为空（且未声明必填）时视为“未填写引用”，不产生外键原因。
        pkg = WORK / "empty_fk"
        if pkg.exists():
            shutil.rmtree(pkg)
        manifest = {
            "tables": [
                {"name": "p", "file": "p.csv",
                 "columns": [{"name": "pid"}], "primary_key": ["pid"]},
                {"name": "c", "file": "c.csv",
                 "columns": [{"name": "cid"}, {"name": "pid"}],
                 "primary_key": ["cid"],
                 "foreign_keys": [
                     {"columns": ["pid"], "target_table": "p",
                      "references": ["pid"]}]},
            ],
        }
        files = {"p.csv": "pid\nP1\n", "c.csv": "cid,pid\nC1,\nC2,P1\n"}
        manifest_path = write_package(pkg, files, manifest)
        out = pkg / "out"
        proc = run_cli(pkg, manifest_path, out)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(valid_rows(out, "c"),
                         [["cid", "pid"], ["C1", ""], ["C2", "P1"]])
        self.assertEqual(read_quarantine(out / "quarantine.csv"), [])

    def test_logical_rows_with_embedded_newline(self) -> None:
        pkg = WORK / "scanner_unit"
        if pkg.exists():
            shutil.rmtree(pkg)
        pkg.mkdir(parents=True)
        path = pkg / "f.csv"
        path.write_text('a,b\n1,"x\ny"\n2,z\n', encoding="utf-8")
        rows = list(logical_rows(path, "f.csv"))
        self.assertEqual([r[0] for r in rows], [1, 2, 3])  # 表头=1，数据逻辑号 2、3
        self.assertEqual(rows[1][1], ["1", "x\ny"])

    def test_unclosed_quote_detected(self) -> None:
        pkg = WORK / "scanner_unit2"
        if pkg.exists():
            shutil.rmtree(pkg)
        pkg.mkdir(parents=True)
        path = pkg / "f.csv"
        path.write_text('a,b\n1,"oops\n2,z\n', encoding="utf-8")
        with self.assertRaises(StructureError):
            list(logical_rows(path, "f.csv"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
