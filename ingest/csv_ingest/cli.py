"""命令行入口。

原子交付策略：所有处理先在输出目录旁边的隐藏临时工作目录中完成
（暂存 SQLite、valid/、quarantine.csv、报告），全部成功后才重命名到最终
输出目录。任何一步失败都会删除临时目录，最终位置不会出现半成品；
若最终位置已有内容，默认拒绝（``--force`` 可先清空）。
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

from .engine import IngestionEngine
from .errors import IngestError
from .manifest import load_manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="csv-ingest",
        description="多表 CSV 数据包校验入库：输出有效表、隔离记录与汇总报告。")
    parser.add_argument("--input-dir", required=True,
                        help="包含清单与全部 CSV 的输入目录")
    parser.add_argument("--manifest", required=True,
                        help="JSON 清单路径（必须位于输入目录内，可给相对输入目录的路径）")
    parser.add_argument("--output-dir", required=True,
                        help="交付目录：valid/、quarantine.csv、report.json、summary.txt")
    parser.add_argument("--batch-size", type=int, default=1000,
                        help="SQLite 批量提交与流式输出批大小（默认 1000，调小可进一步压缩内存）")
    parser.add_argument("--force", action="store_true",
                        help="输出目录已存在且非空时先清空")
    parser.add_argument("--keep-db", action="store_true",
                        help="保留暂存 SQLite（默认随临时工作目录一起删除）")
    return parser


def _prepare_output(output_dir: Path, force: bool) -> None:
    if output_dir.exists():
        if not output_dir.is_dir():
            raise IngestError(f"输出路径 {output_dir.name} 已存在且不是目录")
        if any(output_dir.iterdir()):
            if not force:
                raise IngestError(
                    f"输出目录 {output_dir.name} 非空；拒绝覆盖（如需清空请加 --force）")
            shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    # 先建占位，保证流程中任何失败都能被观察到并清理；成功后内容原子替换。


def run(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.batch_size < 1:
        print("错误：--batch-size 必须 >= 1", file=sys.stderr)
        return 2

    input_dir = Path(args.input_dir)
    if not input_dir.is_dir():
        print(f"错误：输入目录不存在或不是目录：{input_dir.name}", file=sys.stderr)
        return 2
    manifest_path = Path(args.manifest)
    if not manifest_path.is_absolute():
        manifest_path = input_dir / manifest_path
    # 清单自身也不允许越出输入目录。
    try:
        manifest_path.resolve().relative_to(input_dir.resolve())
    except (OSError, ValueError):
        print("错误：清单路径越出输入目录", file=sys.stderr)
        return 2

    output_dir = Path(args.output_dir)
    try:
        _prepare_output(output_dir, args.force)
    except IngestError as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 2

    parent = output_dir.parent
    parent.mkdir(parents=True, exist_ok=True)
    work_dir = Path(tempfile.mkdtemp(prefix=".ingest-work-", dir=parent))
    engine = None
    succeeded = False
    try:
        manifest = load_manifest(manifest_path)
        engine = IngestionEngine(manifest, input_dir, work_dir, args.batch_size)
        # 1) 所有文件结构性解析完整后才进入判定与交付。
        engine.stage_all()
        # 1b) 数据全部落盘后建立规范化键索引（内存有界，索引随磁盘增长）。
        engine.create_indexes()
        # 2) 表内判定：重复主键（全组隔离）+ 必填缺失。
        engine.mark_duplicate_keys()
        engine.mark_missing_required()
        # 3) 跨表外键不动点传播。
        engine.mark_foreign_keys()
        # 4) 先在工作目录生成全部交付物。
        staging_out = work_dir / "deliver"
        report = engine.write_outputs(staging_out)
        if args.keep_db:
            shutil.copy2(engine.db_path, staging_out / "staging.sqlite3")
        engine.close()
        engine = None

        # 5) 原子替换：清空正式输出目录占位后，移入全部交付物。
        for child in output_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
        for child in staging_out.iterdir():
            shutil.move(str(child), output_dir / child.name)
        succeeded = True
    except IngestError as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return exc.exit_code
    except OSError as exc:
        print(f"错误：文件操作失败：{exc}", file=sys.stderr)
        return 1
    finally:
        if engine is not None:
            engine.close()
        shutil.rmtree(work_dir, ignore_errors=True)
        if not succeeded and output_dir.exists() and output_dir.is_dir() \
                and not any(output_dir.iterdir()):
            # 失败时只可能留下我们自己创建的空占位目录，删除以免造成“看似完成”的错觉。
            shutil.rmtree(output_dir, ignore_errors=True)

    totals = report["totals"]
    print(
        f"完成：读取 {totals['read']} 行，有效 {totals['valid']} 行，"
        f"隔离 {totals['quarantined']} 行。交付目录：{output_dir}")
    return 0


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
