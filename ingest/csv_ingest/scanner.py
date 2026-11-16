"""CSV 结构性扫描与流式逻辑记录读取。

设计要点：

* 逻辑记录序号（logical record number, lno）按 **CSV 逻辑记录** 计数：
  带引号字段中嵌入的换行只是同一逻辑记录的一部分，不会被当成多条记录。
* 先用字节级状态机预检整份文件，专门识别“引号未闭合”——Python 的
  :class:`csv.reader` 对未闭合引号不会抛异常，会静默返回残缺行，所以必须自查。
* 解码使用严格 UTF-8（``errors="strict"``，兼容去掉 UTF-8 BOM），
  编码错误立刻使整包失败。
* 读取全程流式，逐行交给上层，不把整张表读进内存。
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterator

from .errors import StructureError

_CHUNK_SIZE = 1 << 20  # 1 MiB，预检按块读取，内存有界


def _iter_bytes(path: Path) -> Iterator[int]:
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(_CHUNK_SIZE)
            if not chunk:
                return
            yield from chunk


def ensure_quotes_closed(path: Path) -> None:
    """字节级扫描整份文件，若存在未闭合的引号字段则抛 :class:`StructureError`。

    语义与 Python 默认（QUOTE_MINIMAL / 双引号转义）一致：
    引号外的 ``"`` 开始一个引号字段；引号内 ``""`` 是转义的单个引号；
    引号内可包含任意换行；引号字段结束后回到引号外状态。
    """
    in_quotes = False
    quote_pending = False  # 刚在引号内读到一个引号（可能是结束，也可能是 "" 的第一个）
    physical_line = 1
    line_quoted = False    # 当前物理行是否有引号字段参与，用于报错定位
    prev = -1
    try:
        for byte in _iter_bytes(path):
            if byte == 0x22:  # "
                line_quoted = True
                if not in_quotes:
                    in_quotes = True
                    quote_pending = False
                elif quote_pending:
                    # "" 的第二个引号：转义引号，仍在字段内
                    quote_pending = False
                else:
                    quote_pending = True
            else:
                if quote_pending:
                    # 任何非引号字符（含逗号、换行）都确认引号已结束。
                    in_quotes = False
                    quote_pending = False
                if byte == 0x0A:  # \n；\r\n 只在 \r 处计一次
                    if prev != 0x0D:
                        physical_line += 1
                    if not in_quotes:
                        line_quoted = False
                elif byte == 0x0D:  # \r（含老 Mac 风格的单独 \r）
                    physical_line += 1
                    if not in_quotes:
                        line_quoted = False
            prev = byte
    except OSError as exc:
        raise StructureError(f"文件 {path.name} 无法读取：{exc}") from exc

    if in_quotes:
        raise StructureError(
            f"文件 {path.name} 在物理行 {physical_line} 附近存在未闭合的 CSV 引号字段"
            + ("（该行含引号字段）" if line_quoted else ""))


def logical_rows(path: Path, file_label: str
                 ) -> Iterator[tuple[int, list[str]]]:
    """逐逻辑记录产出 ``(逻辑记录序号, 字段列表)``，序号从 1 开始。

    空物理行被 csv 模块报告为 ``[]``，直接跳过且不占逻辑记录号——
    这与 RFC 4180 对空行的处理一致。
    """
    ensure_quotes_closed(path)
    try:
        # utf-8-sig：若存在 BOM 则剥除，无 BOM 时与普通 utf-8 等价；严格模式拒绝坏字节。
        fh = path.open("r", encoding="utf-8-sig", errors="strict", newline="")
    except OSError as exc:
        raise StructureError(f"文件 {file_label} 无法打开：{exc}") from exc
    try:
        reader = csv.reader(fh)
        lno = 0
        while True:
            try:
                row = next(reader)
            except StopIteration:
                return
            except UnicodeDecodeError as exc:
                raise StructureError(
                    f"文件 {file_label} 存在 UTF-8 编码错误：{exc}") from exc
            except csv.Error as exc:
                raise StructureError(f"文件 {file_label} CSV 解析失败：{exc}") from exc
            if not row:
                continue  # 空物理行
            lno += 1
            yield lno, row
    finally:
        fh.close()


def read_header(path: Path, file_label: str) -> list[str]:
    """读取第一个逻辑记录作为表头。"""
    try:
        _, header = next(logical_rows(path, file_label))
    except StopIteration:
        raise StructureError(f"文件 {file_label} 为空，缺少表头")
    return header


def check_header(path: Path, file_label: str, expected_columns: tuple[str, ...]) -> None:
    """表头必须与清单声明的列**按顺序、按名称**完全一致，否则整包失败。"""
    header = read_header(path, file_label)
    expected = list(expected_columns)
    if header != expected:
        raise StructureError(
            f"文件 {file_label} 表头与清单不符：\n"
            f"  清单声明: {expected}\n"
            f"  实际表头: {header}")
