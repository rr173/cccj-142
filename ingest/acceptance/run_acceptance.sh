#!/usr/bin/env bash
# 一键复做：构建手算数据包 -> 运行入库 -> 保存调用命令与结果 -> 独立断言。
# 所有 Python 调用均使用 -I（隔离模式），不加载工作目录中的任何 .py。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
DATA="$ROOT/acceptance/data"
OUT="$ROOT/acceptance/output"
LOG="$ROOT/acceptance/run.log"

rm -rf "$DATA" "$OUT"
mkdir -p "$DATA" "$OUT"

{
  echo "### 1. 构建手算数据包（不调用正式校验代码）"
  python3 -I "$HERE/build_demo.py" "$DATA"

  echo
  echo "### 2. 对好包执行入库"
  python3 -I "$ROOT/bin/csv-ingest" \
    --input-dir "$DATA/package_good" \
    --manifest  "$DATA/package_good/manifest.json" \
    --output-dir "$OUT/good" --batch-size 13 --force

  echo
  echo "### 3. 对损坏包执行入库（预期失败，退出码 3）"
  set +e
  python3 -I "$ROOT/bin/csv-ingest" \
    --input-dir "$DATA/package_bad" \
    --manifest  "$DATA/package_bad/manifest.json" \
    --output-dir "$OUT/bad" --batch-size 13 --force
  BAD_RC=$?
  set -e
  echo "损坏包退出码=$BAD_RC（期望 3）"
  test "$BAD_RC" -eq 3
  test ! -e "$OUT/bad" || test -z "$(ls -A "$OUT/bad")"

  echo
  echo "### 4. 独立手算断言"
  python3 -I "$HERE/verify_demo.py" "$DATA" "$OUT/verify"

  echo
  echo "### 5. 交付物清单"
  find "$OUT/good" -type f | sort
} 2>&1 | tee "$LOG"

echo
echo "复做完成，完整调用与结果已保存：$LOG"
