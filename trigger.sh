#!/usr/bin/env bash
# trigger.sh
# 人工触发楼盘数据采集与分析的交互式脚本。
#
# 用法：
#   cd /Users/freja_issac/zr/housing-tracker
#   ./trigger.sh
#
# 也可直接传参（跳过交互）：
#   ./trigger.sh --date 2026-04-21 --viz

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="$(which python3)"
TODAY="$(date +%Y-%m-%d)"

if [ -z "$PYTHON_BIN" ]; then
    echo "[错误] 未找到 python3，请先安装 Python 3.9+"
    exit 1
fi

# ── 若有命令行参数，直接透传给 run_daily.py ─────────────────────────────
if [ $# -gt 0 ]; then
    echo "[直接执行] python3 run_daily.py $*"
    cd "$SCRIPT_DIR"
    exec "$PYTHON_BIN" "$SCRIPT_DIR/run_daily.py" "$@"
fi

# ── 交互模式 ──────────────────────────────────────────────────────────────
echo "=============================="
echo "  楼盘数据 · 人工触发分析"
echo "=============================="
echo ""

# 1. 选择日期
read -rp "请输入日期 [默认今天 $TODAY]: " INPUT_DATE
DATE="${INPUT_DATE:-$TODAY}"
if ! [[ "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "[错误] 日期格式不正确，请使用 YYYY-MM-DD"
    exit 1
fi

# 2. 选择执行模式
echo ""
echo "请选择执行模式："
echo "  1) 采集 + 分析           （默认）"
echo "  2) 采集 + 分析 + 启动可视化"
echo "  3) 仅启动可视化（使用已有数据）"
read -rp "输入序号 [1/2/3，默认 1]: " MODE_INPUT
MODE="${MODE_INPUT:-1}"

# 3. 端口（仅模式2/3需要）
PORT=8050
if [[ "$MODE" == "2" || "$MODE" == "3" ]]; then
    read -rp "可视化端口 [默认 8050]: " PORT_INPUT
    PORT="${PORT_INPUT:-8050}"
fi

# 4. 构建参数并执行
echo ""
echo "=============================="
case "$MODE" in
    1)
        echo "执行：采集 + 分析 · 日期=$DATE"
        CMD=("$PYTHON_BIN" "$SCRIPT_DIR/run_daily.py" "--date" "$DATE")
        ;;
    2)
        echo "执行：采集 + 分析 + 可视化 · 日期=$DATE · 端口=$PORT"
        CMD=("$PYTHON_BIN" "$SCRIPT_DIR/run_daily.py" "--date" "$DATE" "--viz" "--port" "$PORT")
        ;;
    3)
        echo "执行：仅可视化 · 日期=$DATE · 端口=$PORT"
        CMD=("$PYTHON_BIN" "$SCRIPT_DIR/run_daily.py" "--date" "$DATE" "--only-viz" "--port" "$PORT")
        ;;
    *)
        echo "[错误] 无效选项：$MODE"
        exit 1
        ;;
esac
echo "=============================="
echo ""

cd "$SCRIPT_DIR"
exec "${CMD[@]}"
