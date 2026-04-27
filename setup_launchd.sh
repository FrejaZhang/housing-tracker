#!/usr/bin/env bash
# setup_launchd.sh
# 用 macOS launchd 替代 cron，解决睡眠期间任务被跳过的问题。
# launchd 会在机器唤醒后检测错过的任务，保证每天必然运行一次。
#
# 用法：
#   cd /Users/freja_issac/zr/housing-tracker
#   chmod +x setup_launchd.sh
#
#   # 使用默认触发时间（每天 0:10）
#   ./setup_launchd.sh
#
#   # 自定义触发时间（例如每天 13:30）
#   ./setup_launchd.sh --hour 13 --minute 30
#
#   # 卸载 launchd 任务
#   ./setup_launchd.sh --uninstall

set -e

# ── 默认触发时间（可通过参数覆盖） ──────────────────────────────────
HOUR=0
MINUTE=10

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LABEL="com.housing-tracker.daily"
PLIST_TEMPLATE="$SCRIPT_DIR/com.housing-tracker.daily.plist"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
INSTALLED_PLIST="$LAUNCH_AGENTS_DIR/$LABEL.plist"
UNINSTALL=false

# ── 参数解析 ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --hour)
            HOUR="$2"; shift 2 ;;
        --minute)
            MINUTE="$2"; shift 2 ;;
        --uninstall)
            UNINSTALL=true; shift ;;
        *)
            echo "[错误] 未知参数: $1"
            echo "用法: $0 [--hour H] [--minute M] [--uninstall]"
            exit 1 ;;
    esac
done

# ── 卸载模式 ─────────────────────────────────────────────────────────
if $UNINSTALL; then
    echo "正在卸载 launchd 任务..."
    if launchctl list | grep -q "$LABEL" 2>/dev/null; then
        launchctl unload "$INSTALLED_PLIST" 2>/dev/null && echo "  ✓ 已从 launchd 卸载"
    else
        echo "  [提示] launchd 中未找到该任务（可能已卸载）"
    fi
    if [ -f "$INSTALLED_PLIST" ]; then
        rm "$INSTALLED_PLIST" && echo "  ✓ 已删除 plist 文件: $INSTALLED_PLIST"
    fi
    echo "卸载完成。"
    exit 0
fi

# ── 检查 Python ───────────────────────────────────────────────────────
PYTHON_BIN="$(which python3 2>/dev/null || true)"
if [ -z "$PYTHON_BIN" ]; then
    echo "[错误] 未找到 python3，请先安装 Python 3.9+"
    exit 1
fi
PYTHON_BIN="$(python3 -c 'import sys; print(sys.executable)')"
PYTHON_DIR="$(dirname "$PYTHON_BIN")"

# ── 检查依赖 ──────────────────────────────────────────────────────────
echo "检查 Python 依赖..."
"$PYTHON_BIN" -c "import requests, bs4, pandas, playwright, dash" 2>/dev/null || {
    echo "安装依赖中..."
    "$PYTHON_BIN" -m pip install -r "$SCRIPT_DIR/requirements.txt"
    "$PYTHON_BIN" -m playwright install chromium
}

# ── 校验触发时间 ──────────────────────────────────────────────────────
if ! [[ "$HOUR"   =~ ^[0-9]+$ ]] || [ "$HOUR"   -lt 0 ] || [ "$HOUR"   -gt 23 ]; then
    echo "[错误] --hour 必须是 0~23 之间的整数"; exit 1
fi
if ! [[ "$MINUTE" =~ ^[0-9]+$ ]] || [ "$MINUTE" -lt 0 ] || [ "$MINUTE" -gt 59 ]; then
    echo "[错误] --minute 必须是 0~59 之间的整数"; exit 1
fi

# ── 生成 plist ────────────────────────────────────────────────────────
mkdir -p "$LAUNCH_AGENTS_DIR"

if [ ! -f "$PLIST_TEMPLATE" ]; then
    echo "[错误] plist 模板不存在: $PLIST_TEMPLATE"
    exit 1
fi

# 替换占位符，生成最终 plist
sed \
    -e "s|__PYTHON_BIN__|$PYTHON_BIN|g" \
    -e "s|__SCRIPT_DIR__|$SCRIPT_DIR|g" \
    -e "s|__PYTHON_DIR__|$PYTHON_DIR|g" \
    -e "s|__HOME__|$HOME|g" \
    -e "s|__HOUR__|$HOUR|g" \
    -e "s|__MINUTE__|$MINUTE|g" \
    "$PLIST_TEMPLATE" > "$INSTALLED_PLIST"

echo "  ✓ plist 已写入: $INSTALLED_PLIST"

# ── 如果已加载，先卸载旧版本 ─────────────────────────────────────────
if launchctl list | grep -q "$LABEL" 2>/dev/null; then
    echo "  检测到旧任务，正在重新加载..."
    launchctl unload "$INSTALLED_PLIST" 2>/dev/null || true
fi

# ── 加载到 launchd ────────────────────────────────────────────────────
launchctl load "$INSTALLED_PLIST"
echo "  ✓ launchd 任务已加载"

# ── 移除旧 cron 任务（如果存在） ─────────────────────────────────────
EXISTING_CRON="$(crontab -l 2>/dev/null || true)"
if echo "$EXISTING_CRON" | grep -q "housing-tracker-daily"; then
    echo ""
    echo "  检测到旧 cron 任务，正在自动移除..."
    echo "$EXISTING_CRON" | grep -v "housing-tracker" | crontab -
    echo "  ✓ 旧 cron 任务已清除"
fi

# ── 完成提示 ─────────────────────────────────────────────────────────
printf '\n'
printf '==============================\n'
printf '配置完成！\n'
printf '  • 调度方式: launchd（macOS 原生，唤醒后可补跑）\n'
printf '  • 触发时间: 每天 %02d:%02d\n' "$HOUR" "$MINUTE"
printf '  • 任务标识: %s\n' "$LABEL"
printf '  • plist 路径: %s\n' "$INSTALLED_PLIST"
printf '  • 日志文件: %s/cron.log\n' "$SCRIPT_DIR"
printf '\n'
printf '── 常用命令 ──────────────────────────────────────────────\n'
printf '  手动立即执行:   python3 %s/run_daily.py\n' "$SCRIPT_DIR"
printf '  手动发送简报:   python3 %s/notifier.py\n' "$SCRIPT_DIR"
printf '  查看任务状态:   launchctl list | grep housing-tracker\n'
printf '  查看日志:       tail -f %s/cron.log\n' "$SCRIPT_DIR"
printf '  修改触发时间:   %s/setup_launchd.sh --hour H --minute M\n' "$SCRIPT_DIR"
printf '  卸载任务:       %s/setup_launchd.sh --uninstall\n' "$SCRIPT_DIR"
printf '\n'
printf '── 推送配置（任选其一）──────────────────────────────────\n'
printf '\n'
printf '  方案A：飞书群机器人（当前已选，推荐）\n'
printf '    1. 飞书群 → 群设置 → 群机器人 → 添加机器人 → 自定义机器人\n'
printf '    2. 安全设置选"自定义关键词"，填：日报\n'
printf '    3. 复制 Webhook 地址\n'
printf '    4. 在 housing-tracker/config.py 末尾追加：\n'
printf '         FEISHU_WEBHOOK = "你的Webhook地址"\n'
printf '    5. 验证：python3 %s/notifier.py --test\n' "$SCRIPT_DIR"
printf '\n'
printf '  方案B：钉钉群机器人\n'
printf '    1. 群设置 → 智能群助手 → 添加机器人 → 自定义\n'
printf '    2. 安全设置选"自定义关键词"，填：日报\n'
printf '    3. 在 config.py 末尾追加：\n'
printf '         DINGTALK_WEBHOOK = "你的Webhook地址"\n'
printf '\n'
printf '  方案C：Server酱（微信订阅号推送）\n'
printf '    访问 https://sct.ftqq.com/ 获取 SendKey\n'
printf '    在 config.py 末尾追加：SERVERCHAN_KEY = "你的SendKey"\n'
printf '──────────────────────────────────────────────────────────\n'
printf '==============================\n'
