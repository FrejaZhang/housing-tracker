#!/usr/bin/env bash
# setup_cron.sh
# 一键配置 macOS cron 定时任务，每天 13:00 自动执行数据采集与分析。
#
# 用法：
#   cd /Users/freja_issac/zr/housing-tracker
#   chmod +x setup_cron.sh
#   ./setup_cron.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="$(which python3)"

if [ -z "$PYTHON_BIN" ]; then
    echo "[错误] 未找到 python3，请先安装 Python 3.9+"
    exit 1
fi

# 检查依赖是否已安装
echo "检查 Python 依赖..."
$PYTHON_BIN -c "import requests, bs4, pandas, playwright, dash" 2>/dev/null || {
    echo "安装依赖中..."
    $PYTHON_BIN -m pip install -r "$SCRIPT_DIR/requirements.txt"
    $PYTHON_BIN -m playwright install chromium
}

# cron 任务行：每天 13:00 执行，日志追加到 cron.log
CRON_JOB="0 13 * * * cd $SCRIPT_DIR && $PYTHON_BIN $SCRIPT_DIR/run_daily.py >> $SCRIPT_DIR/cron.log 2>&1"
CRON_MARK="# housing-tracker-daily"

# 检查是否已存在相同任务
EXISTING_CRON="$(crontab -l 2>/dev/null || true)"
if echo "$EXISTING_CRON" | grep -qF "housing-tracker-daily"; then
    echo "[提示] cron 任务已存在，跳过添加。"
    echo "当前 cron 任务："
    crontab -l | grep "housing-tracker"
else
    # 追加新任务
    (echo "$EXISTING_CRON"; echo "$CRON_MARK"; echo "$CRON_JOB") | crontab -
    echo "[成功] 已添加 cron 定时任务（每天 13:00 执行）："
    echo "  $CRON_JOB"
fi

echo ""
echo "=============================="
echo "配置完成！"
echo "  • 每天 13:00 自动采集、分析并推送简报"
echo "  • 日志文件: $SCRIPT_DIR/cron.log"
echo "  • 手动执行: python3 $SCRIPT_DIR/run_daily.py"
echo "  • 启动可视化: python3 $SCRIPT_DIR/run_daily.py --only-viz"
echo "  • 查看定时任务: crontab -l"
echo "  • 删除定时任务: crontab -e  （删除含 housing-tracker 的行）"
echo ""
echo "── 推送配置（任选其一）────────────────────────────────"
echo ""
echo "  方案A：钉钉群机器人（推荐，最简单，支持多人）"
echo "    1. 在钉钉建一个群，把朋友拉进来"
echo "    2. 群设置 → 智能群助手 → 添加机器人 → 自定义"
echo "    3. 机器人名字：房价简报，安全设置选\"自定义关键词\"，填：日报"
echo "    4. 复制 Webhook 地址（格式：https://oapi.dingtalk.com/robot/send?access_token=xxx）"
echo "    5. 在 housing-tracker/config.py 末尾追加："
echo "         DINGTALK_WEBHOOK = \"你的Webhook地址\""
echo "    6. 验证：python3 $SCRIPT_DIR/notifier.py --test"
echo ""
echo "  方案B：Server酱（微信扫码即用，但消息在订阅号）"
echo "    1. 访问 https://sct.ftqq.com/ 用微信扫码登录"
echo "    2. 复制你的 SendKey（格式：SCT...）"
echo "    3. 在 housing-tracker/config.py 末尾追加："
echo "         SERVERCHAN_KEY = \"你的SendKey\""
echo "    4. 验证：python3 $SCRIPT_DIR/notifier.py --test"
echo ""
echo "  方案C：WxPusher（支持多人通知）"
echo "    1. 访问 https://wxpusher.zjiecode.com/ 注册"
echo "    2. 创建应用，获取 AppToken"
echo "    3. 微信关注公众号并绑定，获取 UID"
echo "    4. 在 housing-tracker/config.py 末尾追加："
echo "         WXPUSHER_APP_TOKEN = \"你的AppToken\""
echo "         WXPUSHER_UID       = \"你的UID\""
echo "    5. 验证：python3 $SCRIPT_DIR/notifier.py --test"
echo "────────────────────────────────────────────────────────"
echo "  手动发送今日简报: python3 $SCRIPT_DIR/notifier.py"
echo "=============================="
