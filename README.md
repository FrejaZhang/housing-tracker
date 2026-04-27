# housing-tracker
# 修改触发时间（如改回 13:30）
./setup_launchd.sh --hour 13 --minute 30

# 查看任务状态
launchctl list | grep housing-tracker

# 查看运行日志
tail -f /Users/freja_issac/zr/housing-tracker/cron.log

# 卸载
./setup_launchd.sh --uninstall
