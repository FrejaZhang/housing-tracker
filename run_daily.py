"""
run_daily.py
每日主入口：按序执行 采集 → 分析 → （可选）启动可视化
支持命令行参数，适合 cron 调用。

用法：
  python run_daily.py                    # 采集+分析今日数据
  python run_daily.py --date 2026-04-21  # 指定日期
  python run_daily.py --viz              # 采集+分析后启动可视化服务
  python run_daily.py --only-viz         # 仅启动可视化（不重新采集）
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# ── 日志配置 ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("run_daily")


def setup_date_log(date_str: str) -> logging.FileHandler:
    """将日志同时写入当日目录下的 run.log。"""
    from config import DATA_DIR
    log_dir = Path(DATA_DIR) / date_str
    log_dir.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(log_dir / "run.log", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s"))
    logging.getLogger().addHandler(fh)
    return fh


def main():
    parser = argparse.ArgumentParser(description="楼盘数据每日采集与分析")
    parser.add_argument("--date",     default=None,  help="指定日期 YYYY-MM-DD，默认今日")
    parser.add_argument("--viz",      action="store_true", help="采集分析完成后启动可视化服务")
    parser.add_argument("--only-viz", action="store_true", help="跳过采集/分析，仅启动可视化")
    parser.add_argument("--port",     type=int, default=8050, help="可视化服务端口，默认 8050")
    args = parser.parse_args()

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    fh = setup_date_log(date_str)

    logger.info(f"====== 任务开始: {date_str} ======")

    try:
        if not args.only_viz:
            # ── Step 1: 采集 ─────────────────────────────────────────
            logger.info("--- Step 1: 数据采集 ---")
            from scraper import run_scraper
            run_scraper(date_str)
            logger.info("--- Step 1 完成 ---")

            # ── Step 2: 分析 ─────────────────────────────────────────
            logger.info("--- Step 2: 变动分析 ---")
            from analyzer import run_analyzer
            run_analyzer(date_str)
            logger.info("--- Step 2 完成 ---")

            # ── Step 3: 微信简报推送 ──────────────────────────────────
            logger.info("--- Step 3: 微信简报推送 ---")
            try:
                from notifier import send_report
                ok = send_report(date_str)
                if ok:
                    logger.info("--- Step 3 完成（推送成功） ---")
                else:
                    logger.warning("--- Step 3 完成（未配置推送渠道或推送失败，不影响主流程） ---")
            except Exception as notify_err:
                logger.warning(f"--- Step 3 推送异常（不影响主流程）: {notify_err} ---")

        # ── Step 4: 可视化（可选） ────────────────────────────────────
        if args.viz or args.only_viz:
            logger.info(f"--- Step 4: 启动可视化服务 port={args.port} ---")
            from visualizer import create_app
            app = create_app(date_str)
            print(f"\n可视化服务已启动: http://127.0.0.1:{args.port}/\n按 Ctrl+C 停止\n")
            app.run(debug=False, port=args.port)

    except KeyboardInterrupt:
        logger.info("用户中断")
    except Exception as e:
        logger.exception(f"执行异常: {e}")
        sys.exit(1)
    finally:
        logger.info(f"====== 任务结束: {date_str} ======")
        logging.getLogger().removeHandler(fh)


if __name__ == "__main__":
    main()
