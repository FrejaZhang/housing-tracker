"""
notifier.py
简报推送模块

支持四种推送渠道（任选其一）：
  1. 飞书群机器人 (推荐，最简单，支持多人)
     在飞书群添加机器人，复制 Webhook URL
     在 config.py 中设置 FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
     安全设置建议：自定义关键词 = "日报"

  2. 钉钉群机器人
     在 config.py 中设置 DINGTALK_WEBHOOK = "https://oapi.dingtalk.com/robot/send?access_token=xxx"

  3. Server酱 (微信扫码即用，但消息在订阅号)
     注册地址: https://sct.ftqq.com/
     在 config.py 中设置 SERVERCHAN_KEY = "xxx"

  4. WxPusher (免费，支持多人推送)
     注册地址: https://wxpusher.zjiecode.com/
     在 config.py 中设置 WXPUSHER_APP_TOKEN = "xxx" 和 WXPUSHER_UID = "xxx"

用法：
  python notifier.py                    # 发送今日简报
  python notifier.py --date 2026-04-21  # 发送指定日期简报
  python notifier.py --test             # 发送测试消息验证配置
"""

import argparse
import logging
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from config import DATA_DIR

# ─── 从 config.py 读取可选配置（也可用环境变量覆盖） ──────────────────
try:
    from config import FEISHU_WEBHOOK as _CFG_FEISHU_WEBHOOK
except ImportError:
    _CFG_FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/73d3c44f-eeed-4e82-8397-7fbb05c60e3f"

try:
    from config import DINGTALK_WEBHOOK as _CFG_DINGTALK_WEBHOOK
except ImportError:
    _CFG_DINGTALK_WEBHOOK = ""

try:
    from config import SERVERCHAN_KEY as _CFG_SERVERCHAN_KEY
except ImportError:
    _CFG_SERVERCHAN_KEY = ""

try:
    from config import WXPUSHER_APP_TOKEN as _CFG_WXPUSHER_APP_TOKEN
except ImportError:
    _CFG_WXPUSHER_APP_TOKEN = ""

try:
    from config import WXPUSHER_UID as _CFG_WXPUSHER_UID
except ImportError:
    _CFG_WXPUSHER_UID = ""

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# 1. 简报生成
# ─────────────────────────────────────────────────────────────────────

def build_report(date_str: str) -> tuple[str, str]:
    """
    读取指定日期的分析结果，生成微信简报。
    返回 (title, content) 两个字符串。
    title  ：消息标题（Server酱 desp 会显示在微信通知栏）
    content：Markdown 正文
    """
    today_dir = Path(DATA_DIR) / date_str

    # ── 签约统计 ──────────────────────────────────────────────────────
    sign_stats_path = today_dir / "sign_stats.csv"
    sign_area = sign_price = sign_count = 0
    if sign_stats_path.exists():
        df = pd.read_csv(sign_stats_path)
        if not df.empty:
            row = df.iloc[0]
            sign_count = int(row.get("已签约套数", 0))
            sign_area  = float(str(row.get("已签约面积", 0)).replace(",", "") or 0)
            sign_price = float(str(row.get("成交均价", 0)).replace(",", "") or 0)

    total_amount = sign_area * sign_price / 1e8  # 亿元

    # ── 变动记录 ──────────────────────────────────────────────────────
    changes_path = today_dir / "changes.csv"
    sign_changes: list[dict] = []
    house_changes: list[dict] = []

    if changes_path.exists():
        try:
            df_ch = pd.read_csv(changes_path)
            if not df_ch.empty:
                sign_ch  = df_ch[df_ch["变动类型"] == "签约统计变动"]
                house_ch = df_ch[df_ch["变动类型"] == "房屋状态变动"]
                sign_changes  = sign_ch.to_dict("records")
                house_changes = house_ch.to_dict("records")
        except Exception as e:
            logger.warning(f"读取 changes.csv 失败: {e}")

    # ── 楼盘基本信息（可选） ──────────────────────────────────────────
    project_name = "北京某楼盘"
    project_path = today_dir / "project_info.csv"
    if project_path.exists():
        try:
            pf = pd.read_csv(project_path)
            if not pf.empty and "项目名称" in pf.columns:
                project_name = str(pf.iloc[0]["项目名称"])
        except Exception:
            pass

    # ── 拼装标题 ──────────────────────────────────────────────────────
    title = f"🏠 {project_name} · {date_str} 日报"

    # ── 拼装正文（Markdown） ──────────────────────────────────────────
    lines: list[str] = []
    lines.append(f"## {title}")
    lines.append("")
    lines.append("### 📊 今日签约概况")
    lines.append(f"- 已签约套数：**{sign_count} 套**")
    lines.append(f"- 已签约面积：**{sign_area:,.2f} ㎡**")
    lines.append(f"- 成交均价：**{sign_price:,.0f} 元/㎡**")
    lines.append(f"- 累计签约总额：约 **{total_amount:.2f} 亿元**")
    lines.append("")

    # 签约统计变动
    if sign_changes:
        lines.append("### 📈 签约变动（较前日）")
        for r in sign_changes:
            new_area  = r.get("新增签约面积", 0)
            new_total = r.get("新增签约总价", 0)
            prev_p    = r.get("前日成交均价", 0)
            today_p   = r.get("今日成交均价", 0)
            try:
                new_total_f  = float(str(new_total).replace(",", "") or 0)
                new_area_f   = float(str(new_area).replace(",", "") or 0)
                prev_p_f     = float(str(prev_p).replace(",", "") or 0)
                today_p_f    = float(str(today_p).replace(",", "") or 0)
                price_diff   = today_p_f - prev_p_f
                price_arrow  = "↑" if price_diff >= 0 else "↓"
                lines.append(f"- 新增签约面积：{new_area_f:+,.2f} ㎡")
                lines.append(f"- 新增签约金额：{new_total_f:+,.0f} 元")
                lines.append(f"- 均价变动：{prev_p_f:,.0f} → {today_p_f:,.0f} 元/㎡（{price_arrow}{abs(price_diff):,.0f}）")
                # 若当日存在房屋变动，额外计算新增签约单价
                if house_changes and new_area_f > 0:
                    new_unit_price = new_total_f / new_area_f
                    lines.append(f"- 新增签约单价：**{new_unit_price:,.0f} 元/㎡**（新增签约金额 / 新增签约面积）")
            except Exception:
                lines.append(f"- {r.get('备注', '')}")
        lines.append("")

    # 逐户状态变动
    if house_changes:
        lines.append(f"### 🏘 房屋状态变动（共 {len(house_changes)} 套）")

        # 按楼栋分组
        building_map: dict[str, list[str]] = {}
        for r in house_changes:
            building = str(r.get("楼栋", "未知楼栋"))
            room = str(r.get("房间号", "") or "").strip()
            if not room:
                unit = str(r.get("单元", "")).strip()
                hno  = str(r.get("房号", "")).strip()
                room = f"{unit}-{hno}" if unit and hno else (unit or hno or "?")
            status_today = str(r.get("今日状态", ""))

            # 补充建筑面积和户型
            area   = str(r.get("建筑面积", "") or "").strip()
            layout = str(r.get("户型", "") or "").strip()
            detail_parts = [f"{room}({status_today})"]
            if area:
                detail_parts.append(f"{area}㎡")
            if layout:
                detail_parts.append(layout)
            building_map.setdefault(building, []).append(" ".join(detail_parts))

        for bld, rooms in sorted(building_map.items()):
            lines.append(f"- **{bld}**：")
            for room_info in rooms:
                lines.append(f"  - {room_info}")
        lines.append("")
    else:
        lines.append("### 🏘 房屋状态变动")
        lines.append("- 今日无房屋状态变动")
        lines.append("")

    lines.append("---")
    lines.append(f"*数据采集时间：{date_str} 13:00 自动运行*")
    lines.append("*数据来源：北京市住建委网签备案*")

    content = "\n".join(lines)
    return title, content


# ─────────────────────────────────────────────────────────────────────
# 2. 推送渠道
# ─────────────────────────────────────────────────────────────────────

def _get_env(key: str, cfg_val: str) -> str:
    """优先读环境变量，其次用 config.py 中的值。"""
    return os.environ.get(key, cfg_val or "").strip()


def push_feishu(title: str, content: str) -> bool:
    """
    飞书群机器人推送。
    配置：在 config.py 中设置 FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
    安全设置建议：自定义关键词 = "日报"
    文档：https://open.feishu.cn/document/client-docs/bot-v3/add-custom-bot
    """
    webhook = _get_env("FEISHU_WEBHOOK", _CFG_FEISHU_WEBHOOK)
    if not webhook:
        logger.warning("未配置 FEISHU_WEBHOOK，跳过飞书推送")
        return False

    # 飞书富文本格式
    payload = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": title,
                    "content": [
                        [{"tag": "text", "text": content}]
                    ]
                }
            }
        }
    }

    try:
        resp = requests.post(webhook, json=payload, timeout=15)
        data = resp.json()
        if data.get("code") == 0:
            logger.info("飞书推送成功")
            return True
        else:
            logger.error(f"飞书推送失败: {data}")
            return False
    except Exception as e:
        logger.error(f"飞书推送异常: {e}")
        return False


def push_dingtalk(title: str, content: str) -> bool:
    """
    钉钉群机器人推送。
    配置：在 config.py 中设置 DINGTALK_WEBHOOK = "https://oapi.dingtalk.com/robot/send?access_token=xxx"
    文档：https://open.dingtalk.com/document/isvapp-server/custom-robot-access
    """
    webhook = _get_env("DINGTALK_WEBHOOK", _CFG_DINGTALK_WEBHOOK)
    if not webhook:
        logger.warning("未配置 DINGTALK_WEBHOOK，跳过钉钉推送")
        return False

    # 钉钉 Markdown 不支持某些特殊字符，做简单转义
    safe_content = content.replace("#", "").replace("*", "")

    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": title,
            "text": f"## {title}\n\n{safe_content}",
        },
    }

    try:
        resp = requests.post(webhook, json=payload, timeout=15)
        data = resp.json()
        if data.get("errcode") == 0:
            logger.info("钉钉推送成功")
            return True
        else:
            logger.error(f"钉钉推送失败: {data}")
            return False
    except Exception as e:
        logger.error(f"钉钉推送异常: {e}")
        return False


def push_serverchan(title: str, content: str) -> bool:
    """
    Server酱推送。
    环境变量：SERVERCHAN_KEY
    注册：https://sct.ftqq.com/
    """
    key = _get_env("SERVERCHAN_KEY", _CFG_SERVERCHAN_KEY)
    if not key:
        logger.warning("未配置 SERVERCHAN_KEY，跳过 Server酱推送")
        return False

    url = f"https://sctapi.ftqq.com/{key}.send"
    try:
        resp = requests.post(
            url,
            data={"title": title, "desp": content},
            timeout=15,
        )
        data = resp.json()
        if data.get("code") == 0:
            logger.info("Server酱推送成功")
            return True
        else:
            logger.error(f"Server酱推送失败: {data}")
            return False
    except Exception as e:
        logger.error(f"Server酱推送异常: {e}")
        return False


def push_wxpusher(title: str, content: str) -> bool:
    """
    WxPusher 推送。
    环境变量：WXPUSHER_APP_TOKEN, WXPUSHER_UID
    注册：https://wxpusher.zjiecode.com/
    """
    token = _get_env("WXPUSHER_APP_TOKEN", _CFG_WXPUSHER_APP_TOKEN)
    uid   = _get_env("WXPUSHER_UID",       _CFG_WXPUSHER_UID)
    if not token or not uid:
        logger.warning("未配置 WXPUSHER_APP_TOKEN / WXPUSHER_UID，跳过 WxPusher 推送")
        return False

    url = "https://wxpusher.zjiecode.com/api/send/message"
    try:
        resp = requests.post(
            url,
            json={
                "appToken": token,
                "content": content,
                "summary": title,
                "contentType": 3,    # 3 = Markdown
                "uids": [uid],
            },
            timeout=15,
        )
        data = resp.json()
        if data.get("code") == 1000:
            logger.info("WxPusher 推送成功")
            return True
        else:
            logger.error(f"WxPusher 推送失败: {data}")
            return False
    except Exception as e:
        logger.error(f"WxPusher 推送异常: {e}")
        return False


def send_report(date_str: str | None = None) -> bool:
    """
    生成并发送简报。按顺序尝试已配置的渠道，任一成功即返回 True。
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"生成 {date_str} 简报...")
    title, content = build_report(date_str)
    logger.info(f"简报标题: {title}")

    ok = False
    ok = push_feishu(title, content)     or ok
    ok = push_dingtalk(title, content)   or ok
    ok = push_serverchan(title, content) or ok
    ok = push_wxpusher(title, content)   or ok

    if not ok:
        logger.warning(
            "所有推送渠道均未配置或推送失败。\n"
            "请配置以下任一环境变量：\n"
            "  飞书: FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/xxx\n"
            "  钉钉: DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=xxx\n"
            "  Server酱: SERVERCHAN_KEY=xxx\n"
            "  WxPusher: WXPUSHER_APP_TOKEN=xxx  WXPUSHER_UID=xxx"
        )
    return ok


def send_failure_alert(date_str: str | None = None, error_msg: str = "") -> bool:
    """
    推送任务运行失败告警到已配置的渠道（飞书优先）。
    date_str  : 失败的任务日期
    error_msg : 异常信息摘要
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    title = f"❌ 楼盘数据采集失败 · {date_str}"

    lines = [
        f"## {title}",
        "",
        f"**日期**：{date_str}",
        f"**时间**：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "### 错误信息",
        f"```\n{error_msg[:800]}\n```" if error_msg else "（无详细错误信息）",
        "",
        "---",
        "*请检查日志文件或手动重新运行 `python run_daily.py`*",
    ]
    content = "\n".join(lines)

    logger.info(f"推送失败告警: {title}")

    ok = False
    ok = push_feishu(title, content)     or ok
    ok = push_dingtalk(title, content)   or ok
    ok = push_serverchan(title, content) or ok
    ok = push_wxpusher(title, content)   or ok

    if not ok:
        logger.warning("失败告警推送也未成功，请检查推送渠道配置")
    return ok


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="发送楼盘日报到微信")
    parser.add_argument("--date",  default=None, help="日期 YYYY-MM-DD，默认今日")
    parser.add_argument("--test",  action="store_true", help="发送测试消息")
    args = parser.parse_args()

    if args.test:
        # --test 发送今日真实简报内容（与正式推送完全一致）
        date_str = args.date or datetime.now().strftime("%Y-%m-%d")
        logger.info(f"[测试模式] 发送 {date_str} 完整简报...")
        ok = send_report(date_str)
        if ok:
            logger.info("[测试模式] 推送成功，内容与正式日报一致")
        else:
            logger.warning("[测试模式] 推送失败，请检查各渠道配置")
    else:
        send_report(args.date)
