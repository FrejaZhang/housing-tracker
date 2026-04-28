"""
scraper.py
数据采集模块：
  1. 抓取主页面 → project_info.csv + sign_stats.csv
  2. 抓取楼盘列表页 → building_list.csv（动态解析当天所有楼栋）
  3. 逐栋抓取楼盘表 → house_status_{楼号}.csv
     * 通过 Playwright 加载楼盘表，逐一点击可点击房号链接进入详情页
     * 获取：房间号、单元、房号、建筑面积、套内面积、户型、拟售单价、状态
     * 不可点击的格子（已售出）不记录
  4. 每个页面用 Playwright 截图保存至 screenshots/
  5. 原始 HTML 保存至 raw_html/
"""

import os
import re
import time
import logging
import asyncio
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import pandas as pd

from config import (
    DATA_DIR, URL_MAIN, URL_BUILDING_LIST, URL_BUILDING_DETAIL,
    COLOR_STATUS_MAP, BUILDINGS_FALLBACK,
    REQUEST_DELAY, REQUEST_RETRIES, REQUEST_TIMEOUT, REQUEST_HEADERS,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# 目录工具
# ─────────────────────────────────────────────────────────────────────

def get_day_dirs(date_str: str) -> tuple[Path, Path, Path]:
    """返回 (day_dir, screenshots_dir, raw_html_dir)，并确保目录存在。"""
    day_dir   = Path(DATA_DIR) / date_str
    shots_dir = day_dir / "screenshots"
    html_dir  = day_dir / "raw_html"
    for d in (day_dir, shots_dir, html_dir):
        d.mkdir(parents=True, exist_ok=True)
    return day_dir, shots_dir, html_dir


# ─────────────────────────────────────────────────────────────────────
# HTTP 请求（带重试）
# ─────────────────────────────────────────────────────────────────────

def fetch_html(url: str) -> str | None:
    """带重试的 HTTP GET，返回响应文本；失败返回 None。"""
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            resp = requests.get(
                url, headers=REQUEST_HEADERS,
                timeout=REQUEST_TIMEOUT
            )
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except Exception as e:
            wait = 2 ** attempt
            logger.warning(f"[fetch_html] 第{attempt}次失败 {url}: {e}，{wait}s 后重试")
            time.sleep(wait)
    logger.error(f"[fetch_html] 全部重试失败: {url}")
    return None


# ─────────────────────────────────────────────────────────────────────
# Playwright 截图
# ─────────────────────────────────────────────────────────────────────

async def _take_screenshot_async(url: str, save_path: str) -> bool:
    """使用 Playwright 对单个页面全页截图（async）。"""
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                user_agent=REQUEST_HEADERS["User-Agent"],
            )
            page = await context.new_page()
            await page.goto(url, wait_until="networkidle", timeout=60_000)
            await page.screenshot(path=save_path, full_page=True)
            await browser.close()
        return True
    except Exception as e:
        logger.error(f"[screenshot] 截图失败 {url}: {e}")
        return False


def take_screenshot(url: str, save_path: str) -> bool:
    """同步包装 Playwright 截图。"""
    return asyncio.run(_take_screenshot_async(url, save_path))


# ─────────────────────────────────────────────────────────────────────
# 解析：主页面
# ─────────────────────────────────────────────────────────────────────

def parse_main_page(html: str) -> tuple[dict, dict]:
    """
    解析主页面，返回:
      project_info: 项目基本信息字段字典
      sign_stats:   签约统计字段字典
    """
    soup = BeautifulSoup(html, "lxml")

    # ── 项目基本信息 ──────────────────────────────────────────────────
    info = {}
    # 通用策略：找所有 label/th+td 对
    for row in soup.select("tr"):
        cells = row.find_all(["th", "td"])
        for i in range(0, len(cells) - 1, 2):
            key = cells[i].get_text(strip=True).rstrip("：:")
            val = cells[i + 1].get_text(strip=True)
            if key:
                info[key] = val

    # ── 签约统计：找表头行，取下一行数据 ────────────────────────────────
    sign_stats: dict = {}

    # 策略：找包含"已签约套数"的 tr（表头行），取同表下一个含数字的 tr
    for table in soup.find_all("table"):
        header_row = None
        for row in table.find_all("tr"):
            row_text = row.get_text()
            if "已签约套数" in row_text and "已签约面积" in row_text:
                header_row = row
                break
        if not header_row:
            continue

        # 解析表头列名
        header_cells = [td.get_text(strip=True) for td in header_row.find_all(["td", "th"])]

        # 找后续第一个含数字数据的行
        found = False
        for row in header_row.find_next_siblings("tr"):
            cells = row.find_all(["td", "th"])
            texts = [c.get_text(strip=True) for c in cells]
            # 检查是否含数值（签约套数/面积/均价）
            numeric_count = sum(1 for t in texts if re.match(r'^[\d,.]+$', t))
            if numeric_count >= 2:
                # 按列对齐，跳过"住宅"等用途列
                data_idx = 0
                for col_name in header_cells:
                    if "已签约套数" in col_name:
                        # 找对应列索引
                        for ci, t in enumerate(texts):
                            if re.match(r'^\d+$', t) and not sign_stats.get("已签约套数"):
                                sign_stats["已签约套数"] = t
                                break
                    elif "已签约面积" in col_name:
                        for ci, t in enumerate(texts):
                            if re.match(r'^[\d.]+$', t) and '.' in t and not sign_stats.get("已签约面积"):
                                sign_stats["已签约面积"] = t
                                break
                    elif "成交均价" in col_name:
                        for ci, t in enumerate(texts):
                            if re.match(r'^[\d.]+$', t) and '.' in t and not sign_stats.get("成交均价"):
                                # 避免和已签约面积重复取同一个值
                                if t != sign_stats.get("已签约面积"):
                                    sign_stats["成交均价"] = t
                                    break
                found = True
                break
        if found:
            break

    # 备用：直接用正则从全文提取
    if not sign_stats:
        text = soup.get_text()
        m = re.search(r"已签约套数\D{0,10}(\d+)", text)
        if m:
            sign_stats["已签约套数"] = m.group(1)
        m = re.search(r"已签约面积[^\d]{0,20}([\d.]+)", text)
        if m:
            sign_stats["已签约面积"] = m.group(1)
        m = re.search(r"成交均价[^\d]{0,20}([\d.]+)", text)
        if m:
            sign_stats["成交均价"] = m.group(1)

    return info, sign_stats


# ─────────────────────────────────────────────────────────────────────
# 解析：楼盘列表页 → 动态获取所有楼栋
# ─────────────────────────────────────────────────────────────────────

def parse_building_list(html: str) -> list[dict]:
    """
    动态解析楼盘列表页，返回楼栋列表。
    每条记录包含：楼号、批准销售套数、批准销售面积、销售状态、拟售均价、building_id
    """
    soup = BeautifulSoup(html, "lxml")
    buildings: list[dict] = []

    # 找包含楼栋详情链接的行
    for row in soup.select("tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        # 查找 "房屋标识" 链接（进入楼盘表的 <a> 标签）
        building_id = None
        for a in row.find_all("a", href=True):
            href = a["href"]
            m = re.search(r"buildingId[=\s]*([0-9]+)", href)
            if m:
                building_id = m.group(1)
                break
        # 也从 onclick 属性中提取
        if not building_id:
            for el in row.find_all(attrs={"onclick": True}):
                m = re.search(r"buildingId[=\s]*([0-9]+)", el["onclick"])
                if m:
                    building_id = m.group(1)
                    break

        if not building_id:
            continue

        texts = [c.get_text(strip=True) for c in cells]

        # 跳过表头行（第一列文本过长或包含"销售楼名"等表头关键词）
        first_cell = texts[0] if texts else ""
        if len(first_cell) > 30 or "销售楼名" in first_cell or "楼盘表" in first_cell:
            continue

        record = {"building_id": building_id}

        # 列顺序：销售楼号、批准销售套数、批准销售面积、销售状态、拟售均价
        col_names = ["销售楼号", "批准销售套数", "批准销售面积", "销售状态", "拟售均价"]
        for idx, col in enumerate(col_names):
            if idx < len(texts):
                record[col] = texts[idx]

        buildings.append(record)

    return buildings


# ─────────────────────────────────────────────────────────────────────
# 从楼盘表 HTML 解析房间号 + 状态（不进入详情页）
# ─────────────────────────────────────────────────────────────────────

def parse_houses_from_building_html(html: str, building_name: str) -> list[dict]:
    """
    直接从楼盘表页面 HTML 解析所有可见房间的房间号和颜色状态。
    返回列表，每条记录：楼栋、房间号、单元、房号、状态
    不进入任何详情页，建筑面积/户型等静态字段由调用方从历史数据补充。

    解析策略：
      - <a href*="houseId">  → 可点击房间（绿色可售）
      - <a href="#">         → 不可点击但有颜色的房间（已预订粉色/网上联机备案棕色等）
      两类均需记录，确保追踪所有非"不可售"状态的房间。
    """
    soup = BeautifulSoup(html, "lxml")
    color_re = re.compile(r"background[:\s]*(#[0-9a-fA-F]{3,6})", re.IGNORECASE)
    houses: list[dict] = []
    seen: set[str] = set()

    def _extract_color(a_tag) -> str:
        """从 <a> 标签向上最多4层找背景颜色，返回小写十六进制色码或空串。"""
        parent = a_tag.parent
        for _ in range(4):
            if parent is None:
                break
            style = parent.get("style", "")
            m = color_re.search(style)
            if m:
                return m.group(1).lower()
            parent = parent.parent
        return ""

    def _extract_unit(a_tag) -> str:
        """向上遍历祖先，找含'单元'字样的文本，返回如 '1单元'。"""
        cell = a_tag.find_parent("td") or a_tag.find_parent("div")
        if not cell:
            return ""
        for ancestor in cell.parents:
            ancestor_text = ancestor.get_text(separator=" ", strip=True)
            m_unit = re.search(r"(\d+单元)", ancestor_text)
            if m_unit:
                return m_unit.group(1)
            if ancestor.name in ("table", "body"):
                break
        return ""

    def _add_house(room_no: str, color: str, unit_hint: str = "") -> None:
        """将解析结果加入 houses 列表（去重）。"""
        if not room_no:
            return

        # 忽略"不可售"灰色房间（#cccccc）
        status = COLOR_STATUS_MAP.get(color, "")
        if not status or status == "不可售":
            return

        # 拼装标准房间号 "1单元-702"
        if unit_hint and not re.match(r"^\d+单元", room_no):
            full_room_no = f"{unit_hint}-{room_no}"
        else:
            full_room_no = room_no

        if full_room_no in seen:
            return
        seen.add(full_room_no)

        # 拆分单元和房号
        m_split = re.match(r"^(.+单元)[_\-–]?(.+)$", full_room_no)
        if m_split:
            unit_val = m_split.group(1)
            room_val = m_split.group(2)
        else:
            unit_val = unit_hint
            room_val = full_room_no

        houses.append({
            "楼栋":  building_name,
            "房间号": full_room_no,
            "单元":  unit_val,
            "房号":  room_val,
            "状态":  status,
        })

    # ── 解析所有 <a> 标签（含 houseId 的可点击 + href="#" 的状态房间） ──
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        room_no = a.get_text(strip=True)
        if not room_no:
            continue

        # 仅处理：进入详情页的（houseId）或占位锚点（#）
        if "houseId" not in href and href != "#":
            continue

        color = _extract_color(a)
        if not color:
            # houseId 链接若无颜色，默认可售
            if "houseId" in href:
                color = "#33cc00"
            else:
                continue

        unit = _extract_unit(a)
        _add_house(room_no, color, unit)

    return houses


# ─────────────────────────────────────────────────────────────────────
# 从历史 CSV 构建静态房间信息字典（建筑面积/套内面积/户型/拟售单价）
# ─────────────────────────────────────────────────────────────────────

def load_static_house_info(building_name: str) -> dict[str, dict]:
    """
    扫描 DATA_DIR 下所有历史日期，找最早包含该楼栋 house_status CSV 的记录，
    返回 {房间号: {建筑面积, 套内面积, 户型, 拟售单价}} 字典。
    """
    data_root = Path(DATA_DIR)
    date_pat = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    safe_name = re.sub(r'[\\/:*?"<>|]', "_", building_name)
    static_info: dict[str, dict] = {}

    # 按日期升序遍历，越早的数据越完整（包含已售出房间）
    date_dirs = sorted(
        [d for d in data_root.iterdir() if d.is_dir() and date_pat.match(d.name)],
        key=lambda d: d.name,
    )
    for d in date_dirs:
        csv_path = d / f"house_status_{safe_name}.csv"
        if not csv_path.exists():
            continue
        try:
            df = pd.read_csv(csv_path)
            for _, row in df.iterrows():
                room_no = str(row.get("房间号", "")).strip()
                if not room_no or room_no in static_info:
                    continue
                static_info[room_no] = {
                    "建筑面积": row.get("建筑面积", ""),
                    "套内面积": row.get("套内面积", ""),
                    "户型":    row.get("户型", ""),
                    "拟售单价": row.get("拟售单价", ""),
                }
        except Exception as e:
            logger.warning(f"[static_info] 读取失败 {csv_path}: {e}")

    logger.info(f"  [{building_name}] 历史静态信息: {len(static_info)} 条")
    return static_info


# ─────────────────────────────────────────────────────────────────────
# 核心：单栋楼 Playwright 采集（仅楼盘表页面，不进入详情页）
# ─────────────────────────────────────────────────────────────────────

async def _fetch_building_houses_async(
    building_id: str,
    building_name: str,
    shots_dir: Path,
    html_dir: Path,
) -> list[dict]:
    """
    用 Playwright 加载楼盘表页面，截图+保存 HTML，
    直接从页面 HTML 解析所有可点击房号及颜色状态。
    不进入详情页，静态字段（建筑面积/户型等）由调用方从历史数据补充。
    """
    from playwright.async_api import async_playwright

    url = URL_BUILDING_DETAIL.format(building_id=building_id)
    safe_name = re.sub(r'[\\/:*?"<>|]', "_", building_name)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=REQUEST_HEADERS["User-Agent"],
        )
        page = await context.new_page()

        # ── 1. 加载楼盘表页面 ──────────────────────────────────────────
        try:
            await page.goto(url, wait_until="networkidle", timeout=60_000)
        except Exception as e:
            logger.warning(f"[fetch_houses] 楼盘表加载超时，尝试继续: {e}")

        # ── 2. 截图 ────────────────────────────────────────────────────
        try:
            await page.screenshot(
                path=str(shots_dir / f"house_{safe_name}.png"),
                full_page=True,
            )
        except Exception as e:
            logger.warning(f"[fetch_houses] 截图失败 {building_name}: {e}")

        # ── 3. 保存原始 HTML ───────────────────────────────────────────
        building_html = await page.content()
        (html_dir / f"house_{safe_name}.html").write_text(building_html, encoding="utf-8")

        await page.close()
        await browser.close()

    # ── 4. 从 HTML 直接解析房间+状态（不进详情页） ────────────────────
    houses = parse_houses_from_building_html(building_html, building_name)
    logger.info(f"  [{building_name}] 从楼盘表解析到房间: {len(houses)} 个")
    return houses


def fetch_building_houses(
    building_id: str,
    building_name: str,
    shots_dir: Path,
    html_dir: Path,
) -> list[dict]:
    """同步包装：调用 Playwright 逐户采集单栋楼信息。"""
    return asyncio.run(
        _fetch_building_houses_async(building_id, building_name, shots_dir, html_dir)
    )


# ─────────────────────────────────────────────────────────────────────
# 主采集流程
# ─────────────────────────────────────────────────────────────────────

def run_scraper(date_str: str | None = None) -> str:
    """
    执行当日全量采集，返回本次采集的日期字符串。

    参数:
        date_str: 可选，指定采集日期（格式 YYYY-MM-DD），默认为今天
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    day_dir, shots_dir, html_dir = get_day_dirs(date_str)
    error_log_path = day_dir / "errors.log"

    log_handler = logging.FileHandler(error_log_path, encoding="utf-8")
    log_handler.setLevel(logging.WARNING)
    logger.addHandler(log_handler)

    logger.info(f"===== 开始采集 {date_str} =====")

    # ── 1. 主页面 ──────────────────────────────────────────────────────
    logger.info("采集主页面...")
    main_html = fetch_html(URL_MAIN)
    if main_html:
        (html_dir / "main_page.html").write_text(main_html, encoding="utf-8")
        take_screenshot(URL_MAIN, str(shots_dir / "main_page.png"))
        time.sleep(REQUEST_DELAY)

        project_info, sign_stats = parse_main_page(main_html)

        # 保存 project_info.csv
        pd.DataFrame([project_info]).to_csv(
            day_dir / "project_info.csv", index=False, encoding="utf-8-sig"
        )

        # 保存 sign_stats.csv
        sign_stats["采集日期"] = date_str
        pd.DataFrame([sign_stats]).to_csv(
            day_dir / "sign_stats.csv", index=False, encoding="utf-8-sig"
        )
        logger.info(f"主页面解析完成: 签约统计={sign_stats}")
    else:
        logger.error("主页面抓取失败")

    # ── 2. 楼盘列表页 ──────────────────────────────────────────────────
    logger.info("采集楼盘列表页...")
    list_html = fetch_html(URL_BUILDING_LIST)
    buildings: list[dict] = []

    if list_html:
        (html_dir / "building_list.html").write_text(list_html, encoding="utf-8")
        take_screenshot(URL_BUILDING_LIST, str(shots_dir / "building_list.png"))
        time.sleep(REQUEST_DELAY)

        buildings = parse_building_list(list_html)
        logger.info(f"动态获取楼栋数量: {len(buildings)}")

        if buildings:
            pd.DataFrame(buildings).to_csv(
                day_dir / "building_list.csv", index=False, encoding="utf-8-sig"
            )
        else:
            logger.warning("楼盘列表页解析结果为空，使用备用配置")
            buildings = [
                {"销售楼号": k, "building_id": v}
                for k, v in BUILDINGS_FALLBACK.items()
            ]
            pd.DataFrame(buildings).to_csv(
                day_dir / "building_list.csv", index=False, encoding="utf-8-sig"
            )
    else:
        logger.error("楼盘列表页抓取失败，使用备用配置")
        buildings = [
            {"销售楼号": k, "building_id": v}
            for k, v in BUILDINGS_FALLBACK.items()
        ]

    # ── 3. 逐栋楼盘表（Playwright 逐户点击详情页） ────────────────────
    for b in buildings:
        building_id   = str(b.get("building_id", ""))
        building_name = b.get("销售楼号", building_id)

        if not building_id:
            continue

        safe_name = re.sub(r'[\\/:*?"<>|]', "_", building_name)
        logger.info(f"采集楼盘表: {building_name} (id={building_id})")

        houses = fetch_building_houses(building_id, building_name, shots_dir, html_dir)

        if houses:
            # 用历史 CSV 补充静态字段（建筑面积/套内面积/户型/拟售单价）
            static_info = load_static_house_info(building_name)
            for h in houses:
                room_no = h.get("房间号", "")
                info = static_info.get(room_no, {})
                h.setdefault("建筑面积", info.get("建筑面积", ""))
                h.setdefault("套内面积", info.get("套内面积", ""))
                h.setdefault("户型",    info.get("户型", ""))
                h.setdefault("拟售单价", info.get("拟售单价", ""))

            # CSV 列顺序：楼栋、房间号、单元、房号、建筑面积、套内面积、户型、拟售单价、状态
            col_order = ["楼栋", "房间号", "单元", "房号", "建筑面积", "套内面积", "户型", "拟售单价", "状态"]
            df = pd.DataFrame(houses)
            for col in col_order:
                if col not in df.columns:
                    df[col] = ""
            df = df[col_order]
            df.to_csv(
                day_dir / f"house_status_{safe_name}.csv",
                index=False, encoding="utf-8-sig"
            )
            logger.info(f"  → 采集到 {len(houses)} 户（可售/可点击）")
        else:
            logger.warning(f"  → {building_name} 未采集到房屋数据（可能全部不可点击）")

        time.sleep(REQUEST_DELAY)

    logger.info(f"===== 采集完成 {date_str} =====")
    logger.removeHandler(log_handler)
    return date_str


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    result = run_scraper(date_arg)
    print(f"采集完成: {result}")
