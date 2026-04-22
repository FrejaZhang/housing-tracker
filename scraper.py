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
# 解析：房间详情页
# ─────────────────────────────────────────────────────────────────────

def parse_house_detail(html: str, building_name: str, status: str) -> dict | None:
    """
    解析单个房间详情页，返回房间信息字典。
    字段：楼栋、房间号、单元、房号、建筑面积、套内面积、户型、拟售单价、状态
    """
    soup = BeautifulSoup(html, "lxml")

    # 按 <td>字段名</td><td>值</td> 模式提取
    field_map = {
        "房 间 号":         "房间号",
        "房间号":           "房间号",
        "规划设计用途":     "规划设计用途",
        "户　　型":         "户型",
        "户型":             "户型",
        "建筑面积":         "建筑面积",
        "套内面积":         "套内面积",
        "按建筑面积拟售单价": "拟售单价",
    }

    data: dict = {"楼栋": building_name, "状态": status}

    tds = soup.find_all("td")
    for i, td in enumerate(tds):
        key_raw = td.get_text(strip=True)
        # 移除全角空格/不可见字符后匹配
        key_clean = key_raw.replace("\u3000", "").replace(" ", "").strip()
        for pattern, field in field_map.items():
            pattern_clean = pattern.replace("\u3000", "").replace(" ", "").strip()
            if key_clean == pattern_clean and i + 1 < len(tds):
                val = tds[i + 1].get_text(strip=True)
                # 面积/单价只取数字
                if field in ("建筑面积", "套内面积", "拟售单价"):
                    m = re.search(r"[\d.]+", val)
                    val = m.group() if m else val
                if field not in data or not data[field]:
                    data[field] = val
                break

    # 从房间号拆分单元和房号
    room_no = data.get("房间号", "")
    if room_no:
        # 格式通常为 "1单元-702" 或 "2单元-1201"
        m = re.match(r"^(.+单元)[_\-–]?(.+)$", room_no)
        if m:
            data["单元"] = m.group(1)
            data["房号"] = m.group(2)
        else:
            data["单元"] = ""
            data["房号"] = room_no
    else:
        data["单元"] = ""
        data["房号"] = ""

    return data


# ─────────────────────────────────────────────────────────────────────
# 核心：单栋楼 Playwright 逐户采集
# ─────────────────────────────────────────────────────────────────────

async def _fetch_building_houses_async(
    building_id: str,
    building_name: str,
    shots_dir: Path,
    html_dir: Path,
) -> list[dict]:
    """
    用 Playwright 加载楼盘表页面，找出所有可点击房号链接，
    逐一进入详情页抓取房间信息。
    返回该楼栋所有可售户的信息列表（不可点击的跳过）。
    """
    from playwright.async_api import async_playwright

    url = URL_BUILDING_DETAIL.format(building_id=building_id)
    color_re = re.compile(r"background[:\s]*(#[0-9a-fA-F]{3,6})", re.IGNORECASE)
    houses: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=REQUEST_HEADERS["User-Agent"],
        )

        # ── 1. 加载楼盘表页面 ──────────────────────────────────────────
        page = await context.new_page()
        try:
            await page.goto(url, wait_until="networkidle", timeout=60_000)
        except Exception as e:
            logger.warning(f"[fetch_houses] 楼盘表加载超时，尝试继续: {e}")

        # 截图
        safe_name = re.sub(r'[\\/:*?"<>|]', "_", building_name)
        try:
            await page.screenshot(
                path=str(shots_dir / f"house_{safe_name}.png"),
                full_page=True,
            )
        except Exception as e:
            logger.warning(f"[fetch_houses] 截图失败 {building_name}: {e}")

        # 保存原始 HTML
        building_html = await page.content()
        (html_dir / f"house_{safe_name}.html").write_text(building_html, encoding="utf-8")

        # ── 2. 收集所有可点击房号链接及其颜色（状态） ──────────────────
        house_links = await page.query_selector_all('a[href*="houseId"]')
        logger.info(f"  [{building_name}] 找到可点击房号: {len(house_links)} 个")

        # 预先收集 href 和颜色，避免点击后元素失效
        link_infos: list[tuple[str, str]] = []
        for a in house_links:
            href = await a.get_attribute("href") or ""
            # 从父 div 的 style 获取背景颜色
            parent_div = await a.evaluate_handle("el => el.closest('div[style*=\"background\"]')")
            color = ""
            try:
                style_val = await parent_div.get_property("style")
                style_str = await style_val.json_value()
                # style_str 是 CSSStyleDeclaration 对象，直接取 background
                bg = await parent_div.evaluate("el => el.style.background || el.style.backgroundColor")
                # 转换 rgb() → hex 或直接取 hex
                if bg:
                    if bg.startswith("#"):
                        color = bg.lower()
                    elif bg.startswith("rgb"):
                        nums = re.findall(r"\d+", bg)
                        if len(nums) >= 3:
                            color = "#{:02x}{:02x}{:02x}".format(*[int(x) for x in nums[:3]])
            except Exception:
                pass
            if not color:
                # fallback：从 href 上层 HTML 片段解析
                try:
                    outer = await a.evaluate("el => el.parentElement ? el.parentElement.outerHTML : ''")
                    m = color_re.search(outer)
                    if m:
                        color = m.group(1).lower()
                except Exception:
                    pass

            status = COLOR_STATUS_MAP.get(color, "可售" if color else "可售")
            link_infos.append((href, status))

        # ── 3. 逐一访问详情页 ──────────────────────────────────────────
        base_url = "http://bjjs.zjw.beijing.gov.cn"
        for idx, (href, status) in enumerate(link_infos):
            detail_url = base_url + href if href.startswith("/") else href
            detail_page = await context.new_page()
            try:
                await detail_page.goto(detail_url, wait_until="networkidle", timeout=30_000)
                detail_html = await detail_page.content()
                house = parse_house_detail(detail_html, building_name, status)
                if house and house.get("房间号"):
                    houses.append(house)
                    logger.debug(f"    [{idx+1}/{len(link_infos)}] {house.get('房间号')} "
                                 f"建面={house.get('建筑面积')} 套内={house.get('套内面积')}")
                else:
                    logger.warning(f"    [{idx+1}/{len(link_infos)}] 详情页解析失败: {detail_url}")
            except Exception as e:
                logger.warning(f"    [{idx+1}/{len(link_infos)}] 详情页加载失败: {detail_url}: {e}")
            finally:
                await detail_page.close()

            # 小延迟避免请求过快
            await asyncio.sleep(0.8)

        await page.close()
        await browser.close()

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
