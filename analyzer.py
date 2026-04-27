"""
analyzer.py
变动分析模块：
  1. 读取今日与最近前一日的 sign_stats.csv，分析签约面积/均价变动
     新增总价 = 今日签约面积 × 今日成交均价 - 前日签约面积 × 前日成交均价
  2. 读取今日与前日所有 house_status_*.csv，找出从"可售"变为其他状态的具体户
  3. 将所有变动记录写入 changes.csv
"""

import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import DATA_DIR

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# 工具：查找前一个已有日期目录
# ─────────────────────────────────────────────────────────────────────

def find_prev_date_dir(today_str: str) -> Path | None:
    """
    扫描 DATA_DIR 下所有 YYYY-MM-DD 目录，
    返回严格小于 today_str 的最大日期目录；不存在则返回 None。
    """
    data_root = Path(DATA_DIR)
    date_pat   = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    candidates = sorted(
        [d for d in data_root.iterdir() if d.is_dir() and date_pat.match(d.name)],
        key=lambda d: d.name,
    )
    for d in reversed(candidates):
        if d.name < today_str:
            return d
    return None


# ─────────────────────────────────────────────────────────────────────
# 1. 签约统计变动分析
# ─────────────────────────────────────────────────────────────────────

def analyze_sign_stats(today_dir: Path, prev_dir: Path | None) -> list[dict]:
    """
    对比签约统计，返回变动记录列表。
    计算逻辑：
      新增面积  = 今日签约面积 - 前日签约面积
      新增总价  = 今日签约面积 × 今日成交均价 - 前日签约面积 × 前日成交均价
    """
    changes: list[dict] = []

    today_csv = today_dir / "sign_stats.csv"
    if not today_csv.exists():
        logger.warning("今日 sign_stats.csv 不存在，跳过签约分析")
        return changes

    today_df = pd.read_csv(today_csv)
    if today_df.empty:
        return changes

    today_row = today_df.iloc[0]

    def to_float(val) -> float:
        try:
            return float(str(val).replace(",", "").strip())
        except (ValueError, TypeError):
            return 0.0

    today_area  = to_float(today_row.get("已签约面积", 0))
    today_price = to_float(today_row.get("成交均价", 0))

    prev_area  = 0.0
    prev_price = 0.0

    if prev_dir:
        prev_csv = prev_dir / "sign_stats.csv"
        if prev_csv.exists():
            prev_df = pd.read_csv(prev_csv)
            if not prev_df.empty:
                prev_row   = prev_df.iloc[0]
                prev_area  = to_float(prev_row.get("已签约面积", 0))
                prev_price = to_float(prev_row.get("成交均价", 0))

    area_changed  = abs(today_area  - prev_area)  > 0.01
    price_changed = abs(today_price - prev_price) > 0.01

    if area_changed or price_changed:
        new_area  = today_area - prev_area
        new_total = today_area * today_price - prev_area * prev_price

        record = {
            "变动类型":     "签约统计变动",
            "楼栋":         "",
            "单元":         "",
            "房号":         "",
            "前日状态":     "",
            "今日状态":     "",
            "前日签约面积": prev_area,
            "今日签约面积": today_area,
            "前日成交均价": prev_price,
            "今日成交均价": today_price,
            "新增签约面积": round(new_area, 2),
            "新增签约总价": round(new_total, 2),
            "备注": (
                f"面积变动: {prev_area:.2f}→{today_area:.2f} ㎡；"
                f"均价变动: {prev_price:.0f}→{today_price:.0f} 元/㎡"
            ),
        }
        changes.append(record)
        logger.info(f"签约统计变动: 新增面积={new_area:.2f}㎡ 新增总价={new_total:.0f}元")

    return changes


# ─────────────────────────────────────────────────────────────────────
# 2. 逐户状态变动分析
# ─────────────────────────────────────────────────────────────────────

def _build_static_info_from_history(data_root: Path, today_str: str) -> dict[tuple[str, str], dict]:
    """
    扫描所有历史日期（早于 today_str），合并所有 house_status_*.csv，
    返回 {(楼栋, 房间号): {建筑面积, 套内面积, 户型, 拟售单价}} 字典。
    用于在变动记录中补充静态字段。
    """
    date_pat = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    static: dict[tuple[str, str], dict] = {}

    date_dirs = sorted(
        [d for d in data_root.iterdir() if d.is_dir() and date_pat.match(d.name) and d.name < today_str],
        key=lambda d: d.name,
    )
    for d in date_dirs:
        for csv_file in d.glob("house_status_*.csv"):
            try:
                df = pd.read_csv(csv_file)
                for _, row in df.iterrows():
                    building = str(row.get("楼栋", "")).strip()
                    room_no  = str(row.get("房间号", "")).strip()
                    if not building or not room_no:
                        continue
                    key = (building, room_no)
                    if key not in static:
                        static[key] = {
                            "建筑面积": row.get("建筑面积", ""),
                            "套内面积": row.get("套内面积", ""),
                            "户型":    row.get("户型", ""),
                            "拟售单价": row.get("拟售单价", ""),
                        }
            except Exception as e:
                logger.warning(f"[static_info] 读取失败 {csv_file}: {e}")

    return static


def _load_already_reported(data_root: Path, today_str: str) -> set[tuple[str, str]]:
    """
    读取所有历史 changes.csv（早于 today_str），
    返回已经上报过变动的 (楼栋, 房间号) 集合，用于去重。
    """
    reported: set[tuple[str, str]] = set()
    date_pat = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    for d in data_root.iterdir():
        if not d.is_dir() or not date_pat.match(d.name) or d.name >= today_str:
            continue
        changes_csv = d / "changes.csv"
        if not changes_csv.exists():
            continue
        try:
            df = pd.read_csv(changes_csv)
            if df.empty:
                continue
            if "变动类型" not in df.columns:
                continue
            house_df = df[df["变动类型"] == "房屋状态变动"]
            for _, r in house_df.iterrows():
                building = str(r.get("楼栋", "")).strip()
                room_no  = str(r.get("房间号", "")).strip()
                if building and room_no:
                    reported.add((building, room_no))
        except Exception:
            pass
    return reported


def analyze_house_status(today_dir: Path, prev_dir: Path | None) -> list[dict]:
    """
    对比所有楼栋的逐户状态，返回从"可售"变为其他状态的记录列表。
    - 今日消失（曾可售，今日不再出现）→ 视为"已签约"
    - 今日仍在但状态已变 → 记录实际新状态
    - 去重：已在历史 changes.csv 上报过的房间不重复上报
    """
    changes: list[dict] = []

    if prev_dir is None:
        logger.info("无前日数据，跳过逐户状态对比")
        return changes

    # 加载今日所有楼栋 CSV → {楼栋: DataFrame}
    today_by_building: dict[str, pd.DataFrame] = {}
    for csv_file in sorted(today_dir.glob("house_status_*.csv")):
        try:
            df = pd.read_csv(csv_file)
            if df.empty:
                continue
            df.columns = df.columns.str.strip()
            building = str(df["楼栋"].iloc[0]).strip()
            today_by_building[building] = df
        except Exception as e:
            logger.warning(f"读取今日 {csv_file} 失败: {e}")

    if not today_by_building:
        logger.warning("今日无 house_status_*.csv 文件")
        return changes

    data_root = today_dir.parent
    date_pat = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    all_date_dirs = sorted(
        [d for d in data_root.iterdir() if d.is_dir() and date_pat.match(d.name) and d.name < today_dir.name],
        key=lambda d: d.name,
        reverse=True,
    )

    static_info = _build_static_info_from_history(data_root, today_dir.name)
    already_reported = _load_already_reported(data_root, today_dir.name)

    for building, today_df in today_by_building.items():
        safe_name = re.sub(r'[\\/:*?"<>|]', "_", building)
        today_df.columns = today_df.columns.str.strip()
        for col in ["楼栋", "房间号", "单元", "房号", "状态"]:
            if col not in today_df.columns:
                today_df[col] = ""

        use_room_no = "房间号" in today_df.columns

        if use_room_no:
            today_rooms: set[str] = set(today_df["房间号"].astype(str).str.strip())
        else:
            today_rooms = set(
                str(r["单元"]).strip() + "-" + str(r["房号"]).strip()
                for _, r in today_df.iterrows()
            )

        # 累积所有历史"曾为可售"的房间
        ever_saleable: dict[str, dict] = {}
        for hist_dir in reversed(all_date_dirs):
            csv_path = hist_dir / f"house_status_{safe_name}.csv"
            if not csv_path.exists():
                continue
            try:
                hdf = pd.read_csv(csv_path)
                if hdf.empty:
                    continue
                hdf.columns = hdf.columns.str.strip()
                for _, r in hdf[hdf["状态"] == "可售"].iterrows():
                    if use_room_no and "房间号" in hdf.columns:
                        key = str(r.get("房间号", "")).strip()
                    else:
                        key = str(r.get("单元", "")).strip() + "-" + str(r.get("房号", "")).strip()
                    if key:
                        ever_saleable[key] = r.to_dict()
            except Exception:
                pass

        if not ever_saleable:
            continue

        prev_saleable_rooms = set(ever_saleable.keys())
        newly_gone = prev_saleable_rooms - today_rooms

        today_status_map: dict[str, str] = {}
        if use_room_no:
            for _, r in today_df.iterrows():
                key = str(r.get("房间号", "")).strip()
                today_status_map[key] = str(r.get("状态", "")).strip()
        else:
            for _, r in today_df.iterrows():
                key = str(r.get("单元", "")).strip() + "-" + str(r.get("房号", "")).strip()
                today_status_map[key] = str(r.get("状态", "")).strip()

        status_changed = {
            k for k in (prev_saleable_rooms & today_rooms)
            if today_status_map.get(k, "可售") != "可售"
        }

        all_changed = newly_gone | status_changed

        # 去重：历史 changes.csv 已上报过的跳过
        all_changed = {
            k for k in all_changed
            if (building, ever_saleable.get(k, {}).get("房间号", k) if use_room_no else k) not in already_reported
        }

        for room_key in sorted(all_changed):
            if room_key in newly_gone:
                today_status = "已签约"
            else:
                today_status = today_status_map.get(room_key, "已签约")

            row_info = ever_saleable.get(room_key, {})
            room_no  = row_info.get("房间号", room_key) if use_room_no else room_key
            unit     = row_info.get("单元", "")
            room_id  = row_info.get("房号", "")
            sinfo    = static_info.get((building, str(room_no)), {})

            record = {
                "变动类型":     "房屋状态变动",
                "楼栋":         building,
                "前日状态":     "可售",
                "今日状态":     today_status,
                "建筑面积":     sinfo.get("建筑面积", row_info.get("建筑面积", "")),
                "套内面积":     sinfo.get("套内面积", row_info.get("套内面积", "")),
                "户型":         sinfo.get("户型", row_info.get("户型", "")),
                "拟售单价":     sinfo.get("拟售单价", row_info.get("拟售单价", "")),
                "前日签约面积": "",
                "今日签约面积": "",
                "前日成交均价": "",
                "今日成交均价": "",
                "新增签约面积": "",
                "新增签约总价": "",
                "备注":         f"可售 → {today_status}",
                "房间号":       room_no,
                "单元":         unit,
                "房号":         room_id,
            }
            changes.append(record)

    logger.info(f"逐户状态变动: {len(changes)} 户从'可售'转变")
    return changes
    changes: list[dict] = []

    if prev_dir is None:
        logger.info("无前日数据，跳过逐户状态对比")
        return changes

    # 加载今日所有楼栋 CSV → {楼栋: DataFrame}
    today_by_building: dict[str, pd.DataFrame] = {}
    for csv_file in sorted(today_dir.glob("house_status_*.csv")):
        try:
            df = pd.read_csv(csv_file)
            if df.empty:
                continue
            df.columns = df.columns.str.strip()
            building = str(df["楼栋"].iloc[0]).strip()
            today_by_building[building] = df
        except Exception as e:
            logger.warning(f"读取今日 {csv_file} 失败: {e}")

    if not today_by_building:
        logger.warning("今日无 house_status_*.csv 文件")
        return changes

    # 所有历史日期目录（降序，最近的在前）
    data_root = today_dir.parent
    date_pat = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    all_date_dirs = sorted(
        [d for d in data_root.iterdir() if d.is_dir() and date_pat.match(d.name) and d.name < today_dir.name],
        key=lambda d: d.name,
        reverse=True,
    )

    # 历史静态信息（用于补充建筑面积/户型）
    static_info = _build_static_info_from_history(data_root, today_dir.name)

    # 对每栋楼处理
    for building, today_df in today_by_building.items():
        safe_name = re.sub(r'[\\/:*?"<>|]', "_", building)

        # 确保今日 df 有必要列
        today_df.columns = today_df.columns.str.strip()
        for col in ["楼栋", "房间号", "单元", "房号", "状态"]:
            if col not in today_df.columns:
                today_df[col] = ""

        use_room_no = "房间号" in today_df.columns

        # 今日该楼栋所有房间号集合
        if use_room_no:
            today_rooms: set[str] = set(today_df["房间号"].astype(str).str.strip())
        else:
            today_rooms = set(
                (str(r["单元"]).strip() + "-" + str(r["房号"]).strip())
                for _, r in today_df.iterrows()
            )

        # 找最近一个有该楼栋记录的历史日期（作为对比起点）
        prev_df_for_building: pd.DataFrame | None = None
        prev_date_name = ""
        for hist_dir in all_date_dirs:
            csv_path = hist_dir / f"house_status_{safe_name}.csv"
            if csv_path.exists():
                try:
                    tmp = pd.read_csv(csv_path)
                    if not tmp.empty:
                        tmp.columns = tmp.columns.str.strip()
                        prev_df_for_building = tmp
                        prev_date_name = hist_dir.name
                        break
                except Exception:
                    pass

        if prev_df_for_building is None:
            logger.debug(f"  [{building}] 无历史数据，跳过")
            continue

        # 累积所有历史中"曾为可售"的房间（从最早到最近，确保不遗漏任何曾可售的房间）
        ever_saleable: dict[str, dict] = {}   # room_key → row info
        for hist_dir in reversed(all_date_dirs):   # 升序：越早越先，越晚越覆盖
            csv_path = hist_dir / f"house_status_{safe_name}.csv"
            if not csv_path.exists():
                continue
            try:
                hdf = pd.read_csv(csv_path)
                if hdf.empty:
                    continue
                hdf.columns = hdf.columns.str.strip()
                for _, r in hdf[hdf["状态"] == "可售"].iterrows():
                    if use_room_no and "房间号" in hdf.columns:
                        key = str(r.get("房间号", "")).strip()
                    else:
                        key = str(r.get("单元", "")).strip() + "-" + str(r.get("房号", "")).strip()
                    if key:
                        ever_saleable[key] = r.to_dict()
            except Exception:
                pass

        if not ever_saleable:
            continue

        # 找出：曾可售 但 今日不在 或 今日状态已变
        prev_saleable_rooms = set(ever_saleable.keys())
        newly_gone = prev_saleable_rooms - today_rooms   # 今日消失

        # 今日仍在但状态变了（从可售变为其他）
        today_status_map: dict[str, str] = {}
        if use_room_no:
            for _, r in today_df.iterrows():
                key = str(r.get("房间号", "")).strip()
                today_status_map[key] = str(r.get("状态", "")).strip()
        else:
            for _, r in today_df.iterrows():
                key = str(r.get("单元", "")).strip() + "-" + str(r.get("房号", "")).strip()
                today_status_map[key] = str(r.get("状态", "")).strip()

        status_changed = {
            k for k in (prev_saleable_rooms & today_rooms)
            if today_status_map.get(k, "可售") != "可售"
        }

        all_changed = newly_gone | status_changed

        # 去重：已在 prev_date 之前就已变动的房间不重复上报
        # 只上报"在 prev_date_name 那天仍为可售，今日变动"的房间
        if prev_df_for_building is not None:
            prev_df_for_building.columns = prev_df_for_building.columns.str.strip()
            if use_room_no and "房间号" in prev_df_for_building.columns:
                prev_saleable_in_prev_date = set(
                    prev_df_for_building[prev_df_for_building["状态"] == "可售"]["房间号"].astype(str).str.strip()
                )
            else:
                prev_saleable_in_prev_date = set(
                    str(r["单元"]).strip() + "-" + str(r["房号"]).strip()
                    for _, r in prev_df_for_building[prev_df_for_building["状态"] == "可售"].iterrows()
                )
            # 只上报在最近前一次记录中仍然可售的房间（才是本次新变动）
            all_changed = all_changed & prev_saleable_in_prev_date

        for room_key in sorted(all_changed):
            if room_key in newly_gone:
                today_status = "已签约"
            else:
                today_status = today_status_map.get(room_key, "已签约")

            row_info = ever_saleable.get(room_key, {})
            room_no  = row_info.get("房间号", room_key) if use_room_no else room_key
            unit     = row_info.get("单元", "")
            room_id  = row_info.get("房号", "")
            sinfo    = static_info.get((building, str(room_no)), {})

            record = {
                "变动类型":     "房屋状态变动",
                "楼栋":         building,
                "前日状态":     "可售",
                "今日状态":     today_status,
                "建筑面积":     sinfo.get("建筑面积", row_info.get("建筑面积", "")),
                "套内面积":     sinfo.get("套内面积", row_info.get("套内面积", "")),
                "户型":         sinfo.get("户型", row_info.get("户型", "")),
                "拟售单价":     sinfo.get("拟售单价", row_info.get("拟售单价", "")),
                "前日签约面积": "",
                "今日签约面积": "",
                "前日成交均价": "",
                "今日成交均价": "",
                "新增签约面积": "",
                "新增签约总价": "",
                "备注":         f"可售 → {today_status}",
                "房间号":       room_no,
                "单元":         unit,
                "房号":         room_id,
            }
            changes.append(record)

    logger.info(f"逐户状态变动: {len(changes)} 户从'可售'转变")
    return changes


# ─────────────────────────────────────────────────────────────────────
# 主分析流程
# ─────────────────────────────────────────────────────────────────────

def run_analyzer(date_str: str | None = None) -> str:
    """
    执行当日变动分析，将结果写入 changes.csv，返回日期字符串。
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    today_dir = Path(DATA_DIR) / date_str
    if not today_dir.exists():
        logger.error(f"今日数据目录不存在: {today_dir}")
        return date_str

    prev_dir = find_prev_date_dir(date_str)
    if prev_dir:
        logger.info(f"对比日期: {date_str} vs {prev_dir.name}")
    else:
        logger.info("未找到前日数据，仅记录今日数据快照")

    all_changes: list[dict] = []
    all_changes.extend(analyze_sign_stats(today_dir, prev_dir))
    all_changes.extend(analyze_house_status(today_dir, prev_dir))

    changes_path = today_dir / "changes.csv"
    if all_changes:
        df = pd.DataFrame(all_changes)
        df.insert(0, "采集日期", date_str)
        df.to_csv(changes_path, index=False, encoding="utf-8-sig")
        logger.info(f"变动记录已写入: {changes_path}，共 {len(all_changes)} 条")
    else:
        # 写空文件以标记"已分析但无变动"
        pd.DataFrame(columns=[
            "采集日期", "变动类型", "楼栋", "房间号", "单元", "房号",
            "前日状态", "今日状态", "建筑面积", "套内面积", "户型", "拟售单价",
            "前日签约面积", "今日签约面积", "前日成交均价", "今日成交均价",
            "新增签约面积", "新增签约总价", "备注",
        ]).to_csv(changes_path, index=False, encoding="utf-8-sig")
        logger.info(f"今日无变动，已写入空变动文件: {changes_path}")

    return date_str


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    result = run_analyzer(date_arg)
    print(f"分析完成: {result}")
