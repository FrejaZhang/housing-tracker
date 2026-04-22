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

def analyze_house_status(today_dir: Path, prev_dir: Path | None) -> list[dict]:
    """
    对比所有楼栋的逐户状态，返回从"可售"变为其他状态的记录列表。
    """
    changes: list[dict] = []

    if prev_dir is None:
        logger.info("无前日数据，跳过逐户状态对比")
        return changes

    # 加载今日所有 house_status_*.csv
    today_frames: list[pd.DataFrame] = []
    for csv_file in sorted(today_dir.glob("house_status_*.csv")):
        df = pd.read_csv(csv_file)
        today_frames.append(df)

    if not today_frames:
        logger.warning("今日无 house_status_*.csv 文件")
        return changes

    today_all = pd.concat(today_frames, ignore_index=True)

    # 加载前日所有 house_status_*.csv
    prev_frames: list[pd.DataFrame] = []
    for csv_file in sorted(prev_dir.glob("house_status_*.csv")):
        df = pd.read_csv(csv_file)
        prev_frames.append(df)

    if not prev_frames:
        logger.info("前日无 house_status_*.csv 文件，跳过逐户对比")
        return changes

    prev_all = pd.concat(prev_frames, ignore_index=True)

    # 统一列名（容错）
    for df in (today_all, prev_all):
        df.columns = df.columns.str.strip()

    # 构造主键：房间号已包含单元信息（如"1单元-702"），用楼栋+房间号定位唯一房间
    # 兼容新旧格式：优先用"房间号"，不存在则退回"楼栋+单元+房号"
    if "房间号" in today_all.columns and "房间号" in prev_all.columns:
        key_cols = ["楼栋", "房间号"]
        # 补充缺失列
        for df in (today_all, prev_all):
            for col in key_cols:
                if col not in df.columns:
                    df[col] = ""
        id_cols_for_output = ["楼栋", "房间号", "单元", "房号"]
        for df in (today_all, prev_all):
            for col in ["单元", "房号"]:
                if col not in df.columns:
                    df[col] = ""
    else:
        key_cols = ["楼栋", "单元", "房号"]
        for col in key_cols:
            if col not in today_all.columns:
                today_all[col] = ""
            if col not in prev_all.columns:
                prev_all[col] = ""
        id_cols_for_output = key_cols

    # 筛选前日为"可售"的记录
    prev_saleable = prev_all[prev_all["状态"] == "可售"].copy()
    if prev_saleable.empty:
        logger.info("前日无'可售'记录，跳过逐户对比")
        return changes

    # 合并对比
    merge_right_cols = key_cols + ["状态"]
    for col in merge_right_cols:
        if col not in today_all.columns:
            today_all[col] = ""

    merged = prev_saleable.merge(
        today_all[merge_right_cols],
        on=key_cols,
        how="left",
        suffixes=("_前日", "_今日"),
    )

    # 找出今日状态不再是"可售"的记录（含 NaN 表示新日期无此房）
    changed = merged[
        (merged["状态_今日"].isna()) | (merged["状态_今日"] != "可售")
    ]

    for _, row in changed.iterrows():
        today_status = row.get("状态_今日", "")
        if pd.isna(today_status):
            today_status = "（数据缺失）"

        record = {
            "变动类型":     "房屋状态变动",
            "楼栋":         row.get("楼栋", ""),
            "前日状态":     "可售",
            "今日状态":     today_status,
            "前日签约面积": "",
            "今日签约面积": "",
            "前日成交均价": "",
            "今日成交均价": "",
            "新增签约面积": "",
            "新增签约总价": "",
            "备注":         f"可售 → {today_status}",
        }
        # 补充房间标识字段
        if "房间号" in row.index:
            record["房间号"] = row.get("房间号", "")
        record["单元"] = row.get("单元", "")
        record["房号"] = row.get("房号", "")
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
            "前日状态", "今日状态",
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
