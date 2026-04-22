"""
visualizer.py
Dash 多标签页可视化：
  Tab1 - 项目概览信息
  Tab2 - 楼盘列表表格
  Tab3 - 逐户状态（下拉选楼栋，颜色热力网格）
  Tab4 - 签约统计趋势（历史折线图）
  Tab5 - 当日变动记录

用法：
  python visualizer.py                  # 展示今日数据
  python visualizer.py --date 2026-04-21  # 展示指定日期
"""

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, dash_table, Input, Output, callback
import dash_bootstrap_components as dbc

from config import DATA_DIR, COLOR_STATUS_MAP

# ── 状态 → 颜色（用于热力网格） ──────────────────────────────────────
STATUS_COLOR = {v: k for k, v in COLOR_STATUS_MAP.items()}
STATUS_COLOR.update({
    "可售":            "#33cc00",
    "已签约":          "#ff0000",
    "已预订":          "#ffcc99",
    "网上联机备案":    "#d2691e",
    "已办理预售项目抵押": "#ffff00",
    "资格核验中":      "#00ffff",
    "不可售":          "#cccccc",
    "（数据缺失）":    "#888888",
})

# ── 通用表格样式 ──────────────────────────────────────────────────────
TABLE_STYLE = {
    "style_table":  {"overflowX": "auto"},
    "style_cell":   {"textAlign": "left", "padding": "6px 12px", "fontSize": 13},
    "style_header": {"backgroundColor": "#2c3e50", "color": "white", "fontWeight": "bold"},
    "style_data_conditional": [
        {"if": {"row_index": "odd"}, "backgroundColor": "#f9f9f9"}
    ],
}


# ─────────────────────────────────────────────────────────────────────
# 数据加载工具
# ─────────────────────────────────────────────────────────────────────

def load_csv(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            return pd.read_csv(path)
        except Exception:
            pass
    return pd.DataFrame()


def get_all_dates() -> list[str]:
    """返回 data/ 下所有有效日期目录，升序。"""
    data_root = Path(DATA_DIR)
    pat = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    return sorted([d.name for d in data_root.iterdir() if d.is_dir() and pat.match(d.name)])


# ─────────────────────────────────────────────────────────────────────
# Tab1：项目概览
# ─────────────────────────────────────────────────────────────────────

def build_tab1(date_str: str) -> html.Div:
    day_dir  = Path(DATA_DIR) / date_str
    info_df  = load_csv(day_dir / "project_info.csv")
    sign_df  = load_csv(day_dir / "sign_stats.csv")

    # 项目信息卡片
    info_rows = []
    if not info_df.empty:
        row = info_df.iloc[0]
        for col in info_df.columns:
            info_rows.append(
                html.Tr([html.Td(col, style={"fontWeight": "bold", "width": "30%"}),
                         html.Td(str(row[col]))])
            )

    # 签约统计卡片
    sign_rows = []
    if not sign_df.empty:
        row = sign_df.iloc[0]
        for col in sign_df.columns:
            sign_rows.append(
                html.Tr([html.Td(col, style={"fontWeight": "bold", "width": "40%"}),
                         html.Td(str(row[col]))])
            )

    return html.Div([
        html.H4(f"项目概览  |  {date_str}", style={"marginBottom": 20}),
        dbc.Row([
            dbc.Col([
                html.H5("项目基本信息"),
                html.Table(info_rows,
                           style={"width": "100%", "borderCollapse": "collapse",
                                  "border": "1px solid #ddd"}),
            ], md=7),
            dbc.Col([
                html.H5("签约统计"),
                html.Table(sign_rows,
                           style={"width": "100%", "borderCollapse": "collapse",
                                  "border": "1px solid #ddd"}),
            ], md=4),
        ]),
    ], style={"padding": 20})


# ─────────────────────────────────────────────────────────────────────
# Tab2：楼盘列表
# ─────────────────────────────────────────────────────────────────────

def build_tab2(date_str: str) -> html.Div:
    day_dir = Path(DATA_DIR) / date_str
    df = load_csv(day_dir / "building_list.csv")

    if df.empty:
        return html.Div(html.P("暂无楼盘列表数据"), style={"padding": 20})

    return html.Div([
        html.H4(f"楼盘列表  |  {date_str}  （共 {len(df)} 栋）",
                style={"marginBottom": 20}),
        dash_table.DataTable(
            data=df.to_dict("records"),
            columns=[{"name": c, "id": c} for c in df.columns],
            **TABLE_STYLE,
            page_size=30,
        ),
    ], style={"padding": 20})


# ─────────────────────────────────────────────────────────────────────
# Tab3：逐户状态（热力网格）
# ─────────────────────────────────────────────────────────────────────

def build_tab3_layout(date_str: str) -> html.Div:
    """返回含下拉框和图表区域的布局（回调在外部注册）。"""
    day_dir = Path(DATA_DIR) / date_str
    csv_files = sorted(day_dir.glob("house_status_*.csv"))
    building_options = []
    for f in csv_files:
        name = f.stem.replace("house_status_", "")
        building_options.append({"label": name, "value": str(f)})

    if not building_options:
        return html.Div(html.P("暂无楼盘表数据"), style={"padding": 20})

    return html.Div([
        html.H4(f"逐户状态  |  {date_str}", style={"marginBottom": 16}),
        dbc.Row([
            dbc.Col([
                html.Label("选择楼栋："),
                dcc.Dropdown(
                    id="building-dropdown",
                    options=building_options,
                    value=building_options[0]["value"],
                    clearable=False,
                    style={"width": 280},
                ),
            ], md=4),
        ], style={"marginBottom": 16}),
        html.Div(id="house-grid"),
        # 图例
        html.Div(
            [html.Span(
                f"  {status}  ",
                style={
                    "backgroundColor": color,
                    "padding": "3px 10px",
                    "marginRight": 8,
                    "borderRadius": 4,
                    "fontSize": 12,
                    "border": "1px solid #aaa",
                }
            ) for status, color in STATUS_COLOR.items() if status in COLOR_STATUS_MAP.values()],
            style={"marginTop": 16},
        ),
    ], style={"padding": 20})


def build_house_grid(csv_path: str) -> html.Div:
    """根据 CSV 路径生成房屋格子网格（适配新字段：房间号、单元、房号、建筑面积、套内面积）。"""
    df = load_csv(Path(csv_path))
    if df.empty:
        return html.P("该楼栋暂无数据")

    # 新字段结构：楼栋、房间号、单元、房号、建筑面积、套内面积、户型、拟售单价、状态
    # 按单元分组，每格显示房号+建筑面积
    has_new_fields = "房间号" in df.columns

    if has_new_fields:
        # 按单元分组展示
        units = sorted(df["单元"].dropna().unique().tolist()) if "单元" in df.columns else [""]
        units = [u for u in units if u] or [""]

        # 每单元内按房号排序
        unit_sections = []
        for unit in units:
            unit_df = df[df["单元"] == unit] if unit else df
            unit_df = unit_df.copy()

            # 尝试按房号数字排序
            def sort_key(r):
                m = re.search(r"(\d+)$", str(r.get("房号", "")))
                return int(m.group(1)) if m else 0

            unit_df["_sort"] = unit_df.apply(sort_key, axis=1)
            unit_df = unit_df.sort_values("_sort").drop(columns=["_sort"])

            cell_divs = []
            for _, h in unit_df.iterrows():
                status   = str(h.get("状态", ""))
                color    = STATUS_COLOR.get(status, "#dddddd")
                room_no  = str(h.get("房号", ""))
                room_full = str(h.get("房间号", ""))
                area_j   = str(h.get("建筑面积", ""))
                area_t   = str(h.get("套内面积", ""))
                huxing   = str(h.get("户型", ""))
                price    = str(h.get("拟售单价", ""))
                tooltip  = (
                    f"{room_full}\n状态：{status}"
                    + (f"\n户型：{huxing}" if huxing and huxing != "nan" else "")
                    + (f"\n建筑面积：{area_j}㎡" if area_j and area_j != "nan" else "")
                    + (f"\n套内面积：{area_t}㎡" if area_t and area_t != "nan" else "")
                    + (f"\n拟售单价：{price}元/㎡" if price and price != "nan" else "")
                )
                cell_divs.append(
                    html.Div(
                        [
                            html.Div(room_no, style={"fontSize": 11, "fontWeight": "bold"}),
                            html.Div(
                                f"{area_j}㎡" if area_j and area_j != "nan" else "",
                                style={"fontSize": 10, "color": "#333"},
                            ),
                        ],
                        title=tooltip,
                        style={
                            "display":         "inline-block",
                            "width":           68,
                            "height":          44,
                            "lineHeight":      "1.3",
                            "textAlign":       "center",
                            "padding":         "4px 2px",
                            "backgroundColor": color,
                            "border":          "1px solid #aaa",
                            "margin":          3,
                            "borderRadius":    4,
                            "overflow":        "hidden",
                            "cursor":          "default",
                            "verticalAlign":   "top",
                        }
                    )
                )

            unit_sections.append(html.Div([
                html.Div(
                    unit or "全部",
                    style={"fontWeight": "bold", "marginBottom": 6, "fontSize": 13,
                           "borderBottom": "1px solid #ddd", "paddingBottom": 4},
                ),
                html.Div(cell_divs, style={"display": "flex", "flexWrap": "wrap"}),
            ], style={"marginBottom": 20}))

        # 汇总统计
        total = len(df)
        status_counts = df["状态"].value_counts().to_dict()
        summary_items = [html.Span(f"共 {total} 户  ", style={"fontWeight": "bold"})]
        for s, cnt in status_counts.items():
            c = STATUS_COLOR.get(s, "#ddd")
            summary_items.append(html.Span(
                f"{s}: {cnt}  ",
                style={"backgroundColor": c, "padding": "2px 8px",
                       "borderRadius": 3, "marginRight": 6, "fontSize": 12},
            ))

        return html.Div([
            html.Div(summary_items, style={"marginBottom": 12}),
            *unit_sections,
        ])

    else:
        # 兼容旧字段格式（楼层+单元+房号）
        units = df["单元"].unique().tolist() if "单元" in df.columns else [""]
        floors = sorted(df["楼层"].unique().tolist(), reverse=True) if "楼层" in df.columns else [""]

        rows_html = []
        header_cells = [html.Th("楼层 \\ 单元", style={"width": 80, "textAlign": "center"})]
        for unit in units:
            header_cells.append(html.Th(unit or "", style={"textAlign": "center", "padding": "4px 8px"}))
        rows_html.append(html.Tr(header_cells))

        for floor in floors:
            floor_cells = [html.Td(floor or "", style={"fontWeight": "bold", "padding": "4px 8px"})]
            for unit in units:
                mask = pd.Series([True] * len(df))
                if "楼层" in df.columns:
                    mask &= df["楼层"] == floor
                if "单元" in df.columns:
                    mask &= df["单元"] == unit
                floor_houses = df[mask]

                cell_divs = []
                for _, h in floor_houses.iterrows():
                    status = h.get("状态", "")
                    color  = STATUS_COLOR.get(status, "#dddddd")
                    room   = str(h.get("房号", ""))
                    area   = str(h.get("面积", ""))
                    tooltip = f"{room}\n{status}" + (f"\n{area}㎡" if area and area != "nan" else "")
                    cell_divs.append(
                        html.Div(
                            room,
                            title=tooltip,
                            style={
                                "display":         "inline-block",
                                "width":           52,
                                "height":          28,
                                "lineHeight":      "28px",
                                "textAlign":       "center",
                                "fontSize":        11,
                                "backgroundColor": color,
                                "border":          "1px solid #aaa",
                                "margin":          2,
                                "borderRadius":    3,
                                "overflow":        "hidden",
                                "whiteSpace":      "nowrap",
                                "cursor":          "default",
                            }
                        )
                    )
                floor_cells.append(html.Td(cell_divs, style={"padding": "2px 4px"}))
            rows_html.append(html.Tr(floor_cells))

        return html.Table(rows_html, style={"borderCollapse": "collapse", "width": "100%"})


# ─────────────────────────────────────────────────────────────────────
# Tab4：签约趋势
# ─────────────────────────────────────────────────────────────────────

def build_tab4() -> html.Div:
    all_dates = get_all_dates()
    records   = []
    for d in all_dates:
        df = load_csv(Path(DATA_DIR) / d / "sign_stats.csv")
        if not df.empty:
            row = df.iloc[0].to_dict()
            row["日期"] = d
            records.append(row)

    if not records:
        return html.Div(html.P("暂无历史签约数据"), style={"padding": 20})

    trend_df = pd.DataFrame(records)

    def to_float_series(s: pd.Series) -> pd.Series:
        return pd.to_numeric(
            s.astype(str).str.replace(",", "").str.strip(),
            errors="coerce"
        )

    figs = []
    for col, title, yaxis in [
        ("已签约面积", "已签约面积趋势（㎡）", "㎡"),
        ("成交均价",   "成交均价趋势（元/㎡）", "元/㎡"),
        ("已签约套数", "已签约套数趋势",       "套"),
    ]:
        if col in trend_df.columns:
            y = to_float_series(trend_df[col])
            fig = go.Figure(go.Scatter(
                x=trend_df["日期"], y=y,
                mode="lines+markers",
                name=col,
                line={"width": 2},
                marker={"size": 6},
            ))
            fig.update_layout(
                title=title,
                xaxis_title="日期",
                yaxis_title=yaxis,
                height=320,
                margin={"t": 40, "b": 40, "l": 60, "r": 20},
            )
            figs.append(dcc.Graph(figure=fig))

    return html.Div([
        html.H4("签约统计历史趋势", style={"marginBottom": 20}),
        *figs,
        html.H5("历史数据明细", style={"marginTop": 20}),
        dash_table.DataTable(
            data=trend_df.to_dict("records"),
            columns=[{"name": c, "id": c} for c in trend_df.columns],
            **TABLE_STYLE,
            sort_action="native",
        ),
    ], style={"padding": 20})


# ─────────────────────────────────────────────────────────────────────
# Tab5：变动记录
# ─────────────────────────────────────────────────────────────────────

def build_tab5(date_str: str) -> html.Div:
    day_dir    = Path(DATA_DIR) / date_str
    changes_df = load_csv(day_dir / "changes.csv")

    if changes_df.empty:
        return html.Div([
            html.H4(f"当日变动记录  |  {date_str}", style={"marginBottom": 20}),
            html.P("今日暂无变动记录（或尚未运行分析）"),
        ], style={"padding": 20})

    sign_changes  = changes_df[changes_df["变动类型"] == "签约统计变动"]
    house_changes = changes_df[changes_df["变动类型"] == "房屋状态变动"]

    content = [
        html.H4(f"当日变动记录  |  {date_str}  （共 {len(changes_df)} 条）",
                style={"marginBottom": 20}),
    ]

    if not sign_changes.empty:
        content.append(html.H5("签约统计变动"))
        content.append(dash_table.DataTable(
            data=sign_changes.to_dict("records"),
            columns=[{"name": c, "id": c} for c in sign_changes.columns],
            **TABLE_STYLE,
            style_table={"overflowX": "auto", "marginBottom": 24},
        ))

    if not house_changes.empty:
        content.append(html.H5(f"房屋状态变动（可售→其他）  共 {len(house_changes)} 户"))
        content.append(dash_table.DataTable(
            data=house_changes.to_dict("records"),
            columns=[{"name": c, "id": c} for c in house_changes.columns],
            **TABLE_STYLE,
            page_size=50,
            filter_action="native",
            sort_action="native",
        ))

    return html.Div(content, style={"padding": 20})


# ─────────────────────────────────────────────────────────────────────
# 应用入口
# ─────────────────────────────────────────────────────────────────────

def create_app(date_str: str) -> Dash:
    app = Dash(
        __name__,
        external_stylesheets=[dbc.themes.FLATLY],
        suppress_callback_exceptions=True,
    )

    all_dates     = get_all_dates()
    date_options  = [{"label": d, "value": d} for d in reversed(all_dates)]
    if not date_options:
        date_options = [{"label": date_str, "value": date_str}]

    app.layout = dbc.Container([
        dbc.NavbarSimple(
            brand="北京住建委 · 水畔芳邻嘉园 · 楼盘数据追踪",
            brand_href="#",
            color="dark",
            dark=True,
            style={"marginBottom": 20},
        ),
        dbc.Row([
            dbc.Col([
                html.Label("选择日期："),
                dcc.Dropdown(
                    id="date-selector",
                    options=date_options,
                    value=date_str,
                    clearable=False,
                    style={"width": 200},
                ),
            ], md=3),
        ], style={"marginBottom": 12}),

        dbc.Tabs([
            dbc.Tab(label="项目概览",   tab_id="tab1"),
            dbc.Tab(label="楼盘列表",   tab_id="tab2"),
            dbc.Tab(label="逐户状态",   tab_id="tab3"),
            dbc.Tab(label="签约趋势",   tab_id="tab4"),
            dbc.Tab(label="当日变动",   tab_id="tab5"),
        ], id="tabs", active_tab="tab1"),

        html.Div(id="tab-content", style={"marginTop": 12}),
    ], fluid=True)

    @app.callback(
        Output("tab-content", "children"),
        Input("tabs", "active_tab"),
        Input("date-selector", "value"),
    )
    def render_tab(active_tab: str, selected_date: str):
        if not selected_date:
            selected_date = date_str
        if active_tab == "tab1":
            return build_tab1(selected_date)
        elif active_tab == "tab2":
            return build_tab2(selected_date)
        elif active_tab == "tab3":
            return build_tab3_layout(selected_date)
        elif active_tab == "tab4":
            return build_tab4()
        elif active_tab == "tab5":
            return build_tab5(selected_date)
        return html.P("请选择标签页")

    @app.callback(
        Output("house-grid", "children"),
        Input("building-dropdown", "value"),
    )
    def update_house_grid(csv_path: str):
        if not csv_path:
            return html.P("请选择楼栋")
        return build_house_grid(csv_path)

    return app


def main():
    parser = argparse.ArgumentParser(description="楼盘数据可视化")
    parser.add_argument("--date", default=None, help="指定日期 YYYY-MM-DD，默认今日")
    parser.add_argument("--port", type=int, default=8050, help="监听端口，默认 8050")
    args = parser.parse_args()

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    print(f"启动可视化服务，日期={date_str}，端口={args.port}")
    print(f"浏览器访问: http://127.0.0.1:{args.port}/")

    app = create_app(date_str)
    app.run(debug=False, port=args.port)


if __name__ == "__main__":
    main()
