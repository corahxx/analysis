# handlers/operator_handler.py - 各运营商数据 P2：多 Sheet 标准产品 + 兼容旧 2.1～2.11 维度

from io import BytesIO
from typing import List, Optional, Tuple

import pandas as pd

from .data_utils import (
    agg_pile_count,
    agg_station_count,
    filter_stations_with_more_than_2_piles,
    pile_count_col,
)

# 运营商概况产品：Sheet 顺序
OPERATOR_WORKBOOK_SHEET_TITLES: Tuple[str, ...] = (
    "公共充电设施",
    "共享私桩",
    "公用充电桩",
    "专用充电桩",
    "直流桩",
    "交流桩",
    "三相交流桩",
    "充电功率",
    "充电电量",
    "充电站",
    "换电站",
)

OPERATOR_PRODUCT_COLUMNS = ["运营商", "数值", "环比变化", "环比增速"]


def _operator_col(df: pd.DataFrame) -> Optional[str]:
    if "运营商名称" in df.columns:
        return "运营商名称"
    if "上报机构" in df.columns:
        return "上报机构"
    return None


def _empty_operator_product_table() -> pd.DataFrame:
    return pd.DataFrame(columns=OPERATOR_PRODUCT_COLUMNS)


def _filter_by_charging_type_convert(df: pd.DataFrame, expected: str) -> pd.DataFrame:
    if "充电桩类型_转换" not in df.columns:
        return df.iloc[0:0].copy()
    mask = df["充电桩类型_转换"].astype(str).str.strip() == expected
    return df.loc[mask]


def _operator_rowcount_table(sub: pd.DataFrame, op_col: str) -> pd.DataFrame:
    """按运营商统计 sub 内行数，降序。"""
    if sub.empty or op_col not in sub.columns:
        return _empty_operator_product_table()
    g = sub.groupby(sub[op_col].fillna("未知").astype(str), dropna=False).size()
    g = g.sort_values(ascending=False)
    if g.empty:
        return _empty_operator_product_table()
    return pd.DataFrame(
        {
            "运营商": g.index.tolist(),
            "数值": g.values.astype(int).tolist(),
            "环比变化": [""] * len(g),
            "环比增速": [""] * len(g),
        }
    )


def _operator_power_sum_table(df: pd.DataFrame, op_col: str) -> pd.DataFrame:
    """按运营商对 额定功率 求和，降序。"""
    if df.empty or "额定功率" not in df.columns:
        return _empty_operator_product_table()
    d = df.copy()
    d["_pwr_"] = pd.to_numeric(d["额定功率"], errors="coerce")
    d = d.dropna(subset=["_pwr_"])
    if d.empty:
        return _empty_operator_product_table()
    g = d.groupby(d[op_col].fillna("未知").astype(str), dropna=False)["_pwr_"].sum()
    g = g.sort_values(ascending=False)
    return pd.DataFrame(
        {
            "运营商": g.index.tolist(),
            "数值": g.values.tolist(),
            "环比变化": [""] * len(g),
            "环比增速": [""] * len(g),
        }
    )


def get_operator_workbook_tables(
    df: pd.DataFrame, for_pile: bool = True
) -> List[Tuple[str, pd.DataFrame]]:
    """[(Sheet 名, DataFrame), ...]，列固定为 运营商/数值/环比变化/环比增速。"""
    empty = _empty_operator_product_table()
    op_col = _operator_col(df)

    if not for_pile or op_col is None:
        return [(name, empty.copy()) for name in OPERATOR_WORKBOOK_SHEET_TITLES]

    public = _operator_rowcount_table(df, op_col)
    dc = _operator_rowcount_table(_filter_by_charging_type_convert(df, "直流"), op_col)
    ac = _operator_rowcount_table(_filter_by_charging_type_convert(df, "交流"), op_col)
    pwr = _operator_power_sum_table(df, op_col)

    return [
        ("公共充电设施", public),
        ("共享私桩", empty.copy()),
        ("公用充电桩", empty.copy()),
        ("专用充电桩", empty.copy()),
        ("直流桩", dc),
        ("交流桩", ac),
        ("三相交流桩", empty.copy()),
        ("充电功率", pwr),
        ("充电电量", empty.copy()),
        ("充电站", empty.copy()),
        ("换电站", empty.copy()),
    ]


def write_operator_workbook_bytes(df: pd.DataFrame, for_pile: bool = True) -> BytesIO:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, tbl in get_operator_workbook_tables(df, for_pile=for_pile):
            tbl.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    buf.seek(0)
    return buf


def list_operator_workbook_sheet_titles() -> List[str]:
    return list(OPERATOR_WORKBOOK_SHEET_TITLES)


# ---------- 以下为旧版 2.1～2.11 维度（兼容） ----------

OPERATOR_DIMENSIONS: List[Tuple[str, str, bool]] = [
    ("2.1", "2.1 公共充电设施", False),
    ("2.2", "2.2 公共桩及共享私桩", True),
    ("2.3", "2.3 公用充电桩", False),
    ("2.4", "2.4 专用充电桩", False),
    ("2.5", "2.5 直流桩", False),
    ("2.6", "2.6 交流桩", False),
    ("2.7", "2.7 三相交流桩", False),
    ("2.8", "2.8 充电功率", False),
    ("2.9", "2.9 充电电量", True),
    ("2.10", "2.10 充电站", False),
    ("2.11", "2.11 换电站", True),
]


def _filter_by_dimension(df: pd.DataFrame, dimension_key: str, for_pile: bool) -> pd.DataFrame:
    """按维度 key 筛选 df（仅桩表有筛选；站表 2.10 外不做类型筛选）。"""
    if not for_pile:
        if dimension_key == "2.10":
            return df
        return df
    if dimension_key == "2.1":
        return df
    if dimension_key == "2.3":
        if "充电桩属性" not in df.columns:
            return df
        return df[df["充电桩属性"].astype(str).isin(["01公共", "01公用", "01公共/公用"])]
    if dimension_key == "2.4":
        if "充电桩属性" not in df.columns:
            return df
        return df[df["充电桩属性"].astype(str).str.contains("专用", na=False)]
    if dimension_key == "2.5":
        if "充电桩类型" not in df.columns:
            return df
        return df[df["充电桩类型"].astype(str).str.contains("直流", na=False)]
    if dimension_key == "2.6":
        if "充电桩类型" not in df.columns:
            return df
        return df[df["充电桩类型"].astype(str).str.contains("交流", na=False)]
    if dimension_key == "2.7":
        if "充电桩类型" not in df.columns or "额定电压上限" not in df.columns:
            return df
        v = pd.to_numeric(df["额定电压上限"], errors="coerce")
        return df[df["充电桩类型"].astype(str).str.contains("交流", na=False) & (v >= 380)]
    if dimension_key == "2.8":
        if "额定功率" not in df.columns:
            return df
        return df[pd.to_numeric(df["额定功率"], errors="coerce") > 0]
    if dimension_key == "2.10":
        return filter_stations_with_more_than_2_piles(df, True)
    return df


def operator_table(df: pd.DataFrame, for_pile: bool = True) -> pd.DataFrame:
    """2.1 运营商数据明细：排名、运营商、设施总量、环比增量、环比增速（占位）。"""
    return operator_table_by_dimension(df, for_pile, "2.1")


def operator_table_by_dimension(
    df: pd.DataFrame, for_pile: bool, dimension_key: str
) -> pd.DataFrame:
    """按数据维度 2.1～2.11 返回运营商表。占位维度返回空表或说明行。"""
    op_col = _operator_col(df)
    if op_col is None:
        return pd.DataFrame(columns=["排名", "运营商", "设施总量", "环比增量", "环比增速"])

    is_placeholder = next((p for k, _, p in OPERATOR_DIMENSIONS if k == dimension_key), True)
    if is_placeholder:
        return pd.DataFrame(
            columns=["排名", "运营商", "设施总量", "环比增量", "环比增速"],
            data=[("—", "—", "—", "暂无数据或需其他数据源", "—")],
        )

    if dimension_key == "2.10":
        agg = agg_station_count(df, op_col, for_pile, apply_filter=True)
    else:
        sub = _filter_by_dimension(df, dimension_key, for_pile)
        if sub.empty:
            return pd.DataFrame(columns=["排名", "运营商", "设施总量", "环比增量", "环比增速"])
        agg = agg_pile_count(sub, op_col, for_pile)

    if agg.empty:
        return pd.DataFrame(columns=["排名", "运营商", "设施总量", "环比增量", "环比增速"])
    agg = agg.sort_values(ascending=False).reset_index()
    agg.columns = ["运营商", "设施总量"]
    agg.insert(0, "排名", range(1, len(agg) + 1))
    agg["环比增量"] = "—"
    agg["环比增速"] = "—"
    return agg


def get_operator_dimension_options() -> List[Tuple[str, str]]:
    """返回 [(key, 显示名称), ...]。"""
    return [(k, name) for k, name, _ in OPERATOR_DIMENSIONS]
