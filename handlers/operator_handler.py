# handlers/operator_handler.py - 各运营商数据 P2：数据维度 2.1～2.11

from typing import Optional, List, Tuple
import pandas as pd

from .data_utils import (
    pile_count_col,
    agg_pile_count,
    agg_station_count,
    filter_stations_with_more_than_2_piles,
    station_id_col,
)

# 数据维度选项：(key, 显示名称, 是否占位)
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


def _operator_col(df: pd.DataFrame) -> Optional[str]:
    if "运营商名称" in df.columns:
        return "运营商名称"
    if "上报机构" in df.columns:
        return "上报机构"
    return None


def _filter_by_dimension(df: pd.DataFrame, dimension_key: str, for_pile: bool) -> pd.DataFrame:
    """按维度 key 筛选 df（仅桩表有筛选；站表 2.10 外不做类型筛选）。"""
    if not for_pile:
        if dimension_key == "2.10":
            return df  # 站表下 2.10 用 filter_stations_with_more_than_2_piles 在聚合里处理
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
        # 充电站数：按运营商统计充电站（站内桩数>2）
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
