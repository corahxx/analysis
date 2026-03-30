# handlers/citygroup_handler.py - 核心城市群 P5：区域内省份对比（口径：序号/充电站内部编号+站内>2）

from typing import Optional
import pandas as pd

from .data_utils import (
    agg_pile_count,
    agg_station_count,
    format_share_ratios_4dp_max_remainder_floats,
)


def _province_col(df: pd.DataFrame) -> Optional[str]:
    if "省份_中文" in df.columns:
        return "省份_中文"
    if "省份" in df.columns:
        return "省份"
    return None


# 简单映射：省份 -> 城市群（可后续扩展）
CITY_GROUP_MAP = {
    "江苏省": "华东（长三角）",
    "浙江省": "华东（长三角）",
    "上海市": "华东（长三角）",
    "安徽省": "华东（长三角）",
}


def citygroup_provinces_table(df: pd.DataFrame, for_pile: bool = True, group_name: str = "华东（长三角）") -> pd.DataFrame:
    """区域内省份对比：省份、数量、占比、环比（占位）。数量用序号(桩)或充电站内部编号站内>2(站)。"""
    prov_col = _province_col(df)
    if prov_col is None:
        return pd.DataFrame(columns=["省份", "数量", "占比", "环比"])
    provinces_in_group = [p for p, g in CITY_GROUP_MAP.items() if g == group_name]
    if not provinces_in_group:
        return pd.DataFrame(columns=["省份", "数量", "占比", "环比"])
    sub = df[df[prov_col].astype(str).isin(provinces_in_group)]
    if sub.empty:
        return pd.DataFrame(columns=["省份", "数量", "占比", "环比"])
    if for_pile:
        agg = agg_pile_count(sub, prov_col, True)
    else:
        agg = agg_station_count(sub, prov_col, False, apply_filter=True)
    if agg.empty:
        return pd.DataFrame(columns=["省份", "数量", "占比", "环比"])
    total = agg.sum()
    out = agg.sort_values(ascending=False).reset_index()
    out.columns = ["省份", "数量"]
    out["占比"] = format_share_ratios_4dp_max_remainder_floats(
        [float(x) for x in out["数量"].tolist()]
    )
    out["环比"] = "—"
    return out
