# handlers/citygroup_handler.py - 核心城市群（区域内省份对比）

from typing import Optional
import pandas as pd


def _province_col(df: pd.DataFrame) -> Optional[str]:
    if "省份_中文" in df.columns:
        return "省份_中文"
    if "省份" in df.columns:
        return "省份"
    return None


# 简单映射：省份 -> 城市群（可后续扩展）
CITY_GROUP_MAP = {
    "江苏省": "华东（长三角）", "浙江省": "华东（长三角）", "上海市": "华东（长三角）", "安徽省": "华东（长三角）",
}


def citygroup_provinces_table(df: pd.DataFrame, for_pile: bool = True, group_name: str = "华东（长三角）") -> pd.DataFrame:
    """区域内省份对比：省份、数量、占比、环比（占位）。"""
    prov_col = _province_col(df)
    id_col = "充电桩编号" if for_pile and "充电桩编号" in df.columns else "所属充电站编号" if "所属充电站编号" in df.columns else None
    if prov_col is None or id_col is None:
        return pd.DataFrame(columns=["省份", "数量", "占比", "环比"])
    reverse = {v: k for k, v in CITY_GROUP_MAP.items()}
    provinces_in_group = [p for p, g in CITY_GROUP_MAP.items() if g == group_name]
    sub = df[df[prov_col].astype(str).isin(provinces_in_group)]
    if sub.empty:
        return pd.DataFrame(columns=["省份", "数量", "占比", "环比"])
    if for_pile and id_col == "充电桩编号":
        agg = sub.groupby(sub[prov_col].fillna(""), dropna=False)[id_col].nunique()
    else:
        agg = sub.groupby(sub[prov_col].fillna(""), dropna=False).size()
    total = agg.sum()
    out = agg.sort_values(ascending=False).reset_index()
    out.columns = ["省份", "数量"]
    out["占比"] = (out["数量"] / total * 100).round(1).astype(str) + "%"
    out["环比"] = "—"
    return out
