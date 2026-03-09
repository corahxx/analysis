# handlers/national_handler.py - 全国数据（汇总卡片 + 各省排名表）

from typing import Optional, Tuple
import pandas as pd


def _province_col(df: pd.DataFrame) -> Optional[str]:
    if "省份_中文" in df.columns:
        return "省份_中文"
    if "省份" in df.columns:
        return "省份"
    return None


def national_summary_cards(df: pd.DataFrame, for_pile: bool = True) -> dict:
    """全国汇总指标：总量、环比增量、环比增速（后两项占位）。"""
    id_col = "充电桩编号" if for_pile and "充电桩编号" in df.columns else None
    if for_pile and id_col:
        total = df[id_col].nunique()
    elif not for_pile and "所属充电站编号" in df.columns:
        total = df["所属充电站编号"].nunique()
    else:
        total = len(df)
    return {
        "总量": total,
        "环比增量": "—",
        "环比增速": "—",
    }


def province_ranking_table(df: pd.DataFrame, for_pile: bool = True, top_n: int = 31) -> pd.DataFrame:
    """各省数量排名表：排名、省份、数量、全国占比、环比增速（占位）。"""
    prov_col = _province_col(df)
    id_col = "充电桩编号" if for_pile and "充电桩编号" in df.columns else None
    if not for_pile:
        id_col = "所属充电站编号" if "所属充电站编号" in df.columns else None
    if prov_col is None or id_col is None:
        return pd.DataFrame(columns=["排名", "省份", "数量", "全国占比", "环比增速"])
    total = df[id_col].nunique() if for_pile else len(df)
    agg = df.groupby(df[prov_col].fillna("未知"), dropna=False)[id_col].nunique() if for_pile else df.groupby(df[prov_col].fillna("未知"), dropna=False).size()
    agg = agg.sort_values(ascending=False).head(top_n)
    out = pd.DataFrame({
        "排名": range(1, len(agg) + 1),
        "省份": agg.index.astype(str).tolist(),
        "数量": agg.values.tolist(),
        "全国占比": [f"{(v / total * 100):.1f}%" for v in agg.values],
        "环比增速": ["—"] * len(agg),
    })
    return out
