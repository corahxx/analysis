# handlers/national_handler.py - 全国数据 P3：3.1 公共充电桩总数、3.2 充电站总数(>2)、各省排名

from typing import Optional, Tuple
import pandas as pd

from .data_utils import (
    count_piles,
    count_stations,
    agg_pile_count,
    agg_station_count,
)


def _province_col(df: pd.DataFrame) -> Optional[str]:
    if "省份_中文" in df.columns:
        return "省份_中文"
    if "省份" in df.columns:
        return "省份"
    return None


def national_summary_cards(df: pd.DataFrame, for_pile: bool = True) -> dict:
    """全国汇总指标：总量（桩表用序号计数，站表用充电站内部编号去重且站内桩数>2）、环比增量、环比增速（占位）。"""
    if for_pile:
        total = count_piles(df, True)
    else:
        total = count_stations(df, False, apply_filter=True)
    return {
        "总量": total,
        "环比增量": "—",
        "环比增速": "—",
    }


def province_ranking_table(df: pd.DataFrame, for_pile: bool = True, top_n: int = 31) -> pd.DataFrame:
    """各省数量排名表：排名、省份、数量、全国占比、环比增速（占位）。桩表按序号计数，站表按充电站内部编号(站内>2)去重。"""
    prov_col = _province_col(df)
    if prov_col is None:
        return pd.DataFrame(columns=["排名", "省份", "数量", "全国占比", "环比增速"])
    if for_pile:
        agg = agg_pile_count(df, prov_col, True)
    else:
        agg = agg_station_count(df, prov_col, False, apply_filter=True)
    if agg.empty:
        return pd.DataFrame(columns=["排名", "省份", "数量", "全国占比", "环比增速"])
    total = agg.sum()
    agg = agg.sort_values(ascending=False).head(top_n)
    out = pd.DataFrame({
        "排名": range(1, len(agg) + 1),
        "省份": agg.index.astype(str).tolist(),
        "数量": agg.values.tolist(),
        "全国占比": [f"{(v / total * 100):.1f}%" for v in agg.values],
        "环比增速": ["—"] * len(agg),
    })
    return out
