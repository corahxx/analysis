# handlers/province_handler.py - 各省数据 P4：4.1 省级维度概览（单省多维度）

from typing import Optional
import pandas as pd

from .data_utils import count_piles, count_stations


def _province_col(df: pd.DataFrame) -> Optional[str]:
    if "省份_中文" in df.columns:
        return "省份_中文"
    if "省份" in df.columns:
        return "省份"
    return None


def province_overview_table(df: pd.DataFrame, province: str, for_pile: bool = True) -> pd.DataFrame:
    """单省多维度概览：数据维度、数值、环比变化、全国排名（占位）。充电桩数用序号计数，充电站数用充电站内部编号且站内桩数>2。"""
    prov_col = _province_col(df)
    if prov_col is None:
        return pd.DataFrame(columns=["数据维度", "数值", "环比变化", "全国排名"])
    sub = df[df[prov_col].astype(str) == province]
    if sub.empty:
        return pd.DataFrame(columns=["数据维度", "数值", "环比变化", "全国排名"])
    rows = []
    pile_count = count_piles(sub, for_pile)
    rows.append(("公共充电桩", f"{pile_count:,} 台", "—", "—"))
    station_count = count_stations(sub, for_pile, apply_filter=True)
    rows.append(("充电站", f"{station_count:,} 站", "—", "—"))
    if for_pile and "额定功率" in sub.columns:
        p = pd.to_numeric(sub["额定功率"], errors="coerce").sum()
        rows.append(("充电功率（合计）", f"{p:,.0f} kW", "—", "—"))
    if not for_pile and "站点总装机功率" in sub.columns:
        p = pd.to_numeric(sub["站点总装机功率"], errors="coerce").sum()
        rows.append(("站点总装机功率（合计）", f"{p:,.0f} kW", "—", "—"))
    if not rows:
        rows.append(("设施总量", str(len(sub)), "—", "—"))
    return pd.DataFrame(rows, columns=["数据维度", "数值", "环比变化", "全国排名"])
