# handlers/province_handler.py - 各省数据（单省多维度概览）

from typing import Optional
import pandas as pd


def _province_col(df: pd.DataFrame) -> Optional[str]:
    if "省份_中文" in df.columns:
        return "省份_中文"
    if "省份" in df.columns:
        return "省份"
    return None


def province_overview_table(df: pd.DataFrame, province: str, for_pile: bool = True) -> pd.DataFrame:
    """单省多维度概览：数据维度、数值、环比变化、全国排名（占位）。"""
    prov_col = _province_col(df)
    if prov_col is None:
        return pd.DataFrame(columns=["数据维度", "数值", "环比变化", "全国排名"])
    sub = df[df[prov_col].astype(str) == province]
    if sub.empty:
        return pd.DataFrame(columns=["数据维度", "数值", "环比变化", "全国排名"])
    rows = []
    if for_pile and "充电桩编号" in df.columns:
        rows.append(("公共充电桩", f"{sub['充电桩编号'].nunique():,} 台", "—", "—"))
    if "所属充电站编号" in df.columns:
        rows.append(("充电站", f"{sub['所属充电站编号'].nunique():,} 站", "—", "—"))
    if for_pile and "额定功率" in sub.columns:
        p = pd.to_numeric(sub["额定功率"], errors="coerce").sum()
        rows.append(("充电功率（合计）", f"{p:,.0f} kW", "—", "—"))
    if not for_pile and "站点总装机功率" in sub.columns:
        p = pd.to_numeric(sub["站点总装机功率"], errors="coerce").sum()
        rows.append(("站点总装机功率（合计）", f"{p:,.0f} kW", "—", "—"))
    if not rows:
        rows.append(("设施总量", str(len(sub)), "—", "—"))
    return pd.DataFrame(rows, columns=["数据维度", "数值", "环比变化", "全国排名"])
