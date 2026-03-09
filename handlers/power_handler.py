# handlers/power_handler.py - 功率段分布（充电桩按额定功率，充电站按站点总装机功率）

from typing import Optional, Tuple
import pandas as pd


POWER_BINS = [
    (0, 120, "p<120kW"),
    (120, 250, "120≤p<250kW"),
    (250, 480, "250≤p<480kW"),
    (480, 960, "480≤p<960kW"),
    (960, float("inf"), "960≤p kW"),
]


def _power_column(df: pd.DataFrame, for_pile: bool) -> Optional[str]:
    """返回用于分档的功率列名。"""
    if for_pile and "额定功率" in df.columns:
        return "额定功率"
    if not for_pile and "站点总装机功率" in df.columns:
        return "站点总装机功率"
    return None


def power_distribution_table(df: pd.DataFrame, for_pile: bool = True) -> pd.DataFrame:
    """功率段数量/占比表。桩表用额定功率，站表用站点总装机功率。"""
    col = _power_column(df, for_pile)
    if col is None:
        return pd.DataFrame(columns=["功率段", "数量", "占比", "环比"])
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    if s.empty:
        return pd.DataFrame(columns=["功率段", "数量", "占比", "环比"])
    total = len(s)
    labels = []
    counts = []
    for low, high, label in POWER_BINS:
        if high == float("inf"):
            cnt = (s >= low).sum()
        else:
            cnt = ((s >= low) & (s < high)).sum()
        labels.append(label)
        counts.append(cnt)
    pcts = [f"{(c / total * 100):.1f}%" for c in counts]
    return pd.DataFrame({
        "功率段": labels,
        "数量": counts,
        "占比": pcts,
        "环比": ["—"] * len(labels),
    })


def power_distribution_chart_data(df: pd.DataFrame, for_pile: bool = True) -> Tuple[list, list]:
    """返回 (功率段标签列表, 数量列表) 用于绘图。"""
    t = power_distribution_table(df, for_pile=for_pile)
    if t.empty:
        return [], []
    return t["功率段"].tolist(), t["数量"].tolist()


def power_chart_title_suffix(for_pile: bool) -> str:
    """图表/表标题后缀：按额定功率 或 按站点总装机功率。"""
    return "（按额定功率）" if for_pile else "（按站点总装机功率）"

