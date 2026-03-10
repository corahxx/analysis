# handlers/power_handler.py - 功率段分布 P6（桩表按序号计数，站表站内>2+充电站内部编号去重）

from typing import Optional, Tuple
import pandas as pd

from .data_utils import pile_count_col, filter_stations_with_more_than_2_piles, station_id_col


POWER_BINS = [
    (0, 120, "p<120kW"),
    (120, 250, "120≤p<250kW"),
    (250, 480, "250≤p<480kW"),
    (480, 960, "480≤p<960kW"),
    (960, float("inf"), "960≤p kW"),
]


def _power_column(df: pd.DataFrame, for_pile: bool) -> Optional[str]:
    if for_pile and "额定功率" in df.columns:
        return "额定功率"
    if not for_pile and "站点总装机功率" in df.columns:
        return "站点总装机功率"
    return None


def _assign_power_bin(ser: pd.Series) -> pd.Series:
    """将功率序列映射为功率段标签。"""
    out = pd.Series(index=ser.index, dtype=object)
    for low, high, label in POWER_BINS:
        if high == float("inf"):
            mask = ser >= low
        else:
            mask = (ser >= low) & (ser < high)
        out.loc[mask] = label
    return out


def power_distribution_table(df: pd.DataFrame, for_pile: bool = True) -> pd.DataFrame:
    """功率段数量/占比表。桩表：按额定功率分档，每档对序号 count；站表：站内>2 后按站点总装机功率分档，充电站内部编号去重计数。"""
    col = _power_column(df, for_pile)
    if col is None:
        return pd.DataFrame(columns=["功率段", "数量", "占比", "环比"])
    if for_pile:
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if s.empty:
            return pd.DataFrame(columns=["功率段", "数量", "占比", "环比"])
        df_ = df.loc[s.index].copy()
        df_["_功率段_"] = _assign_power_bin(s)
        pc = pile_count_col(df_, True)
        if pc:
            cnt = df_.groupby("_功率段_", dropna=False)[pc].count()
        else:
            cnt = df_.groupby("_功率段_", dropna=False).size()
    else:
        filtered = filter_stations_with_more_than_2_piles(df, False)
        if filtered.empty:
            return pd.DataFrame(columns=["功率段", "数量", "占比", "环比"])
        p = pd.to_numeric(filtered[col], errors="coerce").dropna()
        if p.empty:
            return pd.DataFrame(columns=["功率段", "数量", "占比", "环比"])
        sid = station_id_col(filtered)
        if sid is None:
            return pd.DataFrame(columns=["功率段", "数量", "占比", "环比"])
        filtered = filtered.loc[p.index].copy()
        filtered["_功率段_"] = _assign_power_bin(p)
        cnt = filtered.groupby("_功率段_", dropna=False)[sid].nunique()
    total = cnt.sum()
    labels = [POWER_BINS[i][2] for i in range(len(POWER_BINS))]
    counts = [int(cnt.get(lb, 0)) for lb in labels]
    pcts = [f"{(c / total * 100):.1f}%" if total else "0%" for c in counts]
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
    return "（按额定功率）" if for_pile else "（按站点总装机功率）"
