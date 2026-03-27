# handlers/data_utils.py - 统计口径：充电桩用序号、充电站用充电站内部编号且站内桩数>2

import math
from typing import List, Optional, Sequence, Tuple, Union

import pandas as pd


def share_as_decimal_4(numerator: Union[int, float], total: Union[int, float]) -> float:
    """占比 numerator/total，为 0～1 的 float，保留四位小数；分母无效时为 0.0。"""
    try:
        t = float(total)
        v = float(numerator)
    except (TypeError, ValueError):
        return 0.0
    if t <= 0:
        return 0.0
    return round(v / t, 4)


def scalar_percent_text_to_decimal_ratio(val):
    """
    若单元格为含半角 % 或全角 ％ 的文本（如 12.34%、12.34％），按「百分数→比例」转为 float 并保留四位小数；
    其余类型与原值不变。
    """
    if val is None:
        return val
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        if isinstance(val, float) and pd.isna(val):
            return val
        return val
    s = str(val).strip()
    if "%" not in s and "\uff05" not in s:
        return val
    s = s.replace("%", "").replace("\uff05", "").strip().replace(",", "")
    if s in ("", "—", "-", "–", "NaN", "nan"):
        return val
    try:
        return round(float(s) / 100.0, 4)
    except (TypeError, ValueError):
        return val


def dataframe_cells_percent_to_decimal_ratio(df: pd.DataFrame) -> pd.DataFrame:
    """对表内所有列逐格：凡文本中含 %／％ 则转为 0～1 小数（四位），用于功率段等多 Sheet 导出。"""
    if df is None or df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        out[c] = out[c].map(scalar_percent_text_to_decimal_ratio)
    return out


def format_share_ratios_4dp_max_remainder_floats(values: Sequence[float]) -> List[float]:
    """
    一组非负数量在同一分母下的占比，为 0～1 的 float（四位精度，万分之一最大余额法，合计为 1）。
    """
    vals = [float(v) for v in values]
    n = len(vals)
    if n == 0:
        return []
    total = sum(vals)
    if total <= 0:
        return [0.0] * n
    exact_bp = [v / total * 10000.0 for v in vals]
    floor_bp = [math.floor(x + 1e-9) for x in exact_bp]
    rem = int(round(10000 - sum(floor_bp)))
    frac = [exact_bp[i] - floor_bp[i] for i in range(n)]
    order_desc = sorted(range(n), key=lambda i: (-frac[i], -vals[i], i))
    adj = list(floor_bp)
    if rem > 0:
        for i in range(rem):
            adj[order_desc[i % n]] += 1
    elif rem < 0:
        order_asc = sorted(range(n), key=lambda i: (frac[i], vals[i], i))
        for i in range(-rem):
            idx = order_asc[i % n]
            if adj[idx] > 0:
                adj[idx] -= 1
    return [round(adj[i] / 10000.0, 4) for i in range(n)]


def format_share_ratios_4dp_max_remainder(values: Sequence[float]) -> List[str]:
    """
    一组非负数量在同一分母下的占比，写为 0～1 的小数字符串（四位）；
    在万分之一上用最大余额法分配，使各值之和为 1.0000。
    """
    return [f"{x:.4f}" for x in format_share_ratios_4dp_max_remainder_floats(values)]


def pile_count_col(df: pd.DataFrame, for_pile: bool) -> Optional[str]:
    """充电桩数量计数列：优先序号，无数则充电桩编号或 None（用行数）。"""
    if for_pile:
        if "序号" in df.columns:
            return "序号"
        if "充电桩编号" in df.columns:
            return "充电桩编号"
    return None


def count_piles(df: pd.DataFrame, for_pile: bool) -> int:
    """充电桩数量：桩表用序号 count 或充电桩编号 nunique/行数；站表用行数。"""
    if for_pile:
        if "序号" in df.columns:
            return int(df["序号"].count())
        if "充电桩编号" in df.columns:
            return int(df["充电桩编号"].nunique())
        return len(df)
    return len(df)


def station_id_col(df: pd.DataFrame) -> Optional[str]:
    """充电站标识列：充电站内部编号 或 所属充电站编号。"""
    if "充电站内部编号" in df.columns:
        return "充电站内部编号"
    if "所属充电站编号" in df.columns:
        return "所属充电站编号"
    return None


def filter_stations_with_more_than_2_piles(
    df: pd.DataFrame, for_pile: bool, pile_col: Optional[str] = None
) -> pd.DataFrame:
    """仅保留站内桩数>2的站对应的行。桩表：按充电站内部编号分组计桩数再过滤；站表：站点内桩总数>2。"""
    if for_pile:
        sid = station_id_col(df)
        if sid is None:
            return df
        pc = pile_col or pile_count_col(df, True)
        if pc is None:
            cnt = df.groupby(sid, dropna=False).size()
        else:
            cnt = df.groupby(sid, dropna=False)[pc].count()
        valid = cnt[cnt > 2].index
        return df[df[sid].isin(valid)]
    if "站点内桩总数" in df.columns:
        return df[pd.to_numeric(df["站点内桩总数"], errors="coerce") > 2]
    return df


def count_stations(df: pd.DataFrame, for_pile: bool, apply_filter: bool = True) -> int:
    """充电站数量：充电站内部编号去重，且仅统计站内桩数>2的站。"""
    if apply_filter:
        df = filter_stations_with_more_than_2_piles(df, for_pile)
    sid = station_id_col(df)
    if sid is None:
        return 0
    return int(df[sid].nunique())


def agg_pile_count(df: pd.DataFrame, group_col: str, for_pile: bool) -> pd.Series:
    """按 group_col 分组统计充电桩数量（序号 count 或充电桩编号 nunique）。"""
    pc = pile_count_col(df, for_pile)
    if for_pile and pc:
        if pc == "序号":
            return df.groupby(group_col, dropna=False)[pc].count()
        return df.groupby(group_col, dropna=False)[pc].nunique()
    return df.groupby(group_col, dropna=False).size()


def agg_station_count(
    df: pd.DataFrame, group_col: str, for_pile: bool, apply_filter: bool = True
) -> pd.Series:
    """按 group_col 分组统计充电站数量（充电站内部编号去重，站内>2）。"""
    if apply_filter:
        df = filter_stations_with_more_than_2_piles(df, for_pile)
    sid = station_id_col(df)
    if sid is None:
        return pd.Series(dtype=int)
    return df.groupby(df[group_col].fillna("未知"), dropna=False)[sid].nunique()
