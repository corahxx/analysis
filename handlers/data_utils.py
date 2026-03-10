# handlers/data_utils.py - 统计口径：充电桩用序号、充电站用充电站内部编号且站内桩数>2

from typing import Optional, Tuple
import pandas as pd


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
