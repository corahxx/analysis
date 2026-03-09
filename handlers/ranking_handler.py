# handlers/ranking_handler.py - 排行榜多张表（存量市场份额、城市榜等）

from typing import Optional, Tuple, List
import pandas as pd


def _operator_col(df: pd.DataFrame) -> Optional[str]:
    if "运营商名称" in df.columns:
        return "运营商名称"
    if "上报机构" in df.columns:
        return "上报机构"
    return None


def _city_col(df: pd.DataFrame) -> Optional[str]:
    if "城市_中文" in df.columns:
        return "城市_中文"
    if "城市" in df.columns:
        return "城市"
    return None


def _id_col(df: pd.DataFrame, for_pile: bool) -> Optional[str]:
    if for_pile and "充电桩编号" in df.columns:
        return "充电桩编号"
    if not for_pile and "所属充电站编号" in df.columns:
        return "所属充电站编号"
    return None


def market_share_top(df: pd.DataFrame, for_pile: bool = True, top_n: int = 10) -> pd.DataFrame:
    """存量市场份额榜 Top N：排名、运营商、设施总量、市场份额、环比增速（占位）。"""
    op_col = _operator_col(df)
    id_col = _id_col(df, for_pile)
    if op_col is None or id_col is None:
        return pd.DataFrame(columns=["排名", "运营商", "设施总量", "市场份额", "环比增速"])
    total = df[id_col].nunique() if for_pile else len(df)
    agg = df.groupby(op_col, dropna=False)[id_col].nunique() if for_pile else df.groupby(op_col, dropna=False).size()
    agg = agg.sort_values(ascending=False).head(top_n)
    out = pd.DataFrame({
        "排名": range(1, len(agg) + 1),
        "运营商": agg.index.astype(str).tolist(),
        "设施总量": agg.values.tolist(),
        "市场份额": [f"{(v / total * 100):.1f}%" for v in agg.values],
        "环比增速": ["—"] * len(agg),
    })
    return out


def city_top(df: pd.DataFrame, for_pile: bool = True, top_n: int = 10) -> pd.DataFrame:
    """城市榜 Top N：排名、城市、设施总量、全国占比、环比增速（占位）。"""
    city_col = _city_col(df)
    id_col = _id_col(df, for_pile)
    if city_col is None or id_col is None:
        return pd.DataFrame(columns=["排名", "城市", "设施总量", "全国占比", "环比增速"])
    total = df[id_col].nunique() if for_pile else len(df)
    agg = df.groupby(df[city_col].fillna("未知"), dropna=False)[id_col].nunique() if for_pile else df.groupby(df[city_col].fillna("未知"), dropna=False).size()
    agg = agg.sort_values(ascending=False).head(top_n)
    out = pd.DataFrame({
        "排名": range(1, len(agg) + 1),
        "城市": agg.index.astype(str).tolist(),
        "设施总量": agg.values.tolist(),
        "全国占比": [f"{(v / total * 100):.1f}%" for v in agg.values],
        "环比增速": ["—"] * len(agg),
    })
    return out


def get_all_ranking_tables(df: pd.DataFrame, for_pile: bool = True) -> List[Tuple[str, str, pd.DataFrame]]:
    """返回 [(板块标题, 表标题, DataFrame), ...]。"""
    tables: List[Tuple[str, str, pd.DataFrame]] = []
    t1 = market_share_top(df, for_pile=for_pile)
    if not t1.empty:
        tables.append(("排行榜", "存量市场份额榜 Top10", t1))
    t2 = city_top(df, for_pile=for_pile)
    if not t2.empty:
        tables.append(("排行榜", "城市榜 Top10", t2))
    return tables
