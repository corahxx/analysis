# handlers/ranking_handler.py - 排行榜 P1：1.1 市场份额、1.2 设施销量(占位)、1.3 城市榜、1.4 星级(占位)、1.5 型号榜、1.6 车企私桩(占位)

from typing import Optional, Tuple, List
import pandas as pd

from .data_utils import pile_count_col, agg_pile_count


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


def market_share_top(df: pd.DataFrame, for_pile: bool = True, top_n: int = 10) -> pd.DataFrame:
    """1.1 存量市场份额榜 Top N：排名、运营商、设施总量、市场份额、环比增速（占位）。充电桩数量用序号计数。"""
    op_col = _operator_col(df)
    if op_col is None:
        return pd.DataFrame(columns=["排名", "运营商", "设施总量", "市场份额", "环比增速"])
    agg = agg_pile_count(df, op_col, for_pile)
    if agg.empty:
        return pd.DataFrame(columns=["排名", "运营商", "设施总量", "市场份额", "环比增速"])
    total = agg.sum()
    agg = agg.sort_values(ascending=False).head(top_n)
    out = pd.DataFrame({
        "排名": range(1, len(agg) + 1),
        "运营商": agg.index.astype(str).tolist(),
        "设施总量": agg.values.tolist(),
        "市场份额": [f"{(v / total * 100):.1f}%" for v in agg.values],
        "环比增速": ["—"] * len(agg),
    })
    return out


def facility_sales_top_placeholder() -> pd.DataFrame:
    """1.2 设施销量榜：占位，缺少历史月度数据。"""
    return pd.DataFrame(columns=["排名", "运营商", "设施总量", "备注"])


def city_top(df: pd.DataFrame, for_pile: bool = True, top_n: int = 10) -> pd.DataFrame:
    """1.3 城市榜 Top N：排名、城市、设施总量、全国占比、环比增速（占位）。充电桩数量用序号计数。"""
    city_col = _city_col(df)
    if city_col is None:
        return pd.DataFrame(columns=["排名", "城市", "设施总量", "全国占比", "环比增速"])
    df_ = df.copy()
    df_["_city_grp_"] = df_[city_col].fillna("未知")
    agg = agg_pile_count(df_, "_city_grp_", for_pile)
    if agg.empty:
        return pd.DataFrame(columns=["排名", "城市", "设施总量", "全国占比", "环比增速"])
    total = agg.sum()
    agg = agg.sort_values(ascending=False).head(top_n)
    out = pd.DataFrame({
        "排名": range(1, len(agg) + 1),
        "城市": agg.index.astype(str).tolist(),
        "设施总量": agg.values.tolist(),
        "全国占比": [f"{(v / total * 100):.1f}%" for v in agg.values],
        "环比增速": ["—"] * len(agg),
    })
    return out


def star_station_placeholder() -> pd.DataFrame:
    """1.4 星级场站榜：占位，缺少星级评分字段。"""
    return pd.DataFrame(columns=["排名", "星级", "设施总量", "备注"])


def model_rank_top(df: pd.DataFrame, for_pile: bool = True, top_n: int = 10) -> pd.DataFrame:
    """1.5 型号榜：设备型号、装机量、市场占比、主要生产厂商。仅桩表；充电桩数量用序号计数。"""
    if not for_pile:
        return pd.DataFrame(columns=["排名", "设备型号", "装机量", "市场占比", "主要生产厂商"])
    if "充电桩型号" not in df.columns:
        return pd.DataFrame(columns=["排名", "设备型号", "装机量", "市场占比", "主要生产厂商"])
    pc = pile_count_col(df, True)
    if pc is None:
        agg = df.groupby(df["充电桩型号"].fillna("未知"), dropna=False).size()
    else:
        agg = df.groupby(df["充电桩型号"].fillna("未知"), dropna=False)[pc].count()
    total = agg.sum()
    agg = agg.sort_values(ascending=False).head(top_n)
    manufacturers = []
    if "充电桩生产厂商名称" in df.columns:
        for m in agg.index:
            sub = df[df["充电桩型号"].fillna("未知") == m]
            manufacturers.append(sub["充电桩生产厂商名称"].mode().iloc[0] if not sub.empty and sub["充电桩生产厂商名称"].notna().any() else "—")
    else:
        manufacturers = ["—"] * len(agg)
    out = pd.DataFrame({
        "排名": range(1, len(agg) + 1),
        "设备型号": agg.index.astype(str).tolist(),
        "装机量": agg.values.tolist(),
        "市场占比": [f"{(v / total * 100):.1f}%" for v in agg.values],
        "主要生产厂商": manufacturers,
    })
    return out


def ev_private_placeholder() -> pd.DataFrame:
    """1.6 车企私桩榜：占位，无车企/个人私桩数据。"""
    return pd.DataFrame(columns=["排名", "车企/品牌", "数量", "备注"])


def get_all_ranking_tables(df: pd.DataFrame, for_pile: bool = True) -> List[Tuple[str, str, pd.DataFrame]]:
    """返回 [(板块标题, 表标题, DataFrame), ...]。含 1.1～1.6。"""
    tables: List[Tuple[str, str, pd.DataFrame]] = []
    t1 = market_share_top(df, for_pile=for_pile)
    tables.append(("排行榜", "1.1 存量市场份额榜 Top10", t1))
    tables.append(("排行榜", "1.2 设施销量榜（占位）", facility_sales_top_placeholder()))
    t3 = city_top(df, for_pile=for_pile)
    tables.append(("排行榜", "1.3 城市榜 Top10", t3))
    tables.append(("排行榜", "1.4 星级场站榜（占位）", star_station_placeholder()))
    t5 = model_rank_top(df, for_pile=for_pile)
    tables.append(("排行榜", "1.5 型号榜", t5))
    tables.append(("排行榜", "1.6 车企私桩榜（占位）", ev_private_placeholder()))
    return tables
