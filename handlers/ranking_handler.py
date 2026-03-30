# handlers/ranking_handler.py - 排行榜：六 Sheet 标准列名与导出（按行数统计）

from io import BytesIO
from typing import List, Optional, Tuple

import pandas as pd

from .data_utils import (
    format_share_ratios_4dp_max_remainder_floats,
    share_as_decimal_4,
)


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


def sheet_market_share(df: pd.DataFrame) -> pd.DataFrame:
    """市场份额榜：全量运营商，按公共充电设施总量（行数）降序。"""
    cols = ["运营商", "公共充电设施总量", "市场份额", "环比增速"]
    op_col = _operator_col(df)
    if op_col is None:
        return pd.DataFrame(columns=cols)
    g = df.groupby(df[op_col].fillna("未知").astype(str), dropna=False).size()
    g = g.sort_values(ascending=False)
    total = int(g.sum())
    if total == 0:
        return pd.DataFrame(columns=cols)
    cnts = [float(v) for v in g.values]
    ratios = format_share_ratios_4dp_max_remainder_floats(cnts)
    return pd.DataFrame(
        {
            "运营商": g.index.tolist(),
            "公共充电设施总量": g.values.astype(int).tolist(),
            "市场份额": ratios,
            "环比增速": [""] * len(g),
        }
    )


def sheet_facility_sales(df: pd.DataFrame) -> pd.DataFrame:
    """设施销量榜：仅列出运营商，其余列空。"""
    cols = ["运营商", "新增销量", "环比增量", "环比增速"]
    op_col = _operator_col(df)
    if op_col is None:
        return pd.DataFrame(columns=cols)
    ops = sorted(df[op_col].fillna("未知").astype(str).unique().tolist())
    return pd.DataFrame(
        {
            "运营商": ops,
            "新增销量": [""] * len(ops),
            "环比增量": [""] * len(ops),
            "环比增速": [""] * len(ops),
        }
    )


def sheet_city_top10(df: pd.DataFrame) -> pd.DataFrame:
    """城市榜 Top10：按行数，全国占比分母为全表行数。"""
    cols = ["城市", "公共充电设施总量", "全国占比", "环比增速"]
    city_col = _city_col(df)
    if city_col is None:
        return pd.DataFrame(columns=cols)
    g = df.groupby(df[city_col].fillna("未知").astype(str), dropna=False).size()
    g = g.sort_values(ascending=False).head(10)
    total = len(df)
    if total <= 0:
        return pd.DataFrame(columns=cols)
    # 分母为全表行数，Top10 占比之和一般小于 1，逐行 round 即可
    return pd.DataFrame(
        {
            "城市": g.index.tolist(),
            "公共充电设施总量": g.values.astype(int).tolist(),
            "全国占比": [share_as_decimal_4(v, total) for v in g.values],
            "环比增速": [""] * len(g),
        }
    )


def sheet_star_station(df: pd.DataFrame) -> pd.DataFrame:
    """星级场站榜：仅运营商列有去重名单，其余空。"""
    cols = ["运营商", "星级场站数", "占比", "五星级场站数"]
    op_col = _operator_col(df)
    if op_col is None:
        return pd.DataFrame(columns=cols)
    ops = sorted(df[op_col].fillna("未知").astype(str).unique().tolist())
    return pd.DataFrame(
        {
            "运营商": ops,
            "星级场站数": [""] * len(ops),
            "占比": [""] * len(ops),
            "五星级场站数": [""] * len(ops),
        }
    )


def sheet_model_rank(df: pd.DataFrame, for_pile: bool) -> pd.DataFrame:
    """型号榜：桩表按充电桩型号行数；站表仅表头。"""
    cols = ["设备型号", "装机量"]
    if not for_pile or "充电桩型号" not in df.columns:
        return pd.DataFrame(columns=cols)
    g = df.groupby(df["充电桩型号"].fillna("未知").astype(str), dropna=False).size()
    g = g.sort_values(ascending=False)
    return pd.DataFrame(
        {
            "设备型号": g.index.tolist(),
            "装机量": g.values.astype(int).tolist(),
        }
    )


def sheet_ev_private() -> pd.DataFrame:
    """车企私桩榜：整表占位（仅表头）。"""
    return pd.DataFrame(columns=["车企名称", "私桩安装量", "占比", "环比增速"])


def get_ranking_workbook_tables(
    df: pd.DataFrame, for_pile: bool = True
) -> List[Tuple[str, pd.DataFrame]]:
    """[(Sheet 名, DataFrame), ...]，顺序固定。"""
    return [
        ("市场份额榜", sheet_market_share(df)),
        ("设施销量榜", sheet_facility_sales(df)),
        ("城市榜Top10", sheet_city_top10(df)),
        ("星级场站榜", sheet_star_station(df)),
        ("型号榜", sheet_model_rank(df, for_pile)),
        ("车企私桩榜", sheet_ev_private()),
    ]


def write_ranking_workbook_bytes(df: pd.DataFrame, for_pile: bool = True) -> BytesIO:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, tbl in get_ranking_workbook_tables(df, for_pile=for_pile):
            tbl.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    buf.seek(0)
    return buf


def get_all_ranking_tables(
    df: pd.DataFrame, for_pile: bool = True
) -> List[Tuple[str, str, pd.DataFrame]]:
    """
    兼容旧签名：返回 [(板块, 标题, DataFrame), ...]。
    标题与 Sheet 名一致，便于 UI 下拉。
    """
    out: List[Tuple[str, str, pd.DataFrame]] = []
    for sheet_name, tbl in get_ranking_workbook_tables(df, for_pile=for_pile):
        out.append(("排行榜", sheet_name, tbl))
    return out


# 以下保留名称供外部或测试引用（实现已迁移至 sheet_*）
def market_share_top(df: pd.DataFrame, for_pile: bool = True, top_n: int = 10) -> pd.DataFrame:
    """已弃用：请用 sheet_market_share（全量）。"""
    return sheet_market_share(df)


def city_top(df: pd.DataFrame, for_pile: bool = True, top_n: int = 10) -> pd.DataFrame:
    """已弃用：请用 sheet_city_top10。"""
    return sheet_city_top10(df)


def model_rank_top(df: pd.DataFrame, for_pile: bool = True, top_n: int = 10) -> pd.DataFrame:
    """已弃用：请用 sheet_model_rank（全量型号）。"""
    return sheet_model_rank(df, for_pile)


def facility_sales_top_placeholder() -> pd.DataFrame:
    return sheet_facility_sales(pd.DataFrame())


def star_station_placeholder() -> pd.DataFrame:
    return sheet_star_station(pd.DataFrame())


def ev_private_placeholder() -> pd.DataFrame:
    return sheet_ev_private()
