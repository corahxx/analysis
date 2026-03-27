# handlers/national_handler.py - 全国数据 P3：多 Sheet 全国概况 + 兼容旧卡片/省排名

from io import BytesIO
from typing import List, Optional, Tuple

import pandas as pd

from .data_utils import (
    agg_pile_count,
    agg_station_count,
    count_piles,
    count_stations,
    format_share_ratios_4dp_max_remainder_floats,
    share_as_decimal_4,
)

# 全国概况各 Sheet 统一列（有数据时）
NATIONAL_OVERVIEW_COLUMNS = ["省份", "数量", "全国占比", "环比增速"]


def _province_col(df: pd.DataFrame) -> Optional[str]:
    if "省份_中文" in df.columns:
        return "省份_中文"
    if "省份" in df.columns:
        return "省份"
    return None


def _empty_overview_table() -> pd.DataFrame:
    return pd.DataFrame(columns=NATIONAL_OVERVIEW_COLUMNS)


def province_breakdown_by_row_count(df: pd.DataFrame) -> pd.DataFrame:
    """
    按省统计行数；全国占比 = 该省数量 / 当前 df 行数总和（即本分母为有效分组行数之和）。
    环比增速空字符串。
    """
    prov_col = _province_col(df)
    if prov_col is None or df.empty:
        return _empty_overview_table()
    g = df.groupby(df[prov_col].fillna("未知").astype(str), dropna=False).size()
    g = g.sort_values(ascending=False)
    total = int(g.sum())
    if total == 0:
        return _empty_overview_table()
    cnts = [float(v) for v in g.values]
    ratios = format_share_ratios_4dp_max_remainder_floats(cnts)
    return pd.DataFrame(
        {
            "省份": g.index.tolist(),
            "数量": g.values.astype(int).tolist(),
            "全国占比": ratios,
            "环比增速": [""] * len(g),
        }
    )


def _filter_by_charging_type_convert(df: pd.DataFrame, expected: str) -> pd.DataFrame:
    """筛选 充电桩类型_转换（strip 后精确等于 expected）。"""
    if "充电桩类型_转换" not in df.columns:
        return df.iloc[0:0].copy()
    mask = df["充电桩类型_转换"].astype(str).str.strip() == expected
    return df.loc[mask]


def national_summary_cards(df: pd.DataFrame, for_pile: bool = True) -> dict:
    """全国汇总指标（兼容旧 UI）：总量、环比占位。"""
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
    """各省数量排名表（兼容旧逻辑）：排名、省份、数量、全国占比、环比。"""
    prov_col = _province_col(df)
    if prov_col is None:
        return pd.DataFrame(columns=["排名", "省份", "数量", "全国占比", "环比增速"])
    if for_pile:
        agg = agg_pile_count(df, prov_col, True)
    else:
        agg = agg_station_count(df, prov_col, False, apply_filter=True)
    if agg.empty:
        return pd.DataFrame(columns=["排名", "省份", "数量", "全国占比", "环比增速"])
    national_total = float(agg.sum())
    agg = agg.sort_values(ascending=False).head(top_n)
    return pd.DataFrame(
        {
            "排名": range(1, len(agg) + 1),
            "省份": agg.index.astype(str).tolist(),
            "数量": agg.values.tolist(),
            "全国占比": [share_as_decimal_4(v, national_total) for v in agg.values],
            "环比增速": ["—"] * len(agg),
        }
    )


def get_national_workbook_tables(
    df: pd.DataFrame, for_pile: bool = True
) -> List[Tuple[str, pd.DataFrame]]:
    """
    全国概况多 Sheet：(Sheet 名, DataFrame)。
    充电站/充电电量等占位 Sheet 为仅表头；公共充电桩/交流/直流/交直流仅桩表按行数分省。
    """
    empty = _empty_overview_table()
    charge_station_sheet = _empty_overview_table()

    if not for_pile:
        return [
            ("充电站", charge_station_sheet),
            ("公共充电桩", empty),
            ("交流桩", empty),
            ("直流桩", empty),
            ("充电电量", empty),
            ("共享私桩", empty),
            ("换电站", empty),
            ("换电电量", empty),
            ("私桩及个人充电设施", empty),
            ("随车配建", empty),
            ("交直流桩", empty),
        ]

    public_piles = province_breakdown_by_row_count(df)
    ac = province_breakdown_by_row_count(_filter_by_charging_type_convert(df, "交流"))
    dc = province_breakdown_by_row_count(_filter_by_charging_type_convert(df, "直流"))
    acdc = province_breakdown_by_row_count(_filter_by_charging_type_convert(df, "交直流"))

    return [
        ("充电站", charge_station_sheet),
        ("公共充电桩", public_piles),
        ("交流桩", ac),
        ("直流桩", dc),
        ("充电电量", empty),
        ("共享私桩", empty),
        ("换电站", empty),
        ("换电电量", empty),
        ("私桩及个人充电设施", empty),
        ("随车配建", empty),
        ("交直流桩", acdc),
    ]


def write_national_workbook_bytes(df: pd.DataFrame, for_pile: bool = True) -> BytesIO:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, tbl in get_national_workbook_tables(df, for_pile=for_pile):
            safe = sheet_name[:31]
            if sheet_name == "充电电量" and not tbl.empty and "数量" in tbl.columns:
                tbl = tbl.copy()
                tbl["数量"] = tbl["数量"].apply(
                    lambda x: (
                        int(round(x / 10000.0))
                        if isinstance(x, (int, float))
                        and abs(x / 10000.0 - round(x / 10000.0)) < 1e-9
                        else (x / 10000.0 if isinstance(x, (int, float)) else x)
                    )
                )
            tbl.to_excel(writer, sheet_name=safe, index=False)
    buf.seek(0)
    return buf


NATIONAL_WORKBOOK_SHEET_TITLES: Tuple[str, ...] = (
    "充电站",
    "公共充电桩",
    "交流桩",
    "直流桩",
    "充电电量",
    "共享私桩",
    "换电站",
    "换电电量",
    "私桩及个人充电设施",
    "随车配建",
    "交直流桩",
)


def list_national_sheet_titles() -> List[str]:
    return list(NATIONAL_WORKBOOK_SHEET_TITLES)
