# handlers/province_handler.py - 各省数据 P4：每省一 Sheet 维度表 + 兼容旧单省概览

from io import BytesIO
import re
from typing import List, Optional, Tuple

import pandas as pd

from .data_utils import count_piles, count_stations

# 省级数据产品固定维度行（顺序不可改）
PROVINCE_DIMENSION_ROWS: Tuple[str, ...] = (
    "充电站",
    "公共充电桩",
    "交流桩",
    "直流桩",
    "充电电量",
    "共享私桩",
    "换电站",
    "换电电量",
    "私桩/个人充电设施",
    "随车配建",
    "交直流桩",
)

PROVINCE_PRODUCT_COLUMNS = ["数据维度", "数值", "环比变化"]


def _province_col(df: pd.DataFrame) -> Optional[str]:
    if "省份_中文" in df.columns:
        return "省份_中文"
    if "省份" in df.columns:
        return "省份"
    return None


def _filter_by_charging_type_convert(df: pd.DataFrame, expected: str) -> pd.DataFrame:
    if "充电桩类型_转换" not in df.columns:
        return df.iloc[0:0].copy()
    mask = df["充电桩类型_转换"].astype(str).str.strip() == expected
    return df.loc[mask]


def _sanitize_sheet_name(name: str, used: set) -> str:
    s = str(name).strip() or "Sheet"
    s = re.sub(r'[\[\]\\*/?:]', "_", s)
    s = s[:31] if s else "Sheet"
    base = s
    n = 1
    while s in used:
        suffix = f"_{n}"
        s = (base[: max(1, 31 - len(suffix))] + suffix)[:31]
        n += 1
    used.add(s)
    return s


def _empty_product_table() -> pd.DataFrame:
    return pd.DataFrame(columns=PROVINCE_PRODUCT_COLUMNS)


def province_dimension_product_table(
    df: pd.DataFrame, province: str, for_pile: bool = True
) -> pd.DataFrame:
    """
    单省标准产品表：数据维度 × 数值 × 环比变化（环比均空）。
    桩表填公共/交流/直流/交直流行数；其余维度数值空。站表全部数值空。
    """
    prov_col = _province_col(df)
    if prov_col is None:
        return _empty_product_table()

    sub = df[df[prov_col].fillna("未知").astype(str) == str(province)]

    if not for_pile:
        return pd.DataFrame(
            {
                "数据维度": list(PROVINCE_DIMENSION_ROWS),
                "数值": [""] * len(PROVINCE_DIMENSION_ROWS),
                "环比变化": [""] * len(PROVINCE_DIMENSION_ROWS),
            }
        )

    n_pub = len(sub)
    n_ac = len(_filter_by_charging_type_convert(sub, "交流"))
    n_dc = len(_filter_by_charging_type_convert(sub, "直流"))
    n_acdc = len(_filter_by_charging_type_convert(sub, "交直流"))

    value_map = {
        "充电站": "",
        "公共充电桩": n_pub,
        "交流桩": n_ac,
        "直流桩": n_dc,
        "充电电量": "",
        "共享私桩": "",
        "换电站": "",
        "换电电量": "",
        "私桩/个人充电设施": "",
        "随车配建": "",
        "交直流桩": n_acdc,
    }

    nums = []
    for dim in PROVINCE_DIMENSION_ROWS:
        v = value_map[dim]
        nums.append(v if isinstance(v, int) else "")

    return pd.DataFrame(
        {
            "数据维度": list(PROVINCE_DIMENSION_ROWS),
            "数值": nums,
            "环比变化": [""] * len(PROVINCE_DIMENSION_ROWS),
        }
    )


def list_province_product_names(df: pd.DataFrame) -> List[str]:
    """预览/Sheet 逻辑名列表（与 省份_中文 取值一致，排序后「未知」靠后）。"""
    prov_col = _province_col(df)
    if prov_col is None:
        return []
    names = sorted(
        df[prov_col].fillna("未知").astype(str).unique().tolist(),
        key=lambda x: (x == "未知", x),
    )
    return names


def get_provincial_workbook_tables(
    df: pd.DataFrame, for_pile: bool = True
) -> List[Tuple[str, pd.DataFrame]]:
    """[(sheet_name, 三列表), ...]，每省一 Sheet。"""
    prov_col = _province_col(df)
    if prov_col is None:
        return []
    used: set = set()
    out: List[Tuple[str, pd.DataFrame]] = []
    for prov in list_province_product_names(df):
        sheet = _sanitize_sheet_name(prov, used)
        tbl = province_dimension_product_table(df, prov, for_pile=for_pile)
        out.append((sheet, tbl))
    return out


def write_provincial_workbook_bytes(df: pd.DataFrame, for_pile: bool = True) -> Optional[BytesIO]:
    pairs = get_provincial_workbook_tables(df, for_pile=for_pile)
    if not pairs:
        return None
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, tbl in pairs:
            tbl.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    buf.seek(0)
    return buf


def province_overview_table(df: pd.DataFrame, province: str, for_pile: bool = True) -> pd.DataFrame:
    """单省多维度概览（旧版）：数据维度、数值、环比变化、全国排名（占位）。保留兼容。"""
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
