# handlers/power_handler.py - 功率段分布 P6（按省多 Sheet；桩/站均按行数、额定功率/站点总装机功率分档）

from io import BytesIO
from typing import Dict, List, Optional, Tuple

import pandas as pd

from handlers.data_utils import (
    dataframe_cells_percent_to_decimal_ratio,
    format_share_ratios_4dp_max_remainder_floats,
)

# 固定顺序与文案（与产品约定一致）
POWER_SEGMENT_LABELS = [
    "p < 120kW",
    "120-250kW",
    "250-480kW",
    "480-960kW",
    "960kW+",
]


def _power_column(df: pd.DataFrame, for_pile: bool) -> Optional[str]:
    if for_pile and "额定功率" in df.columns:
        return "额定功率"
    if not for_pile and "站点总装机功率" in df.columns:
        return "站点总装机功率"
    return None


def _province_col(df: pd.DataFrame) -> Optional[str]:
    if "省份_中文" in df.columns:
        return "省份_中文"
    if "省份" in df.columns:
        return "省份"
    return None


def _assign_power_bin(ser: pd.Series) -> pd.Series:
    """将已数值化、无 NA 的功率序列映射为功率段标签。"""
    out = pd.Series(index=ser.index, dtype=object)
    out.loc[ser < 120] = POWER_SEGMENT_LABELS[0]
    out.loc[(ser >= 120) & (ser < 250)] = POWER_SEGMENT_LABELS[1]
    out.loc[(ser >= 250) & (ser < 480)] = POWER_SEGMENT_LABELS[2]
    out.loc[(ser >= 480) & (ser < 960)] = POWER_SEGMENT_LABELS[3]
    out.loc[ser >= 960] = POWER_SEGMENT_LABELS[4]
    return out


def _power_segment_table_from_valid_df(df_valid: pd.DataFrame, col: str) -> pd.DataFrame:
    """df_valid 仅含有效功率行；按行数统计各档。"""
    s = pd.to_numeric(df_valid[col], errors="coerce")
    s = s.dropna()
    if s.empty:
        return pd.DataFrame(columns=["功率段", "数量", "占比", "环比"])
    bins = _assign_power_bin(s)
    cnt = df_valid.loc[s.index].assign(_功率段_=bins).groupby("_功率段_", dropna=False).size()
    total = int(cnt.sum())
    counts = [int(cnt.get(lb, 0)) for lb in POWER_SEGMENT_LABELS]
    pcts = (
        format_share_ratios_4dp_max_remainder_floats([float(c) for c in counts])
        if total
        else [0.0] * len(POWER_SEGMENT_LABELS)
    )
    return pd.DataFrame(
        {
            "功率段": POWER_SEGMENT_LABELS,
            "数量": counts,
            "占比": pcts,
            "环比": [""] * len(POWER_SEGMENT_LABELS),
        }
    )


def power_distribution_table(df: pd.DataFrame, for_pile: bool = True) -> pd.DataFrame:
    """全国汇总（不按省）：五档数量/占比，环比空。桩/站均按行数。"""
    col = _power_column(df, for_pile)
    if col is None:
        return pd.DataFrame(columns=["功率段", "数量", "占比", "环比"])
    s = pd.to_numeric(df[col], errors="coerce")
    valid_idx = s.dropna().index
    if valid_idx.empty:
        return pd.DataFrame(columns=["功率段", "数量", "占比", "环比"])
    return _power_segment_table_from_valid_df(df.loc[valid_idx], col)


def power_distribution_by_province_tables(
    df: pd.DataFrame, for_pile: bool = True
) -> List[Tuple[str, pd.DataFrame]]:
    """
    返回 [(sheet_name, DataFrame), ...]。
    有省份列时按省拆分；无省份列时单表「全部」。
    """
    col = _power_column(df, for_pile)
    if col is None:
        return []
    prov_col = _province_col(df)
    used_names: set = set()
    out: List[Tuple[str, pd.DataFrame]] = []

    if prov_col is None:
        s = pd.to_numeric(df[col], errors="coerce")
        valid = df.loc[s.dropna().index]
        name = _sanitize_sheet_name("全部", used_names)
        tbl = _power_segment_table_from_valid_df(valid, col)
        out.append((name, tbl))
        return out

    df = df.copy()
    df["_prov_"] = df[prov_col].fillna("未知").astype(str)
    for prov in sorted(df["_prov_"].unique(), key=lambda x: (x == "未知", x)):
        sub = df[df["_prov_"] == prov]
        s = pd.to_numeric(sub[col], errors="coerce")
        valid = sub.loc[s.dropna().index]
        sheet = _sanitize_sheet_name(prov, used_names)
        out.append((sheet, _power_segment_table_from_valid_df(valid, col)))
    return out


def _build_power_total_long_table(pairs: List[Tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    """各省分档纵向合并为一表，列：省份、功率段、数量、占比、环比（与分省 Sheet 内容一致）。"""
    rows: List[dict] = []
    for sheet_name, tbl in pairs:
        if tbl is None or tbl.empty:
            continue
        for _, r in tbl.iterrows():
            rows.append(
                {
                    "省份": sheet_name,
                    "功率段": r.get("功率段", ""),
                    "数量": r.get("数量", ""),
                    "占比": r.get("占比", ""),
                    "环比": r.get("环比", ""),
                }
            )
    if not rows:
        return pd.DataFrame(columns=["省份", "功率段", "数量", "占比", "环比"])
    return pd.DataFrame(rows)


def write_power_province_workbook(
    df: pd.DataFrame,
    for_pile: bool = True,
    *,
    prepend_total_sheet: bool = False,
) -> Optional[BytesIO]:
    """
    多 Sheet xlsx；无功率列返回 None。
    prepend_total_sheet=True 时（单独导出功率段产品）：最前增加 Sheet「总」，
    首行表头为 省份 | 功率段 | 数量 | 占比 | 环比，其后为各省分档纵向合并。
    一键七表 ZIP 内仍为 prepend_total_sheet=False，保持与线上一致。
    写出前对各 Sheet 数据区逐格扫描：凡文本中含 %／％ 一律转为 0～1 小数（四位）。
    """
    pairs = power_distribution_by_province_tables(df, for_pile=for_pile)
    if not pairs:
        return None
    buf = BytesIO()
    used_sheets: set = set()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        if prepend_total_sheet:
            total_df = dataframe_cells_percent_to_decimal_ratio(
                _build_power_total_long_table(pairs)
            )
            sum_name = _sanitize_sheet_name("总", used_sheets)
            total_df.to_excel(writer, sheet_name=sum_name[:31], index=False)
        for sheet_name, tbl in pairs:
            safe = _sanitize_sheet_name(sheet_name, used_sheets)[:31]
            dataframe_cells_percent_to_decimal_ratio(tbl).to_excel(
                writer, sheet_name=safe, index=False
            )
    buf.seek(0)
    return buf


def _sanitize_sheet_name(name: str, used: set) -> str:
    import re

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


def list_power_preview_provinces(df: pd.DataFrame, for_pile: bool = True) -> List[str]:
    """供 UI selectbox：逻辑省份名（与 tables 中 sheet 对应）。"""
    col = _power_column(df, for_pile)
    if col is None:
        return []
    prov_col = _province_col(df)
    if prov_col is None:
        return ["全部"]
    df = df.copy()
    names = sorted(
        df[prov_col].fillna("未知").astype(str).unique().tolist(),
        key=lambda x: (x == "未知", x),
    )
    return names


def power_distribution_table_for_province(
    df: pd.DataFrame, for_pile: bool, province: str
) -> pd.DataFrame:
    """预览某一省（或「全部」）的功率段表。"""
    col = _power_column(df, for_pile)
    if col is None:
        return pd.DataFrame(columns=["功率段", "数量", "占比", "环比"])
    prov_col = _province_col(df)
    if prov_col is None or province == "全部":
        s = pd.to_numeric(df[col], errors="coerce")
        return _power_segment_table_from_valid_df(df.loc[s.dropna().index], col)
    sub = df[df[prov_col].fillna("未知").astype(str) == province]
    s = pd.to_numeric(sub[col], errors="coerce")
    return _power_segment_table_from_valid_df(sub.loc[s.dropna().index], col)


def power_distribution_chart_data(
    df: pd.DataFrame, for_pile: bool = True, province: Optional[str] = None
) -> Tuple[list, list]:
    """返回 (功率段标签列表, 数量列表)。province 为 None 时全国汇总；否则指定省或「全部」。"""
    if province is None:
        t = power_distribution_table(df, for_pile=for_pile)
    else:
        t = power_distribution_table_for_province(df, for_pile, province)
    if t.empty:
        return [], []
    return t["功率段"].tolist(), t["数量"].tolist()


def power_chart_title_suffix(for_pile: bool) -> str:
    return "（按额定功率）" if for_pile else "（按站点总装机功率）"
