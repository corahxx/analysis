# handlers/standard00_transform.py — 由标准 00 表生成与线上一致的七类产品 Excel 并打 ZIP

from __future__ import annotations

import re
import zipfile
from io import BytesIO
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from handlers.data_utils import (
    dataframe_cells_percent_to_decimal_ratio,
    format_share_ratios_4dp_max_remainder,
)
from handlers.highway_template import build_highway_workbook_bytes
from handlers.national_handler import NATIONAL_OVERVIEW_COLUMNS, NATIONAL_WORKBOOK_SHEET_TITLES
from handlers.operator_handler import OPERATOR_PRODUCT_COLUMNS, OPERATOR_WORKBOOK_SHEET_TITLES
from handlers.power_handler import POWER_SEGMENT_LABELS
from handlers.province_handler import PROVINCE_DIMENSION_ROWS, PROVINCE_PRODUCT_COLUMNS

# 文件名中的统计期：00表标准化-系统输入-YYMM
_PERIOD_RE = re.compile(r"00表标准化-系统输入-(\d{4})", re.IGNORECASE)

# 全国概况 Sheet → 00「省份」表列名（优先匹配第一个存在的列）
_NATIONAL_PROV_COLS: Dict[str, Tuple[str, ...]] = {
    "充电站": ("充电站",),
    "公共充电桩": ("公共充电桩",),
    "交流桩": ("交流桩",),
    "直流桩": ("直流桩",),
    "充电电量": ("充电电量",),
    "共享私桩": ("共享私桩",),
    "换电站": ("换电站",),
    "换电电量": ("换电电量（万度）", "换电电量"),
    "私桩及个人充电设施": ("私桩及个人充电设施",),
    "随车配建": ("随车配建",),
    "交直流桩": ("交直流桩数量",),
}

# 省级数据维度 → 00「省份」表列名
_PROV_DIM_COLS: Dict[str, Tuple[str, ...]] = {
    "充电站": ("充电站",),
    "公共充电桩": ("公共充电桩",),
    "交流桩": ("交流桩",),
    "直流桩": ("直流桩",),
    "充电电量": ("充电电量",),
    "共享私桩": ("共享私桩",),
    "换电站": ("换电站",),
    "换电电量": ("换电电量（万度）", "换电电量"),
    "私桩/个人充电设施": ("私桩及个人充电设施",),
    "随车配建": ("随车配建",),
    "交直流桩": ("交直流桩数量",),
}

# 运营商概况 Sheet → 00「运营商」表列名
_OP_SHEET_COLS: Dict[str, Tuple[str, ...]] = {
    "公共充电设施": ("公共充电设施总量",),
    "共享私桩": ("共享私桩",),
    "公用充电桩": ("公用充电桩",),
    "专用充电桩": ("专用充电桩",),
    "直流桩": ("直流桩",),
    "交流桩": ("交流桩",),
    "三相交流桩": ("三相交流桩",),
    "充电功率": ("充电功率",),
    "充电电量": ("充电电量（万度）", "充电电量"),
    "充电站": ("充电站",),
    "换电站": ("换电站",),
}


def parse_period_from_filename(filename: str) -> Optional[Tuple[int, int]]:
    """返回 (完整年份, 月)。YYMM → 20YY 年 MM 月。"""
    m = _PERIOD_RE.search(str(filename or ""))
    if not m:
        return None
    yymm = m.group(1)
    yy, mm = int(yymm[:2]), int(yymm[2:])
    if mm < 1 or mm > 12:
        return None
    return 2000 + yy, mm


def _prev_calendar_month(y: int, m: int) -> Tuple[int, int]:
    if m == 1:
        return y - 1, 12
    return y, m - 1


def _num(v) -> Optional[float]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (int, float)) and not pd.isna(v):
        return float(v)
    try:
        x = float(str(v).strip().replace(",", ""))
        if pd.isna(x):
            return None
        return x
    except (TypeError, ValueError):
        return None


# 由标准 00 表生成的产品中，无法产出数值时原为空单元格，统一写反斜杠（不替换原本为 0 的数）
STANDARD00_MISSING_CELL = "\\"

# 仅作占位、无数值含义的横杠（半角连字符、长/短破折号、全角减号等）
_STANDARD00_DASH_ONLY_PLACEHOLDERS = frozenset(("-", "—", "–", "－"))


def _standard00_cell_is_missing(v: object) -> bool:
    if v is None:
        return True
    try:
        if pd.isna(v):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(v, str):
        t = v.strip()
        if t == "":
            return True
        if t in _STANDARD00_DASH_ONLY_PLACEHOLDERS:
            return True
    return False


def standard00_fill_missing_cells(df: pd.DataFrame) -> pd.DataFrame:
    """将 None / NaN / 空字符串 / 仅横杠占位（如 -、—）单元格替换为 '\\'；数值 0 与字符串 '0.0000' 等保持不变。"""
    if df is None or df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        out[c] = out[c].map(
            lambda x: STANDARD00_MISSING_CELL if _standard00_cell_is_missing(x) else x
        )
    return out


def format_pct_share_strings_two_dp(values: Sequence[float]) -> List[str]:
    """
    占比：写为 0～1 的小数字符串，保留四位；同一批在万分之一上最大余额法分配，
    使各值之和为 1.0000（避免逐项四舍五入导致合计漂移）。
    """
    return format_share_ratios_4dp_max_remainder(values)


def _city_display_label(v) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s or None


def _fmt_mom_growth(curr: Optional[float], prev: Optional[float]) -> str:
    """环比增速：(本期−上期)/上期，以小数表示保留四位；无上期或本期为空则空串。"""
    if curr is None:
        return ""
    if prev is None:
        return ""
    if prev == 0 and curr == 0:
        return "0.0000"
    if prev == 0:
        return "—"
    return f"{((curr - prev) / prev):.4f}"


def _fmt_mom_delta(curr: Optional[float], prev: Optional[float]) -> str:
    if curr is None or prev is None:
        return ""
    d = curr - prev
    if abs(d - round(d)) < 1e-9:
        return str(int(round(d)))
    return str(round(d, 4)).rstrip("0").rstrip(".")


def _lookup_prev_operator_value(pmap: Dict[str, float], k: str) -> Optional[float]:
    """
    环比上月字典的键与本月「运营商」列可能不一致，例如本月「深圳车电网」对应上月「车电网」。
    """
    if not k:
        return None
    if k in pmap:
        return pmap[k]
    if k.startswith("深圳") and len(k) > 2:
        sk = k[2:]
        if sk in pmap:
            return pmap[sk]
    pk = f"深圳{k}"
    if pk in pmap:
        return pmap[pk]
    return None


def _pick_col(df: pd.DataFrame, candidates: Tuple[str, ...]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _read_all_sheets(file_like, filename: str) -> Dict[str, pd.DataFrame]:
    raw = file_like
    if hasattr(file_like, "read"):
        raw = file_like.read()
        if hasattr(file_like, "seek"):
            file_like.seek(0)
    buf = BytesIO(raw) if isinstance(raw, bytes) else BytesIO(raw)
    fn = (filename or "").lower()
    engine = "openpyxl" if fn.endswith(".xlsx") else None
    return pd.read_excel(buf, sheet_name=None, header=0, engine=engine)


def _sanitize_sheet_name(name: str, used: set) -> str:
    s = str(name).strip() or "Sheet"
    s = re.sub(r"[\[\]\\*/?:]", "_", s)
    s = s[:31] if s else "Sheet"
    base = s
    n = 1
    while s in used:
        suffix = f"_{n}"
        s = (base[: max(1, 31 - len(suffix))] + suffix)[:31]
        n += 1
    used.add(s)
    return s


def ingest_uploaded_workbooks(
    files: List,
) -> Tuple[Dict[Tuple[int, int], Dict[str, pd.DataFrame]], List[str]]:
    """
    files: Streamlit UploadedFile 列表。
    返回 (period -> {sheet_name: df}, warnings)。
    同一 YYMM 多次上传时后者覆盖前者。
    """
    warnings: List[str] = []
    snaps: Dict[Tuple[int, int], Dict[str, pd.DataFrame]] = {}
    for f in files:
        name = getattr(f, "name", "") or "unknown"
        per = parse_period_from_filename(name)
        if per is None:
            warnings.append(f"已跳过（文件名需含 00表标准化-系统输入-YYMM）：{name}")
            continue
        try:
            sheets = _read_all_sheets(f, name)
        except Exception as e:
            warnings.append(f"读取失败 {name}：{e}")
            continue
        if per in snaps:
            warnings.append(f"统计期 {per[0]:04d}-{per[1]:02d} 重复上传，已用后文件覆盖：{name}")
        snaps[per] = sheets
    return snaps, warnings


def _province_df(sheets: Dict[str, pd.DataFrame]) -> Optional[pd.DataFrame]:
    for key in ("省份",):
        if key in sheets and not sheets[key].empty:
            return sheets[key].copy()
    return None


def _operator_df(sheets: Dict[str, pd.DataFrame]) -> Optional[pd.DataFrame]:
    if "运营商" in sheets and not sheets["运营商"].empty:
        return sheets["运营商"].copy()
    return None


def build_national_workbook(
    cur: Dict[str, pd.DataFrame],
    prev: Optional[Dict[str, pd.DataFrame]],
) -> BytesIO:
    prov = _province_df(cur)
    prov_prev = _province_df(prev) if prev else None
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_title in NATIONAL_WORKBOOK_SHEET_TITLES:
            cols = _NATIONAL_PROV_COLS.get(sheet_title)
            if prov is None or not cols:
                pd.DataFrame(columns=NATIONAL_OVERVIEW_COLUMNS).to_excel(
                    writer, sheet_name=sheet_title[:31], index=False
                )
                continue
            src_col = _pick_col(prov, cols)
            if src_col is None:
                pd.DataFrame(columns=NATIONAL_OVERVIEW_COLUMNS).to_excel(
                    writer, sheet_name=sheet_title[:31], index=False
                )
                continue
            prev_col = _pick_col(prov_prev, cols) if prov_prev is not None else None
            pmap: Dict[str, float] = {}
            if prov_prev is not None and prev_col:
                for _, r in prov_prev.iterrows():
                    k = str(r.get("省份", "")).strip()
                    if not k:
                        continue
                    v = _num(r.get(prev_col))
                    if v is not None:
                        pmap[k] = v
            rows = []
            vals: List[float] = []
            for _, r in prov.iterrows():
                k = str(r.get("省份", "")).strip()
                if not k:
                    continue
                v = _num(r.get(src_col))
                if v is None:
                    continue
                vals.append(v)
                rows.append((k, v))
            pcts = format_pct_share_strings_two_dp(vals)
            recs = []
            for (k, v), pct in zip(rows, pcts):
                qty_out = v
                if sheet_title == "充电电量":
                    qty_out = v / 10000.0
                    if abs(qty_out - round(qty_out)) < 1e-9:
                        qty_out = int(round(qty_out))
                else:
                    if abs(v - round(v)) < 1e-9:
                        qty_out = int(round(v))
                recs.append(
                    {
                        "省份": k,
                        "数量": qty_out,
                        "全国占比": pct,
                        "环比增速": _fmt_mom_growth(v, pmap.get(k)),
                    }
                )
            recs.sort(key=lambda x: x["数量"] if isinstance(x["数量"], (int, float)) else 0, reverse=True)
            standard00_fill_missing_cells(
                pd.DataFrame(recs, columns=NATIONAL_OVERVIEW_COLUMNS)
            ).to_excel(writer, sheet_name=sheet_title[:31], index=False)
    buf.seek(0)
    return buf


def build_provincial_workbook(
    cur: Dict[str, pd.DataFrame],
    prev: Optional[Dict[str, pd.DataFrame]],
) -> Optional[BytesIO]:
    prov = _province_df(cur)
    if prov is None or "省份" not in prov.columns:
        return None
    prov_prev = _province_df(prev) if prev else None
    used: set = set()
    pairs: List[Tuple[str, pd.DataFrame]] = []
    for _, r in prov.iterrows():
        pname = str(r.get("省份", "")).strip()
        if not pname:
            continue
        sheet = _sanitize_sheet_name(pname, used)
        prev_row = None
        if prov_prev is not None and "省份" in prov_prev.columns:
            m = prov_prev[prov_prev["省份"].astype(str).str.strip() == pname]
            if not m.empty:
                prev_row = m.iloc[0]
        vals: List[str] = []
        moms: List[str] = []
        for dim in PROVINCE_DIMENSION_ROWS:
            cands = _PROV_DIM_COLS.get(dim, ())
            c = _pick_col(prov, cands) if cands else None
            if c is None:
                vals.append("")
                moms.append("")
                continue
            cv = _num(r.get(c))
            pv = None
            if prev_row is not None and prov_prev is not None:
                pc = _pick_col(prov_prev, cands)
                if pc is not None:
                    pv = _num(prev_row.get(pc))
            # 充电电量：展示与环比均为原值/10000；换电电量等其它维度不改
            scale = 10000.0 if dim == "充电电量" else 1.0
            if cv is None:
                vals.append("")
                moms.append("")
            else:
                disp = cv / scale
                if abs(disp - round(disp)) < 1e-9:
                    vals.append(str(int(round(disp))))
                else:
                    vals.append(str(disp))
                pv_disp = (pv / scale) if pv is not None else None
                moms.append(_fmt_mom_delta(disp, pv_disp))
        tbl = pd.DataFrame(
            {
                "数据维度": list(PROVINCE_DIMENSION_ROWS),
                "数值": vals,
                "环比变化": moms,
            }
        )
        pairs.append((sheet, tbl))
    if not pairs:
        return None
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sname, tbl in pairs:
            standard00_fill_missing_cells(tbl).to_excel(
                writer, sheet_name=sname[:31], index=False
            )
    buf.seek(0)
    return buf


def build_ratio_workbook(cur: Dict[str, pd.DataFrame]) -> BytesIO:
    prov = _province_df(cur)
    empty_cols = ["省份", "新能源车保有量", "公共充电设施数量", "车桩比"]
    if prov is None or "省份" not in prov.columns:
        buf = BytesIO()
        pd.DataFrame(columns=empty_cols).to_excel(buf, index=False, engine="openpyxl")
        buf.seek(0)
        return buf
    col_fac = _pick_col(prov, ("公共充电设施数量", "公共充电桩"))
    col_veh = _pick_col(prov, ("新能源车保有量",))
    rows = []
    for _, r in prov.iterrows():
        p = str(r.get("省份", "")).strip()
        if not p:
            continue
        fac = _num(r.get(col_fac)) if col_fac else None
        veh = _num(r.get(col_veh)) if col_veh else None
        ratio = ""
        if fac is not None and veh is not None and fac > 0:
            ratio = f"{(veh / fac):.4f}"
        rows.append(
            {
                "省份": p,
                "新能源车保有量": "" if veh is None else (int(veh) if abs(veh - round(veh)) < 1e-9 else veh),
                "公共充电设施数量": "" if fac is None else (int(fac) if abs(fac - round(fac)) < 1e-9 else fac),
                "车桩比": ratio,
            }
        )
    df = pd.DataFrame(rows, columns=empty_cols)
    df = df.sort_values("公共充电设施数量", ascending=False, na_position="last")
    buf = BytesIO()
    standard00_fill_missing_cells(df).to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf


def build_power_workbook_standard00(
    cur: Dict[str, pd.DataFrame],
    prev: Optional[Dict[str, pd.DataFrame]],
) -> BytesIO:
    """
    00 表路径下无明细桩功率：各档数量为 0，占比写四位小数，环比为数量环比（与上月同省同档比）。
    上月无该省时按 0 对比；无上期文件则环比列为空。
    """
    prov = _province_df(cur)
    used: set = set()
    rows_meta: List[Tuple[str, str]] = []
    if prov is not None and "省份" in prov.columns:
        for _, r in prov.iterrows():
            p = str(r.get("省份", "")).strip()
            if p:
                rows_meta.append((_sanitize_sheet_name(p, used), p))
    if not rows_meta:
        used_empty: set = set()
        rows_meta = [(_sanitize_sheet_name("全部", used_empty), "全部")]
    zero_counts = [0] * len(POWER_SEGMENT_LABELS)
    share_dec = format_pct_share_strings_two_dp([float(x) for x in zero_counts])
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sn, pkey in rows_meta:
            if prev is None:
                mom = [""] * len(POWER_SEGMENT_LABELS)
            else:
                prev_c = [0] * len(POWER_SEGMENT_LABELS)
                mom = [
                    _fmt_mom_growth(float(c), float(pc))
                    for c, pc in zip(zero_counts, prev_c)
                ]
            tbl = pd.DataFrame(
                {
                    "功率段": POWER_SEGMENT_LABELS,
                    "数量": zero_counts,
                    "占比": share_dec,
                    "环比": mom,
                }
            )
            standard00_fill_missing_cells(
                dataframe_cells_percent_to_decimal_ratio(tbl)
            ).to_excel(writer, sheet_name=sn[:31], index=False)
    buf.seek(0)
    return buf


def build_ranking_workbook(
    cur: Dict[str, pd.DataFrame],
    prev: Optional[Dict[str, pd.DataFrame]],
) -> BytesIO:
    op = _operator_df(cur)
    op_prev = _operator_df(prev) if prev else None

    # 市场份额榜
    mcols = ["运营商", "公共充电设施总量", "市场份额", "环比增速"]
    if op is not None and "运营商" in op.columns and "公共充电设施总量" in op.columns:
        kv: List[Tuple[str, float]] = []
        for _, r in op.iterrows():
            k = str(r["运营商"]).strip()
            v = _num(r.get("公共充电设施总量"))
            if not k or v is None:
                continue
            kv.append((k, v))
        pmap: Dict[str, float] = {}
        if op_prev is not None and "运营商" in op_prev.columns and "公共充电设施总量" in op_prev.columns:
            for _, r in op_prev.iterrows():
                k = str(r["运营商"]).strip()
                pv = _num(r.get("公共充电设施总量"))
                if k and pv is not None:
                    pmap[k] = pv
        kv_sorted = sorted(kv, key=lambda x: -x[1])
        mvals = [v for _, v in kv_sorted]
        mpcts = format_pct_share_strings_two_dp(mvals)
        ms = []
        for (k, v), pct in zip(kv_sorted, mpcts):
            ms.append(
                {
                    "运营商": k,
                    "公共充电设施总量": int(v) if abs(v - round(v)) < 1e-9 else v,
                    "市场份额": pct,
                    "环比增速": _fmt_mom_growth(v, _lookup_prev_operator_value(pmap, k)),
                }
            )
        df_ms = pd.DataFrame(ms, columns=mcols)
    else:
        df_ms = pd.DataFrame(columns=mcols)

    # 设施销量榜
    scols = ["运营商", "新增销量", "环比增量", "环比增速"]
    if op is not None and "运营商" in op.columns:
        sal_prev: Dict[str, float] = {}
        if op_prev is not None and "运营商" in op_prev.columns and "新增销量" in op_prev.columns:
            for _, r in op_prev.iterrows():
                k = str(r["运营商"]).strip()
                pv = _num(r.get("新增销量"))
                if k and pv is not None:
                    sal_prev[k] = pv
        sales_rows = []
        for _, r in op.iterrows():
            k = str(r["运营商"]).strip()
            if not k:
                continue
            cv = _num(r.get("新增销量")) if "新增销量" in op.columns else None
            sales_rows.append(
                {
                    "运营商": k,
                    "新增销量": "" if cv is None else (int(cv) if abs(cv - round(cv)) < 1e-9 else cv),
                    "环比增量": _fmt_mom_delta(cv, _lookup_prev_operator_value(sal_prev, k)),
                    "环比增速": _fmt_mom_growth(cv, _lookup_prev_operator_value(sal_prev, k)),
                }
            )
        df_sales = pd.DataFrame(sales_rows, columns=scols)
    else:
        df_sales = pd.DataFrame(columns=scols)

    # 城市榜 Top10
    ccols = ["城市", "公共充电设施总量", "全国占比", "环比增速"]
    city_prev_map: Dict[str, float] = {}
    if prev and "城市" in prev and not prev["城市"].empty:
        cp = prev["城市"]
        if "城市" in cp.columns and "公共充电设施总量" in cp.columns:
            for _, r in cp.iterrows():
                kk = _city_display_label(r.get("城市"))
                if not kk:
                    continue
                pv = _num(r.get("公共充电设施总量"))
                if pv is not None:
                    city_prev_map[kk] = pv
    df_city = pd.DataFrame(columns=ccols)
    if "城市" in cur and not cur["城市"].empty:
        cd = cur["城市"]
        if "城市" in cd.columns and "公共充电设施总量" in cd.columns:
            cvals: List[Tuple[str, float]] = []
            for _, r in cd.iterrows():
                kk = _city_display_label(r.get("城市"))
                if not kk:
                    continue
                v = _num(r.get("公共充电设施总量"))
                if v is None:
                    continue
                cvals.append((kk, v))
            cvals.sort(key=lambda x: -x[1])
            national_total = sum(v for _, v in cvals)
            top = cvals[:10]
            cr = []
            for kk, v in top:
                pct = f"{(v / national_total):.4f}" if national_total > 0 else "0.0000"
                cr.append(
                    {
                        "城市": kk,
                        "公共充电设施总量": int(v) if abs(v - round(v)) < 1e-9 else v,
                        "全国占比": pct,
                        "环比增速": _fmt_mom_growth(v, city_prev_map.get(kk)),
                    }
                )
            df_city = pd.DataFrame(cr, columns=ccols)

    # 星级场站榜
    stcols = ["运营商", "星级场站数", "占比", "五星级场站数"]
    if op is not None and "运营商" in op.columns and "星级场站数" in op.columns:
        star_rows = []
        for _, r in op.iterrows():
            k = str(r["运营商"]).strip()
            v = _num(r.get("星级场站数"))
            if not k or v is None:
                continue
            star_rows.append((k, v))
        star_sorted = sorted(star_rows, key=lambda x: -x[1])
        svals = [v for _, v in star_sorted]
        spcts = format_pct_share_strings_two_dp(svals)
        df_star = pd.DataFrame(
            [
                {
                    "运营商": k,
                    "星级场站数": int(v) if abs(v - round(v)) < 1e-9 else v,
                    "占比": pct,
                    "五星级场站数": "",
                }
                for (k, v), pct in zip(star_sorted, spcts)
            ],
            columns=stcols,
        )
    else:
        df_star = pd.DataFrame(columns=stcols)

    # 型号榜（排除占位型号，按装机量降序取前 10）
    mocols = ["设备型号", "装机量"]
    _model_rank_skip = frozenset(("未知", "【未知】", "充电桩", "【充电桩】"))
    df_model = pd.DataFrame(columns=mocols)
    if "型号" in cur and not cur["型号"].empty:
        md = cur["型号"]
        if "设备型号" in md.columns and "装机量" in md.columns:
            mr = []
            for _, r in md.iterrows():
                k = r.get("设备型号")
                if k is None or (isinstance(k, float) and pd.isna(k)):
                    continue
                k = str(k).strip()
                if not k or k in _model_rank_skip:
                    continue
                v = _num(r.get("装机量"))
                if v is None:
                    continue
                mr.append((k, v))
            mr.sort(key=lambda x: -x[1])
            mr = mr[:10]
            df_model = pd.DataFrame(
                [
                    {
                        "设备型号": k,
                        "装机量": int(v) if abs(v - round(v)) < 1e-9 else v,
                    }
                    for k, v in mr
                ],
                columns=mocols,
            )

    # 车企私桩榜
    evcols = ["车企名称", "私桩安装量", "占比", "环比增速"]
    ev_prev: Dict[str, float] = {}
    if prev and "车企" in prev and not prev["车企"].empty:
        ed = prev["车企"]
        if "车企名称" in ed.columns and "私桩安装量" in ed.columns:
            for _, r in ed.iterrows():
                k = str(r.get("车企名称", "")).strip()
                pv = _num(r.get("私桩安装量"))
                if k and pv is not None:
                    ev_prev[k] = pv
    df_ev = pd.DataFrame(columns=evcols)
    if "车企" in cur and not cur["车企"].empty:
        ed = cur["车企"]
        if "车企名称" in ed.columns and "私桩安装量" in ed.columns:
            evr = []
            for _, r in ed.iterrows():
                k = str(r.get("车企名称", "")).strip()
                v = _num(r.get("私桩安装量"))
                if not k or v is None:
                    continue
                evr.append((k, v))
            ev_sorted = sorted(evr, key=lambda x: -x[1])
            evals = [v for _, v in ev_sorted]
            evpcts = format_pct_share_strings_two_dp(evals)
            df_ev = pd.DataFrame(
                [
                    {
                        "车企名称": k,
                        "私桩安装量": int(v) if abs(v - round(v)) < 1e-9 else v,
                        "占比": pct,
                        "环比增速": _fmt_mom_growth(v, ev_prev.get(k)),
                    }
                    for (k, v), pct in zip(ev_sorted, evpcts)
                ],
                columns=evcols,
            )

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        standard00_fill_missing_cells(df_ms).to_excel(
            writer, sheet_name="市场份额榜"[:31], index=False
        )
        standard00_fill_missing_cells(df_sales).to_excel(
            writer, sheet_name="设施销量榜"[:31], index=False
        )
        standard00_fill_missing_cells(df_city).to_excel(
            writer, sheet_name="城市榜Top10"[:31], index=False
        )
        standard00_fill_missing_cells(df_star).to_excel(
            writer, sheet_name="星级场站榜"[:31], index=False
        )
        standard00_fill_missing_cells(df_model).to_excel(
            writer, sheet_name="型号榜"[:31], index=False
        )
        standard00_fill_missing_cells(df_ev).to_excel(
            writer, sheet_name="车企私桩榜"[:31], index=False
        )
    buf.seek(0)
    return buf


def _swap_station_operator_table(
    cur: Dict[str, pd.DataFrame],
    prev: Optional[Dict[str, pd.DataFrame]],
) -> pd.DataFrame:
    """表7「换电站」Sheet：数据来自 00 表「换电站-运营商」。"""
    sub = cur.get("换电站-运营商") if cur else None
    sprev = prev.get("换电站-运营商") if prev else None
    empty = pd.DataFrame(columns=OPERATOR_PRODUCT_COLUMNS)
    if sub is None or sub.empty or "运营商" not in sub.columns:
        return empty
    vcol = _pick_col(sub, ("数量", "换电站"))
    if vcol is None:
        return empty
    pvcol = _pick_col(sprev, ("数量", "换电站")) if sprev is not None else None
    pmap: Dict[str, float] = {}
    if sprev is not None and "运营商" in sprev.columns and pvcol:
        for _, r in sprev.iterrows():
            k = str(r["运营商"]).strip()
            pv = _num(r.get(pvcol))
            if k and pv is not None:
                pmap[k] = pv
    recs = []
    for _, r in sub.iterrows():
        k = str(r["运营商"]).strip()
        if not k:
            continue
        cv = _num(r.get(vcol))
        if cv is None:
            continue
        pv = _lookup_prev_operator_value(pmap, k)
        recs.append(
            {
                "运营商": k,
                "数值": int(cv) if abs(cv - round(cv)) < 1e-9 else cv,
                "环比变化": _fmt_mom_delta(cv, pv),
                "环比增速": _fmt_mom_growth(cv, pv),
            }
        )
    recs.sort(
        key=lambda x: x["数值"] if isinstance(x["数值"], (int, float)) else 0,
        reverse=True,
    )
    return pd.DataFrame(recs, columns=OPERATOR_PRODUCT_COLUMNS)


def build_operator_workbook(
    cur: Dict[str, pd.DataFrame],
    prev: Optional[Dict[str, pd.DataFrame]],
) -> BytesIO:
    op = _operator_df(cur)
    op_prev = _operator_df(prev) if prev else None
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_title in OPERATOR_WORKBOOK_SHEET_TITLES:
            if sheet_title == "换电站":
                df_sw = _swap_station_operator_table(cur, prev)
                if not df_sw.empty:
                    standard00_fill_missing_cells(df_sw).to_excel(
                        writer, sheet_name=sheet_title[:31], index=False
                    )
                    continue
            cands = _OP_SHEET_COLS.get(sheet_title, ())
            src_col = _pick_col(op, cands) if op is not None else None
            empty = pd.DataFrame(columns=OPERATOR_PRODUCT_COLUMNS)
            if op is None or "运营商" not in op.columns or src_col is None:
                empty.to_excel(writer, sheet_name=sheet_title[:31], index=False)
                continue
            prev_col = _pick_col(op_prev, cands) if op_prev is not None else None
            pmap: Dict[str, float] = {}
            if op_prev is not None and prev_col and "运营商" in op_prev.columns:
                for _, r in op_prev.iterrows():
                    k = str(r["运营商"]).strip()
                    pv = _num(r.get(prev_col))
                    if k and pv is not None:
                        pmap[k] = pv
            recs = []
            for _, r in op.iterrows():
                k = str(r["运营商"]).strip()
                if not k:
                    continue
                cv = _num(r.get(src_col))
                if cv is None:
                    continue
                pv = _lookup_prev_operator_value(pmap, k)
                recs.append(
                    {
                        "运营商": k,
                        "数值": int(cv) if abs(cv - round(cv)) < 1e-9 else cv,
                        "环比变化": _fmt_mom_delta(cv, pv),
                        "环比增速": _fmt_mom_growth(cv, pv),
                    }
                )
            recs.sort(
                key=lambda x: x["数值"] if isinstance(x["数值"], (int, float)) else 0,
                reverse=True,
            )
            standard00_fill_missing_cells(
                pd.DataFrame(recs, columns=OPERATOR_PRODUCT_COLUMNS)
            ).to_excel(writer, sheet_name=sheet_title[:31], index=False)
    buf.seek(0)
    return buf


def _write_seven_standard00_into_zip(
    zf: zipfile.ZipFile,
    arc_prefix: str,
    snapshots: Dict[Tuple[int, int], Dict[str, pd.DataFrame]],
    target_period: Tuple[int, int],
) -> None:
    """
    将某一统计期的七份 xlsx 写入已打开的 ZipFile。
    arc_prefix 不含尾部斜杠，例如：根文件夹/2602。
    环比数据仅当 snapshots 中存在 **自然月上一期** 键时填充，否则环比类列为空。
    """
    y, m = target_period
    prev_p = _prev_calendar_month(y, m)
    cur = snapshots[target_period]
    prev = snapshots.get(prev_p)

    def add(arc: str, data: bytes) -> None:
        zf.writestr(f"{arc_prefix}/{arc}", data)

    bio = build_ratio_workbook(cur)
    add("01_车桩比.xlsx", bio.getvalue())
    add(
        "02_高速公路_占位.xlsx",
        build_highway_workbook_bytes(
            _province_df(cur), fill_empty_with=STANDARD00_MISSING_CELL
        ).getvalue(),
    )
    add("03_功率段分布.xlsx", build_power_workbook_standard00(cur, prev).getvalue())
    add("04_排行榜.xlsx", build_ranking_workbook(cur, prev).getvalue())
    add("05_全国概况.xlsx", build_national_workbook(cur, prev).getvalue())
    pb = build_provincial_workbook(cur, prev)
    if pb is not None:
        add("06_省级数据.xlsx", pb.getvalue())
    else:
        _e = BytesIO()
        pd.DataFrame(columns=PROVINCE_PRODUCT_COLUMNS).to_excel(
            _e, index=False, engine="openpyxl"
        )
        _e.seek(0)
        add("06_省级数据.xlsx", _e.getvalue())
    add("07_运营商概况.xlsx", build_operator_workbook(cur, prev).getvalue())


def build_standard00_zip_bytes(
    snapshots: Dict[Tuple[int, int], Dict[str, pd.DataFrame]],
    target_period: Tuple[int, int],
    export_date: str,
) -> Optional[BytesIO]:
    """单期 ZIP：根目录为「标准化数据产品_00表转化_{YYMM}_{export_date}」（兼容旧调用）。"""
    if target_period not in snapshots:
        return None
    y, m = target_period
    yymm = f"{(y % 100):02d}{m:02d}"
    prefix = f"标准化数据产品_00表转化_{yymm}_{export_date}"
    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        _write_seven_standard00_into_zip(zf, prefix, snapshots, target_period)
    zip_buf.seek(0)
    return zip_buf


def build_standard00_multi_month_zip_bytes(
    snapshots: Dict[Tuple[int, int], Dict[str, pd.DataFrame]],
    selected_periods: List[Tuple[int, int]],
    export_date: str,
) -> Optional[BytesIO]:
    """
    多期合一 ZIP：根目录「标准化数据产品_00表转化_{export_date}」，其下每个 YYMM 子目录各含 01～07 共七个 xlsx。
    selected_periods 按时间升序写入；未出现在 snapshots 中的期别会被忽略。
    """
    periods = sorted({p for p in selected_periods if p in snapshots})
    if not periods:
        return None
    root = f"标准化数据产品_00表转化_{export_date}"
    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for y, m in periods:
            yymm = f"{(y % 100):02d}{m:02d}"
            _write_seven_standard00_into_zip(zf, f"{root}/{yymm}", snapshots, (y, m))
    zip_buf.seek(0)
    return zip_buf


def list_sorted_periods(snapshots: Dict[Tuple[int, int], Dict[str, pd.DataFrame]]) -> List[Tuple[int, int]]:
    return sorted(snapshots.keys())
