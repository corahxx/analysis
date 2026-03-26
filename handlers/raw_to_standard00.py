# handlers/raw_to_standard00.py — 能源局等「原始汇总表」→ 标准 00 表（系统输入）工作簿

from __future__ import annotations

import re
from io import BytesIO
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd

# 输出 Sheet 与列顺序（与 00表标准化-系统输入-YYMM.xlsx 一致）
PROVINCE_COLUMNS: List[str] = [
    "省份",
    "公共充电桩",
    "新能源车保有量",
    "公共充电设施数量",
    "高速公路沿线已建设及预留建设充电停车位服务区",
    "高速公路沿线已建设充电停车位总数",
    "充电站",
    "交流桩",
    "直流桩",
    "交直流桩数量",
    "共享私桩",
    "换电站",
    "换电电量（万度）",
    "私桩及个人充电设施",
    "随车配建",
    "充电电量",
]

OPERATOR_COLUMNS: List[str] = [
    "运营商",
    "公共充电设施总量",
    "新增销量",
    "星级场站数",
    "共享私桩",
    "公用充电桩",
    "专用充电桩",
    "直流桩",
    "交流桩",
    "三相交流桩",
    "充电功率",
    "充电电量（万度）",
    "充电站",
    "换电站",
]

CITY_COLUMNS = ["城市", "公共充电设施总量"]
MODEL_COLUMNS = ["设备型号", "装机量"]
OEM_COLUMNS = ["车企名称", "私桩安装量"]
SWAP_OP_COLUMNS = ["运营商", "数量"]


def _norm_sheet_for_match(name: str) -> str:
    """
    归一化 Sheet 名用于规则匹配（不假定与标准 00 表一致）。
    去掉书名号/括号、尾部的 !！、以及「-能源局」「--能源局」等后缀。
    """
    t = str(name).strip()
    for ch in "\u300c\u300d\u300e\u300f\uff08\uff09（）【】[]「」『』":
        t = t.replace(ch, "")
    t = re.sub(r"[!！…]+$", "", t)
    while re.search(r"(-+能源局)+$", t):
        t = re.sub(r"(-+能源局)+$", "", t).strip()
    t = t.replace("＋", "+")
    return t.strip()


def _rule_ok(norm: str, rule: Dict[str, Any]) -> bool:
    for s in rule.get("all", []):
        if s not in norm:
            return False
    for s in rule.get("not_any", []):
        if s in norm:
            return False
    return True


def _first_sheet(
    sheet_names: Sequence[str], rules: Sequence[Dict[str, Any]]
) -> Optional[str]:
    """按规则顺序：每条规则在「工作簿原始顺序」下取第一张匹配的表。"""
    for rule in rules:
        for sn in sheet_names:
            if _rule_ok(_norm_sheet_for_match(sn), rule):
                return sn
    return None


@dataclass(frozen=True)
class RawSheetPlan:
    """原始汇总簿中各逻辑表的解析结果（值为工作簿内真实 Sheet 名，未命中为 None）。"""

    province_pub_ac_dc: Optional[str] = None
    province_map_alt: Optional[str] = None
    highway: Optional[str] = None
    station_wide: Optional[str] = None
    share_private: Optional[str] = None
    swap_facility: Optional[str] = None
    swap_kwh: Optional[str] = None
    private_province: Optional[str] = None
    charge_kwh_by_province: Optional[str] = None
    op_new: Optional[str] = None
    op_rank: Optional[str] = None
    op_pub_share: Optional[str] = None
    op_gongyong: Optional[str] = None
    op_zhuanyong: Optional[str] = None
    op_dc: Optional[str] = None
    op_ac: Optional[str] = None
    op_ac3: Optional[str] = None
    op_power: Optional[str] = None
    op_kwh: Optional[str] = None
    op_station: Optional[str] = None
    oem_private: Optional[str] = None
    model_top_mfr: Optional[str] = None


def resolve_raw_workbook_sheets(sheet_names: Sequence[str]) -> RawSheetPlan:
    """
    根据《00表合集》等多期文件归纳的「关键词 + 排除」规则解析 Sheet。
    规则按优先级排列；同名类表取工作簿中靠前的一张。
    """
    sn = list(sheet_names)

    def pick(rules: Sequence[Dict[str, Any]]) -> Optional[str]:
        return _first_sheet(sn, rules)

    # 主省表不用「各省公共桩-地图」（该表为另一版式，仅作 province_map_alt）
    province_pub = pick(
        [
            {"all": ["公共桩", "各省", "分交直流"]},
            {"all": ["公共桩各省分交直流"]},
            {
                "all": ["公共桩保有量"],
                "not_any": ["汇总"],
            },
            {"all": ["充电桩", "省份", "建设"]},
        ]
    )
    province_map = pick([{"all": ["各省公共桩", "地图"]}])

    highway = pick(
        [
            {"all": ["高速公路"], "not_any": ["运营商", "充电站", "各省分"]},
            {"all": ["11", "高速"]},
        ]
    )

    station_wide = pick([{"all": ["充电站", "各省"]}])

    share_private = pick(
        [
            {
                "all": ["共享私桩"],
                "not_any": ["公共+2", "公共＋2", "1公共"],
            },
        ]
    )

    swap_facility = pick([{"all": ["换电设施"]}])

    swap_kwh = pick(
        [
            {
                "all": ["换电", "电量"],
                "not_any": ["月份"],
            },
        ]
    )

    private_province = pick(
        [
            {
                "all": ["私桩", "各省"],
                "not_any": ["车企", "增长", "top10", "TOP10"],
            },
        ]
    )

    charge_kwh = pick(
        [
            {"all": ["各省", "充电电量"]},
            {
                "all": ["省", "充电电量"],
                "not_any": ["运营商", "同比", "环比"],
            },
        ]
    )

    op_new = pick([{"all": ["运营商", "新表"]}])
    op_rank = pick(
        [
            {"all": ["运营商", "排名"], "not_any": ["新表"]},
        ]
    )

    op_pub_share = pick(
        [
            {"all": ["公共+2", "共享"]},
            {"all": ["公共", "共享私桩"], "not_any": []},
        ]
    )

    op_gongyong = pick(
        [{"all": ["运营商", "公用桩"], "not_any": ["专用", "直流桩", "交流桩"]}]
    )
    op_zhuanyong = pick(
        [{"all": ["运营商", "专用桩"], "not_any": ["公用桩"]}]
    )
    op_dc = pick(
        [
            {"all": ["运营商", "直流桩"], "not_any": ["公用桩", "专用桩"]},
        ]
    )
    op_ac = pick(
        [
            {"all": ["运营商", "交流桩"], "not_any": ["公用桩", "专用桩", "直流桩"]},
        ]
    )
    op_ac3 = pick([{"all": ["三相交流"]}])
    op_power = pick([{"all": ["运营商", "充电功率"]}])
    op_kwh = pick(
        [
            {"all": ["运营商", "充电电量"]},
        ]
    )
    op_station = pick(
        [
            {
                "all": ["充电站", "运营商"],
                "not_any": ["各省"],
            },
        ]
    )

    oem = pick([{"all": ["私桩", "车企"], "not_any": ["各省", "增长"]}])

    model_top = pick(
        [
            {"all": ["250", "制造"]},
            {"all": ["kW", "制造"]},
            {"all": ["TOP5", "制造"]},
            {"all": ["公共桩", "TOP5", "制造"]},
        ]
    )

    return RawSheetPlan(
        province_pub_ac_dc=province_pub,
        province_map_alt=province_map,
        highway=highway,
        station_wide=station_wide,
        share_private=share_private,
        swap_facility=swap_facility,
        swap_kwh=swap_kwh,
        private_province=private_province,
        charge_kwh_by_province=charge_kwh,
        op_new=op_new,
        op_rank=op_rank,
        op_pub_share=op_pub_share,
        op_gongyong=op_gongyong,
        op_zhuanyong=op_zhuanyong,
        op_dc=op_dc,
        op_ac=op_ac,
        op_ac3=op_ac3,
        op_power=op_power,
        op_kwh=op_kwh,
        op_station=op_station,
        oem_private=oem,
        model_top_mfr=model_top,
    )


def load_sheet_plan(path: Union[str, Path]) -> RawSheetPlan:
    xl = pd.ExcelFile(path, engine="openpyxl")
    try:
        return resolve_raw_workbook_sheets(xl.sheet_names)
    finally:
        xl.close()


def parse_yyyymm_from_filename(name: str) -> Optional[str]:
    """
    从文件名提取 6 位年月（如 202602），返回标准 00 表后缀 YYMM（2602）。
    例：…图表-202602 - 能源局.xlsx → 2602
    """
    m = re.search(r"(20\d{2})(0[1-9]|1[0-2])", str(name))
    if not m:
        return None
    y, mo = m.group(1), m.group(2)
    return f"{int(y) % 100:02d}{int(mo):02d}"


def _norm_province_key(s: str) -> str:
    t = str(s).strip().replace(" ", "").replace("\u3000", "")
    return t


def _lookup_province_metric(maps: Dict[str, float], province_display: str):
    """高速等表用「广东」，主表用「广东省」——按去后缀、全名多键尝试。"""
    k = _norm_province_key(province_display)
    if k in maps:
        return maps[k]
    for suf in ("省", "市", "壮族自治区", "回族自治区", "维吾尔自治区", "自治区"):
        if k.endswith(suf) and len(k) > len(suf):
            short = k[: -len(suf)]
            if short in maps:
                return maps[short]
    for extra in ("省", "市"):
        if (k + extra) in maps:
            return maps[k + extra]
    return pd.NA


def _lookup_operator_subtable(maps: Dict[str, float], op: str):
    """
    主表（如 1运营商新表）常用「深圳车电网」，各分项子表（如三相交流）可能仅写「车电网」。
    先精确键，再尝试去掉「深圳」前缀或补上「深圳」前缀。
    """
    k = str(op).strip()
    if not k:
        return pd.NA
    if k in maps:
        return maps[k]
    if k.startswith("深圳") and len(k) > 2:
        short = k[2:]
        if short in maps:
            return maps[short]
    pk = f"深圳{k}"
    if pk in maps:
        return maps[pk]
    return pd.NA


def _operator_name_variants(name: str) -> set:
    """与 _lookup_operator_subtable 一致：主表「深圳X」与子表「X」视为同一实体。"""
    s = str(name).strip()
    if not s:
        return set()
    v = {s}
    if s.startswith("深圳") and len(s) > 2:
        v.add(s[2:])
    if not s.startswith("深圳"):
        v.add(f"深圳{s}")
    return v


def _map_key_already_on_main_column(map_key: str, main_names: set) -> bool:
    """子表键是否与主表第一列已有某运营商（含深圳/简称变体）为同一实体。"""
    k = str(map_key).strip()
    if not k:
        return True
    vk = _operator_name_variants(k)
    for m in main_names:
        if vk & _operator_name_variants(m):
            return True
    return False


def _dedupe_equivalent_extra_names(sorted_names: List[str]) -> List[str]:
    """仅追加子表多出的运营商时，去掉彼此等价（如 车电网 vs 深圳车电网）的重复项。"""
    kept: List[str] = []
    acc_sets: List[set] = []
    for ex in sorted_names:
        vx = _operator_name_variants(ex)
        if any(vx & s for s in acc_sets):
            continue
        kept.append(ex)
        acc_sets.append(vx)
    return kept


def _read_excel_safe(
    path: Union[str, Path], sheet: Optional[str], header=None
) -> pd.DataFrame:
    if not sheet:
        return pd.DataFrame()
    try:
        return pd.read_excel(path, sheet_name=sheet, header=header, engine="openpyxl")
    except (ValueError, KeyError, OSError):
        return pd.DataFrame()


def _series_from_wide_station(path: Path, plan: RawSheetPlan) -> Dict[str, float]:
    """充电站分省宽表：第 2 行省名，第 3 行保有量。"""
    raw = _read_excel_safe(path, plan.station_wide, header=None)
    if raw.shape[0] < 3:
        return {}
    names = raw.iloc[1].tolist()
    vals = raw.iloc[2].tolist()
    out: Dict[str, float] = {}
    for i, nm in enumerate(names):
        if i == 0 or pd.isna(nm) or str(nm).strip() in ("省级行政区域", "序号"):
            continue
        v = vals[i] if i < len(vals) else None
        nv = pd.to_numeric(v, errors="coerce")
        if pd.notna(nv):
            out[_norm_province_key(nm)] = float(nv)
    return out


def _series_highway(path: Path, plan: RawSheetPlan) -> Tuple[Dict[str, float], Dict[str, float]]:
    """(服务区 dict, 停车位总数 dict)，键为 norm 省名。"""
    df = _read_excel_safe(path, plan.highway, header=0)
    if df.empty or "省份" not in df.columns:
        return {}, {}
    cols = list(df.columns)
    # 列顺序：序号、省份、停车位总数、服务区
    svc_col = None
    park_col = None
    for c in cols:
        cs = str(c).replace("\n", "")
        if "预留" in cs or "服务区" in cs:
            svc_col = c
        if "停车位总数" in cs and "预留" not in cs:
            park_col = c
    svc: Dict[str, float] = {}
    park: Dict[str, float] = {}
    for _, r in df.iterrows():
        p = r.get("省份")
        if pd.isna(p):
            continue
        k = _norm_province_key(p)
        if svc_col is not None:
            v = pd.to_numeric(r.get(svc_col), errors="coerce")
            if pd.notna(v):
                svc[k] = float(v)
        if park_col is not None:
            v = pd.to_numeric(r.get(park_col), errors="coerce")
            if pd.notna(v):
                park[k] = float(v)
    return svc, park


def _series_share_private(path: Path, plan: RawSheetPlan) -> Dict[str, float]:
    raw = _read_excel_safe(path, plan.share_private, header=None)
    out: Dict[str, float] = {}
    hdr_row = None
    for i in range(min(5, len(raw))):
        row = raw.iloc[i].astype(str).tolist()
        if any("省份_中文" in str(x) for x in row):
            hdr_row = i
            break
    if hdr_row is None:
        return out
    sub = raw.iloc[hdr_row + 1 :].copy()
    # 找列索引
    hdr = raw.iloc[hdr_row].tolist()
    ic, iq = None, None
    for j, h in enumerate(hdr):
        hs = str(h)
        if "省份_中文" in hs:
            ic = j
        if hs.strip() == "数量" and iq is None:
            iq = j
    if ic is None or iq is None:
        return out
    for _, r in sub.iterrows():
        prov = r.iloc[ic] if ic < len(r) else None
        if pd.isna(prov):
            continue
        q = pd.to_numeric(r.iloc[iq] if iq < len(r) else None, errors="coerce")
        if pd.notna(q):
            out[_norm_province_key(prov)] = float(q)
    return out


def _series_swap_station_count(path: Path, plan: RawSheetPlan) -> Dict[str, float]:
    """换电设施：序号/省份/数量 三列块。"""
    raw = _read_excel_safe(path, plan.swap_facility, header=None)
    out: Dict[str, float] = {}
    for _, r in raw.iterrows():
        if len(r) < 3:
            continue
        a, b, c = r.iloc[0], r.iloc[1], r.iloc[2]
        if str(b).strip() in ("省份", "总计", "合计") or pd.isna(b):
            continue
        if str(a) == "序号" or str(b) == "省份":
            continue
        q = pd.to_numeric(c, errors="coerce")
        if pd.notna(q) and str(b).strip() != "总计":
            out[_norm_province_key(b)] = float(q)
    return out


def _series_swap_kwh_wan(path: Path, plan: RawSheetPlan) -> Dict[str, float]:
    df = _read_excel_safe(path, plan.swap_kwh, header=None)
    out: Dict[str, float] = {}
    hdr = None
    for i in range(min(8, len(df))):
        row = [str(x) for x in df.iloc[i].tolist()]
        if any("省级行政区域" in x for x in row) or any("行政区域名称" in x for x in row):
            hdr = i
            break
    if hdr is None:
        return out
    names = df.iloc[hdr].tolist()
    ic = None
    iv = None
    for j, h in enumerate(names):
        hs = str(h)
        if "行政区域" in hs and "名称" in hs:
            ic = j
        if "万度" in hs and "月度" in hs:
            iv = j
    if ic is None:
        for j, h in enumerate(names):
            if "省份" in str(h):
                ic = j
                break
    if iv is None:
        for j, h in enumerate(names):
            if "万度" in str(h):
                iv = j
                break
    if ic is None or iv is None:
        return out
    for _, r in df.iloc[hdr + 1 :].iterrows():
        p = r.iloc[ic] if ic < len(r) else None
        if pd.isna(p):
            continue
        v = pd.to_numeric(r.iloc[iv] if iv < len(r) else None, errors="coerce")
        if pd.notna(v):
            out[_norm_province_key(p)] = float(v)
    return out


def _series_private_province(path: Path, plan: RawSheetPlan) -> Dict[str, float]:
    raw = _read_excel_safe(path, plan.private_province, header=None)
    out: Dict[str, float] = {}
    hdr = None
    for i in range(min(6, len(raw))):
        row = raw.iloc[i].astype(str).tolist()
        if any("省份_中文" in x for x in row):
            hdr = i
            break
    if hdr is None:
        return out
    hdr_cells = raw.iloc[hdr].tolist()
    ic = iq = None
    for j, h in enumerate(hdr_cells):
        if "省份_中文" in str(h):
            ic = j
        if str(h).strip() == "数量":
            iq = j
    if ic is None or iq is None:
        return out
    for _, r in raw.iloc[hdr + 1 :].iterrows():
        p = r.iloc[ic] if ic < len(r) else None
        if pd.isna(p):
            continue
        q = pd.to_numeric(r.iloc[iq] if iq < len(r) else None, errors="coerce")
        if pd.notna(q):
            out[_norm_province_key(p)] = float(q)
    return out


def _coerce_province_base_columns(base: pd.DataFrame) -> pd.DataFrame:
    """各期原始表「合计(台)」与「合计(个)」等列名不一致时统一到 build 逻辑使用的列名。"""
    if base.empty:
        return base
    ren: Dict[str, str] = {}
    if "合计(个)" not in base.columns and "合计(台)" in base.columns:
        ren["合计(台)"] = "合计(个)"
    if "交流桩数量(个)" not in base.columns and "交流桩数量(台)" in base.columns:
        ren["交流桩数量(台)"] = "交流桩数量(个)"
    if "直流桩数量(个)" not in base.columns and "直流桩数量(台)" in base.columns:
        ren["直流桩数量(台)"] = "直流桩数量(个)"
    if "交直流桩数量(台)" not in base.columns and "交直流桩数量(个)" in base.columns:
        ren["交直流桩数量(个)"] = "交直流桩数量(台)"
    return base.rename(columns=ren) if ren else base


def _series_charge_electricity(path: Path, plan: RawSheetPlan) -> Dict[str, float]:
    """各省充电电量：电量列 ×10000 与标准 00 表「充电电量」一致。"""
    df = _read_excel_safe(path, plan.charge_kwh_by_province, header=1)
    if "省份" not in df.columns or "电量" not in df.columns:
        return {}
    out: Dict[str, float] = {}
    for _, r in df.iterrows():
        p = r["省份"]
        if pd.isna(p):
            continue
        v = pd.to_numeric(r["电量"], errors="coerce")
        if pd.notna(v):
            out[_norm_province_key(p)] = float(v) * 10000.0
    return out


def build_province_dataframe(
    path: Path, plan: Optional[RawSheetPlan] = None
) -> pd.DataFrame:
    if plan is None:
        plan = load_sheet_plan(path)
    base = _read_excel_safe(path, plan.province_pub_ac_dc, header=0)
    base = _coerce_province_base_columns(base)
    if base.empty or "省级行政区域" not in base.columns:
        base = _read_excel_safe(path, plan.province_map_alt, header=1)
        if not base.empty:
            base = base.rename(columns={"数量": "合计(个)"})
            name_col = "省份" if "省份" in base.columns else "省级行政区域"
            if name_col in base.columns:
                base = base.rename(columns={name_col: "省级行政区域"})
            base = _coerce_province_base_columns(base)
    rows = []
    hw_svc, hw_park = _series_highway(path, plan)
    st = _series_from_wide_station(path, plan)
    sh = _series_share_private(path, plan)
    sw = _series_swap_station_count(path, plan)
    sk = _series_swap_kwh_wan(path, plan)
    pv = _series_private_province(path, plan)
    ce = _series_charge_electricity(path, plan)
    for _, r in base.iterrows():
        pname = r.get("省级行政区域")
        if pd.isna(pname) or str(pname).strip() in ("总计", "合计", "省级行政区域"):
            continue
        key = _norm_province_key(pname)
        pub = pd.to_numeric(r.get("合计(个)"), errors="coerce")
        if pd.isna(pub):
            pub = pd.to_numeric(r.get("交流桩数量(个)"), errors="coerce")
            if pd.isna(pub):
                continue
        ac = pd.to_numeric(r.get("交流桩数量(个)"), errors="coerce")
        dc = pd.to_numeric(r.get("直流桩数量(个)"), errors="coerce")
        acdc = pd.to_numeric(r.get("交直流桩数量(台)"), errors="coerce")
        pname_s = str(pname).strip()
        rows.append(
            {
                "省份": pname_s,
                "公共充电桩": int(pub) if pub == int(pub) else float(pub),
                "新能源车保有量": pd.NA,
                "公共充电设施数量": int(pub) if pub == int(pub) else float(pub),
                "高速公路沿线已建设及预留建设充电停车位服务区": _lookup_province_metric(
                    hw_svc, pname_s
                ),
                "高速公路沿线已建设充电停车位总数": _lookup_province_metric(hw_park, pname_s),
                "充电站": _lookup_province_metric(st, pname_s),
                "交流桩": int(ac) if pd.notna(ac) and ac == int(ac) else ac,
                "直流桩": int(dc) if pd.notna(dc) and dc == int(dc) else dc,
                "交直流桩数量": int(acdc) if pd.notna(acdc) and acdc == int(acdc) else acdc,
                "共享私桩": _lookup_province_metric(sh, pname_s),
                "换电站": _lookup_province_metric(sw, pname_s),
                "换电电量（万度）": _lookup_province_metric(sk, pname_s),
                "私桩及个人充电设施": _lookup_province_metric(pv, pname_s),
                "随车配建": _lookup_province_metric(pv, pname_s),
                "充电电量": _lookup_province_metric(ce, pname_s),
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=PROVINCE_COLUMNS)
    return out[PROVINCE_COLUMNS]


def _read_operator_swap_station_from_facility_sheet(
    path: Path, sheet: Optional[str]
) -> Dict[str, float]:
    """
    换电设施 Sheet 内常先为分省块，其后才有「运营商」「数量」表头（不在首行）。
    扫描表头行后按列读取运营商与数量。
    """
    if not sheet:
        return {}
    raw = _read_excel_safe(path, sheet, header=None)
    if raw.empty:
        return {}
    ncols = raw.shape[1]
    hdr_io_iq: Optional[Tuple[int, int, int]] = None
    for i in range(min(45, len(raw))):
        op_idxs: List[int] = []
        qty_idxs: List[int] = []
        for j in range(ncols):
            cell = raw.iloc[i, j]
            if pd.isna(cell):
                continue
            s = str(cell).strip()
            if s == "运营商":
                op_idxs.append(j)
            elif s == "数量":
                qty_idxs.append(j)
        if not op_idxs or not qty_idxs:
            continue
        pair: Optional[Tuple[int, int]] = None
        for io in op_idxs:
            for iq in qty_idxs:
                if iq > io:
                    pair = (io, iq)
                    break
            if pair:
                break
        if pair:
            hdr_io_iq = (i, pair[0], pair[1])
            break
    if hdr_io_iq is None:
        return {}
    hdr_row, io, iq = hdr_io_iq
    out: Dict[str, float] = {}
    for ri in range(hdr_row + 1, len(raw)):
        r = raw.iloc[ri]
        op = r.iloc[io] if io < len(r) else None
        if pd.isna(op):
            continue
        ok = str(op).strip()
        if not ok or ok in ("运营商", "合计", "总计", "省份"):
            continue
        q = pd.to_numeric(r.iloc[iq] if iq < len(r) else None, errors="coerce")
        if pd.notna(q):
            out[ok] = float(q)
    return out


def _read_operator_electricity_map(path: Path, sheet: Optional[str]) -> Dict[str, float]:
    """
    Sheet 名同时含「运营商」「充电电量」的表：用「运营商」+「电量」列填标准 00「充电电量（万度）」。
    常见首行标题、第 2 行表头（header=1）；也兼容首行即表头或无表头扫描。
    """
    if not sheet:
        return {}
    for hdr in (1, 0, 2):
        df = _read_excel_safe(path, sheet, header=hdr)
        if df.empty or "运营商" not in df.columns:
            continue
        qcol = None
        for c in df.columns:
            if str(c).strip() == "电量":
                qcol = c
                break
        if qcol is None:
            for c in df.columns:
                cs = str(c).strip()
                if "电量" in cs and "占比" not in cs and "累计" not in cs and "环比" not in cs:
                    qcol = c
                    break
        if qcol is None:
            continue
        out: Dict[str, float] = {}
        for _, r in df.iterrows():
            op = r.get("运营商")
            if pd.isna(op):
                continue
            ok = str(op).strip()
            if not ok or ok in ("运营商", "合计", "总计"):
                continue
            v = pd.to_numeric(r.get(qcol), errors="coerce")
            if pd.notna(v):
                out[ok] = float(v)
        if out:
            return out
    raw = _read_excel_safe(path, sheet, header=None)
    for i in range(min(15, len(raw))):
        cells = raw.iloc[i].tolist()
        row_s = [str(x) for x in cells]
        if not any("运营商" in s for s in row_s):
            continue
        if not any(str(x).strip() == "电量" for x in cells):
            continue
        io = iv = None
        for j, x in enumerate(cells):
            xs = str(x).strip()
            if xs == "运营商":
                io = j
            elif xs == "电量":
                iv = j
        if io is None or iv is None:
            continue
        out2: Dict[str, float] = {}
        for _, r in raw.iloc[i + 1 :].iterrows():
            op = r.iloc[io] if io < len(r) else None
            if pd.isna(op):
                continue
            ok = str(op).strip()
            if not ok or ok in ("合计", "总计"):
                continue
            v = pd.to_numeric(r.iloc[iv] if iv < len(r) else None, errors="coerce")
            if pd.notna(v):
                out2[ok] = float(v)
        if out2:
            return out2
    return _read_operator_value_table(path, sheet)


def _pick_operator_name_column(df: pd.DataFrame) -> Optional[str]:
    """原始表常见「运营商」「运营商_简称」等。"""
    for pref in ("运营商_简称", "运营商"):
        for c in df.columns:
            if str(c).strip() == pref:
                return c
    for c in df.columns:
        cs = str(c).strip()
        if "运营商" in cs and "充电" not in cs:
            return c
    return None


def _pick_charging_power_value_column(df: pd.DataFrame) -> Optional[str]:
    """优先列名精确为「充电功率」，不用「充电功率1」。"""
    for c in df.columns:
        if str(c).strip() == "充电功率":
            return c
    for c in df.columns:
        cs = str(c).strip()
        if "充电功率" in cs and "充电功率1" != cs and "占比" not in cs and "累计" not in cs:
            return c
    for c in df.columns:
        if str(c).strip() == "充电功率1":
            return c
    return None


def _read_operator_charging_power_map(path: Path, sheet: Optional[str]) -> Dict[str, float]:
    """
    Sheet 名同时含「运营商」「充电功率」：用「运营商」或「运营商_简称」+ 列「充电功率」（非「充电功率1」）填标准 00「充电功率」。
    """
    if not sheet:
        return {}
    for hdr in (1, 0, 2):
        df = _read_excel_safe(path, sheet, header=hdr)
        if df.empty:
            continue
        op_col = _pick_operator_name_column(df)
        pcol = _pick_charging_power_value_column(df)
        if op_col is None or pcol is None:
            continue
        out: Dict[str, float] = {}
        for _, r in df.iterrows():
            op = r.get(op_col)
            if pd.isna(op):
                continue
            ok = str(op).strip()
            if not ok or ok in ("运营商", "运营商_简称", "合计", "总计"):
                continue
            v = pd.to_numeric(r.get(pcol), errors="coerce")
            if pd.notna(v):
                out[ok] = float(v)
        if out:
            return out
    raw = _read_excel_safe(path, sheet, header=None)
    for i in range(min(20, len(raw))):
        cells = raw.iloc[i].tolist()
        io = None
        ip = None
        for j, x in enumerate(cells):
            xs = str(x).strip()
            if xs in ("运营商_简称", "运营商"):
                io = j
                break
        if io is None:
            for j, x in enumerate(cells):
                if "运营商" in str(x).strip():
                    io = j
                    break
        for j, x in enumerate(cells):
            if str(x).strip() == "充电功率":
                ip = j
                break
        if io is None or ip is None:
            continue
        out2: Dict[str, float] = {}
        for _, r in raw.iloc[i + 1 :].iterrows():
            op = r.iloc[io] if io < len(r) else None
            if pd.isna(op):
                continue
            ok = str(op).strip()
            if not ok or ok in ("合计", "总计"):
                continue
            v = pd.to_numeric(r.iloc[ip] if ip < len(r) else None, errors="coerce")
            if pd.notna(v):
                out2[ok] = float(v)
        if out2:
            return out2
    return _read_operator_value_table(path, sheet)


def _read_operator_value_table(path: Path, sheet: Optional[str]) -> Dict[str, float]:
    raw = _read_excel_safe(path, sheet, header=None)
    out: Dict[str, float] = {}
    hdr = None
    for i in range(min(10, len(raw))):
        row = [str(x) for x in raw.iloc[i].tolist()]
        has_op = any("运营商" in c for c in row)
        has_qty_col = any(
            "桩数量" in c or "站数量" in c or str(c).strip() == "数量" for c in row
        )
        if has_op and has_qty_col:
            hdr = i
            break
    if hdr is None:
        return out
    hdr_cells = raw.iloc[hdr].tolist()
    io = iv = None
    for j, h in enumerate(hdr_cells):
        hs = str(h)
        if "运营商" in hs and io is None:
            io = j
        if "桩数量" in hs:
            iv = j
            break
    if iv is None:
        for j, h in enumerate(hdr_cells):
            hs = str(h)
            if "站数量" in hs:
                iv = j
                break
    if io is None:
        for j, h in enumerate(hdr_cells):
            if "运营商" in str(h):
                io = j
                break
    if iv is None:
        for j, h in enumerate(hdr_cells):
            hs = str(h)
            if hs.strip() == "数量" or ("数量" in hs and "累计" not in hs):
                iv = j
                break
    if io is None or iv is None:
        return out
    for _, r in raw.iloc[hdr + 1 :].iterrows():
        op = r.iloc[io] if io < len(r) else None
        if pd.isna(op):
            continue
        v = pd.to_numeric(r.iloc[iv] if iv < len(r) else None, errors="coerce")
        if pd.notna(v):
            out[str(op).strip()] = float(v)
    return out


def build_operator_dataframe(
    path: Path, plan: Optional[RawSheetPlan] = None
) -> pd.DataFrame:
    if plan is None:
        plan = load_sheet_plan(path)
    base = _read_excel_safe(path, plan.op_new, header=1)
    if base.empty or "运营商" not in base.columns:
        base = _read_excel_safe(path, plan.op_rank, header=1)
    # 取第一个「数量」列作为公共总量
    qty_col = None
    for c in base.columns:
        if str(c).strip() == "数量":
            qty_col = c
            break
    if qty_col is None:
        return pd.DataFrame(columns=OPERATOR_COLUMNS)
    ops = []
    seen_main_op: set = set()
    for _, r in base.iterrows():
        op = r.get("运营商")
        if pd.isna(op):
            continue
        ok = str(op).strip()
        if not ok:
            continue
        q = pd.to_numeric(r.get(qty_col), errors="coerce")
        if pd.isna(q):
            continue
        if ok in seen_main_op:
            continue
        seen_main_op.add(ok)
        ops.append((ok, float(q)))
    ops.sort(key=lambda x: -x[1])
    pub_share = _read_operator_value_table(path, plan.op_pub_share)
    raw_ps = _read_excel_safe(path, plan.op_pub_share, header=None)
    share_map: Dict[str, float] = {}
    hdr = None
    for i in range(min(8, len(raw_ps))):
        row_cells = raw_ps.iloc[i].tolist()
        row_str = [str(x) for x in row_cells]
        if any("运营商" in s for s in row_str) and any("共享私桩" in s for s in row_str):
            hdr = i
            break
    if hdr is not None:
        h = raw_ps.iloc[hdr].tolist()
        iop = h.index("运营商") if "运营商" in h else None
        cols = list(h)
        ish = None
        for j, x in enumerate(cols):
            if str(x).strip() == "共享私桩":
                ish = j
                break
        if iop is not None and ish is not None:
            for _, r in raw_ps.iloc[hdr + 1 :].iterrows():
                op = r.iloc[iop]
                if pd.isna(op):
                    continue
                sv = pd.to_numeric(r.iloc[ish] if ish < len(r) else None, errors="coerce")
                if pd.notna(sv):
                    share_map[str(op).strip()] = float(sv)

    gy = _read_operator_value_table(path, plan.op_gongyong)
    zy = _read_operator_value_table(path, plan.op_zhuanyong)
    dc = _read_operator_value_table(path, plan.op_dc)
    ac = _read_operator_value_table(path, plan.op_ac)
    ac3 = _read_operator_value_table(path, plan.op_ac3)
    pwr = _read_operator_charging_power_map(path, plan.op_power)
    okwh = _read_operator_electricity_map(path, plan.op_kwh)
    ost = _read_operator_value_table(path, plan.op_station)
    oswap = _read_operator_swap_station_from_facility_sheet(path, plan.swap_facility)

    recs = []
    for op, total in ops:
        recs.append(
            {
                "运营商": op,
                "公共充电设施总量": int(total) if total == int(total) else total,
                "新增销量": pd.NA,
                "星级场站数": pd.NA,
                "共享私桩": _lookup_operator_subtable(share_map, op),
                "公用充电桩": _lookup_operator_subtable(gy, op),
                "专用充电桩": _lookup_operator_subtable(zy, op),
                "直流桩": _lookup_operator_subtable(dc, op),
                "交流桩": _lookup_operator_subtable(ac, op),
                "三相交流桩": _lookup_operator_subtable(ac3, op),
                "充电功率": _lookup_operator_subtable(pwr, op),
                "充电电量（万度）": _lookup_operator_subtable(okwh, op),
                "充电站": _lookup_operator_subtable(ost, op),
                "换电站": _lookup_operator_subtable(oswap, op),
            }
        )
    main_names = {op for op, _ in ops}
    skip_labels = frozenset({"合计", "总计", "运营商"})
    sub_maps = (share_map, gy, zy, dc, ac, ac3, pwr, okwh, ost, oswap)
    extra_ops: List[str] = []
    seen_extra: set = set()
    for mp in sub_maps:
        for raw_k in mp.keys():
            ks = str(raw_k).strip()
            if not ks or ks in skip_labels:
                continue
            if _map_key_already_on_main_column(ks, main_names):
                continue
            if ks in seen_extra:
                continue
            seen_extra.add(ks)
            extra_ops.append(ks)
    extra_ops.sort()
    extra_ops = _dedupe_equivalent_extra_names(extra_ops)
    present_ops = {str(r["运营商"]).strip() for r in recs}
    for ex in extra_ops:
        vx = _operator_name_variants(ex)
        if any(vx & _operator_name_variants(p) for p in present_ops):
            continue
        present_ops.add(ex)
        recs.append(
            {
                "运营商": ex,
                "公共充电设施总量": pd.NA,
                "新增销量": pd.NA,
                "星级场站数": pd.NA,
                "共享私桩": _lookup_operator_subtable(share_map, ex),
                "公用充电桩": _lookup_operator_subtable(gy, ex),
                "专用充电桩": _lookup_operator_subtable(zy, ex),
                "直流桩": _lookup_operator_subtable(dc, ex),
                "交流桩": _lookup_operator_subtable(ac, ex),
                "三相交流桩": _lookup_operator_subtable(ac3, ex),
                "充电功率": _lookup_operator_subtable(pwr, ex),
                "充电电量（万度）": _lookup_operator_subtable(okwh, ex),
                "充电站": _lookup_operator_subtable(ost, ex),
                "换电站": _lookup_operator_subtable(oswap, ex),
            }
        )
    out_df = pd.DataFrame(recs, columns=OPERATOR_COLUMNS)
    if not out_df.empty and out_df["运营商"].duplicated().any():
        out_df = out_df.drop_duplicates(subset=["运营商"], keep="first")
    return out_df


def build_oem_dataframe(
    path: Path, plan: Optional[RawSheetPlan] = None
) -> pd.DataFrame:
    if plan is None:
        plan = load_sheet_plan(path)
    raw = _read_excel_safe(path, plan.oem_private, header=None)
    hdr = None
    for i in range(min(8, len(raw))):
        row = [str(x) for x in raw.iloc[i].tolist()]
        if "企业" in row and "数量" in row:
            hdr = i
            break
    if hdr is None:
        return pd.DataFrame(columns=OEM_COLUMNS)
    h = raw.iloc[hdr].tolist()
    ie = iq = None
    for j, x in enumerate(h):
        if str(x).strip() == "企业":
            ie = j
        if str(x).strip() == "数量":
            iq = j
            break
    if ie is None or iq is None:
        return pd.DataFrame(columns=OEM_COLUMNS)
    rows = []
    for _, r in raw.iloc[hdr + 1 :].iterrows():
        name = r.iloc[ie] if ie < len(r) else None
        if pd.isna(name):
            continue
        q = pd.to_numeric(r.iloc[iq] if iq < len(r) else None, errors="coerce")
        if pd.notna(q):
            rows.append(
                {
                    "车企名称": str(name).strip(),
                    "私桩安装量": int(q) if q == int(q) else float(q),
                }
            )
    return pd.DataFrame(rows, columns=OEM_COLUMNS)


def build_model_dataframe(
    path: Path, plan: Optional[RawSheetPlan] = None
) -> pd.DataFrame:
    if plan is None:
        plan = load_sheet_plan(path)
    raw = _read_excel_safe(path, plan.model_top_mfr, header=None)
    hdr = None
    for i in range(min(6, len(raw))):
        row = [str(x) for x in raw.iloc[i].tolist()]
        if "运营商" in row or "制造商" in row:
            hdr = i
            break
    if hdr is None:
        return pd.DataFrame(columns=MODEL_COLUMNS)
    h = raw.iloc[hdr].tolist()
    iname = None
    iv = None
    for j, x in enumerate(h):
        xs = str(x)
        if "制造商" in xs or "运营商" in xs:
            iname = j
        if "桩数量" in xs:
            iv = j
            break
    if iname is None or iv is None:
        return pd.DataFrame(columns=MODEL_COLUMNS)
    rows = []
    for _, r in raw.iloc[hdr + 1 :].iterrows():
        nm = r.iloc[iname] if iname < len(r) else None
        if pd.isna(nm):
            continue
        q = pd.to_numeric(r.iloc[iv] if iv < len(r) else None, errors="coerce")
        if pd.notna(q):
            rows.append(
                {
                    "设备型号": str(nm).strip(),
                    "装机量": int(q) if q == int(q) else float(q),
                }
            )
    return pd.DataFrame(rows, columns=MODEL_COLUMNS)


def build_city_placeholder() -> pd.DataFrame:
    return pd.DataFrame(columns=CITY_COLUMNS)


def build_swap_operator_placeholder() -> pd.DataFrame:
    """无原始簿时仅占位表头；有原始簿请用 build_swap_operator_dataframe。"""
    return pd.DataFrame(columns=SWAP_OP_COLUMNS)


def build_swap_operator_dataframe(
    path: Path, plan: Optional[RawSheetPlan] = None
) -> pd.DataFrame:
    """
    标准 00 表「换电站-运营商」Sheet：与「运营商」Sheet 的「换电站」列同源，
    取自原始簿「换电设施」中扫描到的「运营商」「数量」块（表头非首行）。
    """
    if plan is None:
        plan = load_sheet_plan(path)
    m = _read_operator_swap_station_from_facility_sheet(path, plan.swap_facility)
    if not m:
        return pd.DataFrame(columns=SWAP_OP_COLUMNS)
    rows = []
    for k, v in m.items():
        rows.append(
            {
                "运营商": k,
                "数量": int(v) if abs(v - round(v)) < 1e-9 else float(v),
            }
        )
    rows.sort(
        key=lambda r: r["数量"] if isinstance(r["数量"], (int, float)) else 0,
        reverse=True,
    )
    return pd.DataFrame(rows, columns=SWAP_OP_COLUMNS)


def build_standard00_workbook_bytes(
    path: Union[str, Path],
    *,
    source_filename: Optional[str] = None,
) -> Tuple[BytesIO, str]:
    """
    读取原始 xlsx，生成标准 00 表字节流。
    source_filename：用于从文件名解析 YYMM（临时文件路径时必传原始上传名）。
    返回 (BytesIO, 建议文件名不含路径)。
    """
    path = Path(path)
    nm = source_filename or path.name
    yymm = parse_yyyymm_from_filename(nm) or "YYMM"
    out_name = f"00表标准化-系统输入-{yymm}.xlsx"
    bio = BytesIO()
    plan = load_sheet_plan(path)
    prov = build_province_dataframe(path, plan)
    opdf = build_operator_dataframe(path, plan)
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        prov.to_excel(w, sheet_name="省份", index=False)
        opdf.to_excel(w, sheet_name="运营商", index=False)
        build_city_placeholder().to_excel(w, sheet_name="城市", index=False)
        build_model_dataframe(path, plan).to_excel(w, sheet_name="型号", index=False)
        build_oem_dataframe(path, plan).to_excel(w, sheet_name="车企", index=False)
        build_swap_operator_dataframe(path, plan).to_excel(
            w, sheet_name="换电站-运营商", index=False
        )
    bio.seek(0)
    return bio, out_name


def build_standard00_workbook_from_bytes(
    data: bytes, original_filename: str
) -> Tuple[BytesIO, str]:
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tf:
        tf.write(data)
        tpath = Path(tf.name)
    try:
        return build_standard00_workbook_bytes(tpath, source_filename=original_filename)
    finally:
        try:
            tpath.unlink(missing_ok=True)
        except OSError:
            pass


def build_standard00_workbook_from_uploaded(uploaded_file) -> Tuple[BytesIO, str]:
    """Streamlit UploadedFile。"""
    raw = uploaded_file.read()
    if hasattr(uploaded_file, "seek"):
        uploaded_file.seek(0)
    name = getattr(uploaded_file, "name", "raw.xlsx") or "raw.xlsx"
    return build_standard00_workbook_from_bytes(raw, name)
