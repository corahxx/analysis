# handlers/product_bundle.py - 一键打包七类标准数据产品（单份 ZIP）

import zipfile
from io import BytesIO
from typing import Optional

import pandas as pd


def build_seven_products_zip_bytes(
    df,
    for_pile: bool,
    export_date: str,
    *,
    ratio_mod=None,
    power_mod=None,
    rank_mod=None,
    nat_mod=None,
    prov_mod=None,
    op_mod=None,
) -> Optional[BytesIO]:
    """
    在已加载的 df 上生成七类产品文件并打入 ZIP。
    各 *_mod 由 app 注入（避免循环导入）；缺模块时跳过对应文件。
    """
    buf = BytesIO()
    prefix = f"标准化数据产品_{export_date}"

    def _add_xlsx(arc_name: str, xlsx_bytes: bytes) -> None:
        if xlsx_bytes:
            zf.writestr(f"{prefix}/{arc_name}", xlsx_bytes)

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # 1 车桩比
        if ratio_mod is not None:
            try:
                t = ratio_mod.ratio_vehicle_pile_product_table(df)
                bio = BytesIO()
                t.to_excel(bio, index=False, engine="openpyxl")
                _add_xlsx("01_车桩比.xlsx", bio.getvalue())
            except Exception:
                pass

        # 2 高速公路（占位说明）
        hw = BytesIO()
        pd.DataFrame({"说明": ["高速公路产品待业务定义，此文件为占位。"]}).to_excel(
            hw, index=False, engine="openpyxl"
        )
        _add_xlsx("02_高速公路_占位.xlsx", hw.getvalue())

        # 3 功率段分布
        if power_mod is not None:
            try:
                wb = power_mod.write_power_province_workbook(df, for_pile=for_pile)
                if wb is not None:
                    _add_xlsx("03_功率段分布.xlsx", wb.getvalue())
            except Exception:
                pass

        # 4 排行榜
        if rank_mod is not None:
            try:
                rb = rank_mod.write_ranking_workbook_bytes(df, for_pile=for_pile)
                _add_xlsx("04_排行榜.xlsx", rb.getvalue())
            except Exception:
                pass

        # 5 全国概况
        if nat_mod is not None:
            try:
                nb = nat_mod.write_national_workbook_bytes(df, for_pile=for_pile)
                _add_xlsx("05_全国概况.xlsx", nb.getvalue())
            except Exception:
                pass

        # 6 省级数据
        if prov_mod is not None:
            try:
                pb = prov_mod.write_provincial_workbook_bytes(df, for_pile=for_pile)
                if pb is not None:
                    _add_xlsx("06_省级数据.xlsx", pb.getvalue())
            except Exception:
                pass

        # 7 运营商概况
        if op_mod is not None:
            try:
                ob = op_mod.write_operator_workbook_bytes(df, for_pile=for_pile)
                _add_xlsx("07_运营商概况.xlsx", ob.getvalue())
            except Exception:
                pass

    buf.seek(0)
    return buf
