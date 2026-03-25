# handlers/ratio_handler.py - 车桩比 P8（占位；口径：序号）

import pandas as pd

from .data_utils import count_piles, count_stations, agg_pile_count


def ratio_summary_cards(df: pd.DataFrame, for_pile: bool = True) -> dict:
    """车桩比汇总卡片（占位：无车辆数据时仅显示桩/站总量，口径序号/充电站内部编号+站内>2）。"""
    if for_pile:
        total = count_piles(df, True)
        return {"公共充电桩总量": total, "新能源车保有量": "—", "车桩比": "—"}
    total = count_stations(df, False, apply_filter=True)
    return {"充电站总量": total, "新能源车保有量": "—", "车桩比": "—"}


def ratio_provinces_table(df: pd.DataFrame, for_pile: bool = True) -> pd.DataFrame:
    """各省车桩比排名（占位：无车辆数据；充电桩列用序号计数）。"""
    prov_col = "省份_中文" if "省份_中文" in df.columns else "省份" if "省份" in df.columns else None
    if prov_col is None:
        return pd.DataFrame(columns=["排名", "省份", "车桩比", "保有量", "充电桩", "评价"])
    agg = agg_pile_count(df, prov_col, for_pile).sort_values(ascending=False).head(15)
    return pd.DataFrame({
        "排名": range(1, len(agg) + 1),
        "省份": agg.index.tolist(),
        "车桩比": ["—"] * len(agg),
        "保有量": ["—"] * len(agg),
        "充电桩": agg.values.tolist(),
        "评价": ["—"] * len(agg),
    })


def ratio_vehicle_pile_product_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    车桩比产品表（标准列）：省份、新能源车保有量、公共充电设施数量、车桩比、评价。
    省份分组用「省份_中文」，否则「省份」。
    公共充电设施数量 = 各省数据条数（groupby 后 size）。
    保有量、车桩比、评价暂空（空字符串占位）。
    """
    prov_col = "省份_中文" if "省份_中文" in df.columns else "省份" if "省份" in df.columns else None
    empty_cols = ["省份", "新能源车保有量", "公共充电设施数量", "车桩比", "评价"]
    if prov_col is None:
        return pd.DataFrame(columns=empty_cols)
    grp = df.groupby(df[prov_col].fillna("未知").astype(str), dropna=False).size().reset_index(name="公共充电设施数量")
    grp = grp.rename(columns={prov_col: "省份"})
    grp["新能源车保有量"] = ""
    grp["车桩比"] = ""
    grp["评价"] = ""
    grp = grp.sort_values("公共充电设施数量", ascending=False).reset_index(drop=True)
    return grp[["省份", "新能源车保有量", "公共充电设施数量", "车桩比", "评价"]]
