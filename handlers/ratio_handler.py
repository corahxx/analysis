# handlers/ratio_handler.py - 车桩比（占位表）

import pandas as pd


def ratio_summary_cards(df: pd.DataFrame, for_pile: bool = True) -> dict:
    """车桩比汇总卡片（占位：无车辆数据时仅显示桩/站总量）。"""
    if for_pile and "充电桩编号" in df.columns:
        total = df["充电桩编号"].nunique()
        return {"公共充电桩总量": total, "新能源车保有量": "—", "车桩比": "—"}
    if "所属充电站编号" in df.columns:
        total = df["所属充电站编号"].nunique()
        return {"充电站总量": total, "新能源车保有量": "—", "车桩比": "—"}
    return {"总量": len(df), "车桩比": "—"}


def ratio_provinces_table(df: pd.DataFrame) -> pd.DataFrame:
    """各省车桩比排名（占位：无车辆数据）。"""
    prov_col = "省份_中文" if "省份_中文" in df.columns else "省份" if "省份" in df.columns else None
    if prov_col is None:
        return pd.DataFrame(columns=["排名", "省份", "车桩比", "保有量", "充电桩", "评价"])
    agg = df.groupby(df[prov_col].fillna("未知"), dropna=False).size().sort_values(ascending=False).head(15)
    return pd.DataFrame({
        "排名": range(1, len(agg) + 1),
        "省份": agg.index.tolist(),
        "车桩比": ["—"] * len(agg),
        "保有量": ["—"] * len(agg),
        "充电桩": agg.values.tolist(),
        "评价": ["—"] * len(agg),
    })
