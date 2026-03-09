# handlers/highway_handler.py - 高速公路建设（占位表）

import pandas as pd


def highway_provinces_table(df: pd.DataFrame) -> pd.DataFrame:
    """高速公路各省明细（占位：当前数据无高速专用字段，返回空或基于省份的简单汇总）。"""
    prov_col = "省份_中文" if "省份_中文" in df.columns else "省份" if "省份" in df.columns else None
    if prov_col is None:
        return pd.DataFrame(columns=["省份", "已建设及预留服务区(台)", "已建设停车位(台)"])
    agg = df.groupby(df[prov_col].fillna("未知"), dropna=False).size().sort_values(ascending=False).head(10)
    return pd.DataFrame({
        "省份": agg.index.tolist(),
        "已建设及预留服务区(台)": ["—"] * len(agg),
        "已建设停车位(台)": agg.values.tolist(),
    })
