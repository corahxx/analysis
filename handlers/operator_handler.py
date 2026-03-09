# handlers/operator_handler.py - 各运营商数据

from typing import Optional
import pandas as pd


def _operator_col(df: pd.DataFrame) -> Optional[str]:
    if "运营商名称" in df.columns:
        return "运营商名称"
    if "上报机构" in df.columns:
        return "上报机构"
    return None


def operator_table(df: pd.DataFrame, for_pile: bool = True) -> pd.DataFrame:
    """运营商数据明细：排名、运营商、设施总量、环比增量、环比增速（占位）。"""
    op_col = _operator_col(df)
    id_col = "充电桩编号" if for_pile and "充电桩编号" in df.columns else None
    if not for_pile:
        id_col = "所属充电站编号" if "所属充电站编号" in df.columns else None
    if op_col is None:
        return pd.DataFrame(columns=["排名", "运营商", "设施总量", "环比增量", "环比增速"])
    if for_pile and id_col:
        agg = df.groupby(op_col, dropna=False)[id_col].nunique()
    else:
        agg = df.groupby(op_col, dropna=False).size()
    agg = agg.sort_values(ascending=False).reset_index()
    agg.columns = ["运营商", "设施总量"]
    agg.insert(0, "排名", range(1, len(agg) + 1))
    agg["环比增量"] = "—"
    agg["环比增速"] = "—"
    return agg
