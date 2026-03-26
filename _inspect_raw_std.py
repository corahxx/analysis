# -*- coding: utf-8 -*-
"""Dump raw vs standard 00 workbook structure to JSON (UTF-8)."""
import json
from pathlib import Path

import pandas as pd

RAW = Path(r"D:\充电数据汇总\00表合集\00、充换电数据梳理图表-202602 - 能源局.xlsx")
STD = Path(r"C:\Users\HONOR\Desktop\充电标准七张表输出\00表标准化-系统输入-2602.xlsx")
OUT = Path(__file__).resolve().parent / "_raw_std_inspect.json"


def dump(path: Path, max_sheets: int = 40) -> dict:
    xl = pd.ExcelFile(path, engine="openpyxl")
    info = {"path": str(path), "sheets": {}}
    for i, s in enumerate(xl.sheet_names):
        if i >= max_sheets:
            info["sheets"]["_truncated_"] = len(xl.sheet_names) - max_sheets
            break
        df = pd.read_excel(path, sheet_name=s, header=None)
        sub = df.iloc[:6, : min(14, df.shape[1])]
        head = sub.astype(object).where(pd.notna(sub), None)
        info["sheets"][s] = {
            "shape": [int(df.shape[0]), int(df.shape[1])],
            "head6": head.values.tolist(),
        }
    return info


def main():
    out = {"raw": dump(RAW), "standard": dump(STD, 20)}
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
