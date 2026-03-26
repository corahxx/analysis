# -*- coding: utf-8 -*-
"""Compare one province between raw-derived and standard 00 table."""
import pandas as pd
from pathlib import Path

RAW = Path(r"D:\充电数据汇总\00表合集\00、充换电数据梳理图表-202602 - 能源局.xlsx")
STD = Path(r"C:\Users\HONOR\Desktop\充电标准七张表输出\00表标准化-系统输入-2602.xlsx")

std = pd.read_excel(STD, sheet_name="省份", header=0)
row = std[std["省份"].astype(str).str.strip() == "广东省"].iloc[0]
print("STANDARD 广东省:")
for c in std.columns:
    print(f"  {c!r}: {row[c]!r}")

# raw pieces
m = pd.read_excel(RAW, sheet_name="1各省公共桩-地图-1", header=None)
# row1 is header 序号 省份 数量
hdr = m.iloc[1].tolist()
print("map header", hdr)
dfm = pd.read_excel(RAW, sheet_name="1各省公共桩-地图-1", header=1)
r = dfm[dfm["省份"].astype(str).str.strip() == "广东省"].iloc[0]
print("RAW 地图 广东省 数量", r["数量"])

dc = pd.read_excel(RAW, sheet_name="1交直流-1", header=1)
r2 = dc[dc["省份_中文"].astype(str).str.strip() == "广东省"].iloc[0]
print("RAW 交直流", r2.to_dict())

hw = pd.read_excel(RAW, sheet_name="11高速公路", header=0)
print("11高速 columns", hw.columns.tolist())
# normalize province in hw
hw["_p"] = hw["省份"].astype(str).str.replace(r"\s+", "", regex=True)
sub = hw[hw["_p"].str.contains("广东", na=False)]
print("RAW 高速 广东行\n", sub)
