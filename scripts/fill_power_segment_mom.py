# -*- coding: utf-8 -*-
"""命令行入口：功率段 xlsx 环比填充（逻辑见 handlers.power_table_mom）。"""
from __future__ import annotations

import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _root not in sys.path:
    sys.path.insert(0, _root)

from handlers.power_table_mom import run_fill_power_mom_on_folder  # noqa: E402


def main() -> int:
    if len(sys.argv) < 2:
        print("用法: python scripts/fill_power_segment_mom.py <含xlsx的目录>")
        return 1
    ok, msg, details = run_fill_power_mom_on_folder(sys.argv[1])
    print(msg)
    for line in details:
        print(line)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
