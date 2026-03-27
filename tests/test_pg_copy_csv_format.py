# tests/test_pg_copy_csv_format.py — 验证与 insert_df_to_table_pg_copy 一致的 CSV 缓冲格式（含中文列与特殊字符）

import csv
from io import StringIO

import pandas as pd


def _df_to_copy_buffer(df: pd.DataFrame) -> StringIO:
    buf = StringIO()
    df.to_csv(
        buf,
        index=False,
        header=False,
        encoding="utf-8",
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n",
        doublequote=True,
    )
    buf.seek(0)
    return buf


def test_chinese_columns_and_comma_in_field():
    df = pd.DataFrame(
        {
            "省份_中文": ["江苏,南", "浙江"],
            "备注": ['say "hi"', "a\nb"],
            "序号": [1, 2],
        }
    )
    buf = _df_to_copy_buffer(df)
    buf.seek(0)
    rows = list(csv.reader(buf, delimiter=",", doublequote=True, quotechar='"'))
    assert len(rows) == 2
    assert rows[0][0] == "江苏,南"
    assert rows[0][1] == 'say "hi"'
    assert rows[1][0] == "浙江"
    assert "a" in rows[1][1] and "b" in rows[1][1]


def test_empty_and_nan_like_null():
    df = pd.DataFrame({"a": ["x", "", None], "b": [1.0, float("nan"), 3.0]})
    buf = _df_to_copy_buffer(df)
    text = buf.getvalue()
    assert "x" in text
    lines = [ln for ln in text.strip().split("\n") if ln]
    assert len(lines) == 3


if __name__ == "__main__":
    test_chinese_columns_and_comma_in_field()
    test_empty_and_nan_like_null()
    print("ok")
