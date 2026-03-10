# db_helper.py - evdata 库：表列表、读表、建表、入库

from typing import List, Optional, Tuple
import pandas as pd

from config import DB_CONFIG

# 充电站位置列最长 600 字（与 merge 规范一致）
LOCATION_MAX_LEN = 600
DEFAULT_VARCHAR_LEN = 500
DATE_VARCHAR_LEN = 50


def _engine():
    """返回 SQLAlchemy engine（utf8mb4）。"""
    from sqlalchemy import create_engine
    url = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset=utf8mb4"
    )
    return create_engine(url)


def get_connection():
    """返回 SQLAlchemy engine，供建表与入库共用。"""
    return _engine()


def list_tables() -> List[str]:
    """返回 evdata 库内所有表名列表。连接失败返回空列表。"""
    try:
        import pymysql
        conn = pymysql.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            charset="utf8mb4",
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW TABLES")
                rows = cur.fetchall()
                if rows and len(rows[0]) >= 1:
                    return [r[0] for r in rows]
                return []
        finally:
            conn.close()
    except Exception:
        return []


def read_table(table_name: str) -> Optional[pd.DataFrame]:
    """将指定表读为 DataFrame。表名需存在，否则返回 None。"""
    if not table_name or not table_name.strip():
        return None
    try:
        engine = _engine()
        return pd.read_sql(f"SELECT * FROM `{table_name}`", engine)
    except Exception:
        return None


def _mysql_type(col_name: str, dtype) -> str:
    """根据列名与 dtype 返回 MySQL 类型。经度/纬度 INT；序号不设主键（仅列类型 BIGINT）。"""
    if col_name in ("经度", "纬度"):
        return "INT"
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE"
    if pd.api.types.is_bool_dtype(dtype):
        return "VARCHAR(10)"
    # 字符串或对象
    if col_name == "充电站位置":
        return f"VARCHAR({LOCATION_MAX_LEN})"
    if any(k in col_name for k in ("时间", "日期", "投入使用", "开通", "生产", "入库")):
        return f"VARCHAR({DATE_VARCHAR_LEN})"
    return f"VARCHAR({DEFAULT_VARCHAR_LEN})"


def create_table_from_df(engine, table_name: str, df: pd.DataFrame) -> bool:
    """根据 DataFrame 创建表。若表已存在则不覆盖，返回 False；否则创建并返回 True。"""
    from sqlalchemy import text
    with engine.connect() as conn:
        r = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
        if r.fetchone():
            return False
    cols = []
    for c in df.columns:
        dtype = df[c].dtype
        mysql_t = _mysql_type(c, dtype)
        safe_c = c.replace("`", "``")
        cols.append(f"`{safe_c}` {mysql_t}")
    create_sql = f"CREATE TABLE `{table_name}` (" + ", ".join(cols) + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()
    return True


def insert_df_to_table(
    engine, table_name: str, df: pd.DataFrame
) -> Tuple[int, int, List[str]]:
    """
    将 DataFrame 追加写入表。表必须已存在且列兼容。
    返回 (success_count, fail_count, error_messages)。
    """
    try:
        df.to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
        return (len(df), 0, [])
    except Exception as e:
        err_msg = str(e)
        # 若批量失败，尝试逐行以统计成功/失败
        success = 0
        fail = 0
        errors: List[str] = []
        for i, row in df.iterrows():
            try:
                row_df = pd.DataFrame([row])
                row_df.to_sql(
                    table_name,
                    engine,
                    if_exists="append",
                    index=False,
                )
                success += 1
            except Exception as row_e:
                fail += 1
                if len(errors) < 20:
                    errors.append(f"行{i+1}: {str(row_e)[:200]}")
        if success == 0 and fail == 0:
            return (0, len(df), [err_msg[:500]])
        return (success, fail, errors[:20])
