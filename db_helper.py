# db_helper.py - evdata 库：表列表、读表、建表、入库
# 连接方式与 charging-agent 一致：get_db_url、连接池、test_connection

from typing import List, Optional, Tuple
import os
import pandas as pd

from config import DB_CONFIG

# 充电站位置列最长 600 字（与 merge 规范一致）
LOCATION_MAX_LEN = 600
DEFAULT_VARCHAR_LEN = 500
DATE_VARCHAR_LEN = 50


def _get_config():
    """与 charging-agent 一致：环境变量优先于 config。"""
    return {
        "host": os.getenv("DB_HOST") or DB_CONFIG.get("host", "localhost"),
        "port": int(os.getenv("DB_PORT") or str(DB_CONFIG.get("port", 3306))),
        "user": os.getenv("DB_USER") or DB_CONFIG.get("user", "root"),
        "password": os.getenv("DB_PASSWORD") or DB_CONFIG.get("password", ""),
        "database": os.getenv("DB_NAME") or DB_CONFIG.get("database", "evdata"),
    }


def get_db_url() -> str:
    """与 charging-agent db_utils.get_db_url 一致。"""
    cfg = _get_config()
    return (
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}?charset=utf8mb4"
    )

_global_engine = None


def _engine():
    """返回 SQLAlchemy engine（连接池与 charging-agent 一致，单例复用）。"""
    global _global_engine
    if _global_engine is None:
        from sqlalchemy import create_engine
        _global_engine = create_engine(
            get_db_url(),
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_timeout=30,
        )
    return _global_engine


def get_connection():
    """返回 SQLAlchemy engine，供建表与入库共用。"""
    return _engine()


def test_connection() -> Tuple[bool, str]:
    """与 charging-agent db_utils.test_connection 一致。返回 (success, message)。"""
    engine = None
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(
            get_db_url(),
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "数据库连接成功"
    except Exception as e:
        return False, str(e)[:500]
    finally:
        if engine:
            try:
                engine.dispose()
            except Exception:
                pass


def list_tables_with_status() -> Tuple[List[str], Optional[str]]:
    """返回 (表名列表, 错误信息)。连接失败时 error 为异常信息；成功无表为 ([], None)。"""
    try:
        import pymysql
        cfg = _get_config()
        conn = pymysql.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"],
            database=cfg["database"],
            charset="utf8mb4",
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW TABLES")
                rows = cur.fetchall()
                if rows and len(rows[0]) >= 1:
                    return ([r[0] for r in rows], None)
                return ([], None)
        finally:
            conn.close()
    except Exception as e:
        return ([], str(e)[:500])


def list_tables() -> List[str]:
    """返回 evdata 库内所有表名列表。连接失败返回空列表。"""
    tables, _ = list_tables_with_status()
    return tables


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


def suggest_mysql_type(col_name: str, dtype) -> str:
    """根据列名与 dtype 推断建议的 MySQL 类型（通用，用于「其他」类型表格）。"""
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE"
    if pd.api.types.is_bool_dtype(dtype):
        return "VARCHAR(10)"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return f"VARCHAR({DATE_VARCHAR_LEN})"
    # 字符串、object 或未知
    if col_name == "充电站位置":
        return f"VARCHAR({LOCATION_MAX_LEN})"
    if any(k in col_name for k in ("时间", "日期", "投入使用", "开通", "生产", "入库")):
        return f"VARCHAR({DATE_VARCHAR_LEN})"
    return f"VARCHAR({DEFAULT_VARCHAR_LEN})"


# 可供用户选择的 MySQL 类型（「其他」表结构设置用）
MYSQL_TYPE_OPTIONS = [
    "VARCHAR(50)", "VARCHAR(100)", "VARCHAR(255)", "VARCHAR(500)", "VARCHAR(1000)",
    "TEXT", "INT", "BIGINT", "DOUBLE", "DECIMAL(18,2)", "DATE", "DATETIME",
]


def create_table_from_schema(engine, table_name: str, schema: List[Tuple[str, str]]) -> bool:
    """
    根据 (列名, MySQL类型) 列表创建表。若表已存在则不覆盖，返回 False；否则创建并返回 True。
    schema: [(col_name, mysql_type_str), ...]，如 [("姓名", "VARCHAR(100)"), ("年龄", "INT")]。
    """
    from sqlalchemy import text
    with engine.connect() as conn:
        r = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
        if r.fetchone():
            return False
    cols = []
    for col_name, mysql_t in schema:
        safe_c = col_name.replace("`", "``")
        cols.append(f"`{safe_c}` {mysql_t}")
    create_sql = f"CREATE TABLE `{table_name}` (" + ", ".join(cols) + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()
    return True


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
