# db_helper.py - 表列表、读表、建表、入库（支持 MySQL / PostgreSQL）

from typing import List, Optional, Tuple, Any, Dict
import os
import pandas as pd

from config import DB_CONFIG

# 充电站位置列最长 600 字（与 merge 规范一致）
LOCATION_MAX_LEN = 600
DEFAULT_VARCHAR_LEN = 500
DATE_VARCHAR_LEN = 50

# 默认 PostgreSQL 连接（与界面占位一致；可通过表单或 pg_config 覆盖）
DEFAULT_PG_PORT = 5432
DEFAULT_PG_DATABASE = "evdata"
DEFAULT_PG_PASSWORD = "Admin2026"


def _get_config() -> Dict[str, Any]:
    """MySQL：环境变量优先于 config。"""
    return {
        "host": os.getenv("DB_HOST") or DB_CONFIG.get("host", "localhost"),
        "port": int(os.getenv("DB_PORT") or str(DB_CONFIG.get("port", 3306))),
        "user": os.getenv("DB_USER") or DB_CONFIG.get("user", "root"),
        "password": os.getenv("DB_PASSWORD") or DB_CONFIG.get("password", ""),
        "database": os.getenv("DB_NAME") or DB_CONFIG.get("database", "evdata"),
    }


def _mysql_url(cfg: Dict[str, Any]) -> str:
    return (
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}?charset=utf8mb4"
    )


def _pg_url(pg_config: Dict[str, Any]) -> str:
    """PostgreSQL 连接 URL。pg_config: host, port, user, password, database。"""
    from urllib.parse import quote_plus
    host = pg_config.get("host") or "localhost"
    port = int(pg_config.get("port") or DEFAULT_PG_PORT)
    user = pg_config.get("user") or "postgres"
    password = pg_config.get("password") or DEFAULT_PG_PASSWORD
    database = pg_config.get("database") or DEFAULT_PG_DATABASE
    return f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}"


def get_db_url(backend: str = "mysql", pg_config: Optional[Dict[str, Any]] = None) -> str:
    if backend == "postgresql" and pg_config:
        return _pg_url(pg_config)
    return _mysql_url(_get_config())


_global_mysql_engine = None
_pg_engine_cache: Dict[str, Any] = {}


def _engine_mysql():
    global _global_mysql_engine
    if _global_mysql_engine is None:
        from sqlalchemy import create_engine
        _global_mysql_engine = create_engine(
            _mysql_url(_get_config()),
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_timeout=30,
        )
    return _global_mysql_engine


def _engine_pg(pg_config: Dict[str, Any]):
    """PostgreSQL engine，同配置复用。"""
    from sqlalchemy import create_engine
    key = str(sorted((k, v) for k, v in (pg_config or {}).items()))
    if key not in _pg_engine_cache:
        _pg_engine_cache[key] = create_engine(
            _pg_url(pg_config),
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
    return _pg_engine_cache[key]


def get_connection(backend: str = "mysql", pg_config: Optional[Dict[str, Any]] = None):
    """返回 SQLAlchemy engine。backend: 'mysql' | 'postgresql'；pg_config 仅当 backend=='postgresql' 时使用。"""
    if backend == "postgresql" and pg_config:
        return _engine_pg(pg_config)
    return _engine_mysql()


def test_connection(backend: str = "mysql", pg_config: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
    """测试连接。返回 (success, message)。"""
    engine = None
    try:
        from sqlalchemy import create_engine, text
        if backend == "postgresql" and pg_config:
            url = _pg_url(pg_config)
        else:
            url = _mysql_url(_get_config())
        engine = create_engine(url, pool_size=1, max_overflow=0, pool_pre_ping=True)
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


def list_pg_schemas(pg_config: Dict[str, Any]) -> Tuple[List[str], Optional[str]]:
    """列出 PostgreSQL 中用户可见的业务 schema（排除系统 schema）。"""
    try:
        from sqlalchemy import text
        engine = get_connection("postgresql", pg_config)
        q = text(
            """
            SELECT nspname FROM pg_catalog.pg_namespace
            WHERE nspname NOT IN ('pg_catalog', 'information_schema')
              AND nspname NOT LIKE 'pg_toast%%'
              AND nspname NOT LIKE 'pg_temp%%'
            ORDER BY nspname
            """
        )
        with engine.connect() as conn:
            r = conn.execute(q)
            return ([row[0] for row in r.fetchall()], None)
    except Exception as e:
        return ([], str(e)[:500])


def list_tables_with_status(
    backend: str = "mysql",
    pg_config: Optional[Dict[str, Any]] = None,
    pg_schema: Optional[str] = None,
) -> Tuple[List[str], Optional[str]]:
    """返回 (表名列表, 错误信息)。PostgreSQL 须指定 pg_schema 才列出该 schema 下的表。"""
    if backend == "postgresql" and pg_config:
        if not pg_schema or not str(pg_schema).strip():
            return ([], None)
        try:
            from sqlalchemy import text
            engine = get_connection(backend, pg_config)
            with engine.connect() as conn:
                r = conn.execute(
                    text("SELECT tablename FROM pg_tables WHERE schemaname = :s ORDER BY tablename"),
                    {"s": pg_schema.strip()},
                )
                rows = r.fetchall()
                return ([row[0] for row in rows], None)
        except Exception as e:
            return ([], str(e)[:500])
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


def list_tables(
    backend: str = "mysql",
    pg_config: Optional[Dict[str, Any]] = None,
    pg_schema: Optional[str] = None,
) -> List[str]:
    tables, _ = list_tables_with_status(backend, pg_config, pg_schema)
    return tables


def get_table_column_names(
    table_name: str,
    backend: str = "mysql",
    pg_config: Optional[Dict[str, Any]] = None,
    pg_schema: Optional[str] = None,
    engine: Any = None,
) -> Optional[List[str]]:
    """返回表的实际列名列表（顺序与库一致）；失败返回 None。传入 engine 时忽略 backend/pg_config 建连。"""
    if not table_name or not str(table_name).strip():
        return None
    tn = table_name.strip()
    try:
        from sqlalchemy import text

        eng = engine if engine is not None else get_connection(backend, pg_config)
        try:
            is_pg = eng.dialect.name == "postgresql"
        except Exception:
            is_pg = False
        if is_pg:
            if not pg_schema or not str(pg_schema).strip():
                return None
            sch = pg_schema.strip()
            with eng.connect() as conn:
                r = conn.execute(
                    text(
                        """
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema = :sch AND table_name = :tn
                        ORDER BY ordinal_position
                        """
                    ),
                    {"sch": sch, "tn": tn},
                )
                return [row[0] for row in r.fetchall()]
        with eng.connect() as conn:
            r = conn.execute(
                text(
                    """
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :tn
                    ORDER BY ORDINAL_POSITION
                    """
                ),
                {"tn": tn},
            )
            rows = r.fetchall()
            return [row[0] for row in rows] if rows else []
    except Exception:
        return None


def read_table(
    table_name: str,
    backend: str = "mysql",
    pg_config: Optional[Dict[str, Any]] = None,
    pg_schema: Optional[str] = None,
    columns: Optional[List[str]] = None,
    chunksize: Optional[int] = None,
) -> Optional[pd.DataFrame]:
    """
    读取整表或指定列。columns 非空时仅 SELECT 这些列，显著减少 IO（大表必选）。
    chunksize 非空时按块读取并 concat（内存峰值更低，总时间可能略增）。
    """
    if not table_name or not table_name.strip():
        return None
    try:
        engine = get_connection(backend, pg_config)
        if backend == "postgresql" and pg_config:
            from sqlalchemy import text
            if not pg_schema or not str(pg_schema).strip():
                return None
            qs = pg_schema.strip().replace('"', '""')
            qt = table_name.strip().replace('"', '""')
            if columns:
                cols_sql = ", ".join(
                    '"' + c.replace('"', '""') + '"' for c in columns if c
                )
                sql = f'SELECT {cols_sql} FROM "{qs}"."{qt}"'
            else:
                sql = f'SELECT * FROM "{qs}"."{qt}"'
            with engine.connect() as conn:
                if chunksize:
                    parts = pd.read_sql(text(sql), conn, chunksize=chunksize)
                    return pd.concat(parts, ignore_index=True)
                return pd.read_sql(text(sql), conn)
        if columns:
            cols_sql = ", ".join("`" + c.replace("`", "``") + "`" for c in columns if c)
            sql = f"SELECT {cols_sql} FROM `{table_name.strip()}`"
        else:
            sql = f"SELECT * FROM `{table_name.strip()}`"
        if chunksize:
            parts = pd.read_sql(sql, engine, chunksize=chunksize)
            return pd.concat(parts, ignore_index=True)
        return pd.read_sql(sql, engine)
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


def _quote_ident(engine, name: str, style: str = "mysql") -> str:
    """风格：mysql 用反引号，postgresql 用双引号。"""
    try:
        if engine.dialect.name == "postgresql":
            return '"' + name.replace('"', '""') + '"'
    except Exception:
        pass
    return "`" + name.replace("`", "``") + "`"


def _table_exists_sql(engine, table_name: str, pg_schema: Optional[str] = None):
    """返回 (text_stmt, params) 用于执行后判断表是否存在（PostgreSQL 须传业务 schema）。"""
    from sqlalchemy import text
    try:
        if engine.dialect.name == "postgresql":
            sch = (pg_schema or "public").strip()
            return (
                text("SELECT 1 FROM pg_tables WHERE schemaname = :s AND tablename = :t"),
                {"s": sch, "t": table_name},
            )
    except Exception:
        pass
    return text(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :t"
    ), {"t": table_name}


def table_exists(
    engine,
    table_name: str,
    pg_schema: Optional[str] = None,
) -> bool:
    """判断表是否存在。"""
    from sqlalchemy import text
    if not table_name or not str(table_name).strip():
        return False
    tn = table_name.strip()
    stmt, params = _table_exists_sql(engine, tn, pg_schema)
    with engine.connect() as conn:
        return conn.execute(stmt, params).fetchone() is not None


def _qualified_table_sql(engine, table_name: str, pg_schema: Optional[str] = None) -> str:
    """生成带引号的限定表名，用于 DDL/DML。"""
    q = lambda n: _quote_ident(engine, n)
    tn = table_name.strip()
    try:
        if engine.dialect.name == "postgresql" and pg_schema and str(pg_schema).strip():
            return f"{q(pg_schema.strip())}.{q(tn)}"
    except Exception:
        pass
    return q(tn)


def staging_table_name(target_table: str) -> str:
    """与正式表同结构的临时表名（MySQL 表名最长 64）。"""
    import hashlib

    base = f"{target_table.strip()}_st_import_temp"
    if len(base) <= 64:
        return base
    h = hashlib.md5(target_table.encode("utf-8")).hexdigest()[:10]
    short = f"{target_table.strip()[:44]}_{h}_tmp"
    return short[:64]


def create_table_like(
    engine,
    new_table: str,
    like_table: str,
    pg_schema: Optional[str] = None,
) -> bool:
    """
    按已有表复制结构创建新表。若 new_table 已存在则返回 False。
    PostgreSQL 使用 LIKE ... INCLUDING ALL。
    """
    from sqlalchemy import text
    if not new_table or not like_table:
        return False
    if table_exists(engine, new_table.strip(), pg_schema):
        return False
    qn = _qualified_table_sql(engine, new_table.strip(), pg_schema)
    ql = _qualified_table_sql(engine, like_table.strip(), pg_schema)
    try:
        if engine.dialect.name == "postgresql":
            ddl = f"CREATE TABLE {qn} (LIKE {ql} INCLUDING ALL)"
        else:
            ddl = f"CREATE TABLE {qn} LIKE {ql}"
    except Exception:
        ddl = f"CREATE TABLE {qn} LIKE {ql}"
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
    return True


def truncate_table(engine, table_name: str, pg_schema: Optional[str] = None) -> None:
    from sqlalchemy import text
    qt = _qualified_table_sql(engine, table_name.strip(), pg_schema)
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {qt}"))
        conn.commit()


def ensure_staging_table(
    engine,
    target_table: str,
    pg_schema: Optional[str] = None,
) -> str:
    """确保存在与 target 同结构的 staging 表，返回 staging 表名。"""
    st = staging_table_name(target_table)
    if not table_exists(engine, st, pg_schema):
        create_table_like(engine, st, target_table.strip(), pg_schema)
    return st


def _align_df_to_table_columns(df: pd.DataFrame, table_cols: List[str]) -> pd.DataFrame:
    """仅保留表中存在且 DataFrame 中有的列，顺序与 table_cols 一致。"""
    use = [c for c in table_cols if c in df.columns]
    if not use:
        return pd.DataFrame()
    return df[use].copy()


def _build_insert_from_staging_sql(
    engine,
    target_table: str,
    staging_table: str,
    columns: List[str],
    pg_schema: Optional[str] = None,
) -> Any:
    """生成 INSERT INTO 正式表 ... SELECT ... FROM 临时表（含 uid 去重逻辑）。"""
    from sqlalchemy import text

    qt = _qualified_table_sql(engine, target_table, pg_schema)
    qs = _qualified_table_sql(engine, staging_table, pg_schema)
    qcol = lambda c: _quote_ident(engine, c)
    col_sql = ", ".join(qcol(c) for c in columns)
    is_pg = False
    try:
        is_pg = engine.dialect.name == "postgresql"
    except Exception:
        pass

    has_uid = "uid" in columns
    has_ruku = "入库时间" in columns

    if is_pg and has_uid:
        order_inner = f'{qcol("uid")}'
        if has_ruku:
            order_inner = f'{qcol("uid")}, {qcol("入库时间")} DESC NULLS LAST'
        sql = (
            f"INSERT INTO {qt} ({col_sql}) "
            f"SELECT DISTINCT ON ({qcol('uid')}) {col_sql} FROM {qs} "
            f"ORDER BY {order_inner}"
        )
        return text(sql)

    if not is_pg and has_uid:
        # MySQL 8+ ROW_NUMBER 去重
        qc_list = ", ".join(f"t.{qcol(c)}" for c in columns)
        if has_ruku:
            part_order = f"{qcol('入库时间')} DESC"
        else:
            part_order = f"{qcol(columns[0])}"
        sql = (
            f"INSERT INTO {qt} ({col_sql}) "
            f"SELECT {qc_list} FROM ("
            f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {qcol('uid')} ORDER BY {part_order}) AS _rn "
            f"FROM {qs}"
            f") t WHERE t._rn = 1"
        )
        return text(sql)

    sql = f"INSERT INTO {qt} ({col_sql}) SELECT {col_sql} FROM {qs}"
    return text(sql)


def import_dataframe_via_staging(
    engine,
    target_table: str,
    df: pd.DataFrame,
    pg_schema: Optional[str] = None,
) -> Tuple[bool, str, int, List[str]]:
    """
    先写入与正式表同结构的临时表（TRUNCATE 后全量写入），成功后再插入正式表。
    正式表插入在单事务中执行；失败则回滚对正式表的修改。
    若存在 uid 列，则按与示例 SQL 一致的方式去重后再插入正式表。
    返回 (成功, 说明信息, 写入正式表行数估计, 错误列表)。
    """
    from sqlalchemy import text

    if df is None or df.empty:
        return False, "没有可导入的数据。", 0, []
    tgt = target_table.strip()
    if not table_exists(engine, tgt, pg_schema):
        return False, f"正式表不存在：{tgt}", 0, []

    tcols = get_table_column_names(tgt, engine=engine, pg_schema=pg_schema)
    if not tcols:
        return False, "无法读取正式表列信息。", 0, []

    df_use = _align_df_to_table_columns(df, tcols)
    if df_use.empty:
        return False, "文件列与目标表无交集，无法导入。", 0, []

    try:
        staging = ensure_staging_table(engine, tgt, pg_schema)
    except Exception as e:
        return False, f"创建或校验临时表失败：{str(e)[:500]}", 0, [str(e)[:500]]

    try:
        truncate_table(engine, staging, pg_schema)
    except Exception as e:
        return False, f"清空临时表失败：{str(e)[:500]}", 0, [str(e)[:500]]

    ok_n, fail_n, errs = insert_df_to_table(engine, staging, df_use, pg_schema=pg_schema)
    if fail_n > 0 or ok_n < len(df_use):
        msg = (
            f"数据无法全部进入临时表（成功 {ok_n}，失败 {fail_n}）。"
            "已中止写入正式表，请根据错误信息修正数据或表结构。"
        )
        return False, msg, 0, errs or [f"批量失败，成功 {ok_n}，失败 {fail_n}"]

    insert_cols = [c for c in tcols if c in df_use.columns]
    if not insert_cols:
        return False, "无可用列执行 INSERT。", 0, []

    ins_stmt = _build_insert_from_staging_sql(
        engine, tgt, staging, insert_cols, pg_schema
    )

    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(ins_stmt)
        n_ok = len(df_use)
        return (
            True,
            "已先写入临时表校验通过，并成功提交至正式表（若存在 uid 列，写入正式表前已在临时表结果上按 uid 去重）。",
            n_ok,
            [],
        )
    except Exception as e:
        err = str(e)[:800]
        return (
            False,
            "写入正式表失败（可能含主键/唯一约束与已有数据冲突等），已回滚本次对正式表的插入。"
            f" 原因：{err}",
            0,
            [err],
        )


def _create_table_suffix(engine) -> str:
    try:
        if engine.dialect.name == "postgresql":
            return ""
    except Exception:
        pass
    return " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"


def create_table_from_schema(
    engine,
    table_name: str,
    schema: List[Tuple[str, str]],
    pg_schema: Optional[str] = None,
) -> bool:
    """
    根据 (列名, 类型) 列表创建表。若表已存在则不覆盖，返回 False。
    类型字符串兼容 MySQL/PostgreSQL（PostgreSQL 使用 DOUBLE PRECISION 等）。
    """
    from sqlalchemy import text
    tn = table_name.strip()
    if table_exists(engine, tn, pg_schema):
        return False
    q = lambda n: _quote_ident(engine, n)
    cols = [f"{q(col_name)} {mysql_t}" for col_name, mysql_t in schema]
    qtbl = _qualified_table_sql(engine, tn, pg_schema)
    create_sql = f"CREATE TABLE {qtbl} (" + ", ".join(cols) + ")" + _create_table_suffix(engine)
    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()
    return True


def create_table_from_df(
    engine,
    table_name: str,
    df: pd.DataFrame,
    pg_schema: Optional[str] = None,
) -> bool:
    """根据 DataFrame 创建表。若表已存在则不覆盖，返回 False。"""
    from sqlalchemy import text
    tn = table_name.strip()
    if table_exists(engine, tn, pg_schema):
        return False
    q = lambda n: _quote_ident(engine, n)
    cols = []
    for c in df.columns:
        dtype = df[c].dtype
        mysql_t = _mysql_type(c, dtype)
        cols.append(f"{q(c)} {mysql_t}")
    qtbl = _qualified_table_sql(engine, tn, pg_schema)
    create_sql = f"CREATE TABLE {qtbl} (" + ", ".join(cols) + ")" + _create_table_suffix(engine)
    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()
    return True


def insert_df_to_table(
    engine,
    table_name: str,
    df: pd.DataFrame,
    pg_schema: Optional[str] = None,
) -> Tuple[int, int, List[str]]:
    """
    将 DataFrame 追加写入表。表必须已存在且列兼容。
    PostgreSQL 非 public 时传入 pg_schema。
    返回 (success_count, fail_count, error_messages)。
    """
    kw = {"if_exists": "append", "index": False}
    if pg_schema and str(pg_schema).strip():
        kw["schema"] = pg_schema.strip()
    try:
        df.to_sql(
            table_name,
            engine,
            method="multi",
            chunksize=1000,
            **kw,
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
                    **({"schema": pg_schema.strip()} if pg_schema and str(pg_schema).strip() else {}),
                )
                success += 1
            except Exception as row_e:
                fail += 1
                if len(errors) < 20:
                    errors.append(f"行{i+1}: {str(row_e)[:200]}")
        if success == 0 and fail == 0:
            return (0, len(df), [err_msg[:500]])
        return (success, fail, errors[:20])
