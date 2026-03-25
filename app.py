# app.py - Analysis 系统：侧栏导航（入库 / 标准化数据产品）

import os
import sys
import importlib
from datetime import date
from io import BytesIO
import streamlit as st
import pandas as pd

from config import DB_CONFIG

# 确保项目根目录在 path 中，以便 handlers 作为包导入（相对导入 .data_utils 才能工作）
_app_dir = os.path.dirname(os.path.abspath(__file__))
if _app_dir not in sys.path:
    sys.path.insert(0, _app_dir)


def _load_handler(module_path: str):
    """按包路径加载 handler，失败返回 None。"""
    try:
        return importlib.import_module(module_path)
    except ImportError:
        return None


_rank_mod = _load_handler("handlers.ranking_handler")
_op_mod = _load_handler("handlers.operator_handler")
_nat_mod = _load_handler("handlers.national_handler")
_prov_mod = _load_handler("handlers.province_handler")
_power_mod = _load_handler("handlers.power_handler")
_ratio_mod = _load_handler("handlers.ratio_handler")


def _detect_table_type(df: pd.DataFrame) -> bool:
    """True=充电桩表(pile), False=充电站表(station)。"""
    if df is None or df.empty:
        return True
    if "充电桩编号" in df.columns or "额定功率" in df.columns:
        return True
    if "所属充电站编号" in df.columns and "充电桩编号" not in df.columns:
        return False
    if "站点总装机功率" in df.columns or "充电站内部编号" in df.columns:
        return False
    return True


def _has_province_col(df: pd.DataFrame) -> bool:
    return "省份_中文" in df.columns or "省份" in df.columns


def _has_city_col(df: pd.DataFrame) -> bool:
    return "城市_中文" in df.columns or "城市" in df.columns


def _export_date() -> str:
    return date.today().strftime("%Y%m%d")


def _product_source_signature(
    data_source: str,
    upload,
    backend_product: str,
    pg_schema,
    selected_table: str,
) -> tuple:
    """数据来源签名，用于缓存失效。selected_table 在导入文件模式下传空字符串。"""
    if data_source == "导入文件":
        if upload is None:
            return None
        tt = st.session_state.get("table_type_import", "充电桩表")
        return ("file", str(upload.name), tt)
    tt = st.session_state.get("table_type_db", "充电桩表")
    # cols_slim_v1：库表只读分析列，与旧版全表缓存区分
    return ("db", backend_product, str(selected_table or ""), str(pg_schema or ""), tt, "cols_slim_v1")


def _load_product_dataframe(
    data_source: str,
    upload,
    backend_product: str,
    pg_config,
    pg_schema,
    selected_table: str,
    for_pile: bool = True,
):
    """
    按需读取数据。库表优先只 SELECT 七类产品所需列，减少 IO（大表显著提速）。
    环境变量 ANALYSIS_READ_CHUNKSIZE=50000 时可分块拉取再合并（省内存，略增总时间）。
    """
    import os

    if data_source == "导入文件":
        if upload is None:
            return None, "请先上传文件。"
        try:
            if upload.name.lower().endswith(".csv"):
                df = pd.read_csv(upload, encoding="utf-8-sig")
            else:
                df = pd.read_excel(upload, engine="openpyxl")
            if df is None or df.empty:
                return None, "文件中没有数据。"
            return df, None
        except Exception as e:
            return None, f"读取文件失败：{e}"
    from db_helper import read_table, get_table_column_names

    if not selected_table:
        return None, "请先选择表。"
    chunk_env = os.environ.get("ANALYSIS_READ_CHUNKSIZE", "").strip()
    chunksize = int(chunk_env) if chunk_env.isdigit() else None
    try:
        avail = get_table_column_names(
            selected_table,
            backend=backend_product,
            pg_config=pg_config if backend_product == "postgresql" else None,
            pg_schema=pg_schema if backend_product == "postgresql" else None,
        )
        use_cols = None
        if avail is not None:
            try:
                from handlers.product_slim_columns import resolve_slim_columns_for_products

                picked = resolve_slim_columns_for_products(for_pile, avail)
                if picked:
                    use_cols = picked
            except Exception:
                use_cols = None
        df = read_table(
            selected_table,
            backend=backend_product,
            pg_config=pg_config if backend_product == "postgresql" else None,
            pg_schema=pg_schema if backend_product == "postgresql" else None,
            columns=use_cols,
            chunksize=chunksize,
        )
    except Exception as e:
        return None, f"读取表失败：{e}"
    if df is None or df.empty:
        return None, "读取表失败或表中没有数据。"
    return df, None


def _assets_path(filename: str) -> str:
    """返回 assets 目录下文件的绝对路径。"""
    app_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(app_dir, "assets", filename)


def _banner_with_background(filename: str) -> str:
    """顶部图块：仅图片居左，不修改原图，直接引用。标题由调用方写在图块下方。"""
    import base64
    path = _assets_path(filename)
    img_html = ""
    if os.path.isfile(path):
        try:
            with open(path, "rb") as f:
                b64 = base64.b64encode(f.read()).decode()
            ext = "png" if filename.lower().endswith(".png") else "jpeg"
            img_html = '<img src="data:image/' + ext + ';base64,' + b64 + '" style="max-height:300px;width:auto;display:block;vertical-align:top;" alt="">'
        except Exception:
            pass
    return (
        '<div style="margin-bottom:0;">'
        + img_html
        + "</div>"
    )


def _show_asset_image(filename: str, placeholder_html: str) -> None:
    """若 assets/filename 存在则显示（高度 35%、透明度 80%），否则显示占位 HTML。"""
    path = _assets_path(filename)
    if os.path.isfile(path):
        try:
            from PIL import Image
            import base64
            img = Image.open(path).convert("RGBA")
            w, h = img.size
            new_h = max(1, int(h * 0.35))
            new_w = max(1, int(w * 0.35))
            img = img.resize((new_w, new_h), Image.LANCZOS)
            buf = BytesIO()
            img.save(buf, format="PNG")
            b64 = base64.b64encode(buf.getvalue()).decode()
            st.markdown(
                "<img src=\"data:image/png;base64," + b64 + "\" style=\"opacity:0.8; max-width:100%; height:auto;\" alt=\"示意图\">",
                unsafe_allow_html=True,
            )
        except Exception:
            st.image(path)
    else:
        st.markdown(placeholder_html, unsafe_allow_html=True)


# 必备列（与 merge 规范一致，至少具备关键列即可入库）
PILE_KEY_COLS = ["充电桩编号", "所属充电站编号"]
STATION_KEY_COLS = ["所属充电站编号", "充电站内部编号"]


def _validate_columns(df: pd.DataFrame, for_pile: bool) -> tuple[bool, str]:
    """校验必备列。返回 (ok, message)。"""
    required = PILE_KEY_COLS if for_pile else STATION_KEY_COLS
    missing = [c for c in required if c not in df.columns]
    if missing:
        return False, f"缺少必备列：{', '.join(missing)}"
    return True, ""


st.set_page_config(
    page_title="Analysis 数据分析系统",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------- 侧栏 CSS（prototype 风格：大字体、激活态、左边框） ----------
SIDEBAR_CSS = """
<style>
[data-testid="stSidebar"] .stRadio > label { font-size: 13px !important; color: #aaa !important; }
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label { font-size: 15px !important; padding: 10px 16px !important; }
[data-testid="stSidebar"] section { border-right: 1px solid #e8eaed; background: #fff; }
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label span { font-weight: 600; }
[data-testid="stSidebar"] .sidebar-title-block { font-size: 1.4rem !important; font-weight: 700 !important; color: #1a3c6e !important; padding: 0.75rem 1rem !important; margin: 0 0 0.5rem 0 !important; background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%); border-radius: 6px; }
</style>
"""
st.markdown(SIDEBAR_CSS, unsafe_allow_html=True)

# ---------- 侧栏导航：1. 入库  2. 标准化数据产品 ----------
if "view_mode" not in st.session_state:
    st.session_state.view_mode = "入库"

with st.sidebar:
    st.markdown('<div class="sidebar-title-block">Analysis 数据分析系统</div>', unsafe_allow_html=True)
    view = st.radio(
        "功能",
        options=["入库", "标准化数据产品"],
        index=0 if st.session_state.view_mode == "入库" else 1,
        key="sidebar_nav",
        label_visibility="collapsed",
    )
    st.session_state.view_mode = view

# ---------- 主区：根据 view_mode 渲染 ----------
if st.session_state.view_mode == "入库":
    # ----- 入库页：顶部图块 + 标题（下图下，与 ### 区分） -----
    st.markdown(_banner_with_background("import-banner.png"), unsafe_allow_html=True)
    st.markdown('<p style="color:#546e7a;font-size:0.95rem;font-weight:500;margin:1.5rem 0 1rem 0;letter-spacing:0.02em;">一键入库</p>', unsafe_allow_html=True)

    st.markdown("### 数据库配置")
    db_backend = st.radio(
        "数据库类型",
        options=["MySQL（默认配置）", "PostgreSQL（需配置）"],
        index=0,
        key="db_backend_radio_import",
        horizontal=True,
    )
    backend = "mysql" if "MySQL" in db_backend else "postgresql"
    st.session_state["db_backend"] = backend

    if backend == "mysql":
        c1, c2, c3 = st.columns(3)
        with c1:
            st.text_input("Host", value=DB_CONFIG["host"], key="db_host", disabled=True)
            st.text_input("Port", value=str(DB_CONFIG["port"]), key="db_port", disabled=True)
        with c2:
            st.text_input("User", value=DB_CONFIG["user"], key="db_user", disabled=True)
            st.text_input("Database", value=DB_CONFIG["database"], key="db_name", disabled=True)
        with c3:
            st.text_input("Password", value="********", key="db_pwd", disabled=True, type="password")
        st.caption("与 charging-agent 共用同一配置：可通过环境变量 DB_HOST、DB_PORT、DB_USER、DB_PASSWORD、DB_NAME 覆盖默认值。")
    else:
        _pc0 = st.session_state.get("pg_config") or {}
        with st.expander("PostgreSQL 连接配置", expanded=True):
            pg_host = st.text_input("Host", value=str(_pc0.get("host", "localhost")), key="pg_host_import")
            pg_port = st.text_input("Port", value=str(_pc0.get("port", "5432")), key="pg_port_import")
            pg_user = st.text_input("User", value=str(_pc0.get("user", "postgres")), key="pg_user_import")
            pg_password = st.text_input("Password", type="password", key="pg_password_import", placeholder="输入密码")
            pg_database = st.text_input("Database", value=str(_pc0.get("database", "postgres")), key="pg_database_import")
        st.session_state["pg_config"] = {
            "host": pg_host or "localhost",
            "port": int(pg_port or "5432"),
            "user": pg_user or "postgres",
            "password": pg_password or "",
            "database": pg_database or "postgres",
        }

    if st.button("测试连接", key="test_conn_import"):
        from db_helper import test_connection
        pg_config = st.session_state.get("pg_config") if backend == "postgresql" else None
        ok, msg = test_connection(backend=backend, pg_config=pg_config)
        if ok:
            st.success(msg)
        else:
            st.error("连接失败：" + msg)

    st.markdown("### 入库方式")
    import_mode = st.radio(
        "选择入库方式",
        options=["已有表追加数据", "新增表导入数据"],
        index=0,
        key="import_mode",
        horizontal=True,
    )

    _backend = st.session_state.get("db_backend", "mysql")
    _pg_config = st.session_state.get("pg_config") if _backend == "postgresql" else None

    target_table = ""
    if import_mode == "已有表追加数据":
        _pg_schema_sel = None
        if _backend == "postgresql" and _pg_config:
            from db_helper import list_pg_schemas
            schemas, s_err = list_pg_schemas(_pg_config)
            if s_err:
                st.error("获取 Schema 列表失败：" + s_err)
            elif not schemas:
                st.warning("未找到可用的 Schema。")
            else:
                _def = schemas.index("public") if "public" in schemas else 0
                st.selectbox(
                    "Schema (PostgreSQL)",
                    options=schemas,
                    index=_def,
                    key="pg_schema",
                )
                _pg_schema_sel = st.session_state.get("pg_schema")
        try:
            from db_helper import list_tables_with_status
            tables, db_error = list_tables_with_status(
                backend=_backend,
                pg_config=_pg_config,
                pg_schema=_pg_schema_sel,
            )
        except Exception as e:
            tables, db_error = [], str(e)[:500]
        if db_error:
            st.error("连接失败：" + db_error)
        elif _backend == "postgresql" and _pg_config and not _pg_schema_sel:
            st.info("请先选择 Schema。")
        elif not tables:
            st.warning("连接成功，但当前 Schema 下无表。")
        else:
            target_table = st.selectbox("选择目标表", options=tables, key="import_target_table")
    else:
        new_table_name = st.text_input(
            "新表名称（可选，留空则按日期/类型自动生成）",
            key="new_table_name",
            value="",
        )
        target_table = new_table_name.strip()

    table_type_import = st.selectbox(
        "表类型",
        options=["充电桩表", "充电站表", "其他"],
        index=0,
        key="import_table_type",
        help="充电桩表/充电站表按标准字段校验；其他可导入任意表头并自定义字段类型。",
    )

    st.markdown("### 执行入库")
    import_file = st.file_uploader(
        "选择清洗后文件",
        type=["xlsx", "xls", "csv"],
        key="import_upload",
        help="支持 Excel / CSV，列需与 merge 清洗后一致；选「其他」时可导入任意表头表格",
    )

    df_other_full = None
    if import_file is not None:
        try:
            if table_type_import == "其他":
                import_file.seek(0)
                if import_file.name.lower().endswith(".csv"):
                    df_other_full = pd.read_csv(import_file, encoding="utf-8-sig")
                else:
                    df_other_full = pd.read_excel(import_file, engine="openpyxl")
            else:
                if import_file.name.lower().endswith(".csv"):
                    pd.read_csv(import_file, encoding="utf-8-sig", nrows=5)
                else:
                    pd.read_excel(import_file, engine="openpyxl", nrows=5)
                import_file.seek(0)
        except Exception as e:
            st.error(f"读取文件失败：{e}")

    # ---------- 「其他」类型：识别表头 + 建议字段类型 + 确认后建表并导入 ----------
    if table_type_import == "其他" and df_other_full is not None and not df_other_full.empty:
        st.markdown("### 表结构设置（其他类型表格）")
        st.caption("已识别表头，请核对并调整各列建议的数据库字段类型，确认后将创建新表并导入数据。")
        from db_helper import suggest_mysql_type, MYSQL_TYPE_OPTIONS, get_connection, create_table_from_schema, insert_df_to_table

        cols = list(df_other_full.columns)
        suggested = [suggest_mysql_type(c, df_other_full[c].dtype) for c in cols]
        # 若建议类型不在选项中则加入
        type_choices = list(MYSQL_TYPE_OPTIONS)
        for s in suggested:
            if s and s not in type_choices:
                type_choices.append(s)
        schema = []
        for i, col in enumerate(cols):
            sug = suggested[i] if suggested[i] in type_choices else "VARCHAR(500)"
            idx = type_choices.index(sug) if sug in type_choices else 0
            sel = st.selectbox(
                f"列「{col}」类型",
                options=type_choices,
                index=idx,
                key=f"other_type_{i}_{col}",
            )
            schema.append((col, sel))
        other_table_name = st.text_input(
            "新表名称（必填）",
            value=st.session_state.get("import_other_table_name", ""),
            key="import_other_table_name",
            placeholder="例如：my_data_20250101",
        )
        if st.button("确认表结构并导入", type="primary", key="do_import_other"):
            if not other_table_name or not other_table_name.strip():
                st.warning("请填写新表名称。")
            else:
                final_name = other_table_name.strip()
                _be = st.session_state.get("db_backend", "mysql")
                _pg = st.session_state.get("pg_config") if _be == "postgresql" else None
                engine = get_connection(backend=_be, pg_config=_pg)
                try:
                    created = create_table_from_schema(engine, final_name, schema)
                    if created:
                        st.success(f"已创建新表：{final_name}")
                    else:
                        st.warning(f"表 {final_name} 已存在，将追加数据。")
                    _pg_s_other = st.session_state.get("pg_schema") if _be == "postgresql" else None
                    success, fail, errors = insert_df_to_table(
                        engine, final_name, df_other_full, pg_schema=_pg_s_other
                    )
                    st.metric("成功", success)
                    st.metric("失败", fail)
                    if errors:
                        with st.expander("错误详情（前若干条）"):
                            for err in errors:
                                st.code(err)
                except Exception as e:
                    st.error(f"建表或导入失败：{e}")
        st.markdown("---")
        st.stop()

    if st.button("执行入库", type="primary", key="do_import"):
        if import_file is None:
            st.warning("请先选择清洗后文件。")
        elif import_mode == "已有表追加数据" and not target_table:
            st.warning("请选择目标表。")
        else:
            try:
                if import_file.name.lower().endswith(".csv"):
                    df_import = pd.read_csv(import_file, encoding="utf-8-sig")
                else:
                    df_import = pd.read_excel(import_file, engine="openpyxl")
            except Exception as e:
                st.error(f"读取文件失败：{e}")
                df_import = None

            if df_import is not None and not df_import.empty:
                for_pile = table_type_import == "充电桩表"
                ok, msg = _validate_columns(df_import, for_pile)
                if not ok:
                    st.error(msg)
                else:
                    from db_helper import get_connection, create_table_from_df, insert_df_to_table

                    _be = st.session_state.get("db_backend", "mysql")
                    _pg = st.session_state.get("pg_config") if _be == "postgresql" else None
                    engine = get_connection(backend=_be, pg_config=_pg)
                    final_table = target_table
                    if import_mode == "新增表导入数据" and not final_table:
                        suffix = date.today().strftime("%Y%m%d")
                        final_table = f"pile_{suffix}" if for_pile else f"station_{suffix}"

                    created = False
                    if import_mode == "新增表导入数据":
                        try:
                            created = create_table_from_df(engine, final_table, df_import)
                            if created:
                                st.success(f"已创建新表：{final_table}")
                        except Exception as e:
                            st.error(f"建表失败：{e}")

                    try:
                        _pg_s = st.session_state.get("pg_schema") if _be == "postgresql" else None
                        success, fail, errors = insert_df_to_table(
                            engine, final_table, df_import, pg_schema=_pg_s
                        )
                        st.metric("成功", success)
                        st.metric("失败", fail)
                        if errors:
                            with st.expander("错误详情（前若干条）"):
                                for err in errors:
                                    st.code(err)
                    except Exception as e:
                        st.error(f"写入失败：{e}")
                        st.metric("成功", 0)
                        st.metric("失败", len(df_import))

else:
    # ========== 标准化数据产品页：顶部图块 + 标题（图下，与 ### 区分） ==========
    st.markdown(_banner_with_background("product-banner.png"), unsafe_allow_html=True)
    st.markdown('<p style="color:#546e7a;font-size:0.95rem;font-weight:500;margin:1.5rem 0 1rem 0;letter-spacing:0.02em;">输出标准化产品</p>', unsafe_allow_html=True)

    st.markdown("### 数据来源")
    data_source = st.radio(
        "选择数据来源",
        options=["导入文件", "从数据库选择表"],
        index=0,
        key="data_source",
        horizontal=True,
    )

    upload = None
    backend_product = "mysql"
    tables = []
    db_error = None
    _pg_schema_product = None
    analysis_ready = False

    if data_source == "导入文件":
        st.markdown("### 上传清洗后的表格")
        upload = st.file_uploader("选择文件", type=["xlsx", "xls", "csv"], key="analysis_upload")
        if upload is not None:
            try:
                if upload.name.lower().endswith(".csv"):
                    _preview = pd.read_csv(upload, encoding="utf-8-sig", nrows=200)
                else:
                    _preview = pd.read_excel(upload, engine="openpyxl", nrows=200)
                upload.seek(0)
                detected_pile = _detect_table_type(_preview)
            except Exception:
                detected_pile = True
            table_type_options = ["充电桩表", "充电站表"]
            default_idx = 0 if detected_pile else 1
            st.selectbox(
                "请确认表类型",
                options=table_type_options,
                index=default_idx,
                key="table_type_import",
            )
        analysis_ready = upload is not None

    elif data_source == "从数据库选择表":
        st.markdown("### 选择表")
        db_backend_product = st.radio(
            "数据库类型",
            options=["MySQL（默认配置）", "PostgreSQL（需配置）"],
            index=0 if st.session_state.get("db_backend", "mysql") == "mysql" else 1,
            key="db_backend_radio_product",
            horizontal=True,
        )
        backend_product = "mysql" if "MySQL" in db_backend_product else "postgresql"
        st.session_state["db_backend"] = backend_product
        if backend_product == "postgresql":
            _pc = st.session_state.get("pg_config") or {}
            with st.expander("PostgreSQL 连接配置", expanded=not _pc):
                pg_host_p = st.text_input("Host", value=str(_pc.get("host", "localhost")), key="pg_host_product")
                pg_port_p = st.text_input("Port", value=str(_pc.get("port", "5432")), key="pg_port_product")
                pg_user_p = st.text_input("User", value=str(_pc.get("user", "postgres")), key="pg_user_product")
                pg_password_p = st.text_input("Password", type="password", value=str(_pc.get("password", "")), key="pg_password_product")
                pg_database_p = st.text_input("Database", value=str(_pc.get("database", "postgres")), key="pg_database_product")
            st.session_state["pg_config"] = {
                "host": pg_host_p or "localhost",
                "port": int(pg_port_p or "5432"),
                "user": pg_user_p or "postgres",
                "password": pg_password_p or "",
                "database": pg_database_p or "postgres",
            }
        _pg_schema_product = None
        if backend_product == "postgresql" and st.session_state.get("pg_config"):
            from db_helper import list_pg_schemas
            schemas_p, s_err_p = list_pg_schemas(st.session_state["pg_config"])
            if s_err_p:
                st.error("获取 Schema 列表失败：" + s_err_p)
            elif not schemas_p:
                st.warning("未找到可用的 Schema。")
            else:
                _def_p = schemas_p.index("public") if "public" in schemas_p else 0
                st.selectbox(
                    "Schema (PostgreSQL)",
                    options=schemas_p,
                    index=_def_p,
                    key="pg_schema",
                )
                _pg_schema_product = st.session_state.get("pg_schema")
                _prev_s = st.session_state.get("_prev_pg_schema_product")
                if _prev_s is not None and _prev_s != _pg_schema_product:
                    st.session_state.pop("product_cached_df", None)
                    st.session_state.pop("product_cached_for_pile", None)
                    st.session_state["_product_cache_sig"] = None
                st.session_state["_prev_pg_schema_product"] = _pg_schema_product
        try:
            from db_helper import list_tables_with_status
            tables, db_error = list_tables_with_status(
                backend=backend_product,
                pg_config=st.session_state.get("pg_config") if backend_product == "postgresql" else None,
                pg_schema=_pg_schema_product,
            )
        except Exception as e:
            tables, db_error = [], str(e)[:500]
        if db_error:
            st.error("连接失败：" + db_error)
        elif backend_product == "postgresql" and st.session_state.get("pg_config") and not _pg_schema_product:
            st.info("请先选择 Schema。")
        elif not tables:
            st.warning("连接成功，但当前 Schema 下无表。请检查配置或选择 MySQL。")
        else:
            st.selectbox("选择表", options=tables, key="db_table_select")
            st.selectbox(
                "请确认表类型",
                options=["充电桩表", "充电站表"],
                index=0,
                key="table_type_db",
            )
            analysis_ready = True

    if not analysis_ready:
        if data_source == "导入文件":
            st.info("请上传清洗后的表格文件。")
        st.stop()

    st.info("已选择数据来源，请点击下方产品按钮，系统将按需加载数据并生成。")
    export_date = _export_date()

    # 标准数据产品：七按钮按需展示（见 docs/标准化数据产品_七表UI设计.md）
    if "product_panel" not in st.session_state:
        st.session_state.product_panel = None

    st.markdown("### 标准数据产品（按需生成）")
    _product_names = ["车桩比", "高速公路", "功率段分布", "排行榜", "全国概况", "省级数据", "运营商概况"]
    _row1 = st.columns(4)
    _row2 = st.columns(3)
    for _i, _name in enumerate(_product_names):
        _cols = _row1 if _i < 4 else _row2
        _j = _i if _i < 4 else _i - 4
        with _cols[_j]:
            if st.button(_name, key=f"product_btn_{_name}", use_container_width=True):
                st.session_state.product_panel = _name

    if st.button(
        "一键生成七表（ZIP）",
        key="product_btn_all_seven",
        use_container_width=True,
        help="单次加载数据后打包 7 个 Excel；库表仅读取分析所需列，适合百万级以上行数。",
    ):
        st.session_state.product_panel = "__ALL_SEVEN__"

    _panel = st.session_state.product_panel
    df = None
    for_pile = True
    product_load_error = None

    if _panel is not None:
        _sel_table = "" if data_source == "导入文件" else str(st.session_state.get("db_table_select") or "")
        sig = _product_source_signature(
            data_source, upload, backend_product, _pg_schema_product, _sel_table
        )
        if data_source == "导入文件":
            for_pile = st.session_state.get("table_type_import", "充电桩表") == "充电桩表"
        else:
            for_pile = st.session_state.get("table_type_db", "充电桩表") == "充电桩表"
        if sig is None:
            product_load_error = "数据来源不完整，请重新完成文件上传或库表选择。"
        else:
            _cached_sig = st.session_state.get("_product_cache_sig")
            if (
                _cached_sig == sig
                and st.session_state.get("product_cached_df") is not None
            ):
                df = st.session_state.product_cached_df
            else:
                with st.spinner("正在加载数据…"):
                    df, _err = _load_product_dataframe(
                        data_source,
                        upload,
                        backend_product,
                        st.session_state.get("pg_config"),
                        _pg_schema_product,
                        _sel_table,
                        for_pile=for_pile,
                    )
                if _err:
                    product_load_error = _err
                    df = None
                    st.session_state.pop("product_cached_df", None)
                    st.session_state["_product_cache_sig"] = None
                else:
                    st.session_state.product_cached_df = df
                    st.session_state["_product_cache_sig"] = sig
                    st.caption(f"已加载 {len(df):,} 行 × {len(df.columns)} 列。")
                    if data_source == "从数据库选择表":
                        st.caption(
                            "库表已优先 **仅查询七类产品所需列**（见 `handlers/product_slim_columns.py`），"
                            "可大幅减少网络与解析时间；列名与标准不一致时会自动回退为 `SELECT *`。"
                        )

    if _panel is None:
        st.info("请点击上方按钮查看对应产品。")
    elif product_load_error:
        st.error(product_load_error)
    elif df is None:
        st.warning("无法加载数据。")
    elif _panel == "__ALL_SEVEN__":
        st.markdown("#### 一键生成：七表 ZIP 合集")
        st.caption(
            "打包内容与分别点击七个产品一致；数据只加载一次。大表建议保持库连接稳定，"
            "必要时可在服务器设置环境变量 `ANALYSIS_READ_CHUNKSIZE=80000` 分块读取以降低内存峰值。"
        )
        try:
            from handlers.product_bundle import build_seven_products_zip_bytes

            with st.spinner("正在生成七表并打包 ZIP（大表请稍候）…"):
                zbuf = build_seven_products_zip_bytes(
                    df,
                    for_pile,
                    export_date,
                    ratio_mod=_ratio_mod,
                    power_mod=_power_mod,
                    rank_mod=_rank_mod,
                    nat_mod=_nat_mod,
                    prov_mod=_prov_mod,
                    op_mod=_op_mod,
                )
            if zbuf and zbuf.getvalue():
                st.download_button(
                    "下载 标准化数据产品_七表合集 ZIP",
                    data=zbuf.getvalue(),
                    file_name=f"标准化数据产品_七表合集_{export_date}.zip",
                    mime="application/zip",
                    key="dl_seven_zip",
                )
            else:
                st.warning("打包失败，请检查各模块是否可用。")
        except Exception as e:
            st.error(f"打包失败：{e}")
    elif _panel == "排行榜" and _rank_mod:
        st.markdown("#### 排行榜")
        rank_tables = _rank_mod.get_all_ranking_tables(df, for_pile=for_pile)
        sheet_labels = [title for _grp, title, _tbl in rank_tables]
        sel_rank_idx = st.selectbox(
            "选择 Sheet 预览",
            range(len(sheet_labels)),
            format_func=lambda i: sheet_labels[i],
            key="rank_sheet_preview",
        )
        _grp, _title, preview_tbl = rank_tables[sel_rank_idx]
        st.dataframe(preview_tbl, use_container_width=True, hide_index=True)
        rank_xlsx = _rank_mod.write_ranking_workbook_bytes(df, for_pile=for_pile)
        st.download_button(
            "导出 Excel（六 Sheet）",
            data=rank_xlsx.getvalue(),
            file_name=f"排行榜_{export_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_rank_workbook_xlsx",
        )
    elif _panel == "运营商概况" and _op_mod:
        st.markdown("#### 运营商概况")
        op_tables = _op_mod.get_operator_workbook_tables(df, for_pile=for_pile)
        op_sheet_names = [name for name, _tbl in op_tables]
        sel_op_idx = st.selectbox(
            "选择 Sheet 预览",
            range(len(op_sheet_names)),
            format_func=lambda i: op_sheet_names[i],
            key="operator_sheet_preview",
        )
        _op_title, preview_op = op_tables[sel_op_idx]
        st.dataframe(preview_op, use_container_width=True, hide_index=True)
        op_xlsx = _op_mod.write_operator_workbook_bytes(df, for_pile=for_pile)
        st.download_button(
            "导出 Excel（多 Sheet）",
            data=op_xlsx.getvalue(),
            file_name=f"运营商概况_{export_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_op_workbook_xlsx",
        )
        if not for_pile:
            st.caption("当前为充电站表：各 Sheet 为占位（仅表头）；请使用充电桩表查看公共充电设施/直流/交流/充电功率等统计。")
        elif "运营商名称" not in df.columns and "上报机构" not in df.columns:
            st.caption("当前表无「运营商名称」「上报机构」列，各 Sheet 为仅表头。")
    elif _panel == "全国概况" and _nat_mod:
        st.markdown("#### 全国概况")
        nat_tables = _nat_mod.get_national_workbook_tables(df, for_pile=for_pile)
        sheet_names = [name for name, _tbl in nat_tables]
        sel_nat_idx = st.selectbox(
            "选择 Sheet 预览",
            range(len(sheet_names)),
            format_func=lambda i: sheet_names[i],
            key="national_sheet_preview",
        )
        _n_title, preview_nat = nat_tables[sel_nat_idx]
        st.dataframe(preview_nat, use_container_width=True, hide_index=True)
        if not for_pile:
            st.caption("当前为充电站表：分省桩类型统计类 Sheet 为占位（仅表头）；请使用充电桩表查看公共充电桩/交流/直流/交直流。")
        nat_xlsx = _nat_mod.write_national_workbook_bytes(df, for_pile=for_pile)
        st.download_button(
            "导出 Excel（多 Sheet）",
            data=nat_xlsx.getvalue(),
            file_name=f"全国概况_{export_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_nat_workbook_xlsx",
        )
    elif _panel == "省级数据" and _prov_mod:
        st.markdown("#### 省级数据")
        if not _has_province_col(df):
            st.info("当前表无省份列，省级数据不展示。")
        else:
            provinces = _prov_mod.list_province_product_names(df)
            sel_prov = st.selectbox("选择省份预览", options=provinces, key="sel_province")
            prov_tbl = _prov_mod.province_dimension_product_table(
                df, sel_prov, for_pile=for_pile
            )
            st.dataframe(prov_tbl, use_container_width=True, hide_index=True)
            prov_wb = _prov_mod.write_provincial_workbook_bytes(df, for_pile=for_pile)
            if prov_wb is not None:
                st.download_button(
                    "导出 Excel（按省多 Sheet）",
                    data=prov_wb.getvalue(),
                    file_name=f"省级数据_{export_date}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_prov_workbook_xlsx",
                )
            if not for_pile:
                st.caption("当前为充电站表：各维度数值为空占位；请使用充电桩表查看公共/交流/直流/交直流统计。")
    elif _panel == "功率段分布" and _power_mod:
        power_suffix = _power_mod.power_chart_title_suffix(for_pile)
        st.markdown(f"#### 功率段分布 {power_suffix}")
        prov_list = _power_mod.list_power_preview_provinces(df, for_pile=for_pile)
        power_wb = _power_mod.write_power_province_workbook(df, for_pile=for_pile)
        if prov_list and power_wb is not None:
            sel_power_prov = st.selectbox(
                "选择省份预览",
                options=prov_list,
                key="power_preview_province",
            )
            power_tbl = _power_mod.power_distribution_table_for_province(
                df, for_pile, sel_power_prov
            )
            st.dataframe(power_tbl, use_container_width=True, hide_index=True)
            labels, counts = _power_mod.power_distribution_chart_data(
                df, for_pile=for_pile, province=sel_power_prov
            )
            if labels and counts:
                try:
                    import matplotlib
                    matplotlib.use("Agg")
                    import matplotlib.pyplot as plt
                    fig, ax = plt.subplots(figsize=(8, 4))
                    ax.bar(labels, counts, color="#1abc9c", edgecolor="#16a085")
                    ax.set_ylabel("数量")
                    plt.xticks(rotation=30, ha="right")
                    plt.tight_layout()
                    buf_img = BytesIO()
                    fig.savefig(buf_img, format="png", dpi=100, bbox_inches="tight")
                    buf_img.seek(0)
                    st.image(buf_img)
                    st.download_button(
                        "下载图表 PNG",
                        data=buf_img.getvalue(),
                        file_name=f"功率段分布_{sel_power_prov}_{export_date}.png",
                        mime="image/png",
                        key="dl_power_png",
                    )
                    plt.close(fig)
                except Exception:
                    pass
            st.download_button(
                "导出 Excel（按省多 Sheet）",
                data=power_wb.getvalue(),
                file_name=f"功率段分布_{export_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_power_workbook_xlsx",
            )
        else:
            st.info("当前表无功率列（充电桩表需「额定功率」，充电站表需「站点总装机功率」），功率段分布不展示。")
    elif _panel == "高速公路":
        st.markdown("#### 高速公路")
        st.info("高速公路产品内容待定，暂无数据导出。")
    elif _panel == "车桩比" and _ratio_mod:
        st.markdown("#### 车桩比")
        ratio_tbl = _ratio_mod.ratio_vehicle_pile_product_table(df)
        st.dataframe(ratio_tbl, use_container_width=True, hide_index=True)
        buf = BytesIO()
        ratio_tbl.to_excel(buf, index=False, engine="openpyxl")
        buf.seek(0)
        st.download_button(
            "导出 Excel",
            data=buf.getvalue(),
            file_name=f"车桩比_{export_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_ratio_xlsx",
        )
    elif _panel is not None:
        st.warning("该产品模块未加载或不可用。")
