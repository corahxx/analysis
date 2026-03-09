# app.py - Analysis 系统：侧栏导航（入库 / 标准化数据产品）

import os
import importlib.util
from datetime import date
from io import BytesIO
import streamlit as st
import pandas as pd

from config import DB_CONFIG

# 动态加载 handlers
def _load_handler(module_name: str, file_name: str):
    app_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(app_dir, "handlers", file_name)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_rank_mod = _load_handler("ranking_handler", "ranking_handler.py")
_op_mod = _load_handler("operator_handler", "operator_handler.py")
_nat_mod = _load_handler("national_handler", "national_handler.py")
_prov_mod = _load_handler("province_handler", "province_handler.py")
_power_mod = _load_handler("power_handler", "power_handler.py")
_cg_mod = _load_handler("citygroup_handler", "citygroup_handler.py")
_hw_mod = _load_handler("highway_handler", "highway_handler.py")
_ratio_mod = _load_handler("ratio_handler", "ratio_handler.py")


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
    c1, c2, c3 = st.columns(3)
    with c1:
        st.text_input("Host", value=DB_CONFIG["host"], key="db_host", disabled=True)
        st.text_input("Port", value=str(DB_CONFIG["port"]), key="db_port", disabled=True)
    with c2:
        st.text_input("User", value=DB_CONFIG["user"], key="db_user", disabled=True)
        st.text_input("Database", value=DB_CONFIG["database"], key="db_name", disabled=True)
    with c3:
        st.text_input("Password", value="********", key="db_pwd", disabled=True, type="password")

    st.markdown("### 入库方式")
    import_mode = st.radio(
        "选择入库方式",
        options=["evdata 表追加数据", "新增表导入数据"],
        index=0,
        key="import_mode",
        horizontal=True,
    )

    target_table = ""
    if import_mode == "evdata 表追加数据":
        try:
            from db_helper import list_tables
            tables = list_tables()
        except Exception:
            tables = []
        if not tables:
            st.warning("无法连接数据库或库内无表。请检查 config 配置。")
        else:
            target_table = st.selectbox("选择目标表", options=tables, key="import_target_table")
    else:
        new_table_name = st.text_input(
            "新表名称（可选，留空则按日期/类型自动生成）",
            key="new_table_name",
            value="",
        )
        target_table = new_table_name.strip()

    st.markdown("### 执行入库")
    import_file = st.file_uploader(
        "选择清洗后文件",
        type=["xlsx", "xls", "csv"],
        key="import_upload",
        help="支持 Excel / CSV，列需与 merge 清洗后一致",
    )

    table_type_import = "充电桩表"
    if import_file is not None:
        try:
            if import_file.name.lower().endswith(".csv"):
                _preview_df = pd.read_csv(import_file, encoding="utf-8-sig", nrows=5)
            else:
                _preview_df = pd.read_excel(import_file, engine="openpyxl", nrows=5)
            import_file.seek(0)
            detected_pile = _detect_table_type(_preview_df)
            table_type_import = st.selectbox(
                "表类型",
                options=["充电桩表", "充电站表"],
                index=0 if detected_pile else 1,
                key="import_table_type",
            )
        except Exception as e:
            st.error(f"预览失败：{e}")

    if st.button("执行入库", type="primary", key="do_import"):
        if import_file is None:
            st.warning("请先选择清洗后文件。")
        elif import_mode == "evdata 表追加数据" and not target_table:
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

                    engine = get_connection()
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
                        success, fail, errors = insert_df_to_table(engine, final_table, df_import)
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

    df = None
    for_pile = True

    if data_source == "导入文件":
        st.markdown("### 上传清洗后的表格")
        upload = st.file_uploader("选择文件", type=["xlsx", "xls", "csv"], key="analysis_upload")
        if upload is not None:
            try:
                if upload.name.lower().endswith(".csv"):
                    df = pd.read_csv(upload, encoding="utf-8-sig")
                else:
                    df = pd.read_excel(upload, engine="openpyxl")
            except Exception as e:
                st.error(f"读取文件失败：{e}")
            if df is not None and not df.empty:
                detected_pile = _detect_table_type(df)
                table_type_options = ["充电桩表", "充电站表"]
                default_idx = 0 if detected_pile else 1
                table_type_sel = st.selectbox(
                    "请确认表类型",
                    options=table_type_options,
                    index=default_idx,
                    key="table_type_import",
                )
                for_pile = table_type_sel == "充电桩表"

    elif data_source == "从数据库选择表":
        st.markdown("### 选择表")
        try:
            from db_helper import list_tables, read_table
            tables = list_tables()
        except Exception:
            tables = []
        if not tables:
            st.warning("无法连接数据库或库内无表。请检查 config 配置并确保 evdata 库可访问。")
        else:
            selected_table = st.selectbox("选择表", options=tables, key="db_table_select")
            if st.session_state.get("db_table_loaded") and st.session_state.get("db_table_loaded") != selected_table:
                st.session_state.pop("df_from_db", None)
                st.session_state.pop("db_table_loaded", None)
            if st.button("加载该表", key="load_db_table"):
                try:
                    from db_helper import read_table
                    df = read_table(selected_table)
                except Exception as e:
                    st.error(f"读取表失败：{e}")
                if df is not None and not df.empty:
                    st.session_state["df_from_db"] = df
                    st.session_state["db_table_loaded"] = selected_table
                    st.rerun()
            if st.session_state.get("df_from_db") is not None and st.session_state.get("db_table_loaded") == selected_table:
                df = st.session_state["df_from_db"]
                table_type_options = ["充电桩表", "充电站表"]
                table_type_sel = st.selectbox(
                    "请确认表类型",
                    options=table_type_options,
                    key="table_type_db",
                )
                for_pile = table_type_sel == "充电桩表"

    if df is None or df.empty:
        st.info("请在上方选择数据来源并加载数据后，此处将展示标准化产品。")
        st.stop()

    data_type = "充电桩" if for_pile else "充电站"
    st.success(f"已加载 {len(df):,} 行数据（{data_type}表）。")
    export_date = _export_date()

    # 1) 排行榜
    if _rank_mod:
        st.markdown("### 排行榜")
        rank_tables = _rank_mod.get_all_ranking_tables(df, for_pile=for_pile)
        if not for_pile and not _has_city_col(df):
            st.info("当前为充电站表且无城市列，城市榜不展示。")
        for idx, (_group, title, tbl) in enumerate(rank_tables):
            st.markdown(f"**{title}**")
            st.dataframe(tbl, use_container_width=True, hide_index=True)
            c1, c2 = st.columns(2)
            with c1:
                buf = BytesIO()
                tbl.to_excel(buf, index=False, engine="openpyxl")
                buf.seek(0)
                st.download_button("导出 Excel", data=buf.getvalue(), file_name=f"排行榜_{title[:10]}_{export_date}.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"dl_rank_xlsx_{idx}")
            with c2:
                buf2 = BytesIO()
                tbl.to_csv(buf2, index=False, encoding="utf-8-sig")
                buf2.seek(0)
                st.download_button("导出 CSV", data=buf2.getvalue(), file_name=f"排行榜_{title[:10]}_{export_date}.csv", mime="text/csv", key=f"dl_rank_csv_{idx}")
        st.markdown("---")

    # 2) 各运营商数据
    if _op_mod:
        st.markdown("### 各运营商数据")
        op_tbl = _op_mod.operator_table(df, for_pile=for_pile)
        st.dataframe(op_tbl, use_container_width=True, hide_index=True)
        c1, c2 = st.columns(2)
        with c1:
            buf = BytesIO()
            op_tbl.to_excel(buf, index=False, engine="openpyxl")
            buf.seek(0)
            st.download_button("导出 Excel", data=buf.getvalue(), file_name=f"各运营商数据_{export_date}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_op_xlsx")
        with c2:
            buf2 = BytesIO()
            op_tbl.to_csv(buf2, index=False, encoding="utf-8-sig")
            buf2.seek(0)
            st.download_button("导出 CSV", data=buf2.getvalue(), file_name=f"各运营商数据_{export_date}.csv", mime="text/csv", key="dl_op_csv")
        st.markdown("---")

    # 3) 全国数据
    if _nat_mod:
        st.markdown("### 全国数据")
        cards = _nat_mod.national_summary_cards(df, for_pile=for_pile)
        col1, col2, col3 = st.columns(3)
        col1.metric("全国总量", f"{cards['总量']:,}")
        col2.metric("环比增量", cards["环比增量"])
        col3.metric("环比增速", cards["环比增速"])
        nat_tbl = _nat_mod.province_ranking_table(df, for_pile=for_pile)
        if nat_tbl.empty and not for_pile and not _has_province_col(df):
            st.info("当前为充电站表且无省份列，各省排名不展示。")
        else:
            st.markdown("**各省排名**")
            st.dataframe(nat_tbl, use_container_width=True, hide_index=True)
            c1, c2 = st.columns(2)
            with c1:
                buf = BytesIO()
                nat_tbl.to_excel(buf, index=False, engine="openpyxl")
                buf.seek(0)
                st.download_button("导出 Excel", data=buf.getvalue(), file_name=f"全国数据_各省排名_{export_date}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_nat_xlsx")
            with c2:
                buf2 = BytesIO()
                nat_tbl.to_csv(buf2, index=False, encoding="utf-8-sig")
                buf2.seek(0)
                st.download_button("导出 CSV", data=buf2.getvalue(), file_name=f"全国数据_各省排名_{export_date}.csv", mime="text/csv", key="dl_nat_csv")
        st.markdown("---")

    # 4) 各省数据
    if _prov_mod:
        st.markdown("### 各省数据")
        if not _has_province_col(df):
            st.info("当前表无省份列，各省数据不展示。")
        else:
            prov_col = "省份_中文" if "省份_中文" in df.columns else "省份"
            provinces = sorted(df[prov_col].dropna().astype(str).unique().tolist())
            sel_prov = st.selectbox("选择省份", options=provinces, key="sel_province")
            prov_tbl = _prov_mod.province_overview_table(df, sel_prov, for_pile=for_pile)
            st.dataframe(prov_tbl, use_container_width=True, hide_index=True)
            buf = BytesIO()
            prov_tbl.to_excel(buf, index=False, engine="openpyxl")
            buf.seek(0)
            st.download_button("导出 Excel", data=buf.getvalue(), file_name=f"各省数据_{sel_prov}_{export_date}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_prov_xlsx")
        st.markdown("---")

    # 5) 核心城市群
    if _cg_mod:
        st.markdown("### 核心城市群")
        if not _has_province_col(df):
            st.info("当前表无省份列，核心城市群不展示。")
        else:
            cg_tbl = _cg_mod.citygroup_provinces_table(df, for_pile=for_pile)
            st.dataframe(cg_tbl, use_container_width=True, hide_index=True)
            buf = BytesIO()
            cg_tbl.to_excel(buf, index=False, engine="openpyxl")
            buf.seek(0)
            st.download_button("导出 Excel", data=buf.getvalue(), file_name=f"核心城市群_{export_date}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_cg_xlsx")
        st.markdown("---")

    # 6) 功率段分布
    if _power_mod:
        power_suffix = _power_mod.power_chart_title_suffix(for_pile)
        st.markdown(f"### 功率段分布 {power_suffix}")
        power_tbl = _power_mod.power_distribution_table(df, for_pile=for_pile)
        if not power_tbl.empty:
            st.dataframe(power_tbl, use_container_width=True, hide_index=True)
            labels, counts = _power_mod.power_distribution_chart_data(df, for_pile=for_pile)
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
                    st.download_button("下载图表 PNG", data=buf_img.getvalue(), file_name=f"功率段分布_{export_date}.png", mime="image/png", key="dl_power_png")
                    plt.close(fig)
                except Exception:
                    pass
            buf = BytesIO()
            power_tbl.to_excel(buf, index=False, engine="openpyxl")
            buf.seek(0)
            st.download_button("导出 Excel", data=buf.getvalue(), file_name=f"功率段分布_{export_date}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_power_xlsx")
        else:
            st.info("当前表无功率列（充电桩表需「额定功率」，充电站表需「站点总装机功率」），功率段分布不展示。")
        st.markdown("---")

    # 7) 高速公路建设
    if _hw_mod:
        st.markdown("### 高速公路建设")
        hw_tbl = _hw_mod.highway_provinces_table(df)
        st.dataframe(hw_tbl, use_container_width=True, hide_index=True)
        buf = BytesIO()
        hw_tbl.to_excel(buf, index=False, engine="openpyxl")
        buf.seek(0)
        st.download_button("导出 Excel", data=buf.getvalue(), file_name=f"高速公路建设_{export_date}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_hw_xlsx")
        st.markdown("---")

    # 8) 车桩比
    if _ratio_mod:
        st.markdown("### 车桩比分析")
        ratio_cards = _ratio_mod.ratio_summary_cards(df, for_pile=for_pile)
        cols = st.columns(len(ratio_cards))
        for i, (k, v) in enumerate(ratio_cards.items()):
            cols[i].metric(k, v)
        ratio_tbl = _ratio_mod.ratio_provinces_table(df)
        st.dataframe(ratio_tbl, use_container_width=True, hide_index=True)
        buf = BytesIO()
        ratio_tbl.to_excel(buf, index=False, engine="openpyxl")
        buf.seek(0)
        st.download_button("导出 Excel", data=buf.getvalue(), file_name=f"车桩比_{export_date}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_ratio_xlsx")
