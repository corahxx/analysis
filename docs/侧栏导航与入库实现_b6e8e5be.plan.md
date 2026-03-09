---
name: 侧栏导航与入库实现
overview: 恢复左侧功能栏导航（入库第一项、标准化数据产品第二项），放大侧栏字体并参考 prototype 做 UI；主区增加 banner/卡片等元素与图片；实现真实数据库连接与上传入库，数据格式对齐充电数据示例与 merge 清洗后表结构。
todos: []
isProject: false
---

# 侧栏导航与入库实现计划

## 一、左侧功能栏与主区 UI

### 1.1 侧栏导航结构

- **恢复侧栏切换**：左侧为功能导航，两项顺序为 **1. 入库**、**2. 标准化数据产品**（与“入库在上面、功能2在下面”一致）。
- **侧栏字体与样式**：参考 [prototype.html](file:///C:/Users/HONOR/Desktop/prototype.html) 的 `.sidebar`、`.nav-group`、`.nav-item`：
  - 分组标题（如「功能」）：`font-size` 约 12–13px，颜色 `#aaa`。
  - 导航项：`font-size` 约 15–16px（比当前更大），`padding` 约 10px 16px，左侧 3px 高亮条（激活时 `#1abc9c`），激活态背景 `#e6f7f1`、文字 `#0c6b4f`、字重 600。
  - 整体：白底、右边框 `#e8eaed`，与主区区分。
- **实现方式**：在 [app.py](d:\文件\充电代码工作\analysis\app.py) 中用 `st.sidebar` 内 `st.radio` 或自定义 HTML（`st.markdown(..., unsafe_allow_html=True)`）+ CSS 注入（`st.markdown("<style>...</style>", unsafe_allow_html=True)`）复刻上述样式；用 `st.session_state.view_mode` 记录当前页（"入库" | "标准化数据产品"），主区根据其渲染对应内容。

### 1.2 主区元素与图片

- **参考 prototype**：顶部 banner（`.banner-area`：浅绿渐变背景、说明文案、可选按钮）、标题区（`.page-title`：大标题 + 副标题）、内容区卡片（白底、圆角、轻阴影）。
- **在 Streamlit 中的做法**：
  - 各页顶部用 `st.markdown` 渲染一块「banner」HTML：渐变背景、一句说明（如「将清洗后的数据一键写入数据库」/「生成与开放服务平台一致的数据表」）。
  - 标题使用 `st.title` 或带样式的 `st.markdown`（大号、深色 `#1a3c6e`）。
  - 区块用 `st.container` 或 `st.expander` 包一层，配合自定义 CSS 加圆角、阴影（与 [.streamlit/config.toml](d:\文件\充电代码工作\analysis\.streamlit\config.toml) 中 theme 一致时可再在 CSS 中微调）。
  - **图片**：在项目内新增 `assets/`（如 `analysis/assets/`），放一张示意图（如数据流/充电场景图）；在入库页或标准化产品页用 `st.image("assets/xxx.png")` 放在 banner 旁或说明区，或通过 HTML `<img>` 嵌入。若暂无图，可先用占位 div（带虚线框与“示意图”文字），与 prototype 的 `.qr-placeholder` 类似。

### 1.3 文件与配置

- 在 [app.py](d:\文件\充电代码工作\analysis\app.py) 顶部或各页前注入侧栏与主区 CSS（字体、颜色、圆角、阴影）。
- 可选：将长样式抽到 [.streamlit/config.toml](d:\文件\充电代码工作\analysis\.streamlit\config.toml) 或单独 `.css` 通过 `st.markdown` 引入。

---

## 二、连接数据库并实现上传入库

### 2.1 数据格式与表结构

- **数据形式**：以「充电数据示例」中两份示例表及 [数据格式与入库规范说明.md](D:\文件\充电代码工作\merge\merge_app\数据格式与入库规范说明.md) 为准。即：
  - **充电桩表**：首列「上报机构」+ [table_merge_handler.STANDARD_COLUMNS](D:\文件\充电代码工作\merge\merge_app\handlers\table_merge_handler.py) 中列（序号、充电桩编号、…、充电桩生产厂商类型），与 merge 清洗后输出一致。
  - **充电站表**：首列「上报机构」+ [station_merge_handler.STATION_STANDARD_COLUMNS](D:\文件\充电代码工作\merge\merge_app\handlers\station_merge_handler.py) 中列。
- **入库前校验**：上传后根据列名判断为充电桩表或充电站表（与现有 `_detect_table_type` 一致）；若用户选择「表类型」则以其为准。校验必备列存在后再执行写入。

### 2.2 入库方式与目标表

- **evdata 表追加数据**：目标为 evdata 库内已存在的表。需在 UI 上让用户选择「充电桩表」或「充电站表」并选择目标表名（可从 `db_helper.list_tables()` 拉取列表，或固定为如 `pile_cleaned` / `station_cleaned`）。将上传数据 **append** 到该表。
- **新增表导入数据**：用户输入新表名（或留空时用 `pile_YYYYMMDD` / `station_YYYYMMDD`）。若表不存在则 **CREATE TABLE** 后插入；若已存在则按「追加」处理或提示冲突（建议：存在则追加）。

### 2.3 建表与插入逻辑

- **建表**：根据 DataFrame 的列名与 dtype 生成 MySQL 建表语句。字符串列用 `VARCHAR(长度)`（如充电站位置 600、其余 255 或 500），数值列用 `BIGINT`/`DOUBLE`，日期列用 `DATE` 或 `VARCHAR(50)`。可参考 [数据格式与入库规范说明.md](D:\文件\充电代码工作\merge\merge_app\数据格式与入库规范说明.md) 中“字段形式与单位约定”定长度。
- **插入**：优先使用 `pandas.DataFrame.to_sql(..., if_exists="append", method="multi")` 或逐行 `INSERT`（便于统计单行成功/失败）。使用 [db_helper](d:\文件\充电代码工作\analysis\db_helper.py) 的 DB_CONFIG 建立连接（pymysql 或 sqlalchemy.create_engine）。
- **摘要**：执行结束后返回并展示 **成功条数**、**失败条数**；若有失败，可记录前 N 条错误原因（如唯一键冲突、类型错误、截断等）在 expander 中展示。

### 2.4 模块划分

- **db_helper.py**：在现有 `list_tables`、`read_table` 基础上增加：
  - `get_connection()`：返回 pymysql 连接或 SQLAlchemy engine，供入库与读表共用。
  - `create_table_from_df(engine, table_name, df, table_type)`：按表类型（pile/station）与 df 列生成 CREATE TABLE 并执行。
  - `insert_df_to_table(engine, table_name, df)`：将 df 写入表（if_exists="append" 或逐行），返回 `(success_count, fail_count, error_messages)`。
- **app.py 入库页**：在「执行入库」按钮逻辑中：读取上传文件为 DataFrame → 校验表类型与必备列 → 根据入库方式调用上述函数 → 展示摘要（成功/失败条数及可选错误列表）。

### 2.5 依赖

- 若使用 `to_sql`，需 `sqlalchemy` 与 `pymysql`；[requirements.txt](d:\文件\充电代码工作\analysis\requirements.txt) 中已有 `pymysql`，可增加 `sqlalchemy`。

---

## 三、实施顺序建议

1. **app.py 侧栏**：改为侧栏两项（入库、标准化数据产品），用 session_state 切换主区内容；注入侧栏 CSS，放大字体并套用 prototype 风格（颜色、激活态、左边框）。
2. **主区 UI**：在入库页与标准化产品页顶部增加 banner（HTML+CSS）；标题与卡片样式统一；在 `assets/` 增加占位图或一张示意图并在页中引用。
3. **db_helper 扩展**：实现 `get_connection`、`create_table_from_df`、`insert_df_to_table`（或等价函数），处理编码与类型映射。
4. **入库页逻辑**：连接「选择清洗后文件」+「表类型」+「入库方式」+「目标表名」到上述 DB 函数，执行后展示成功/失败摘要；错误信息可折叠展示。
5. **测试**：用「充电数据示例」中两个示例表（或 merge 导出的清洗后表）做一次「新增表导入」与一次「evdata 表追加」验证。

---

## 四、不确定点

- **充电数据示例** 若为 merge **前**的原始表（多 sheet、表头在第二行等），则入库前是否需要先走一遍 merge+清洗再入库？当前计划假定上传的是 **清洗后** 的单表（与 merge 输出一致），直接写入 MySQL。若需支持“原始表上传后自动合并+清洗再入库”，则需在 analysis 中复用或调用 merge_app 的合并与清洗逻辑，范围会扩大。
- **evdata 表追加** 时目标表是否固定为两张（如 `pile_cleaned`、`station_cleaned`）并由用户选择其一，还是从库内任意表下拉选择？建议：下拉列出 evdata 内所有表，用户选一张，再选「充电桩表」或「充电站表」以校验列是否匹配。

按上述顺序可实现：侧栏导航（入库在上、标准化产品在下）、侧栏与主区 UI 参考 prototype、真实连接数据库并支持上传数据写入（格式对齐示例与规范）。
