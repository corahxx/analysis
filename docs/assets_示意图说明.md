# 示意图等静态资源

将图片放入本目录后，在页面中会自动显示（无需改代码）：

- **import-banner.png**：入库页示意图（数据流/充电场景）。不放则显示虚线占位框。
- **product-banner.png**：标准化数据产品页示意图。不放则显示占位框。

引用方式在 app.py 中已实现：通过 `_show_asset_image("文件名", "占位 HTML")`，若文件存在则用 `st.image(路径)` 显示，否则显示占位。路径由 `_assets_path(filename)` 基于 app.py 所在目录计算，因此无论从哪个目录执行 `streamlit run app.py` 都能正确找到图片。
