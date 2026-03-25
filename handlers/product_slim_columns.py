# handlers/product_slim_columns.py - 标准数据产品从库读取时的「瘦身列」集合（避免 SELECT * 扫全列）

from typing import Collection, List

# 七类产品在充电桩表模式下可能用到的列（与表中实际列名求交集后读取）
SLIM_COLUMNS_PILE: tuple = (
    "省份_中文",
    "省份",
    "额定功率",
    "充电桩类型_转换",
    "运营商名称",
    "上报机构",
    "城市_中文",
    "城市",
    "充电桩型号",
)

# 充电站表模式
SLIM_COLUMNS_STATION: tuple = (
    "省份_中文",
    "省份",
    "站点总装机功率",
    "运营商名称",
    "上报机构",
    "城市_中文",
    "城市",
)


def resolve_slim_columns_for_products(for_pile: bool, available_columns: Collection[str]) -> List[str]:
    """
    返回按标准顺序排列、且在当前表中存在的列名列表。
    若与表无交集则返回空列表（调用方应回退为 SELECT *）。
    """
    avail = set(available_columns or ())
    want = SLIM_COLUMNS_PILE if for_pile else SLIM_COLUMNS_STATION
    return [c for c in want if c in avail]
