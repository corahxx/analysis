# config.py - Analysis 系统配置
# 支持环境变量覆盖，优先级：环境变量 > 配置文件（用于一键入库）

import os

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '3306')),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'caam'),
    'database': os.getenv('DB_NAME', 'evdata')
}
