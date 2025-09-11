#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Apache Superset Configuration for Nginx Analytics
Superset配置文件 - Nginx日志分析系统
"""

import os
from datetime import timedelta

# Flask应用配置
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'nginx_analytics_secret_key_change_in_production')
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24  # 24小时

# 数据库配置 - 使用SQLite避免PostgreSQL驱动问题
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# Redis缓存配置
REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', 'redis_password')

# 缓存配置
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': REDIS_PORT,
    'CACHE_REDIS_PASSWORD': REDIS_PASSWORD,
    'CACHE_REDIS_DB': 1,
}

# 结果后端配置
RESULTS_BACKEND = {
    'cache_type': 'RedisCache',
    'cache_default_timeout': 86400,  # 24小时
    'cache_key_prefix': 'superset_results_',
    'cache_redis_host': REDIS_HOST,
    'cache_redis_port': REDIS_PORT,
    'cache_redis_password': REDIS_PASSWORD,
    'cache_redis_db': 2,
}

# Celery配置
class CeleryConfig(object):
    BROKER_URL = f'redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/3'
    CELERY_IMPORTS = ('superset.sql_lab', )
    CELERY_RESULT_BACKEND = f'redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/4'
    CELERYD_LOG_LEVEL = 'DEBUG'
    CELERYD_PREFETCH_MULTIPLIER = 1
    CELERY_ACKS_LATE = True

CELERY_CONFIG = CeleryConfig

# 安全配置 - 禁用HTTPS重定向
TALISMAN_ENABLED = False
ENABLE_PROXY_FIX = False

# 功能标志
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'DASHBOARD_RBAC': True,
    'ENABLE_ADVANCED_DATA_TYPES': True,
    'VERSIONED_EXPORT': True,
}

# 日志配置 - 修复日志目录问题
ENABLE_TIME_ROTATE = True  
TIME_ROTATE_LOG_LEVEL = 'INFO'
# 确保日志目录存在
import os
logs_dir = '/app/superset_home/logs'
os.makedirs(logs_dir, exist_ok=True)
FILENAME = os.path.join(logs_dir, 'superset.log')

# 数据库查询限制
ROW_LIMIT = 5000
VIZ_ROW_LIMIT = 10000
SQL_MAX_ROW = 100000
SUPERSET_WEBSERVER_TIMEOUT = 300

# 上传配置
UPLOAD_FOLDER = '/app/superset_home/uploads'
IMG_UPLOAD_FOLDER = '/app/superset_home/uploads'
IMG_UPLOAD_URL = '/static/uploads/'

# 邮件配置(可选)
SMTP_HOST = os.environ.get('SMTP_HOST', '')
SMTP_STARTTLS = True
SMTP_SSL = False
SMTP_USER = os.environ.get('SMTP_USER', '')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '')
SMTP_MAIL_FROM = os.environ.get('SMTP_MAIL_FROM', 'superset@nginx-analytics.com')

# 时区配置
DEFAULT_TIMEZONE = "Asia/Shanghai"

# 语言配置
LANGUAGES = {
    'en': {'flag': 'us', 'name': 'English'},
    'zh': {'flag': 'cn', 'name': 'Chinese'},
}

# 自定义CSS(可选)
CSS_DEFAULT_CONTENT = """
.navbar-brand {
    font-weight: bold;
}
"""

# 安全管理器
CUSTOM_SECURITY_MANAGER = None

# 数据库连接池配置
SQLALCHEMY_ENGINE_OPTIONS = {
    'pool_pre_ping': True,
    'pool_recycle': 300,
    'echo': False,
}

# WebDriver配置(用于报表导出)
WEBDRIVER_TYPE = "chrome"
WEBDRIVER_OPTION_ARGS = [
    "--headless",
    "--disable-gpu",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--window-size=1920,1080",
]

print("✅ Superset配置加载完成 - Nginx Analytics")