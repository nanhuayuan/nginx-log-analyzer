# -*- coding: utf-8 -*-
"""
ClickHouse配置文件
升级后替换SQLite配置
"""

import os
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# ClickHouse数据库配置
CLICKHOUSE = {
    'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
    'port': int(os.getenv('CLICKHOUSE_PORT', 8123)),
    'username': os.getenv('CLICKHOUSE_USER', 'analytics_user'),
    'password': os.getenv('CLICKHOUSE_PASSWORD', 'analytics_password'),
    'database': os.getenv('CLICKHOUSE_DB', 'nginx_analytics'),
    
    # 连接池配置
    'pool_size': 10,
    'max_overflow': 20,
    'pool_timeout': 30,
    'pool_recycle': 3600,
    
    # 查询配置
    'query_timeout': 60,
    'insert_timeout': 300,
    'compress': True,
    'compression_method': 'gzip',
    
    # 批处理配置
    'batch_size': 10000,
    'buffer_size': 1000000,
}

# Web服务配置（保持不变）
WEB_SERVER = {
    'host': '0.0.0.0',
    'port': 5000,
    'debug': False  # 生产环境关闭调试模式
}

# 分析配置（优化ClickHouse性能）
ANALYSIS = {
    # 慢请求阈值(秒) - ClickHouse环境可以降低阈值
    'slow_threshold': 2.0,
    
    # 成功状态码
    'success_codes': ['200'],
    
    # 异常检测敏感度
    'anomaly_sensitivity': 2.0,
    
    # 最小统计样本数 - ClickHouse可以处理更小的样本
    'min_sample_size': 50,
    
    # 查询优化配置
    'query_cache_ttl': 300,  # 查询缓存5分钟
    'aggregation_cache_ttl': 600,  # 聚合缓存10分钟
}

# 维度定义（保持不变）
DIMENSIONS = {
    # 平台维度
    'platform': {
        'iOS_SDK': {'pattern': r'(wst-sdk-ios|zgt-ios/)', 'priority': 1},
        'Android_SDK': {'pattern': r'(wst-sdk-android|zgt-android/)', 'priority': 2},
        'Java_SDK': {'pattern': r'(wst-sdk-java)', 'priority': 3},
        'Android': {'pattern': r'android \d+\.|android;|linux.*android|dalvik.*android', 'priority': 4},
        'iOS': {'pattern': r'iphone os|ipad|ipod|cpu os.*like mac os x|ios \d+\.|cfnetwork.*darwin', 'priority': 5},
        'macOS': {'pattern': r'macintosh.*mac os x|macos', 'priority': 6},
        'Windows': {'pattern': r'windows nt \d+\.\d+', 'priority': 7},
        'Bot/Spider': {'pattern': r'(spider|bot|crawler|curl|wget)', 'priority': 8},
        'HTTP_Client': {'pattern': r'(okhttp|retrofit|volley|alamofire)', 'priority': 9},
        'Other': {'pattern': r'.*', 'priority': 999}
    },
    
    # 入口来源维度
    'entry_source': {
        'Internal': {'keywords': ['wechat', 'weixin'], 'priority': 1},
        'Search_Engine': {'keywords': ['baidu', 'google', 'bing'], 'priority': 2},
        'Social_Media': {'keywords': ['weibo', 'qq', 'douyin'], 'priority': 3},
        'Direct': {'keywords': ['-', 'direct'], 'priority': 4},
        'External': {'keywords': ['http'], 'priority': 5},
        'Unknown': {'keywords': [], 'priority': 999}
    },
    
    # API分类维度  
    'api_category': {
        'User_Auth': {'patterns': [r'/api/.*/(login|auth|register|logout)']},
        'Business_Core': {'patterns': [r'/api/.*/order', r'/api/.*/payment', r'/api/.*/user']},
        'System_Config': {'patterns': [r'/api/.*/config', r'/api/.*/settings']},
        'Third_Party': {'patterns': [r'/api/.*/(third|external|gateway)']},
        'Static_Resource': {'patterns': [r'\.(css|js|png|jpg|gif|ico)$']},
        'Other': {'patterns': [r'.*']}
    }
}

# ClickHouse性能优化配置
CLICKHOUSE_OPTIMIZATION = {
    # 表引擎优化
    'merge_tree_settings': {
        'index_granularity': 8192,
        'parts_to_throw_insert': 300,
        'parts_to_delay_insert': 150,
        'max_parts_in_total': 100000,
    },
    
    # 查询优化
    'query_settings': {
        'max_threads': 8,
        'max_memory_usage': '20000000000',  # 20GB
        'use_uncompressed_cache': 1,
        'allow_experimental_analyzer': 1,
    },
    
    # 插入优化
    'insert_settings': {
        'async_insert': 1,
        'wait_for_async_insert': 1,
        'async_insert_threads': 16,
        'async_insert_max_data_size': 1000000,
    },
}

# 数据保留策略
DATA_RETENTION = {
    # ODS层数据保留策略
    'ods_retention_days': 90,
    
    # DWD层数据保留策略  
    'dwd_retention_days': 365,
    
    # DWS层数据保留策略
    'dws_retention_days': 1095,  # 3年
    
    # ADS层数据保留策略
    'ads_retention_days': 365,
    
    # 自动清理配置
    'auto_cleanup_enabled': True,
    'cleanup_schedule': '0 2 * * *',  # 每天凌晨2点
}

# 监控和告警配置
MONITORING = {
    # 性能监控阈值
    'query_slow_threshold_ms': 5000,
    'insert_slow_threshold_ms': 10000,
    'connection_timeout_threshold': 10,
    
    # 告警配置
    'alerts_enabled': True,
    'alert_channels': {
        'webhook_url': os.getenv('ALERT_WEBHOOK_URL'),
        'email_list': os.getenv('ALERT_EMAILS', '').split(','),
    },
    
    # 健康检查配置
    'health_check_interval': 30,
    'health_check_timeout': 10,
}

# 缓存配置
CACHE = {
    # Redis配置（可选）
    'redis_host': os.getenv('REDIS_HOST', 'localhost'),
    'redis_port': int(os.getenv('REDIS_PORT', 6379)),
    'redis_db': int(os.getenv('REDIS_DB', 0)),
    'redis_password': os.getenv('REDIS_PASSWORD'),
    
    # 应用层缓存配置
    'enable_query_cache': True,
    'query_cache_ttl': 300,
    'enable_stats_cache': True,
    'stats_cache_ttl': 600,
}