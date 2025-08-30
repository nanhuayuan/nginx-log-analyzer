# -*- coding: utf-8 -*-
"""
系统配置文件
"""

import os
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
DATA_ROOT = PROJECT_ROOT.parent / "data" / "demo"

# 数据库配置
DATABASE = {
    'type': 'sqlite',
    'path': PROJECT_ROOT / 'database' / 'nginx_analytics.db',
    'echo': False  # SQL调试日志
}

# 数据源配置
DATA_SOURCE = {
    # 默认CSV数据源路径
    'default_csv_path': DATA_ROOT / "自研Ng2025.05.09日志-样例_分析结果_20250829_224524_temp" / "processed_logs.csv",
    
    # 数据更新间隔(秒)
    'update_interval': 300,
    
    # 数据保留天数
    'retention_days': 30
}

# Web服务配置
WEB_SERVER = {
    'host': '0.0.0.0',
    'port': 5000,
    'debug': True
}

# 分析配置
ANALYSIS = {
    # 慢请求阈值(秒)
    'slow_threshold': 3.0,
    
    # 成功状态码
    'success_codes': ['200'],
    
    # 异常检测敏感度
    'anomaly_sensitivity': 2.0,  # 标准差倍数
    
    # 最小统计样本数
    'min_sample_size': 100
}

# 维度定义
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

# 报警配置
ALERTS = {
    # 错误率阈值
    'error_rate_threshold': 5.0,  # 百分比
    
    # 响应时间阈值
    'response_time_threshold': 3.0,  # 秒
    
    # 流量变化阈值  
    'traffic_change_threshold': 50.0,  # 百分比
    
    # 通知配置(预留)
    'notification': {
        'enabled': False,
        'webhook_url': None,
        'email_list': []
    }
}