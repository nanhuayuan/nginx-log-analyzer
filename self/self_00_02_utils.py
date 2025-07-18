
"""
基础工具函数模块 - 提供通用工具函数
"""

import os
import psutil
import numpy as np
from datetime import datetime

def log_info(message, show_memory=False, level="INFO"):
    """输出日志信息，可选显示内存使用情况"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    memory_info = ""
    if show_memory:
        process = psutil.Process(os.getpid())
        memory_usage_mb = process.memory_info().rss / 1024 / 1024
        memory_info = f" [内存: {memory_usage_mb:.2f} MB]"

    print(f"[{timestamp}] [{level}]{memory_info} {message}")


def monitor_memory():
    """监控当前内存使用情况"""
    process = psutil.Process(os.getpid())
    memory_usage_mb = process.memory_info().rss / 1024 / 1024
    log_info(f"当前内存使用: {memory_usage_mb:.2f} MB", level="MEMORY")
    return memory_usage_mb


def format_memory_usage():
    """格式化内存使用情况为字符串"""
    process = psutil.Process(os.getpid())
    memory_usage_mb = process.memory_info().rss / 1024 / 1024
    return f"{memory_usage_mb:.2f} MB"


def extract_app_name(filename):
    """从日志文件名中提取应用名称"""
    base_name = os.path.basename(filename)
    parts = base_name.split('_')
    if len(parts) >= 2:
        return '_'.join(parts[:-1]) if parts[-1].endswith('.log') else '_'.join(parts[:-2])
    return base_name.split('.')[0]


def extract_service_from_path(path):
    """从请求路径中提取服务名称"""
    if not path or not isinstance(path, str):
        return ""
    path = path.split('?')[0]
    if not path.startswith('/'):
        path = '/' + path
    parts = path.strip('/').split('/')
    if len(parts) > 0:
        if parts[0] == 'api' and len(parts) > 1:
            return parts[1]
        return parts[0]

    return ""


def get_distribution_stats(values_array, metric_name):
    """计算数组的分布统计指标，包括平均值、中位数、最小值、最大值和分位数"""
    if len(values_array) == 0:
        return {}
        
    stats = {
        f'avg_{metric_name}': np.mean(values_array),
        f'min_{metric_name}': np.min(values_array),
        f'max_{metric_name}': np.max(values_array),
        f'median_{metric_name}': np.median(values_array),
    }
    
    # 添加分位数
    for percentile in [50, 90, 95, 99]:
        stats[f'p{percentile}_{metric_name}'] = np.percentile(values_array, percentile)
        
    return stats


def calculate_time_percentages(time_values):
    """计算各时间阶段的占比百分比"""
    total = sum(time_values.values())
    if total <= 0:
        return {k: 0 for k in time_values}
    
    return {k: (v / total) * 100 for k, v in time_values.items()}


def calculate_time_metrics(time_stats):
    """计算各个时间指标的统计数据，包括平均值、中位数和分位数"""
    metrics = {}
    
    for time_key, metric_data in time_stats.items():
        metrics[time_key] = {}
        for metric, values in metric_data.items():
            if not values:
                continue
                
            values_array = np.array(values)
            metrics[time_key][metric] = {
                'avg': np.mean(values_array),
                'min': np.min(values_array),
                'max': np.max(values_array),
                'median': np.median(values_array),
                'p50': np.percentile(values_array, 50),
                'p90': np.percentile(values_array, 90),
                'p95': np.percentile(values_array, 95),
                'p99': np.percentile(values_array, 99)
            }
            
    return metrics





