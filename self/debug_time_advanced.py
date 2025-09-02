#!/usr/bin/env python3
"""
调试高级时间维度分析器
检查为什么只生成概览页
"""

import pandas as pd
from datetime import datetime
import numpy as np

def debug_analyzer():
    """调试分析器逻辑"""
    print("🔍 调试高级时间维度分析器...")
    
    # 模拟小量数据测试
    test_data = create_debug_data()
    print(f"✅ 创建测试数据: {len(test_data)} 条记录")
    
    # 检查时间字段处理
    processed_data = preprocess_time_fields(test_data)
    print("✅ 时间字段处理完成")
    
    # 检查维度分组
    check_dimension_grouping(processed_data)
    
    # 检查结果生成
    check_result_generation()

def create_debug_data():
    """创建调试用的测试数据"""
    base_time = datetime(2025, 7, 20, 10, 0, 0)
    
    data = []
    for i in range(10):  # 只创建10条记录用于调试
        arrival_time = base_time + pd.Timedelta(seconds=i*10)
        completion_time = arrival_time + pd.Timedelta(seconds=np.random.randint(1, 5))
        
        data.append({
            'arrival_time': arrival_time.strftime('%Y-%m-%d %H:%M:%S'),
            'time': completion_time.strftime('%Y-%m-%d %H:%M:%S'),
            'arrival_timestamp': arrival_time.timestamp(),
            'timestamp': completion_time.timestamp(),
            'total_request_duration': np.random.uniform(0.5, 3.0),
            'upstream_response_time': np.random.uniform(0.1, 2.0),
            'upstream_header_time': np.random.uniform(0.1, 1.0),
            'upstream_connect_time': np.random.uniform(0.01, 0.5),
            'status': np.random.choice([200, 201, 400, 500], p=[0.7, 0.2, 0.05, 0.05]),
            'client_ip': f"192.168.1.{i % 5 + 1}"
        })
    
    return pd.DataFrame(data)

def preprocess_time_fields(chunk):
    """预处理时间字段"""
    chunk = chunk.copy()
    
    # 转换时间戳
    for col in ['timestamp', 'arrival_timestamp']:
        if col in chunk.columns:
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
    
    # 转换时间字段
    for col in ['time', 'arrival_time']:
        if col in chunk.columns:
            chunk[col] = pd.to_datetime(chunk[col], errors='coerce')
    
    # 创建时间维度字段（基于完成时间和到达时间）
    if 'time' in chunk.columns:
        dt = pd.to_datetime(chunk['time'])
        chunk['completion_daily'] = dt.dt.strftime('%Y-%m-%d')
        chunk['completion_hourly'] = dt.dt.strftime('%Y-%m-%d %H:00')
        chunk['completion_minute'] = dt.dt.strftime('%Y-%m-%d %H:%M')
        chunk['completion_second'] = dt.dt.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"  完成时间维度样例:")
        print(f"    daily: {chunk['completion_daily'].iloc[0]}")
        print(f"    hourly: {chunk['completion_hourly'].iloc[0]}")
        print(f"    minute: {chunk['completion_minute'].iloc[0]}")
        print(f"    second: {chunk['completion_second'].iloc[0]}")
    
    if 'arrival_time' in chunk.columns:
        dt_arrival = pd.to_datetime(chunk['arrival_time'])
        chunk['arrival_daily'] = dt_arrival.dt.strftime('%Y-%m-%d')
        chunk['arrival_hourly'] = dt_arrival.dt.strftime('%Y-%m-%d %H:00')
        chunk['arrival_minute'] = dt_arrival.dt.strftime('%Y-%m-%d %H:%M')
        chunk['arrival_second'] = dt_arrival.dt.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"  到达时间维度样例:")
        print(f"    daily: {chunk['arrival_daily'].iloc[0]}")
        print(f"    hourly: {chunk['arrival_hourly'].iloc[0]}")
    
    return chunk

def check_dimension_grouping(data):
    """检查维度分组逻辑"""
    print("🔍 检查维度分组...")
    
    dimensions = ['daily', 'hourly', 'minute', 'second']
    
    for dimension in dimensions:
        completion_col = f'completion_{dimension}'
        if completion_col in data.columns:
            groups = data.groupby(completion_col)
            group_count = len(groups)
            print(f"  ✅ {dimension}: {group_count} 个时间组")
            
            # 显示分组详情
            for name, group in groups:
                print(f"    {name}: {len(group)} 条记录")
        else:
            print(f"  ❌ {dimension}: 缺少 {completion_col} 列")

def check_result_generation():
    """检查结果生成逻辑"""
    print("🔍 检查结果生成逻辑...")
    
    # 模拟结果结构
    mock_results = {
        'daily': {
            '2025-07-20': {
                'total_requests': 10,
                'success_requests': 8,
                'slow_requests': 2,
                'success_rate': 80.0,
                'slow_rate': 20.0,
                'qps': 0.00009,
                'new_connections': 10,
                'concurrent_connections': 2,
                'active_connections': 8,
                'unique_ips': 5,
                'percentiles': {
                    'total_request_duration': {'P50': 1.5, 'P95': 2.8, 'P99': 2.9}
                }
            }
        },
        'hourly': {
            '2025-07-20 10:00': {
                'total_requests': 10,
                'success_requests': 8,
                'slow_requests': 2
            }
        }
    }
    
    print("  ✅ 模拟结果结构:")
    for dimension, data in mock_results.items():
        print(f"    {dimension}: {len(data)} 个时间组")
        for time_key, stats in data.items():
            print(f"      {time_key}: {len(stats)} 个指标")
    
    # 检查条件判断
    for dimension in ['daily', 'hourly', 'minute', 'second']:
        has_data = dimension in mock_results and mock_results[dimension]
        print(f"  📊 {dimension} 满足条件: {has_data}")

def check_excel_creation_logic():
    """检查Excel创建逻辑"""
    print("🔍 检查Excel创建逻辑...")
    
    # 模拟Excel创建过程
    dimensions = [
        ('日期维度分析', 'daily'),
        ('小时维度分析', 'hourly'),
        ('分钟维度分析', 'minute'),
        ('秒级维度分析', 'second')
    ]
    
    mock_results = {
        'daily': {'2025-07-20': {'total_requests': 10}},
        'hourly': {},  # 空数据
        'minute': {'2025-07-20 10:30': {'total_requests': 5}},
        'second': None  # None数据
    }
    
    for sheet_name, dimension in dimensions:
        # 检查条件：dimension in results and results[dimension]
        condition1 = dimension in mock_results
        condition2 = mock_results[dimension] if condition1 else False
        
        print(f"  📋 {sheet_name}:")
        print(f"    dimension in results: {condition1}")
        print(f"    results[dimension]: {condition2}")
        print(f"    会创建工作表: {condition1 and condition2}")

if __name__ == "__main__":
    debug_analyzer()
    print("\n" + "="*50)
    check_excel_creation_logic()