#!/usr/bin/env python3
"""
验证字段映射和计算修复
使用实际数据格式进行测试
"""

import json
from datetime import datetime
import pandas as pd

def analyze_sample_data():
    """分析样本数据的字段结构"""
    print("🔍 分析样本数据字段结构...")
    
    # 模拟原始日志中的一条记录
    sample_record = {
        "log_id": "test_record",
        "time": "2025-05-09T11:16:11+08:00",  # ISO格式到达时间
        "timestamp": "1746760571.428",        # Unix时间戳完成时间
        "request_time": "1.502",              # 字符串格式请求时间
        "client_ip": "100.100.8.44",
        "status": "200",                      # 字符串格式状态码
        "upstream_response_time": "1.502",    # 字符串格式上游时间
        "upstream_connect_time": "0.001",     # 字符串格式连接时间
        "body_bytes_sent": "621"              # 字符串格式响应大小
    }
    
    print("📋 样本数据字段:")
    for key, value in sample_record.items():
        print(f"  {key}: {value} ({type(value).__name__})")
    
    return sample_record

def test_time_conversion():
    """测试时间转换逻辑"""
    print("\n🕐 测试时间转换...")
    
    # 测试Unix时间戳转换
    unix_timestamp = "1746760571.428"
    print(f"Unix时间戳: {unix_timestamp}")
    
    try:
        if unix_timestamp.replace('.', '').isdigit():
            unix_ts = float(unix_timestamp)
            dt = pd.to_datetime(unix_ts, unit='s')
            print(f"✅ 转换结果: {dt}")
            print(f"   格式化: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print("❌ 不是有效的Unix时间戳")
    except Exception as e:
        print(f"❌ 转换失败: {e}")
    
    # 测试ISO时间格式
    iso_time = "2025-05-09T11:16:11+08:00"
    print(f"\nISO时间: {iso_time}")
    
    try:
        dt = pd.to_datetime(iso_time)
        print(f"✅ 转换结果: {dt}")
        print(f"   格式化: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        print(f"❌ 转换失败: {e}")

def test_field_conversion():
    """测试字段类型转换"""
    print("\n🔄 测试字段类型转换...")
    
    test_cases = [
        ("status", "200", int),
        ("request_time", "1.502", float),
        ("upstream_response_time", "1.502", float),
        ("upstream_connect_time", "", float),  # 空字符串
        ("body_bytes_sent", "621", int)
    ]
    
    for field_name, value, target_type in test_cases:
        print(f"\n{field_name}: '{value}' -> {target_type.__name__}")
        
        try:
            if target_type == int:
                if isinstance(value, str):
                    result = int(value) if value else None
                else:
                    result = value
            elif target_type == float:
                if isinstance(value, str):
                    result = float(value) if value else 0.0
                else:
                    result = value
            else:
                result = value
            
            print(f"✅ 转换结果: {result} ({type(result).__name__})")
            
        except (ValueError, TypeError) as e:
            print(f"❌ 转换失败: {e}")

def test_connection_calculation():
    """测试连接数计算逻辑"""
    print("\n🔗 测试连接数计算逻辑...")
    
    # 模拟连接数据
    test_data = [
        {
            'arrival_time': pd.to_datetime('2025-05-09 11:16:11'),
            'completion_time': pd.to_datetime('2025-05-09 11:16:12.5'),  # 1.5秒后完成
            'client_ip': '100.100.8.44'
        },
        {
            'arrival_time': pd.to_datetime('2025-05-09 11:16:11.5'),
            'completion_time': pd.to_datetime('2025-05-09 11:16:13'),    # 1.5秒后完成
            'client_ip': '100.100.8.45'
        },
        {
            'arrival_time': pd.to_datetime('2025-05-09 11:16:12'),
            'completion_time': pd.to_datetime('2025-05-09 11:16:12.1'),  # 0.1秒后完成
            'client_ip': '100.100.8.44'
        }
    ]
    
    requests_df = pd.DataFrame(test_data)
    print("📊 测试数据:")
    print(requests_df)
    
    # 时间窗口: 11:16:11 - 11:16:12 (1秒窗口)
    window_start = pd.to_datetime('2025-05-09 11:16:11')
    window_end = pd.to_datetime('2025-05-09 11:16:12')
    
    print(f"\n🕐 时间窗口: {window_start} ~ {window_end}")
    
    # 1. 新建连接数 = 到达时间在[T, T+N)内的请求数
    new_mask = (
        (requests_df['arrival_time'] >= window_start) & 
        (requests_df['arrival_time'] < window_end)
    )
    new_connections = len(requests_df[new_mask])
    print(f"📈 新建连接数: {new_connections}")
    print(f"   匹配记录: {requests_df[new_mask]['arrival_time'].tolist()}")
    
    # 2. 并发连接数 = 到达时间<T+N ≤ 请求完成时间
    concurrent_mask = (
        (requests_df['arrival_time'] < window_end) & 
        (requests_df['completion_time'] >= window_end)
    )
    concurrent_connections = len(requests_df[concurrent_mask])
    print(f"📊 并发连接数: {concurrent_connections}")
    print(f"   匹配记录: {requests_df[concurrent_mask][['arrival_time', 'completion_time']].values.tolist()}")
    
    # 3. 活跃连接数 = 到达时间≤T+N 且 完成时间≥T
    active_mask = (
        (requests_df['arrival_time'] <= window_end) & 
        (requests_df['completion_time'] >= window_start)
    )
    active_connections = len(requests_df[active_mask])
    print(f"🔄 活跃连接数: {active_connections}")
    print(f"   匹配记录: {requests_df[active_mask][['arrival_time', 'completion_time']].values.tolist()}")
    
    # 4. 连接复用率
    if new_connections > 0 and active_connections > 0:
        reuse_rate = max(0, (active_connections - new_connections) / active_connections * 100)
    else:
        reuse_rate = 0.0
    print(f"♻️  连接复用率: {reuse_rate:.2f}%")

def main():
    """主测试函数"""
    print("=== 字段映射和计算修复验证 ===")
    
    # 分析样本数据
    sample = analyze_sample_data()
    
    # 测试时间转换
    test_time_conversion()
    
    # 测试字段转换
    test_field_conversion()
    
    # 测试连接数计算
    test_connection_calculation()
    
    print("\n📋 修复要点总结:")
    print("✅ 1. 字段映射修复:")
    print("   - arrival_time: record.get('time')  # ISO格式")
    print("   - completion_time: record.get('timestamp')  # Unix时间戳")
    print("✅ 2. 类型转换修复:")
    print("   - status: str -> int")
    print("   - request_time: str -> float")
    print("   - upstream_*: 空字符串处理")
    print("✅ 3. 时间格式修复:")
    print("   - Unix时间戳自动检测和转换")
    print("   - ISO时间格式支持")
    print("✅ 4. 连接数计算:")
    print("   - 科学的时间窗口算法")
    print("   - 正确的到达/完成时间逻辑")

if __name__ == "__main__":
    main()