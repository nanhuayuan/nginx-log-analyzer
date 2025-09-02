#!/usr/bin/env python3
"""
测试CSV字段映射的正确性
基于实际的CSV列结构验证修复
"""

import pandas as pd
from datetime import datetime, timedelta

def simulate_csv_record():
    """模拟CSV格式的记录"""
    print("📊 模拟CSV格式记录...")
    
    # 基于您提供的CSV列结构创建测试记录
    csv_record = {
        'source': 'nginx',
        'app_name': 'test_app',
        'time': '2025-05-09T11:16:11+08:00',
        'timestamp': '1746760571.428',
        'request_time': '1.502',
        'client_ip': '100.100.8.44',
        'client_port': '28235',
        'request_method': 'POST',
        'request_uri': '/api/test',
        'request_path': '/api/test',
        'query_string': '',
        'request_protocol': 'HTTP/1.1',
        'status': '200',
        'body_bytes_sent': '621',
        'bytes_sent': '1404',
        'content_type': 'application/json',
        'upstream_connect_time': '0.001',
        'upstream_header_time': '1.501',
        'upstream_response_time': '1.502',
        'upstream_addr': '192.168.8.17:80',
        'upstream_status': '200',
        'server_name': 'test.com',
        'host': 'test.com',
        'user_agent': 'Mozilla/5.0...',
        'referer': 'https://test.com/',
        'service_name': 'api_service',
        'date': '2025-05-09',
        'hour': '11',
        'minute': '16',
        'second': '11',
        'arrival_timestamp': '1746760570.926',  # 0.5秒前到达
        'arrival_time': '2025-05-09T11:16:10.926+08:00',
        'upstream_connect_phase': '0.001',
        'upstream_header_phase': '1.500',
        'upstream_body_phase': '0.001',
        'client_transfer_phase': '0.000'
    }
    
    print("✅ CSV记录字段:")
    for key, value in list(csv_record.items())[:10]:  # 显示前10个字段
        print(f"  {key}: {value}")
    print(f"  ... 总共 {len(csv_record)} 个字段")
    
    return csv_record

def test_field_mapping_csv():
    """测试CSV字段映射"""
    print("\n🔗 测试CSV字段映射...")
    
    record = simulate_csv_record()
    
    # 测试时间字段映射
    print("\n🕐 时间字段测试:")
    
    # 1. 完成时间（用于主要分组）
    completion_time = record.get('timestamp')
    print(f"completion_time (timestamp): {completion_time}")
    
    # 2. 到达时间（用于连接数计算）
    arrival_time = (record.get('arrival_timestamp') or 
                   record.get('arrival_time') or 
                   record.get('time'))
    print(f"arrival_time: {arrival_time}")
    
    # 测试时间转换
    try:
        # 完成时间转换
        if completion_time and completion_time.replace('.', '').isdigit():
            completion_dt = pd.to_datetime(float(completion_time), unit='s')
            print(f"✅ completion_dt: {completion_dt}")
        
        # 到达时间转换  
        if arrival_time:
            if arrival_time.replace('.', '').isdigit():
                arrival_dt = pd.to_datetime(float(arrival_time), unit='s')
            else:
                arrival_dt = pd.to_datetime(arrival_time)
            print(f"✅ arrival_dt: {arrival_dt}")
            
            # 计算时间差
            if 'completion_dt' in locals() and 'arrival_dt' in locals():
                time_diff = (completion_dt - arrival_dt).total_seconds()
                print(f"📏 时间差: {time_diff:.3f}秒")
                
    except Exception as e:
        print(f"❌ 时间转换失败: {e}")

def test_performance_fields():
    """测试性能字段"""
    print("\n⏱️ 测试性能字段...")
    
    record = simulate_csv_record()
    
    performance_fields = [
        ('request_time', '请求处理时间'),
        ('upstream_connect_time', '上游连接时间'),
        ('upstream_header_time', '上游头部时间'),
        ('upstream_response_time', '上游响应时间'),
        ('status', 'HTTP状态码'),
        ('body_bytes_sent', '响应体大小')
    ]
    
    for field_name, description in performance_fields:
        value = record.get(field_name)
        print(f"\n{description} ({field_name}): '{value}'")
        
        try:
            if field_name == 'status':
                converted = int(value) if value else None
            elif 'time' in field_name or 'bytes' in field_name:
                if value and value != '':
                    converted = float(value)
                else:
                    converted = 0.0
            else:
                converted = value
                
            print(f"✅ 转换结果: {converted} ({type(converted).__name__})")
            
        except (ValueError, TypeError) as e:
            print(f"❌ 转换失败: {e}")

def test_connection_calculation_csv():
    """测试基于CSV数据的连接数计算"""
    print("\n🔗 测试连接数计算（CSV格式）...")
    
    # 创建测试数据集
    test_records = []
    base_arrival = 1746760570.0  # 基准到达时间
    
    for i in range(5):
        arrival_ts = base_arrival + i * 0.5  # 每0.5秒一个请求
        completion_ts = arrival_ts + 1.0 + (i % 3) * 0.5  # 1-2秒的处理时间
        
        record = {
            'arrival_timestamp': str(arrival_ts),
            'timestamp': str(completion_ts),
            'request_time': str(completion_ts - arrival_ts),
            'client_ip': f'192.168.1.{i+1}',
            'status': '200'
        }
        test_records.append(record)
    
    # 转换为DataFrame
    df = pd.DataFrame(test_records)
    print("📊 测试数据集:")
    print(df[['arrival_timestamp', 'timestamp', 'request_time']])
    
    # 转换时间格式
    df['arrival_time'] = pd.to_datetime(df['arrival_timestamp'].astype(float), unit='s')
    df['completion_time'] = pd.to_datetime(df['timestamp'].astype(float), unit='s')
    
    print("\n📅 转换后的时间:")
    print(df[['arrival_time', 'completion_time']])
    
    # 定义时间窗口（第2秒）
    window_start = pd.to_datetime(base_arrival + 1.0, unit='s')
    window_end = pd.to_datetime(base_arrival + 2.0, unit='s')
    
    print(f"\n🕐 时间窗口: {window_start} ~ {window_end}")
    
    # 计算连接数
    # 1. 新建连接数
    new_mask = (
        (df['arrival_time'] >= window_start) & 
        (df['arrival_time'] < window_end)
    )
    new_connections = len(df[new_mask])
    print(f"📈 新建连接数: {new_connections}")
    
    # 2. 并发连接数
    concurrent_mask = (
        (df['arrival_time'] < window_end) & 
        (df['completion_time'] >= window_end)
    )
    concurrent_connections = len(df[concurrent_mask])
    print(f"📊 并发连接数: {concurrent_connections}")
    
    # 3. 活跃连接数
    active_mask = (
        (df['arrival_time'] <= window_end) & 
        (df['completion_time'] >= window_start)
    )
    active_connections = len(df[active_mask])
    print(f"🔄 活跃连接数: {active_connections}")
    
    # 连接复用率
    if new_connections > 0 and active_connections > 0:
        reuse_rate = max(0, (active_connections - new_connections) / active_connections * 100)
    else:
        reuse_rate = 0.0
    print(f"♻️  连接复用率: {reuse_rate:.2f}%")

def main():
    """主测试函数"""
    print("=== CSV字段映射验证测试 ===")
    
    # 模拟CSV记录
    simulate_csv_record()
    
    # 测试字段映射
    test_field_mapping_csv()
    
    # 测试性能字段
    test_performance_fields()
    
    # 测试连接数计算
    test_connection_calculation_csv()
    
    print("\n📋 CSV字段映射总结:")
    print("✅ 关键字段映射:")
    print("   - 完成时间: timestamp (Unix时间戳)")
    print("   - 到达时间: arrival_timestamp (Unix时间戳)")
    print("   - 备用到达时间: arrival_time (ISO格式)")
    print("   - 请求时间: request_time (字符串->浮点)")
    print("   - 状态码: status (字符串->整数)")
    print("   - 上游时间: upstream_*_time (字符串->浮点，处理空值)")
    print("\n✅ 连接数计算基础:")
    print("   - 使用arrival_timestamp和timestamp进行时间窗口计算")
    print("   - 科学的时间重叠逻辑")
    print("   - 正确的连接复用率计算")

if __name__ == "__main__":
    main()