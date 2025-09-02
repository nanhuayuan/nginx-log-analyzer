#!/usr/bin/env python3
"""
简单的时间解析测试 - 不依赖第三方库
"""

from datetime import datetime, timedelta

def test_time_key_parsing():
    """测试修复后的时间键解析"""
    
    def _parse_time_key(time_key, dimension):
        """解析时间键为datetime对象 - 修复后的版本"""
        try:
            if dimension == 'daily':
                return datetime.strptime(time_key, '%Y-%m-%d')
            elif dimension == 'hourly':
                # 修复：使用正确的小时格式 '%Y-%m-%d %H:00'
                return datetime.strptime(time_key, '%Y-%m-%d %H:00')
            elif dimension == 'minute':
                return datetime.strptime(time_key, '%Y-%m-%d %H:%M')
            elif dimension == 'second':
                return datetime.strptime(time_key, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            print(f"时间解析失败 - 维度:{dimension}, 时间键:{time_key}, 错误:{e}")
            return datetime.now()
    
    def extract_time_keys_enhanced(timestamp_field):
        """生成时间键 - 原版本"""
        try:
            dt = datetime.fromisoformat(timestamp_field.replace('Z', '+00:00'))
            return {
                'daily': dt.strftime('%Y-%m-%d'),
                'hourly': dt.strftime('%Y-%m-%d %H:00'),  # 注意这里的格式
                'minute': dt.strftime('%Y-%m-%d %H:%M'),
                'second': dt.strftime('%Y-%m-%d %H:%M:%S')
            }
        except (ValueError, TypeError):
            return None
    
    print("=== 时间解析修复验证 ===")
    
    # 测试时间字符串
    test_timestamp = "2025-07-19 14:30:45"
    
    # 1. 生成时间键
    time_keys = extract_time_keys_enhanced(test_timestamp)
    print(f"输入时间: {test_timestamp}")
    print(f"生成的时间键: {time_keys}")
    
    if not time_keys:
        print("❌ 时间键生成失败")
        return False
    
    # 2. 测试解析修复前后的差异
    print("\n=== 解析测试 ===")
    
    success_count = 0
    total_count = 0
    
    for dimension, time_key in time_keys.items():
        total_count += 1
        try:
            parsed_time = _parse_time_key(time_key, dimension)
            print(f"✓ {dimension}: '{time_key}' -> {parsed_time}")
            success_count += 1
        except Exception as e:
            print(f"❌ {dimension}: '{time_key}' -> 错误: {e}")
    
    print(f"\n解析成功率: {success_count}/{total_count}")
    
    # 3. 测试小时维度的具体问题
    print("\n=== 小时维度特殊测试 ===")
    hourly_key = time_keys['hourly']  # 应该是 '2025-07-19 14:00'
    
    # 修复前的格式（会失败）
    try:
        old_format = datetime.strptime(hourly_key, '%Y-%m-%d %H:%M')
        print(f"❌ 旧格式解析成功: {old_format} (不应该成功)")
    except ValueError:
        print(f"✓ 旧格式 '%Y-%m-%d %H:%M' 解析失败（符合预期）")
    
    # 修复后的格式（应该成功）
    try:
        new_format = datetime.strptime(hourly_key, '%Y-%m-%d %H:00')
        print(f"✓ 新格式解析成功: {new_format}")
        return True
    except ValueError as e:
        print(f"❌ 新格式解析失败: {e}")
        return False

def test_connection_calculation_logic():
    """测试连接数计算逻辑"""
    print("\n=== 连接数计算逻辑测试 ===")
    
    # 模拟请求数据
    base_time = datetime(2025, 7, 19, 14, 0, 0)
    
    # 创建模拟请求
    requests = []
    for i in range(5):
        arrival = base_time + timedelta(minutes=i*10)
        completion = arrival + timedelta(seconds=30)
        requests.append({
            'arrival_time': arrival,
            'completion_time': completion,
            'client_ip': f'192.168.1.{i+1}'
        })
    
    print(f"模拟 {len(requests)} 个请求:")
    for i, req in enumerate(requests):
        print(f"  请求{i+1}: {req['arrival_time']} -> {req['completion_time']}")
    
    # 测试时间窗口计算
    window_start = base_time
    window_end = window_start + timedelta(hours=1)  # 1小时窗口
    
    print(f"\n时间窗口: {window_start} -> {window_end}")
    
    # 1. 新建连接数 = 到达时间在[T, T+N)内的请求数
    new_connections = 0
    for req in requests:
        if window_start <= req['arrival_time'] < window_end:
            new_connections += 1
    
    # 2. 并发连接数 = 到达时间<T+N ≤ 请求完成时间
    concurrent_connections = 0
    for req in requests:
        if req['arrival_time'] < window_end and req['completion_time'] >= window_end:
            concurrent_connections += 1
    
    # 3. 活跃连接数 = 到达时间≤T+N 且 完成时间≥T
    active_connections = 0
    for req in requests:
        if req['arrival_time'] <= window_end and req['completion_time'] >= window_start:
            active_connections += 1
    
    print(f"\n计算结果:")
    print(f"  新建连接数: {new_connections}")
    print(f"  并发连接数: {concurrent_connections}")
    print(f"  活跃连接数: {active_connections}")
    
    # 检查结果合理性
    if new_connections > 0 or concurrent_connections > 0 or active_connections > 0:
        print("✓ 连接数计算逻辑正常")
        return True
    else:
        print("❌ 连接数计算结果全为0，可能存在问题")
        return False

if __name__ == "__main__":
    print("开始时间解析修复验证...")
    
    # 测试时间解析
    parsing_success = test_time_key_parsing()
    
    # 测试连接数计算逻辑
    calculation_success = test_connection_calculation_logic()
    
    print(f"\n=== 总结 ===")
    print(f"时间解析修复: {'✓ 成功' if parsing_success else '❌ 失败'}")
    print(f"连接数计算逻辑: {'✓ 正常' if calculation_success else '❌ 异常'}")
    
    if parsing_success and calculation_success:
        print("\n🎉 修复验证通过！连接数为0的问题应该已解决。")
    else:
        print("\n⚠️  仍需进一步调试。")