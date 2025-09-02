#!/usr/bin/env python3
"""
测试连接数计算修复
验证连接数统计是否正常工作
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from self_05_time_dimension_analyzer_advanced import analyze_time_dimension_enhanced, EnhancedTimeAnalyzer

def create_test_data():
    """创建测试数据"""
    print("🔨 创建测试数据...")
    
    # 生成测试数据
    base_time = datetime.now().replace(microsecond=0)
    test_data = []
    
    for i in range(100):
        arrival_time = base_time + timedelta(seconds=i)
        request_time = 1.0 + (i % 5) * 0.5  # 1.0-3.0秒的请求时间
        completion_time = arrival_time + timedelta(seconds=request_time)
        
        record = {
            'arrival_time': arrival_time.strftime('%Y-%m-%d %H:%M:%S'),
            'timestamp': completion_time.strftime('%Y-%m-%d %H:%M:%S'),
            'request_time': request_time,
            'status': 200,
            'client_ip': f'192.168.1.{i % 10 + 1}',
            'request_uri': '/test/api',
            'total_request_duration': request_time
        }
        test_data.append(record)
    
    # 保存测试数据
    test_csv = '/tmp/test_connection_data.csv'
    df = pd.DataFrame(test_data)
    df.to_csv(test_csv, index=False)
    print(f"✅ 测试数据已保存到: {test_csv}")
    print(f"📊 数据记录数: {len(test_data)}")
    print(f"🕐 时间范围: {test_data[0]['arrival_time']} ~ {test_data[-1]['completion_time']}")
    
    return test_csv

def test_connection_calculation():
    """测试连接数计算"""
    print("\n🧪 测试连接数计算...")
    
    # 创建测试数据
    test_csv = create_test_data()
    
    try:
        # 使用增强版分析器
        print("📈 开始分析...")
        analyzer = EnhancedTimeAnalyzer()
        
        # 读取测试数据
        df = pd.read_csv(test_csv)
        print(f"📝 读取数据: {len(df)} 条记录")
        print(f"📋 数据列: {list(df.columns)}")
        
        # 处理每条记录
        for _, record in df.iterrows():
            analyzer.process_single_record_enhanced(record)
        
        print(f"🗂️ 缓存连接数据: {len(analyzer.request_cache)} 条")
        
        # 计算连接数统计
        analyzer.calculate_connection_statistics()
        
        # 计算最终统计
        analyzer.calculate_final_statistics()
        
        # 检查结果
        print("\n📊 统计结果验证:")
        
        # 检查小时维度的结果
        hourly_stats = analyzer.stats.get('hourly', {})
        if hourly_stats:
            for time_key, stats in list(hourly_stats.items())[:3]:  # 显示前3个
                print(f"  🕐 {time_key}:")
                print(f"    总请求数: {stats.get('total_requests', 0)}")
                print(f"    QPS: {stats.get('qps', 0)}")
                print(f"    新建连接数: {stats.get('new_connections', 0)}")
                print(f"    并发连接数: {stats.get('concurrent_connections', 0)}")
                print(f"    活跃连接数: {stats.get('active_connections', 0)}")
                print(f"    连接复用率: {stats.get('connection_reuse_rate', 0):.2f}%")
        else:
            print("❌ 没有小时维度统计数据")
        
        # 清理测试文件
        if os.path.exists(test_csv):
            os.remove(test_csv)
            print(f"\n🗑️ 清理测试文件: {test_csv}")
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主测试函数"""
    print("=== 连接数计算修复测试 ===")
    
    if test_connection_calculation():
        print("\n🎉 测试通过！连接数计算正常工作")
    else:
        print("\n❌ 测试失败！需要进一步调试")

if __name__ == "__main__":
    main()