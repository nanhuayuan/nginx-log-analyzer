#!/usr/bin/env python3
"""
测试高级状态码分析器
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from self_04_status_analyzer_advanced import AdvancedStatusAnalyzer


def create_test_data():
    """创建测试数据"""
    np.random.seed(42)
    
    # 模拟状态码分布
    status_codes = [200, 404, 500, 301, 403, 502]
    status_weights = [0.8, 0.1, 0.05, 0.02, 0.02, 0.01]
    
    # 生成测试数据
    n_records = 10000
    
    data = {
        'response_status_code': np.random.choice(status_codes, n_records, p=status_weights),
        'total_request_duration': np.random.exponential(0.5, n_records),
        'client_ip_address': [f'192.168.1.{np.random.randint(1, 255)}' for _ in range(n_records)],
        'request_path': [f'/api/v{np.random.randint(1, 4)}/endpoint{np.random.randint(1, 10)}' for _ in range(n_records)],
        'application_name': np.random.choice(['app1', 'app2', 'app3'], n_records),
        'service_name': np.random.choice(['service1', 'service2', 'service3'], n_records),
        'http_method': np.random.choice(['GET', 'POST', 'PUT', 'DELETE'], n_records, p=[0.6, 0.2, 0.15, 0.05]),
        'hour': np.random.randint(0, 24, n_records),
        'date': [(datetime.now() - timedelta(days=np.random.randint(0, 7))).strftime('%Y-%m-%d') for _ in range(n_records)],
        'raw_time': [datetime.now().strftime('%Y-%m-%d %H:%M:%S') for _ in range(n_records)]
    }
    
    df = pd.DataFrame(data)
    return df


def test_analyzer():
    """测试分析器"""
    print("🧪 开始测试高级状态码分析器...")
    
    # 创建测试数据
    print("📊 创建测试数据...")
    test_df = create_test_data()
    
    # 保存测试数据
    test_csv = 'test_status_data.csv'
    test_df.to_csv(test_csv, index=False)
    print(f"✅ 测试数据已保存: {test_csv}")
    
    # 初始化分析器
    print("🔧 初始化分析器...")
    analyzer = AdvancedStatusAnalyzer(slow_threshold=1.0)
    
    # 运行分析
    print("🚀 运行分析...")
    try:
        output_path = 'test_status_analysis_advanced.xlsx'
        result = analyzer.analyze_status_codes(test_csv, output_path)
        
        print("✅ 分析完成！")
        print(f"📊 结果保存至: {output_path}")
        print("\n📈 摘要结果:")
        print(result)
        
        # 清理测试文件
        if os.path.exists(test_csv):
            os.remove(test_csv)
        
        return True
        
    except Exception as e:
        print(f"❌ 分析失败: {str(e)}")
        return False


if __name__ == "__main__":
    success = test_analyzer()
    if success:
        print("\n🎉 测试成功！高级状态码分析器工作正常。")
    else:
        print("\n💥 测试失败！请检查代码。")