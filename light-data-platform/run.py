#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
轻量级数据平台启动脚本
"""

import os
import sys
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent))

def test_database():
    """测试数据库初始化"""
    print("=== 测试数据库初始化 ===")
    
    try:
        from database.models import init_db
        engine, SessionLocal = init_db()
        print("[OK] 数据库初始化成功")
        return True
    except Exception as e:
        print(f"[FAIL] 数据库初始化失败: {e}")
        return False

def test_data_enricher():
    """测试数据富化器"""
    print("\n=== 测试数据富化器 ===")
    
    try:
        from utils.data_enricher import DataEnricher
        from config.settings import DIMENSIONS
        
        enricher = DataEnricher(DIMENSIONS)
        
        # 测试数据
        test_record = {
            'timestamp': '2025-08-29 15:30:45',
            'request_full_uri': '/api/user/login',
            'response_status_code': '200',
            'total_request_duration': 1.234,
            'user_agent': 'zgt-ios/1.4.2 (iPhone; iOS 18.6; Scale/3.00)',
            'referer': 'https://weixin.qq.com/some-page'
        }
        
        enriched = enricher.enrich_record(test_record)
        
        print("[OK] 数据富化器工作正常")
        print(f"  平台识别: {enriched['platform']}")
        print(f"  入口来源: {enriched['entry_source']}")
        print(f"  API分类: {enriched['api_category']}")
        return True
        
    except Exception as e:
        print(f"[FAIL] 数据富化器测试失败: {e}")
        return False

def test_ods_processor():
    """测试ODS处理器"""
    print("\n=== 测试ODS处理器 ===")
    
    try:
        from data_pipeline.ods_processor import OdsProcessor
        from config.settings import DATA_SOURCE
        
        processor = OdsProcessor()
        
        # 检查默认CSV文件
        default_csv = DATA_SOURCE['default_csv_path']
        if os.path.exists(default_csv):
            print(f"[OK] 找到默认CSV文件: {default_csv}")
            
            # 读取文件前几行看看结构
            import pandas as pd
            df = pd.read_csv(default_csv, nrows=5)
            print(f"[OK] CSV文件结构验证通过，共 {len(df.columns)} 列")
            print(f"  主要列: {list(df.columns)[:5]}...")
            
        else:
            print(f"[WARN] 默认CSV文件不存在: {default_csv}")
            print("  可以使用其他CSV文件进行测试")
        
        return True
        
    except Exception as e:
        print(f"[FAIL] ODS处理器测试失败: {e}")
        return False

def quick_setup():
    """快速设置和验证"""
    print("=== 轻量级数据平台 - 快速设置 ===\n")
    
    # 测试各组件
    db_ok = test_database()
    enricher_ok = test_data_enricher()
    ods_ok = test_ods_processor()
    
    print(f"\n=== 设置完成 ===")
    print(f"数据库: {'[OK]' if db_ok else '[FAIL]'}")
    print(f"数据富化: {'[OK]' if enricher_ok else '[FAIL]'}")
    print(f"ODS处理: {'[OK]' if ods_ok else '[FAIL]'}")
    
    if all([db_ok, enricher_ok, ods_ok]):
        print("\n[SUCCESS] 所有组件验证通过!")
        print("\n下一步操作:")
        print("1. 加载数据: python data_pipeline/ods_processor.py --csv-path /path/to/your.csv")
        print("2. 查看统计: python data_pipeline/ods_processor.py --stats")
        print("3. 启动Web服务: python web_app/app.py")
    else:
        print("\n[ERROR] 有组件验证失败，请检查错误信息")

def main():
    """主入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description='轻量级数据平台')
    parser.add_argument('--test', action='store_true', help='运行快速测试')
    parser.add_argument('--web', action='store_true', help='启动Web服务')
    parser.add_argument('--load-data', type=str, help='加载指定CSV数据')
    
    args = parser.parse_args()
    
    if args.test:
        quick_setup()
    elif args.web:
        print("启动Web服务...")
        # TODO: 实现Web服务启动
        print("Web服务功能待实现")
    elif args.load_data:
        print(f"加载数据: {args.load_data}")
        # TODO: 调用ODS处理器
        print("数据加载功能待实现")
    else:
        # 默认运行快速测试
        quick_setup()

if __name__ == "__main__":
    main()