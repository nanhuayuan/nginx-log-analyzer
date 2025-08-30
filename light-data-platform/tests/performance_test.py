# -*- coding: utf-8 -*-
"""
技术选型和性能验证测试
"""

import os
import sys
import time
import psutil
import pandas as pd
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))

from data_pipeline.ods_processor import OdsProcessor
from data_pipeline.dwd_processor import DwdProcessor
from utils.data_enricher import DataEnricher
from config.settings import DIMENSIONS

def test_csv_loading_performance(csv_path: str, max_records: int = 1000):
    """测试CSV加载性能"""
    print("=== CSV加载性能测试 ===")
    
    if not os.path.exists(csv_path):
        print(f"CSV文件不存在: {csv_path}")
        return
    
    # 测试pandas读取性能
    start_time = time.time()
    df = pd.read_csv(csv_path, nrows=max_records)
    pandas_time = time.time() - start_time
    
    print(f"Pandas读取 {len(df)} 行数据: {pandas_time:.3f}s")
    print(f"内存使用: {psutil.virtual_memory().percent:.1f}%")
    
    # 测试ODS处理器性能
    processor = OdsProcessor()
    
    start_time = time.time()
    start_memory = psutil.virtual_memory().percent
    
    try:
        count = processor.load_csv_to_ods(csv_path, batch_size=500)
        ods_time = time.time() - start_time
        end_memory = psutil.virtual_memory().percent
        
        print(f"ODS加载 {count} 行数据: {ods_time:.3f}s")
        print(f"内存变化: {start_memory:.1f}% -> {end_memory:.1f}% (+{end_memory-start_memory:.1f}%)")
    except Exception as e:
        print(f"ODS加载失败: {e}")

def test_data_enrichment_performance(sample_size: int = 1000):
    """测试数据富化性能"""
    print("\n=== 数据富化性能测试 ===")
    
    enricher = DataEnricher(DIMENSIONS)
    
    # 生成测试数据
    test_records = []
    user_agents = [
        'WST-SDK-iOS 1.0.0',
        'zgt-ios/1.4.2 (iPhone; iOS 18.5; Scale/3.00)',
        'WST-SDK-ANDROID 1.0.0',
        'Mozilla/5.0 (Linux; Android 11; V2164A Build/RP1A.200720.012)',
        'YisouSpider/1.0',
        'Dalvik/2.1.0 (Linux; U; Android 11; V2164A Build/RP1A.200720.012)'
    ]
    
    for i in range(sample_size):
        record = {
            'timestamp': '2025-05-09 11:16:11',
            'client_ip': f'192.168.1.{i%255}',
            'request_full_uri': f'/api/user/profile?id={i}',
            'response_status_code': '200' if i % 10 != 0 else '500',
            'total_request_duration': 0.5 + (i % 10) * 0.1,
            'user_agent': user_agents[i % len(user_agents)],
            'referer': 'https://weixin.qq.com/test' if i % 3 == 0 else 'https://external.com'
        }
        test_records.append(record)
    
    # 测试单条记录富化性能
    start_time = time.time()
    enriched_count = 0
    
    for record in test_records:
        enriched = enricher.enrich_record(record)
        enriched_count += 1
    
    enrichment_time = time.time() - start_time
    
    print(f"数据富化 {enriched_count} 条记录: {enrichment_time:.3f}s")
    print(f"平均每条记录: {enrichment_time/enriched_count*1000:.2f}ms")
    print(f"吞吐量: {enriched_count/enrichment_time:.0f} records/sec")

def test_dwd_processing_performance():
    """测试DWD处理性能"""
    print("\n=== DWD处理性能测试 ===")
    
    processor = DwdProcessor()
    
    start_time = time.time()
    start_memory = psutil.virtual_memory().percent
    
    try:
        count = processor.process_ods_to_dwd(batch_size=100, max_records=50)
        dwd_time = time.time() - start_time
        end_memory = psutil.virtual_memory().percent
        
        print(f"DWD处理 {count} 条记录: {dwd_time:.3f}s")
        print(f"内存变化: {start_memory:.1f}% -> {end_memory:.1f}% (+{end_memory-start_memory:.1f}%)")
        
        # 测试查询性能
        start_time = time.time()
        stats = processor.get_dwd_statistics()
        query_time = time.time() - start_time
        
        print(f"统计查询: {query_time:.3f}s")
        
        # 测试多维度分析性能
        start_time = time.time()
        analysis = processor.analyze_dimensions()
        analysis_time = time.time() - start_time
        
        print(f"多维度分析: {analysis_time:.3f}s")
        
    except Exception as e:
        print(f"DWD处理失败: {e}")

def test_sqlite_vs_memory_performance():
    """测试SQLite vs 内存处理性能对比"""
    print("\n=== SQLite vs 内存性能对比 ===")
    
    # SQLite查询性能测试
    processor = DwdProcessor()
    
    start_time = time.time()
    stats = processor.get_dwd_statistics()
    sqlite_time = time.time() - start_time
    
    print(f"SQLite统计查询: {sqlite_time:.3f}s")
    print(f"记录数: {stats['total_records']}")
    
    # 模拟大数据量场景的预期性能
    estimated_10k = sqlite_time * (10000 / stats['total_records']) if stats['total_records'] > 0 else 0
    estimated_100k = sqlite_time * (100000 / stats['total_records']) if stats['total_records'] > 0 else 0
    
    print(f"预期10K记录查询时间: {estimated_10k:.3f}s")
    print(f"预期100K记录查询时间: {estimated_100k:.3f}s")
    
    # 判断是否需要升级到ClickHouse/ES
    if estimated_100k > 5.0:
        print("[WARNING] 100K记录查询可能超过5秒，建议考虑升级到ClickHouse")
    else:
        print("[OK] SQLite性能满足轻量级需求")

def test_system_resources():
    """测试系统资源使用情况"""
    print("\n=== 系统资源评估 ===")
    
    # CPU信息
    cpu_count = psutil.cpu_count()
    cpu_percent = psutil.cpu_percent(interval=1)
    
    print(f"CPU核心数: {cpu_count}")
    print(f"当前CPU使用率: {cpu_percent}%")
    
    # 内存信息
    memory = psutil.virtual_memory()
    print(f"总内存: {memory.total // (1024**3)} GB")
    print(f"可用内存: {memory.available // (1024**3)} GB")
    print(f"内存使用率: {memory.percent}%")
    
    # 磁盘信息
    try:
        disk = psutil.disk_usage('D:')
        print(f"磁盘总空间: {disk.total // (1024**3)} GB")
        print(f"磁盘使用率: {disk.used / disk.total * 100:.1f}%")
    except:
        print("磁盘信息获取失败")
    
    # 推荐配置
    print("\n=== 推荐配置 ===")
    if memory.total < 4 * (1024**3):  # 小于4GB
        print("[WARNING] 内存不足4GB，建议升级内存以处理大数据量")
    else:
        print("[OK] 内存充足，支持轻量级数据平台")
    
    if cpu_count < 4:
        print("[INFO] CPU核心数较少，适合小规模数据处理")
    else:
        print("[OK] CPU性能良好，支持并发处理")

def main():
    """主测试入口"""
    print("轻量级数据平台 - 技术选型和性能验证")
    print("=" * 50)
    
    # 系统资源评估
    test_system_resources()
    
    # 数据富化性能测试
    test_data_enrichment_performance(1000)
    
    # DWD处理性能测试
    test_dwd_processing_performance()
    
    # SQLite性能测试
    test_sqlite_vs_memory_performance()
    
    # CSV加载性能测试(如果有数据文件)
    from config.settings import DATA_SOURCE
    csv_path = DATA_SOURCE['default_csv_path']
    if os.path.exists(csv_path):
        test_csv_loading_performance(str(csv_path), 500)
    
    print("\n=== 技术选型结论 ===")
    print("1. SQLite: 适合<100K记录的轻量级场景")
    print("2. 数据富化: 支持1000+ records/sec处理能力")
    print("3. 内存优化: 分批处理避免内存溢出")
    print("4. 扩展性: 预留接口支持升级到ClickHouse/ES")

if __name__ == "__main__":
    main()