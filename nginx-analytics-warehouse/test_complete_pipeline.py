# -*- coding: utf-8 -*-
"""
完整数据流程测试
"""

import sys
import os
from pathlib import Path
sys.path.append(str(Path(__file__).parent / 'processors'))

from nginx_log_processor import NginxLogProcessor
import clickhouse_connect
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_complete_pipeline():
    """测试完整数据管道"""
    
    print("=== Nginx日志分析数据管道测试 ===")
    print()
    
    # 1. 测试ClickHouse连接
    print("1. 测试ClickHouse连接...")
    try:
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='analytics_user',
            password='analytics_password',
            database='nginx_analytics'
        )
        result = client.command("SELECT version()")
        print(f"   [OK] ClickHouse连接成功，版本: {result}")
    except Exception as e:
        print(f"   [ERROR] ClickHouse连接失败: {e}")
        return False
    
    # 2. 检查表结构
    print("\n2. 检查表结构...")
    try:
        tables = client.query("SHOW TABLES").result_rows
        table_count = len(tables)
        print(f"   [OK] 发现 {table_count} 个表:")
        for table in tables[:10]:  # 显示前10个
            print(f"      - {table[0]}")
        if table_count > 10:
            print(f"      ... 还有 {table_count - 10} 个表")
    except Exception as e:
        print(f"   [ERROR] 检查表结构失败: {e}")
        return False
    
    # 3. 检查ODS数据
    print("\n3. 检查ODS层数据...")
    try:
        ods_count = client.command("SELECT count() FROM ods_nginx_raw")
        if ods_count > 0:
            print(f"   [OK] ODS表包含 {ods_count} 条记录")
            
            # 获取时间范围
            time_range = client.query(
                "SELECT min(log_time) as min_time, max(log_time) as max_time FROM ods_nginx_raw"
            ).result_rows[0]
            print(f"   [OK] 数据时间范围: {time_range[0]} 到 {time_range[1]}")
            
            # 状态码分布
            status_dist = client.query(
                "SELECT response_status_code, count() as cnt FROM ods_nginx_raw GROUP BY response_status_code ORDER BY cnt DESC"
            ).result_rows
            print(f"   [OK] 状态码分布:")
            for status, count in status_dist:
                print(f"      - {status}: {count}")
        else:
            print(f"   [WARN] ODS表为空，需要导入nginx日志数据")
    except Exception as e:
        print(f"   [ERROR] 检查ODS数据失败: {e}")
    
    # 4. 测试DWD层
    print("\n4. 检查DWD层数据...")
    try:
        dwd_tables = [t[0] for t in tables if 'dw' in t[0].lower()]
        if dwd_tables:
            print(f"   ✅ 发现 {len(dwd_tables)} 个DWD表:")
            for table in dwd_tables:
                try:
                    count = client.command(f"SELECT count() FROM {table}")
                    print(f"      - {table}: {count} 条记录")
                except:
                    print(f"      - {table}: 查询失败")
        else:
            print("   ⚠️  未找到DWD表")
    except Exception as e:
        print(f"   ❌ 检查DWD数据失败: {e}")
    
    # 5. 测试ADS层
    print("\n5. 检查ADS层数据...")
    try:
        ads_tables = [t[0] for t in tables if t[0].startswith('ads_')]
        if ads_tables:
            print(f"   ✅ 发现 {len(ads_tables)} 个ADS表:")
            for table in ads_tables[:5]:  # 只显示前5个
                try:
                    count = client.command(f"SELECT count() FROM {table}")
                    print(f"      - {table}: {count} 条记录")
                except:
                    print(f"      - {table}: 查询失败")
            if len(ads_tables) > 5:
                print(f"      ... 还有 {len(ads_tables) - 5} 个ADS表")
        else:
            print("   ⚠️  未找到ADS表")
    except Exception as e:
        print(f"   ❌ 检查ADS数据失败: {e}")
    
    # 6. 测试样例查询
    print("\n6. 测试分析查询...")
    try:
        # API性能分析
        api_perf = client.query("""
            SELECT 
                request_uri,
                count() as request_count,
                round(avg(total_request_time), 3) as avg_response_time,
                round(quantile(0.95)(total_request_time), 3) as p95_response_time
            FROM ods_nginx_raw 
            WHERE total_request_time > 0
            GROUP BY request_uri
            ORDER BY avg_response_time DESC
            LIMIT 5
        """).result_rows
        
        if api_perf:
            print("   ✅ API性能分析 (Top 5 慢接口):")
            print("      URI | 请求数 | 平均响应时间 | P95响应时间")
            print("      " + "-" * 60)
            for uri, count, avg_time, p95_time in api_perf:
                uri_short = uri[:30] + "..." if len(uri) > 30 else uri
                print(f"      {uri_short:<33} | {count:>6} | {avg_time:>10}s | {p95_time:>10}s")
        else:
            print("   ⚠️  无性能数据")
            
    except Exception as e:
        print(f"   ❌ 测试查询失败: {e}")
    
    # 7. 检查Grafana连接
    print("\n7. 检查Grafana服务...")
    try:
        import requests
        response = requests.get('http://localhost:3000/api/health', timeout=5)
        if response.status_code == 200:
            health_data = response.json()
            print(f"   ✅ Grafana运行正常，版本: {health_data.get('version', 'unknown')}")
        else:
            print(f"   ❌ Grafana响应异常: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Grafana连接失败: {e}")
    
    print("\n=== 测试完成 ===")
    print("\n📊 访问地址:")
    print("   • ClickHouse HTTP: http://localhost:8123")
    print("   • Grafana: http://localhost:3000 (admin/admin123)")
    
    print("\n🔧 下一步建议:")
    print("   1. 在Grafana中配置ClickHouse数据源")
    print("   2. 创建nginx分析仪表板")
    print("   3. 定期运行日志处理脚本导入新数据")
    
    return True

if __name__ == "__main__":
    test_complete_pipeline()