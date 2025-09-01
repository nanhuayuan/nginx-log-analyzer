#!/usr/bin/env python3
"""
展示当前数据流状态 - 回答用户关于数据流向的问题
"""

import clickhouse_connect

def main():
    try:
        # 连接ClickHouse
        client = clickhouse_connect.get_client(
            host='localhost', 
            port=8123, 
            database='nginx_analytics',
            username='analytics_user',
            password='analytics_password'
        )
        print("SUCCESS: ClickHouse连接成功")
        
        # 检查所有表的数据情况
        print("\n=== 当前数据流状态检查 ===")
        
        tables = [
            ('ODS-原始数据层', 'ods_nginx_raw'),
            ('DWD-业务数据层', 'dwd_nginx_enriched'),
            ('DWS-API性能百分位', 'dws_api_performance_percentiles'),
            ('DWS-客户端行为分析', 'dws_client_behavior_analysis'),
            ('DWS-错误监控', 'dws_error_monitoring'),
            ('DWS-实时QPS排行', 'dws_realtime_qps_ranking'),
            ('DWS-链路分析', 'dws_trace_analysis'),
            ('DWS-上游健康监控', 'dws_upstream_health_monitoring'),
            ('ADS-缓存命中分析', 'ads_cache_hit_analysis'),
            ('ADS-集群性能对比', 'ads_cluster_performance_comparison'),
            ('ADS-热门API榜单', 'ads_top_hot_apis'),
            ('ADS-慢API榜单', 'ads_top_slow_apis')
        ]
        
        # 统计每层数据
        layer_stats = {}
        
        for layer_name, table_name in tables:
            try:
                count = client.command(f'SELECT count() FROM {table_name}')
                layer_stats[table_name] = count
                print(f"{layer_name}: {count} 条记录")
            except Exception as e:
                print(f"{layer_name}: 查询失败 - {e}")
        
        # 分析数据流问题
        print(f"\n=== 数据流分析 ===")
        
        ods_count = layer_stats.get('ods_nginx_raw', 0)
        dwd_count = layer_stats.get('dwd_nginx_enriched', 0) 
        
        print(f"ODS原始数据: {ods_count} 条")
        print(f"DWD业务数据: {dwd_count} 条") 
        
        if ods_count == 0 and dwd_count > 0:
            print("发现问题: ODS为空但DWD有数据，这说明DWD是手动插入的测试数据")
            print("完整流程应该是: nginx日志文件 -> ODS -> DWD -> DWS -> ADS")
        elif ods_count == 0 and dwd_count == 0:
            print("当前状态: 所有数据层都为空，需要处理nginx日志")
        elif ods_count > 0 and dwd_count == 0:
            print("发现问题: ODS有数据但DWD为空，数据转换流程未执行")
        else:
            print(f"数据流状态: ODS({ods_count}) -> DWD({dwd_count})")
        
        # 展示DWD现有数据
        if dwd_count > 0:
            print(f"\n=== DWD现有数据展示 ===")
            dwd_sample = client.query("""
                SELECT client_ip, request_uri, response_status_code, total_request_duration, platform, api_category
                FROM dwd_nginx_enriched 
                LIMIT 5
            """).result_rows
            
            for i, row in enumerate(dwd_sample, 1):
                print(f"{i}. IP:{row[0]} | URI:{row[1]} | Status:{row[2]} | Time:{row[3]}s | Platform:{row[4]} | API:{row[5]}")
        
        # 检查日志文件
        print(f"\n=== 日志文件检查 ===")
        import os
        log_dir = r"D:\nginx_logs\20250901"
        if os.path.exists(log_dir):
            log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]
            print(f"发现 {len(log_files)} 个日志文件: {log_files}")
            
            # 计算日志文件总行数
            total_lines = 0
            for log_file in log_files:
                file_path = os.path.join(log_dir, log_file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = len(f.readlines())
                    total_lines += lines
                    print(f"  {log_file}: {lines} 条日志记录")
            
            print(f"总计: {total_lines} 条nginx日志待处理")
            
            if total_lines > 0 and ods_count == 0:
                print("建议: 使用 nginx_processor_simple.py --date 20250901 来处理这些日志")
        else:
            print(f"日志目录不存在: {log_dir}")
            print("建议: 创建日志目录并放入nginx日志文件")
        
        print(f"\n=== 解决方案 ===")
        print("1. 当前nginx_processor_simple.py脚本包含完整的ETL流程")
        print("2. 支持按日期处理: --date YYYYMMDD")
        print("3. 支持强制重新处理: --force")
        print("4. 支持数据清理: --clear-all")
        print("5. 完整数据流: nginx日志 -> ODS -> DWD -> DWS -> ADS")
        
        print(f"\n命令示例:")
        print(f"  python nginx_processor_simple.py --date 20250901     # 处理指定日期")
        print(f"  python nginx_processor_simple.py --date 20250901 --force  # 强制重新处理")
        print(f"  python nginx_processor_simple.py --clear-all        # 清空所有数据")
        print(f"  python nginx_processor_simple.py --status           # 查看处理状态")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()