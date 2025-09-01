# -*- coding: utf-8 -*-
"""
创建完整的数据仓库表结构并导入数据
"""

import clickhouse_connect
import time

def create_complete_schema():
    """创建完整的表结构"""
    print("连接ClickHouse并创建完整表结构...")
    
    try:
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='analytics_user',
            password='analytics_password',
            database='nginx_analytics'
        )
        
        # 1. 创建基础ADS表用于分析
        print("创建ADS层分析表...")
        
        # API性能分析表
        client.command("""
        CREATE TABLE IF NOT EXISTS ads_api_performance_analysis (
            stat_time DateTime,
            time_granularity LowCardinality(String),
            platform LowCardinality(String), 
            access_type LowCardinality(String),
            api_path String,
            api_module LowCardinality(String),
            api_category LowCardinality(String),
            business_domain LowCardinality(String),
            
            total_requests UInt64,
            unique_clients UInt64,
            qps Float64,
            
            avg_response_time Float64,
            p50_response_time Float64,
            p90_response_time Float64,
            p95_response_time Float64,
            p99_response_time Float64,
            max_response_time Float64,
            
            success_requests UInt64,
            error_requests UInt64,
            success_rate Float64,
            error_rate Float64,
            
            slow_requests UInt64,
            slow_rate Float64,
            
            created_at DateTime DEFAULT now()
        ) ENGINE = SummingMergeTree()
        PARTITION BY toYYYYMM(stat_time)
        ORDER BY (stat_time, platform, api_module, api_path)
        """)
        
        # 状态码分析表
        client.command("""
        CREATE TABLE IF NOT EXISTS ads_status_code_analysis (
            stat_time DateTime,
            time_granularity LowCardinality(String),
            platform LowCardinality(String),
            status_code LowCardinality(String),
            status_category LowCardinality(String),
            
            request_count UInt64,
            percentage Float64,
            
            created_at DateTime DEFAULT now()
        ) ENGINE = SummingMergeTree()
        PARTITION BY toYYYYMM(stat_time) 
        ORDER BY (stat_time, platform, status_code)
        """)
        
        # 时间维度分析表
        client.command("""
        CREATE TABLE IF NOT EXISTS ads_time_dimension_analysis (
            stat_time DateTime,
            time_granularity LowCardinality(String),
            platform LowCardinality(String),
            
            total_requests UInt64,
            avg_qps Float64,
            peak_qps Float64,
            avg_response_time Float64,
            
            success_requests UInt64,
            error_requests UInt64,
            success_rate Float64,
            
            created_at DateTime DEFAULT now()
        ) ENGINE = SummingMergeTree()
        PARTITION BY toYYYYMM(stat_time)
        ORDER BY (stat_time, platform)
        """)
        
        # IP分析表
        client.command("""
        CREATE TABLE IF NOT EXISTS ads_ip_analysis (
            stat_time DateTime,
            time_granularity LowCardinality(String),
            client_ip String,
            ip_category LowCardinality(String),
            
            request_count UInt64,
            unique_apis UInt64,
            avg_response_time Float64,
            error_count UInt64,
            
            created_at DateTime DEFAULT now()
        ) ENGINE = SummingMergeTree()
        PARTITION BY toYYYYMM(stat_time)
        ORDER BY (stat_time, client_ip)
        """)
        
        print("ADS层表创建完成")
        
        # 验证表创建
        tables = client.query("SHOW TABLES FROM nginx_analytics").result_rows
        print(f"当前数据库包含 {len(tables)} 个表:")
        for table in tables:
            count = client.command(f"SELECT count() FROM {table[0]}")
            print(f"  - {table[0]}: {count} 条记录")
            
        return True
        
    except Exception as e:
        print(f"创建表结构失败: {e}")
        return False

def import_sample_data():
    """导入样例数据"""
    print("导入nginx日志样例数据...")
    
    try:
        from processors.nginx_log_processor import NginxLogProcessor
        
        processor = NginxLogProcessor()
        if processor.connect_clickhouse():
            # 导入样例数据
            log_file = r"D:\project\nginx-log-analyzer\data\demo\自研Ng2025.05.09日志-样例\enterprise_space_app_access - 副本.log"
            success, error = processor.process_log_file(log_file)
            print(f"数据导入完成: 成功 {success}，失败 {error}")
            
            # 获取统计
            stats = processor.get_processing_stats()
            print(f"导入后统计: {stats}")
            return True
        else:
            print("无法连接到ClickHouse")
            return False
            
    except Exception as e:
        print(f"数据导入失败: {e}")
        return False

def create_sample_ads_data():
    """创建样例ADS数据"""
    print("生成ADS层样例数据...")
    
    try:
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='analytics_user',
            password='analytics_password',
            database='nginx_analytics'
        )
        
        # 从ODS生成API性能分析数据
        client.command("""
        INSERT INTO ads_api_performance_analysis
        SELECT 
            toStartOfHour(log_time) as stat_time,
            'hour' as time_granularity,
            'Web' as platform,
            'HTTP' as access_type,
            request_uri as api_path,
            splitByChar('/', request_uri)[2] as api_module,
            if(request_uri LIKE '/api/%', 'API', 'Static') as api_category,
            if(request_uri LIKE '/api/%', 'Business', 'Static') as business_domain,
            
            count() as total_requests,
            uniq(client_ip) as unique_clients,
            count() / 3600.0 as qps,
            
            round(avg(total_request_time), 3) as avg_response_time,
            round(quantile(0.50)(total_request_time), 3) as p50_response_time,
            round(quantile(0.90)(total_request_time), 3) as p90_response_time,
            round(quantile(0.95)(total_request_time), 3) as p95_response_time,
            round(quantile(0.99)(total_request_time), 3) as p99_response_time,
            round(max(total_request_time), 3) as max_response_time,
            
            countIf(response_status_code = '200') as success_requests,
            countIf(response_status_code != '200') as error_requests,
            countIf(response_status_code = '200') * 100.0 / count() as success_rate,
            countIf(response_status_code != '200') * 100.0 / count() as error_rate,
            
            countIf(total_request_time > 3.0) as slow_requests,
            countIf(total_request_time > 3.0) * 100.0 / count() as slow_rate,
            
            now() as created_at
        FROM ods_nginx_raw
        WHERE total_request_time > 0
        GROUP BY stat_time, api_path
        """)
        
        # 生成状态码分析数据
        client.command("""
        INSERT INTO ads_status_code_analysis
        SELECT 
            toStartOfHour(log_time) as stat_time,
            'hour' as time_granularity,
            'Web' as platform,
            response_status_code as status_code,
            if(response_status_code LIKE '2%', 'Success', 
               if(response_status_code LIKE '4%', 'Client Error', 'Server Error')) as status_category,
            
            count() as request_count,
            count() * 100.0 / (SELECT count() FROM ods_nginx_raw) as percentage,
            
            now() as created_at
        FROM ods_nginx_raw
        GROUP BY stat_time, response_status_code
        """)
        
        print("ADS层样例数据创建完成")
        return True
        
    except Exception as e:
        print(f"创建ADS数据失败: {e}")
        return False

def main():
    """主函数"""
    print("=== 创建完整数据仓库结构并导入数据 ===")
    print()
    
    # 1. 创建表结构
    if create_complete_schema():
        print("✅ 表结构创建成功")
    else:
        print("❌ 表结构创建失败")
        return
    
    # 2. 导入样例数据
    if import_sample_data():
        print("✅ 样例数据导入成功")
        
        # 3. 生成ADS数据
        if create_sample_ads_data():
            print("✅ ADS分析数据生成成功")
        else:
            print("⚠️ ADS数据生成失败，但基础数据已导入")
    else:
        print("❌ 数据导入失败")
    
    print("\n=== 完成 ===")
    print("现在可以在Grafana和Superset中使用以下表进行分析:")
    print("• ods_nginx_raw - 原始日志数据")
    print("• ads_api_performance_analysis - API性能分析")
    print("• ads_status_code_analysis - 状态码分析")
    print("• ads_time_dimension_analysis - 时间维度分析")
    print("• ads_ip_analysis - IP来源分析")

if __name__ == "__main__":
    main()