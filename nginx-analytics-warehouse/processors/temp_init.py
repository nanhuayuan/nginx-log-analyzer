import sys; sys.stdout.reconfigure(encoding="utf-8")
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ClickHouse数据库初始化脚本
Database Initialization Script for ClickHouse
"""

import clickhouse_connect
import sys
from pathlib import Path

def create_database_and_tables():
    """创建数据库和所有必需的表"""
    
    # 连接配置
    config = {
        'host': 'localhost',
        'port': 8123,
        'username': 'analytics_user',
        'password': 'analytics_password'
    }
    
    print("=" * 60)
    print("   ClickHouse数据库初始化")
    print("   Database Initialization")
    print("=" * 60)
    
    try:
        # 首先连接到默认数据库创建nginx_analytics数据库
        print("步骤1: 连接ClickHouse服务器...")
        client = clickhouse_connect.get_client(
            host=config['host'],
            port=config['port'],
            username=config['username'],
            password=config['password']
        )
        
        # 创建数据库
        print("步骤2: 创建数据库 nginx_analytics...")
        client.command('CREATE DATABASE IF NOT EXISTS nginx_analytics')
        print("[OK] 数据库创建成功")
        
        # 重新连接到nginx_analytics数据库
        print("步骤3: 连接到nginx_analytics数据库...")
        client = clickhouse_connect.get_client(
            host=config['host'],
            port=config['port'],
            database='nginx_analytics',
            username=config['username'],
            password=config['password']
        )
        
        # 创建ODS层表
        print("步骤4: 创建ODS层表 (ods_nginx_raw)...")
        ods_sql = """
        CREATE TABLE IF NOT EXISTS ods_nginx_raw (
            id UUID DEFAULT generateUUIDv4(),
            http_host String,
            remote_addr String,
            log_time DateTime64(3),
            request_method String,
            request_uri String,
            request_protocol String,
            status String,
            response_time Float64,
            response_body_size UInt64,
            referer String,
            user_agent String,
            upstream_response_time String,
            total_request_duration Float64,
            created_at DateTime64(3) DEFAULT now64(3),
            file_source String
        ) ENGINE = MergeTree()
        ORDER BY (log_time, remote_addr)
        """
        client.command(ods_sql)
        print("[OK] ODS表创建成功")
        
        # 创建DWD层表
        print("步骤5: 创建DWD层表 (dwd_nginx_enriched)...")
        dwd_sql = """
        CREATE TABLE IF NOT EXISTS dwd_nginx_enriched (
            id UUID,
            http_host String,
            remote_addr String,
            log_time DateTime64(3),
            request_method String,
            request_uri String,
            request_protocol String,
            status String,
            response_time Float64,
            response_body_size UInt64,
            referer String,
            user_agent String,
            upstream_response_time String,
            total_request_duration Float64,
            
            -- 增强字段
            platform String,
            api_category String,
            is_success UInt8,
            is_slow_request UInt8,
            client_ip String,
            
            created_at DateTime64(3),
            file_source String
        ) ENGINE = MergeTree()
        ORDER BY (log_time, platform, api_category)
        """
        client.command(dwd_sql)
        print("[OK] DWD表创建成功")
        
        # 创建ADS层表 - 热门API统计
        print("步骤6: 创建ADS层表 (ads_top_hot_apis)...")
        ads_sql = """
        CREATE TABLE IF NOT EXISTS ads_top_hot_apis (
            stat_date Date,
            request_uri String,
            platform String,
            request_count UInt64,
            success_count UInt64,
            avg_response_time Float64,
            total_bytes UInt64,
            unique_visitors UInt64,
            created_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree()
        ORDER BY (stat_date, request_uri, platform)
        """
        client.command(ads_sql)
        print("[OK] ADS热门API表创建成功")
        
        # 创建ADS层表 - 平台统计
        print("步骤7: 创建ADS平台统计表 (ads_platform_stats)...")
        ads_platform_sql = """
        CREATE TABLE IF NOT EXISTS ads_platform_stats (
            stat_date Date,
            platform String,
            total_requests UInt64,
            success_requests UInt64,
            failed_requests UInt64,
            success_rate Float64,
            avg_response_time Float64,
            p95_response_time Float64,
            total_bytes UInt64,
            unique_visitors UInt64,
            created_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree()
        ORDER BY (stat_date, platform)
        """
        client.command(ads_platform_sql)
        print("✓ ADS平台统计表创建成功")
        
        # 创建ADS层表 - 状态码统计
        print("步骤8: 创建ADS状态码统计表 (ads_status_stats)...")
        ads_status_sql = """
        CREATE TABLE IF NOT EXISTS ads_status_stats (
            stat_date Date,
            status String,
            platform String,
            request_count UInt64,
            percentage Float64,
            avg_response_time Float64,
            created_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree()
        ORDER BY (stat_date, status, platform)
        """
        client.command(ads_status_sql)
        print("✓ ADS状态码统计表创建成功")
        
        # 验证表创建
        print("步骤9: 验证表结构...")
        tables = client.query("SHOW TABLES").result_rows
        table_names = [table[0] for table in tables]
        
        expected_tables = [
            'ods_nginx_raw',
            'dwd_nginx_enriched', 
            'ads_top_hot_apis',
            'ads_platform_stats',
            'ads_status_stats'
        ]
        
        print("已创建的表:")
        for i, table_name in enumerate(table_names, 1):
            print(f"  {i}. {table_name}")
        
        # 检查是否所有预期的表都已创建
        missing_tables = set(expected_tables) - set(table_names)
        if missing_tables:
            print(f"⚠️  缺少表: {missing_tables}")
            return False
        else:
            print("✓ 所有必需的表都已成功创建")
        
        print("\n" + "=" * 60)
        print("✅ 数据库初始化完成!")
        print("✅ Database initialization completed!")
        print("=" * 60)
        
        print("\n数据库连接信息:")
        print(f"  主机: {config['host']}:{config['port']}")
        print(f"  数据库: nginx_analytics")
        print(f"  用户名: {config['username']}")
        print(f"  Web界面: http://localhost:8123")
        
        return True
        
    except Exception as e:
        print(f"❌ 初始化失败: {e}")
        print("\n请检查:")
        print("1. ClickHouse服务是否正在运行")
        print("2. Docker容器是否启动")
        print("3. 网络连接是否正常")
        print("4. 用户名密码是否正确")
        return False

def verify_database_setup():
    """验证数据库设置"""
    try:
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            database='nginx_analytics',
            username='analytics_user',
            password='analytics_password'
        )
        
        print("\n验证数据库连接...")
        version = client.command('SELECT version()')
        print(f"✓ ClickHouse版本: {version}")
        
        print("\n表结构信息:")
        tables = ['ods_nginx_raw', 'dwd_nginx_enriched', 'ads_top_hot_apis']
        for table in tables:
            count = client.command(f'SELECT count() FROM {table}')
            print(f"  {table}: {count} 条记录")
        
        return True
        
    except Exception as e:
        print(f"❌ 验证失败: {e}")
        return False

def main():
    """主函数"""
    print("开始初始化ClickHouse数据库...")
    
    # 检查是否需要帮助
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help']:
        print("""
使用方法:
  python init_database.py        # 初始化数据库和表
  python init_database.py -h     # 显示帮助

功能:
  1. 创建nginx_analytics数据库
  2. 创建ODS层表 (ods_nginx_raw)
  3. 创建DWD层表 (dwd_nginx_enriched)  
  4. 创建ADS层表 (ads_top_hot_apis, ads_platform_stats, ads_status_stats)
  5. 验证表结构

前置条件:
  - ClickHouse服务运行在localhost:8123
  - 用户analytics_user/analytics_password已配置
  - Docker容器nginx-analytics-clickhouse-simple正在运行
        """)
        return
    
    # 执行初始化
    success = create_database_and_tables()
    
    if success:
        # 验证设置
        verify_database_setup()
        print("\n🎉 可以开始使用nginx日志分析系统了!")
        print("   运行: python main_simple.py process-all")
    else:
        print("\n💡 建议:")
        print("1. 先启动服务: python main_simple.py start-services")
        print("2. 等待服务完全启动后重新运行此脚本")
        sys.exit(1)

if __name__ == "__main__":
    main()