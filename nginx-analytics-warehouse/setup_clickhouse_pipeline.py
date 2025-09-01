# -*- coding: utf-8 -*-
"""
使用现有ClickHouse容器设置nginx分析管道
"""

import os
import sys
from pathlib import Path
import subprocess
import time

class ClickHousePipelineSetup:
    """ClickHouse管道设置"""
    
    def __init__(self):
        self.container_name = "nginx-analytics-clickhouse-simple"
        self.database_name = "nginx_analytics"
        self.ddl_dir = Path(__file__).parent / 'ddl'
        
    def check_container_status(self):
        """检查容器状态"""
        try:
            result = subprocess.run(['docker', 'ps'], capture_output=True, text=True)
            if self.container_name in result.stdout:
                print(f"容器 {self.container_name} 正在运行")
                return True
            else:
                print(f"容器 {self.container_name} 未运行")
                return False
        except Exception as e:
            print(f"检查容器状态失败: {e}")
            return False
    
    def execute_sql_in_container(self, sql_content, database=None):
        """在容器中执行SQL"""
        try:
            db_param = f"-d {database}" if database else ""
            cmd = ['docker', 'exec', self.container_name, 'clickhouse-client', db_param, '-q', sql_content]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                return True, result.stdout
            else:
                return False, result.stderr
        except Exception as e:
            return False, str(e)
    
    def create_database(self):
        """创建数据库"""
        print("创建数据库...")
        success, output = self.execute_sql_in_container(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
        if success:
            print(f"数据库 {self.database_name} 创建成功")
            return True
        else:
            print(f"创建数据库失败: {output}")
            return False
    
    def create_ods_tables(self):
        """创建ODS层表"""
        print("创建ODS层表...")
        
        # 基于现有数据格式的ODS表
        ods_table_sql = """
        CREATE TABLE IF NOT EXISTS ods_nginx_raw (
            id UInt64 DEFAULT generateUUIDv4(),
            log_time DateTime64(3),
            server_name String,
            client_ip String,
            client_port UInt32,
            xff_ip String,
            remote_user String,
            request_method LowCardinality(String),
            request_uri String,
            request_full_uri String,
            http_protocol LowCardinality(String),
            response_status_code LowCardinality(String),
            response_body_size UInt64,
            response_referer String,
            user_agent String,
            upstream_addr String,
            upstream_connect_time Float64,
            upstream_header_time Float64,
            upstream_response_time Float64,
            total_request_time Float64,
            total_bytes_sent UInt64,
            query_string String,
            connection_requests UInt32,
            trace_id String,
            business_sign LowCardinality(String),
            application_name LowCardinality(String),
            service_name LowCardinality(String),
            cache_status LowCardinality(String),
            cluster_node LowCardinality(String),
            log_source_file LowCardinality(String),
            created_at DateTime DEFAULT now(),
            date_partition Date MATERIALIZED toDate(log_time),
            hour_partition UInt8 MATERIALIZED toHour(log_time)
        ) ENGINE = MergeTree()
        PARTITION BY date_partition
        ORDER BY (date_partition, hour_partition, server_name, client_ip, log_time)
        SETTINGS index_granularity = 8192
        """
        
        success, output = self.execute_sql_in_container(ods_table_sql, self.database_name)
        if success:
            print("ODS层表创建成功")
            return True
        else:
            print(f"ODS层表创建失败: {output}")
            return False
    
    def create_sample_ads_table(self):
        """创建示例ADS表"""
        print("创建示例ADS表...")
        
        # 简化的API性能分析表
        ads_table_sql = """
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
            business_success_rate Float64,
            
            slow_requests UInt64,
            very_slow_requests UInt64,
            slow_rate Float64,
            very_slow_rate Float64,
            
            apdex_score Float64,
            user_satisfaction_score Float64,
            
            business_value_score UInt8,
            importance_level LowCardinality(String),
            
            created_at DateTime DEFAULT now()
        ) ENGINE = SummingMergeTree()
        PARTITION BY toYYYYMM(stat_time)
        ORDER BY (stat_time, platform, api_module, api_path)
        TTL stat_time + INTERVAL 2 YEAR
        """
        
        success, output = self.execute_sql_in_container(ads_table_sql, self.database_name)
        if success:
            print("示例ADS表创建成功")
            return True
        else:
            print(f"示例ADS表创建失败: {output}")
            return False
    
    def verify_setup(self):
        """验证设置"""
        print("验证表创建...")
        
        tables_to_check = ['ods_nginx_raw', 'ads_api_performance_analysis']
        all_success = True
        
        for table in tables_to_check:
            success, output = self.execute_sql_in_container(f"EXISTS TABLE {table}", self.database_name)
            if success and '1' in output:
                print(f"表 {table} 存在")
            else:
                print(f"表 {table} 不存在")
                all_success = False
        
        return all_success
    
    def get_pipeline_status(self):
        """获取管道状态"""
        status = {}
        
        # 检查容器状态
        status['container_running'] = self.check_container_status()
        
        # 检查数据库连接
        success, output = self.execute_sql_in_container("SELECT 'OK'", self.database_name)
        status['database_accessible'] = success
        
        # 检查表数量
        success, output = self.execute_sql_in_container(f"SELECT count() FROM system.tables WHERE database = '{self.database_name}'", self.database_name)
        if success:
            status['table_count'] = int(output.strip()) if output.strip().isdigit() else 0
        else:
            status['table_count'] = 0
        
        # 检查数据统计
        if status['table_count'] > 0:
            success, output = self.execute_sql_in_container("SELECT count() FROM ods_nginx_raw", self.database_name)
            if success and output.strip().isdigit():
                status['ods_record_count'] = int(output.strip())
            else:
                status['ods_record_count'] = 0
        
        return status
    
    def setup_pipeline(self):
        """设置完整管道"""
        print("开始设置ClickHouse nginx分析管道...")
        print("=" * 50)
        
        # 检查容器状态
        if not self.check_container_status():
            print("错误: ClickHouse容器未运行")
            return False
        
        success_count = 0
        total_steps = 4
        
        # 创建数据库
        if self.create_database():
            success_count += 1
        
        # 创建ODS表
        if self.create_ods_tables():
            success_count += 1
        
        # 创建示例ADS表
        if self.create_sample_ads_table():
            success_count += 1
        
        # 验证设置
        if self.verify_setup():
            success_count += 1
            print("表结构验证通过")
        
        print(f"\n管道设置完成: {success_count}/{total_steps} 步成功")
        
        if success_count == total_steps:
            print("\n✅ ClickHouse nginx分析管道设置成功!")
            print(f"数据库: {self.database_name}")
            print(f"容器: {self.container_name}")
            print("\n下一步可以:")
            print("  1. 导入nginx日志数据到ODS表")
            print("  2. 配置Grafana连接ClickHouse")
            print("  3. 创建分析仪表板")
            return True
        else:
            print("\n❌ 管道设置不完整，请检查错误信息")
            return False

def main():
    """主函数"""
    setup = ClickHousePipelineSetup()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--status':
        # 查看状态
        status = setup.get_pipeline_status()
        print("ClickHouse管道状态:")
        print(f"  容器运行: {'是' if status['container_running'] else '否'}")
        print(f"  数据库可访问: {'是' if status['database_accessible'] else '否'}")
        print(f"  表数量: {status['table_count']}")
        if 'ods_record_count' in status:
            print(f"  ODS记录数: {status['ods_record_count']}")
    else:
        # 执行设置
        setup.setup_pipeline()

if __name__ == "__main__":
    main()