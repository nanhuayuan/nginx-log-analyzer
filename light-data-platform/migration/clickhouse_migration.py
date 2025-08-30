# -*- coding: utf-8 -*-
"""
ClickHouse升级迁移工具
支持从SQLite平滑迁移到ClickHouse
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))

try:
    import clickhouse_connect
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False

from database.models import get_session, OdsNginxLog, DwdNginxEnriched

class ClickHouseMigration:
    """ClickHouse迁移管理器"""
    
    def __init__(self, host='localhost', port=8123, username='default', password=''):
        if not CLICKHOUSE_AVAILABLE:
            raise ImportError("请先安装ClickHouse客户端: pip install clickhouse-connect")
        
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = None
    
    def connect(self):
        """连接ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password
            )
            print(f"成功连接到ClickHouse: {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"连接ClickHouse失败: {e}")
            return False
    
    def create_database(self, database_name='nginx_analytics'):
        """创建ClickHouse数据库"""
        try:
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            print(f"数据库 {database_name} 创建成功")
        except Exception as e:
            print(f"创建数据库失败: {e}")
    
    def create_tables(self, database_name='nginx_analytics'):
        """创建ClickHouse表结构"""
        
        # ODS层表结构
        ods_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.ods_nginx_log (
            id UInt64,
            timestamp DateTime,
            client_ip String,
            request_method String,
            request_full_uri String,
            request_protocol String,
            response_status_code String,
            response_body_size_kb Float64,
            total_bytes_sent_kb Float64,
            referer String,
            user_agent String,
            total_request_duration Float64,
            upstream_response_time Float64,
            upstream_connect_time Float64,
            upstream_header_time Float64,
            application_name String,
            service_name String,
            source_file String,
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, client_ip)
        PARTITION BY toYYYYMM(timestamp)
        SETTINGS index_granularity = 8192;
        """
        
        # DWD层表结构
        dwd_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.dwd_nginx_enriched (
            id UInt64,
            ods_id UInt64,
            timestamp DateTime,
            date_partition String,
            hour_partition UInt8,
            client_ip String,
            request_uri String,
            request_method String,
            response_status_code String,
            response_time Float64,
            response_size_kb Float64,
            platform String,
            platform_version String,
            entry_source String,
            api_category String,
            application_name String,
            service_name String,
            is_success Bool,
            is_slow Bool,
            data_quality_score Float64,
            has_anomaly Bool,
            anomaly_type String,
            created_at DateTime DEFAULT now(),
            updated_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, platform, api_category)
        PARTITION BY toYYYYMM(timestamp)
        SETTINGS index_granularity = 8192;
        """
        
        # DWS层聚合表(按小时聚合)
        dws_hourly_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.dws_platform_hourly (
            date_partition String,
            hour_partition UInt8,
            platform String,
            total_requests UInt64,
            success_requests UInt64,
            error_requests UInt64,
            slow_requests UInt64,
            avg_response_time Float64,
            p50_response_time Float64,
            p95_response_time Float64,
            p99_response_time Float64,
            max_response_time Float64,
            success_rate Float64,
            error_rate Float64,
            slow_rate Float64,
            unique_ips UInt64,
            total_response_size_mb Float64,
            created_at DateTime DEFAULT now(),
            updated_at DateTime DEFAULT now()
        ) ENGINE = SummingMergeTree()
        ORDER BY (date_partition, hour_partition, platform)
        PARTITION BY toYYYYMM(toDate(date_partition));
        """
        
        try:
            self.client.command(ods_table_sql)
            print("ODS表创建成功")
            
            self.client.command(dwd_table_sql)
            print("DWD表创建成功")
            
            self.client.command(dws_hourly_sql)
            print("DWS表创建成功")
            
        except Exception as e:
            print(f"创建表失败: {e}")
    
    def migrate_ods_data(self, database_name='nginx_analytics', batch_size=10000):
        """迁移ODS数据"""
        session = get_session()
        
        try:
            # 获取总记录数
            total_count = session.query(OdsNginxLog).count()
            print(f"开始迁移ODS数据，总计 {total_count} 条记录")
            
            if total_count == 0:
                print("无ODS数据需要迁移")
                return
            
            # 分批迁移
            migrated_count = 0
            for offset in range(0, total_count, batch_size):
                records = session.query(OdsNginxLog).offset(offset).limit(batch_size).all()
                
                if not records:
                    break
                
                # 转换为DataFrame
                data = []
                for record in records:
                    data.append([
                        record.id,
                        record.timestamp,
                        record.client_ip or '',
                        record.request_method or '',
                        record.request_full_uri or '',
                        record.request_protocol or '',
                        record.response_status_code or '',
                        record.response_body_size_kb or 0.0,
                        record.total_bytes_sent_kb or 0.0,
                        record.referer or '',
                        record.user_agent or '',
                        record.total_request_duration or 0.0,
                        record.upstream_response_time or 0.0,
                        record.upstream_connect_time or 0.0,
                        record.upstream_header_time or 0.0,
                        record.application_name or '',
                        record.service_name or '',
                        record.source_file or '',
                        record.created_at or datetime.now()
                    ])
                
                # 批量插入ClickHouse
                self.client.insert(f'{database_name}.ods_nginx_log', data, 
                                 column_names=[
                                     'id', 'timestamp', 'client_ip', 'request_method', 
                                     'request_full_uri', 'request_protocol', 'response_status_code',
                                     'response_body_size_kb', 'total_bytes_sent_kb', 'referer',
                                     'user_agent', 'total_request_duration', 'upstream_response_time',
                                     'upstream_connect_time', 'upstream_header_time', 'application_name',
                                     'service_name', 'source_file', 'created_at'
                                 ])
                
                migrated_count += len(records)
                print(f"已迁移 {migrated_count}/{total_count} 条ODS记录")
            
            print(f"ODS数据迁移完成，共 {migrated_count} 条记录")
            
        except Exception as e:
            print(f"ODS数据迁移失败: {e}")
        finally:
            session.close()
    
    def migrate_dwd_data(self, database_name='nginx_analytics', batch_size=10000):
        """迁移DWD数据"""
        session = get_session()
        
        try:
            # 获取总记录数
            total_count = session.query(DwdNginxEnriched).count()
            print(f"开始迁移DWD数据，总计 {total_count} 条记录")
            
            if total_count == 0:
                print("无DWD数据需要迁移")
                return
            
            # 分批迁移
            migrated_count = 0
            for offset in range(0, total_count, batch_size):
                records = session.query(DwdNginxEnriched).offset(offset).limit(batch_size).all()
                
                if not records:
                    break
                
                # 转换为ClickHouse格式
                data = []
                for record in records:
                    data.append([
                        record.id,
                        record.ods_id,
                        record.timestamp,
                        record.date_partition or '',
                        record.hour_partition or 0,
                        record.client_ip or '',
                        record.request_uri or '',
                        record.request_method or '',
                        record.response_status_code or '',
                        record.response_time or 0.0,
                        record.response_size_kb or 0.0,
                        record.platform or '',
                        record.platform_version or '',
                        record.entry_source or '',
                        record.api_category or '',
                        record.application_name or '',
                        record.service_name or '',
                        record.is_success or False,
                        record.is_slow or False,
                        record.data_quality_score or 0.0,
                        record.has_anomaly or False,
                        record.anomaly_type or '',
                        record.created_at or datetime.now(),
                        record.updated_at or datetime.now()
                    ])
                
                # 批量插入ClickHouse
                self.client.insert(f'{database_name}.dwd_nginx_enriched', data,
                                 column_names=[
                                     'id', 'ods_id', 'timestamp', 'date_partition', 'hour_partition',
                                     'client_ip', 'request_uri', 'request_method', 'response_status_code',
                                     'response_time', 'response_size_kb', 'platform', 'platform_version',
                                     'entry_source', 'api_category', 'application_name', 'service_name',
                                     'is_success', 'is_slow', 'data_quality_score', 'has_anomaly',
                                     'anomaly_type', 'created_at', 'updated_at'
                                 ])
                
                migrated_count += len(records)
                print(f"已迁移 {migrated_count}/{total_count} 条DWD记录")
            
            print(f"DWD数据迁移完成，共 {migrated_count} 条记录")
            
        except Exception as e:
            print(f"DWD数据迁移失败: {e}")
        finally:
            session.close()
    
    def create_materialized_views(self, database_name='nginx_analytics'):
        """创建物化视图用于实时聚合"""
        
        # 平台小时聚合物化视图
        platform_hourly_mv = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {database_name}.mv_platform_hourly
        TO {database_name}.dws_platform_hourly
        AS SELECT
            date_partition,
            hour_partition,
            platform,
            count(*) as total_requests,
            sum(is_success) as success_requests,
            sum(NOT is_success) as error_requests,
            sum(is_slow) as slow_requests,
            avg(response_time) as avg_response_time,
            quantile(0.5)(response_time) as p50_response_time,
            quantile(0.95)(response_time) as p95_response_time,
            quantile(0.99)(response_time) as p99_response_time,
            max(response_time) as max_response_time,
            avg(is_success) * 100 as success_rate,
            avg(NOT is_success) * 100 as error_rate,
            avg(is_slow) * 100 as slow_rate,
            uniq(client_ip) as unique_ips,
            sum(response_size_kb) / 1024 as total_response_size_mb,
            now() as created_at,
            now() as updated_at
        FROM {database_name}.dwd_nginx_enriched
        GROUP BY date_partition, hour_partition, platform;
        """
        
        try:
            self.client.command(platform_hourly_mv)
            print("物化视图创建成功")
        except Exception as e:
            print(f"创建物化视图失败: {e}")
    
    def verify_migration(self, database_name='nginx_analytics'):
        """验证迁移结果"""
        try:
            # 检查表数据量
            ods_count = self.client.command(f"SELECT count(*) FROM {database_name}.ods_nginx_log")
            dwd_count = self.client.command(f"SELECT count(*) FROM {database_name}.dwd_nginx_enriched")
            
            print(f"\n=== 迁移验证结果 ===")
            print(f"ClickHouse ODS记录数: {ods_count:,}")
            print(f"ClickHouse DWD记录数: {dwd_count:,}")
            
            # 与SQLite数据对比
            session = get_session()
            sqlite_ods = session.query(OdsNginxLog).count()
            sqlite_dwd = session.query(DwdNginxEnriched).count()
            session.close()
            
            print(f"SQLite ODS记录数: {sqlite_ods:,}")
            print(f"SQLite DWD记录数: {sqlite_dwd:,}")
            
            print(f"\n数据一致性检查:")
            print(f"ODS: {'✓' if ods_count == sqlite_ods else '✗'}")
            print(f"DWD: {'✓' if dwd_count == sqlite_dwd else '✗'}")
            
            return ods_count == sqlite_ods and dwd_count == sqlite_dwd
            
        except Exception as e:
            print(f"验证失败: {e}")
            return False
    
    def get_performance_comparison(self, database_name='nginx_analytics'):
        """性能对比测试"""
        try:
            import time
            
            # ClickHouse查询性能测试
            start_time = time.time()
            ck_result = self.client.query(f"""
                SELECT platform, count(*) as cnt, avg(response_time) as avg_time
                FROM {database_name}.dwd_nginx_enriched 
                GROUP BY platform
            """).result_rows
            ck_time = time.time() - start_time
            
            # SQLite查询性能测试
            from data_pipeline.dwd_processor import DwdProcessor
            processor = DwdProcessor()
            start_time = time.time()
            sqlite_result = processor.get_dwd_statistics()
            sqlite_time = time.time() - start_time
            
            print(f"\n=== 性能对比 ===")
            print(f"ClickHouse查询时间: {ck_time:.3f}s")
            print(f"SQLite查询时间: {sqlite_time:.3f}s")
            print(f"性能提升: {sqlite_time/ck_time:.1f}x")
            
        except Exception as e:
            print(f"性能测试失败: {e}")

def main():
    """命令行入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ClickHouse迁移工具')
    parser.add_argument('--host', default='localhost', help='ClickHouse主机地址')
    parser.add_argument('--port', type=int, default=8123, help='ClickHouse端口')
    parser.add_argument('--username', default='default', help='用户名')
    parser.add_argument('--password', default='', help='密码')
    parser.add_argument('--database', default='nginx_analytics', help='数据库名')
    parser.add_argument('--init', action='store_true', help='初始化ClickHouse环境')
    parser.add_argument('--migrate', action='store_true', help='执行数据迁移')
    parser.add_argument('--verify', action='store_true', help='验证迁移结果')
    parser.add_argument('--performance', action='store_true', help='性能对比测试')
    
    args = parser.parse_args()
    
    try:
        migration = ClickHouseMigration(args.host, args.port, args.username, args.password)
        
        if not migration.connect():
            print("无法连接到ClickHouse，请检查服务状态")
            return
        
        if args.init:
            print("初始化ClickHouse环境...")
            migration.create_database(args.database)
            migration.create_tables(args.database)
            migration.create_materialized_views(args.database)
            print("初始化完成")
        
        if args.migrate:
            print("开始数据迁移...")
            migration.migrate_ods_data(args.database)
            migration.migrate_dwd_data(args.database)
            print("数据迁移完成")
        
        if args.verify:
            success = migration.verify_migration(args.database)
            if success:
                print("\n✓ 迁移验证成功")
            else:
                print("\n✗ 迁移验证失败")
        
        if args.performance:
            migration.get_performance_comparison(args.database)
    
    except Exception as e:
        print(f"执行失败: {e}")

if __name__ == "__main__":
    main()