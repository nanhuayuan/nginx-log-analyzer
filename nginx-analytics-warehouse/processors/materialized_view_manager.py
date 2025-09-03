#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
物化视图管理器模块 - 专门负责ClickHouse物化视图的创建和管理
Materialized View Manager Module - Specialized for ClickHouse Materialized View Creation and Management

专门负责：
1. 物化视图的创建、删除、重建
2. 物化视图状态检查和监控
3. ADS层数据的自动聚合配置
4. 异常处理和错误恢复
"""

import logging
import time
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError

class MaterializedViewManager:
    """物化视图管理器 - 专门管理ClickHouse物化视图"""
    
    def __init__(self, host: str = 'localhost', port: int = 8123, 
                 database: str = 'nginx_analytics', user: str = 'analytics_user', 
                 password: str = 'analytics_password'):
        """
        初始化物化视图管理器
        
        Args:
            host: ClickHouse服务器地址
            port: ClickHouse HTTP端口
            database: 数据库名称
            user: 用户名
            password: 密码
        """
        self.config = {
            'host': host,
            'port': port,
            'username': user,
            'password': password,
            'database': database
        }
        self.database = database
        self.client = None
        self.logger = logging.getLogger(__name__)
        
        # 物化视图配置 - 解耦设计，方便维护
        self.materialized_views = self._initialize_view_definitions()
        
    def _initialize_view_definitions(self) -> Dict[str, Dict[str, Any]]:
        """
        初始化物化视图定义 - 集中管理所有物化视图配置
        
        Returns:
            Dict: 物化视图配置字典
        """
        return {
            # 1. API性能分析物化视图
            'mv_api_performance_hourly': {
                'target_table': 'ads_api_performance_analysis',
                'description': '接口性能小时级聚合 - 对应01.接口性能分析.xlsx',
                'sql_template': """
                CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.{view_name}
                TO {database}.{target_table}
                AS SELECT
                    toStartOfHour(log_time) as stat_time,
                    'hour' as time_granularity,
                    platform,
                    access_type,
                    request_uri as api_path,
                    api_module,
                    api_category,
                    business_domain,
                    
                    -- 请求量指标
                    count() as total_requests,
                    uniq(client_ip) as unique_clients,
                    count() / 3600.0 as qps,
                    
                    -- 性能指标
                    avg(total_request_duration) as avg_response_time,
                    quantile(0.5)(total_request_duration) as p50_response_time,
                    quantile(0.9)(total_request_duration) as p90_response_time,
                    quantile(0.95)(total_request_duration) as p95_response_time,
                    quantile(0.99)(total_request_duration) as p99_response_time,
                    max(total_request_duration) as max_response_time,
                    
                    -- 成功率指标
                    countIf(is_success) as success_requests,
                    countIf(is_error) as error_requests,
                    countIf(is_success) * 100.0 / count() as success_rate,
                    countIf(is_error) * 100.0 / count() as error_rate,
                    countIf(is_business_success) * 100.0 / count() as business_success_rate,
                    
                    -- 慢请求分析
                    countIf(is_slow) as slow_requests,
                    countIf(is_very_slow) as very_slow_requests,
                    countIf(is_slow) * 100.0 / count() as slow_rate,
                    countIf(is_very_slow) * 100.0 / count() as very_slow_rate,
                    
                    -- 用户体验指标 (Apdex: 满意阈值1.5s, 容忍阈值6s)
                    (countIf(total_request_duration <= 1.5) + 
                     countIf(total_request_duration > 1.5 AND total_request_duration <= 6.0) * 0.5) 
                     / count() as apdex_score,
                    multiIf(
                        avg(total_request_duration) <= 1.5, 100.0,
                        avg(total_request_duration) <= 6.0, 80.0,
                        60.0
                    ) as user_satisfaction_score,
                    
                    -- 业务价值
                    avg(business_value_score) as avg_business_value_score,
                    multiIf(
                        avg(business_value_score) >= 8, 'Critical',
                        avg(business_value_score) >= 6, 'High', 
                        avg(business_value_score) >= 4, 'Medium',
                        'Low'
                    ) as importance_level,
                    
                    now() as created_at
                FROM {database}.dwd_nginx_enriched_v2
                GROUP BY stat_time, platform, access_type, api_path, api_module, api_category, business_domain
                """
            },
            
            # 2. 服务层级分析物化视图
            'mv_service_level_hourly': {
                'target_table': 'ads_service_level_analysis',
                'description': '服务层级小时级聚合 - 对应02.服务层级分析.xlsx',
                'sql_template': """
                CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.{view_name}
                TO {database}.{target_table}
                AS SELECT
                    toStartOfHour(log_time) as stat_time,
                    'hour' as time_granularity,
                    platform,
                    service_name,
                    cluster_node,
                    upstream_server,
                    
                    -- 服务健康指标
                    count() as total_requests,
                    countIf(is_success) as success_requests,
                    countIf(is_error) as error_requests,
                    countIf(total_request_duration >= 30.0) as timeout_requests,
                    
                    -- 服务性能
                    avg(total_request_duration) as avg_response_time,
                    quantile(0.95)(total_request_duration) as p95_response_time,
                    avg(upstream_response_time) as avg_upstream_time,
                    avg(upstream_connect_time) as avg_connect_time,
                    
                    -- 服务质量
                    countIf(is_success) * 100.0 / count() as availability,
                    countIf(NOT has_anomaly) * 100.0 / count() as reliability,
                    (countIf(is_success) * 0.6 + countIf(NOT has_anomaly) * 0.4) as health_score,
                    
                    -- 容量指标
                    max(connection_requests) as max_concurrent_requests,
                    count() / 3600.0 as avg_qps,
                    count() as request_count_for_peak_qps,
                    
                    now() as created_at
                FROM {database}.dwd_nginx_enriched_v2
                WHERE log_time >= now() - INTERVAL 1 DAY
                GROUP BY stat_time, platform, service_name, cluster_node, upstream_server
                """
            },
            
            # 3. 慢请求分析物化视图
            'mv_slow_request_hourly': {
                'target_table': 'ads_slow_request_analysis',
                'description': '慢请求小时级聚合 - 对应03_慢请求分析.xlsx',
                'sql_template': """
                CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.{view_name}
                TO {database}.{target_table}
                AS SELECT
                    toStartOfHour(log_time) as stat_time,
                    'hour' as time_granularity,
                    platform,
                    access_type,
                    request_uri as api_path,
                    api_module,
                    api_category,
                    business_domain,
                    
                    -- 慢请求统计
                    count() as total_requests,
                    countIf(is_slow) as slow_requests,
                    countIf(is_very_slow) as very_slow_requests,
                    countIf(is_slow) * 100.0 / count() as slow_rate,
                    countIf(is_very_slow) * 100.0 / count() as very_slow_rate,
                    
                    -- 性能分析
                    avg(total_request_duration) as avg_response_time,
                    quantile(0.95)(total_request_duration) as p95_response_time,
                    quantile(0.99)(total_request_duration) as p99_response_time,
                    max(total_request_duration) as max_response_time,
                    
                    -- 慢请求原因分析
                    avgIf(upstream_response_time, is_slow) as avg_upstream_time_slow,
                    avgIf(network_phase, is_slow) as avg_network_time_slow,
                    avgIf(processing_phase, is_slow) as avg_processing_time_slow,
                    
                    -- 影响分析
                    uniq(client_ip) as affected_users,
                    sum(response_body_size) as total_data_transferred,
                    avg(business_value_score) as business_impact_score,
                    
                    now() as created_at
                FROM {database}.dwd_nginx_enriched_v2
                WHERE log_time >= now() - INTERVAL 1 DAY
                GROUP BY stat_time, platform, access_type, api_path, api_module, api_category, business_domain
                """
            },
            
            # 4. 状态码分析物化视图
            'mv_status_code_hourly': {
                'target_table': 'ads_status_code_analysis',
                'description': '状态码小时级聚合 - 对应04.状态码统计.xlsx',
                'sql_template': """
                CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.{view_name}
                TO {database}.{target_table}
                AS SELECT
                    toStartOfHour(log_time) as stat_time,
                    'hour' as time_granularity,
                    platform,
                    access_type,
                    response_status_code as status_code,
                    
                    -- 基础统计
                    count() as request_count,
                    uniq(client_ip) as unique_clients,
                    uniq(request_uri) as unique_apis,
                    
                    -- 分类统计
                    multiIf(
                        response_status_code LIKE '2%', 'Success',
                        response_status_code LIKE '3%', 'Redirect', 
                        response_status_code LIKE '4%', 'Client Error',
                        response_status_code LIKE '5%', 'Server Error',
                        'Other'
                    ) as status_category,
                    
                    -- 性能影响
                    avg(total_request_duration) as avg_response_time,
                    quantile(0.95)(total_request_duration) as p95_response_time,
                    
                    -- 业务影响
                    avg(business_value_score) as avg_business_impact,
                    
                    now() as created_at
                FROM {database}.dwd_nginx_enriched_v2
                WHERE log_time >= now() - INTERVAL 1 DAY
                GROUP BY stat_time, platform, access_type, response_status_code
                """
            },
            
            # 5. 时间维度分析物化视图
            'mv_time_dimension_hourly': {
                'target_table': 'ads_time_dimension_analysis', 
                'description': '时间维度小时级聚合 - 对应05.时间维度分析.xlsx',
                'sql_template': """
                CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.{view_name}
                TO {database}.{target_table}
                AS SELECT
                    toStartOfHour(log_time) as stat_time,
                    'hour' as time_granularity,
                    platform,
                    toHour(log_time) as hour_of_day,
                    toDayOfWeek(log_time) as day_of_week,
                    if(toDayOfWeek(log_time) IN (6, 7), true, false) as is_weekend,
                    
                    -- 流量统计
                    count() as total_requests,
                    count() / 3600.0 as avg_qps,
                    uniq(client_ip) as unique_clients,
                    
                    -- 性能统计
                    avg(total_request_duration) as avg_response_time,
                    quantile(0.95)(total_request_duration) as p95_response_time,
                    
                    -- 质量统计
                    countIf(is_success) * 100.0 / count() as success_rate,
                    countIf(is_error) * 100.0 / count() as error_rate,
                    countIf(is_slow) * 100.0 / count() as slow_rate,
                    
                    -- 用户体验
                    (countIf(total_request_duration <= 1.5) + 
                     countIf(total_request_duration > 1.5 AND total_request_duration <= 6.0) * 0.5) 
                     / count() as apdex_score,
                    
                    now() as created_at
                FROM {database}.dwd_nginx_enriched_v2
                WHERE log_time >= now() - INTERVAL 1 DAY
                GROUP BY stat_time, platform, hour_of_day, day_of_week, is_weekend
                """
            }
        }
    
    def connect(self) -> bool:
        """
        连接ClickHouse数据库
        
        Returns:
            bool: 连接是否成功
        """
        try:
            self.client = clickhouse_connect.get_client(**self.config)
            # 测试连接
            result = self.client.query("SELECT 1")
            self.logger.info(f"✅ 成功连接到ClickHouse: {self.config['host']}:{self.config['port']}")
            return True
        except Exception as e:
            self.logger.error(f"❌ ClickHouse连接失败: {str(e)}")
            return False
    
    def check_view_exists(self, view_name: str) -> bool:
        """
        检查物化视图是否存在
        
        Args:
            view_name: 物化视图名称
            
        Returns:
            bool: 视图是否存在
        """
        try:
            query = f"""
            SELECT count() 
            FROM system.tables 
            WHERE database = '{self.database}' 
            AND name = '{view_name}' 
            AND engine = 'MaterializedView'
            """
            result = self.client.query(query)
            return result.result_rows[0][0] > 0
        except Exception as e:
            self.logger.error(f"检查物化视图存在性失败 {view_name}: {str(e)}")
            return False
    
    def create_materialized_view(self, view_name: str) -> Tuple[bool, str]:
        """
        创建单个物化视图
        
        Args:
            view_name: 物化视图名称
            
        Returns:
            Tuple[bool, str]: (是否成功, 消息)
        """
        if view_name not in self.materialized_views:
            return False, f"未知的物化视图: {view_name}"
        
        view_config = self.materialized_views[view_name]
        
        try:
            # 格式化SQL
            sql = view_config['sql_template'].format(
                database=self.database,
                view_name=view_name,
                target_table=view_config['target_table']
            )
            
            # 执行创建
            start_time = time.time()
            self.client.query(sql)
            duration = time.time() - start_time
            
            success_msg = f"✅ 物化视图 {view_name} 创建成功 ({view_config['description']})，耗时 {duration:.2f}s"
            self.logger.info(success_msg)
            return True, success_msg
            
        except ClickHouseError as e:
            error_msg = f"❌ 物化视图 {view_name} 创建失败: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"❌ 物化视图 {view_name} 创建异常: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def drop_materialized_view(self, view_name: str) -> Tuple[bool, str]:
        """
        删除物化视图
        
        Args:
            view_name: 物化视图名称
            
        Returns:
            Tuple[bool, str]: (是否成功, 消息)
        """
        try:
            sql = f"DROP VIEW IF EXISTS {self.database}.{view_name}"
            self.client.query(sql)
            
            success_msg = f"✅ 物化视图 {view_name} 删除成功"
            self.logger.info(success_msg)
            return True, success_msg
            
        except Exception as e:
            error_msg = f"❌ 物化视图 {view_name} 删除失败: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def create_all_views(self, force_recreate: bool = False) -> Dict[str, Any]:
        """
        创建所有物化视图
        
        Args:
            force_recreate: 是否强制重新创建
            
        Returns:
            Dict: 创建结果统计
        """
        results = {
            'total': len(self.materialized_views),
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'details': []
        }
        
        self.logger.info(f"🚀 开始创建 {results['total']} 个物化视图...")
        
        for view_name in self.materialized_views.keys():
            try:
                # 检查是否已存在
                if self.check_view_exists(view_name):
                    if force_recreate:
                        self.logger.info(f"🔄 物化视图 {view_name} 已存在，强制重新创建...")
                        drop_success, drop_msg = self.drop_materialized_view(view_name)
                        if not drop_success:
                            results['failed'] += 1
                            results['details'].append({'view': view_name, 'status': 'failed', 'message': drop_msg})
                            continue
                    else:
                        skip_msg = f"⏭️ 物化视图 {view_name} 已存在，跳过创建"
                        self.logger.info(skip_msg)
                        results['skipped'] += 1
                        results['details'].append({'view': view_name, 'status': 'skipped', 'message': skip_msg})
                        continue
                
                # 创建物化视图
                success, message = self.create_materialized_view(view_name)
                if success:
                    results['success'] += 1
                    results['details'].append({'view': view_name, 'status': 'success', 'message': message})
                else:
                    results['failed'] += 1
                    results['details'].append({'view': view_name, 'status': 'failed', 'message': message})
                    
            except Exception as e:
                error_msg = f"❌ 处理物化视图 {view_name} 时发生异常: {str(e)}"
                self.logger.error(error_msg)
                results['failed'] += 1
                results['details'].append({'view': view_name, 'status': 'failed', 'message': error_msg})
        
        # 输出总结
        self.logger.info(f"📊 物化视图创建完成: 成功 {results['success']}, 失败 {results['failed']}, 跳过 {results['skipped']}")
        return results
    
    def get_view_status(self) -> List[Dict[str, Any]]:
        """
        获取所有物化视图状态
        
        Returns:
            List: 物化视图状态列表
        """
        status_list = []
        
        for view_name, config in self.materialized_views.items():
            try:
                exists = self.check_view_exists(view_name)
                
                # 获取目标表数据量
                target_count = 0
                if exists:
                    try:
                        query = f"SELECT count() FROM {self.database}.{config['target_table']}"
                        result = self.client.query(query)
                        target_count = result.result_rows[0][0]
                    except:
                        target_count = -1  # 表示查询失败
                
                status_list.append({
                    'view_name': view_name,
                    'target_table': config['target_table'],
                    'description': config['description'],
                    'exists': exists,
                    'target_records': target_count,
                    'status': 'active' if exists and target_count >= 0 else 'inactive'
                })
                
            except Exception as e:
                status_list.append({
                    'view_name': view_name,
                    'target_table': config['target_table'],
                    'description': config['description'],
                    'exists': False,
                    'target_records': -1,
                    'status': 'error',
                    'error': str(e)
                })
        
        return status_list
    
    def print_status_report(self):
        """打印物化视图状态报告"""
        if not self.client:
            if not self.connect():
                return
        
        status_list = self.get_view_status()
        
        print("\n" + "="*80)
        print("📊 物化视图状态报告")
        print("="*80)
        
        for status in status_list:
            icon = "✅" if status['exists'] else "❌"
            records = status['target_records'] if status['target_records'] >= 0 else "N/A"
            
            print(f"{icon} {status['view_name']:<30} → {status['target_table']:<30} ({records:>6} 条)")
            print(f"   📝 {status['description']}")
            
            if 'error' in status:
                print(f"   ⚠️  错误: {status['error']}")
            print()
        
        # 统计
        total = len(status_list)
        active = sum(1 for s in status_list if s['exists'])
        print(f"📈 总计: {total} 个物化视图, {active} 个已激活, {total-active} 个未激活")
        print("="*80)


def main():
    """主函数 - 用于测试和管理物化视图"""
    import sys
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建管理器
    mv_manager = MaterializedViewManager()
    
    if not mv_manager.connect():
        print("❌ 无法连接到ClickHouse，请检查服务状态")
        sys.exit(1)
    
    # 根据参数执行不同操作
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'create':
            force = '--force' in sys.argv
            results = mv_manager.create_all_views(force_recreate=force)
            print(f"\n✅ 物化视图创建完成: 成功 {results['success']}, 失败 {results['failed']}, 跳过 {results['skipped']}")
            
        elif command == 'status':
            mv_manager.print_status_report()
            
        elif command == 'drop':
            if len(sys.argv) > 2:
                view_name = sys.argv[2]
                success, msg = mv_manager.drop_materialized_view(view_name)
                print(msg)
            else:
                print("❌ 请指定要删除的物化视图名称")
                
        else:
            print("❌ 未知命令，支持的命令: create, status, drop")
    else:
        # 默认显示状态
        mv_manager.print_status_report()


if __name__ == "__main__":
    main()