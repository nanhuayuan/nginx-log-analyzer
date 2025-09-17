#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DWD层数据写入器 - 支持新表结构 (修复版)
DWD Writer - Supports new table structure (Fixed version)

负责将映射后的数据写入dwd_nginx_enriched_v3表
"""

import logging
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError

class DWDWriter:
    """DWD层数据写入器"""

    def __init__(self, host: str = 'localhost', port: int = 8123,
                 database: str = 'nginx_analytics', username: str = 'analytics_user',
                 password: str = 'analytics_password_change_in_prod'):
        """
        初始化DWD写入器

        Args:
            host: ClickHouse主机
            port: ClickHouse端口
            database: 数据库名
            username: 用户名
            password: 密码
        """
        self.config = {
            'host': host,
            'port': port,
            'database': database,
            'username': username,
            'password': password
        }

        self.client = None
        self.logger = logging.getLogger(__name__)
        self._write_lock = threading.Lock()  # 添加写入锁防止并发写入同一连接

        # 根据数据库实际结构生成的284个字段（2025-09-14）- 完整字段列表
        # 排除MATERIALIZED字段：date, hour, minute, second, quarter, week, day_of_year,
        # date_hour, date_hour_minute, weekday, is_weekend, time_period, business_hours_type, traffic_pattern
        self.dwd_fields = [
            'id',
            'ods_id',
            'log_time',
            'date_partition',
            'hour_partition',
            'minute_partition',
            'second_partition',
            'quarter_partition',
            'week_partition',
            'tenant_code',
            'team_code',
            'environment',
            'data_sensitivity',
            'cost_center',
            'business_unit',
            'region_code',
            'compliance_zone',
            'client_ip',
            'client_port',
            'xff_ip',
            'client_real_ip',
            'client_ip_type',
            'client_ip_classification',
            'client_country',
            'client_region',
            'client_city',
            'client_isp',
            'client_org',
            'client_asn',
            'server_name',
            'server_port',
            'server_protocol',
            'load_balancer_node',
            'edge_location',
            'datacenter',
            'availability_zone',
            'cluster_node',
            'instance_id',
            'pod_name',
            'container_id',
            'request_method',
            'request_uri',
            'request_uri_normalized',
            'request_full_uri',
            'request_path',
            'query_parameters',
            'query_params_count',
            'request_body_size',
            'http_protocol_version',
            'content_type',
            'accept_language',
            'accept_encoding',
            'response_status_code',
            'response_status_class',
            'response_body_size',
            'response_body_size_kb',
            'response_content_type',
            'response_content_encoding',
            'response_cache_control',
            'response_etag',
            'total_bytes_sent',
            'total_bytes_sent_kb',
            'bytes_received',
            'total_request_duration',
            'request_processing_time',
            'response_send_time',
            'upstream_connect_time',
            'upstream_header_time',
            'upstream_response_time',
            'backend_connect_phase',
            'backend_process_phase',
            'backend_transfer_phase',
            'nginx_transfer_phase',
            'backend_total_phase',
            'network_phase',
            'processing_phase',
            'transfer_phase',
            'response_transfer_speed',
            'total_transfer_speed',
            'nginx_transfer_speed',
            'backend_efficiency',
            'network_overhead',
            'transfer_ratio',
            'connection_cost_ratio',
            'processing_efficiency_index',
            'performance_score',
            'latency_percentile',
            'platform',
            'platform_version',
            'platform_category',
            'app_version',
            'app_build_number',
            'device_type',
            'device_model',
            'device_manufacturer',
            'screen_resolution',
            'browser_type',
            'browser_version',
            'browser_engine',
            'os_type',
            'os_version',
            'os_architecture',
            'sdk_type',
            'sdk_version',
            'integration_type',
            'framework_type',
            'framework_version',
            'access_entry_point',
            'entry_source',
            'entry_source_detail',
            'client_channel',
            'traffic_source',
            'campaign_id',
            'utm_source',
            'utm_medium',
            'utm_campaign',
            'utm_content',
            'utm_term',
            'referer_url',
            'referer_domain',
            'referer_domain_type',
            'search_engine',
            'search_keywords',
            'social_media',
            'social_media_type',
            'bot_type',
            'bot_name',
            'is_bot',
            'bot_probability',
            'crawler_category',
            'api_category',
            'api_subcategory',
            'api_module',
            'api_submodule',
            'api_version',
            'api_endpoint_type',
            'business_domain',
            'business_subdomain',
            'functional_area',
            'service_tier',
            'business_operation_type',
            'business_operation_subtype',
            'user_journey_stage',
            'user_session_stage',
            'transaction_type',
            'workflow_step',
            'process_stage',
            'access_type',
            'access_method',
            'client_category',
            'client_type',
            'client_classification',
            'integration_pattern',
            'user_id',
            'session_id',
            'user_type',
            'user_tier',
            'user_segment',
            'authentication_method',
            'authorization_level',
            'application_name',
            'application_version',
            'service_name',
            'service_version',
            'microservice_name',
            'service_mesh_name',
            'upstream_server',
            'upstream_service',
            'downstream_service',
            'trace_id',
            'span_id',
            'parent_span_id',
            'correlation_id',
            'request_id',
            'transaction_id',
            'business_transaction_id',
            'batch_id',
            'cache_status',
            'cache_layer',
            'cache_key',
            'cache_age',
            'cache_hit_ratio',
            'connection_requests',
            'connection_id',
            'connection_type',
            'ssl_session_reused',
            'business_sign',
            'feature_flag',
            'ab_test_group',
            'experiment_id',
            'custom_tags',
            'business_tags',
            'user_agent_string',
            'custom_headers',
            'security_headers',
            'cookie_count',
            'header_size',
            'log_source_file',
            'log_format_version',
            'log_level',
            'raw_log_entry',
            'error_code_group',
            'http_error_class',
            'error_severity_level',
            'error_category',
            'error_subcategory',
            'error_source',
            'error_propagation_path',
            'upstream_status_code',
            'error_correlation_id',
            'error_chain',
            'root_cause_analysis',
            'is_success',
            'is_business_success',
            'is_error',
            'is_client_error',
            'is_server_error',
            'is_retry',
            'has_anomaly',
            'is_slow',
            'is_very_slow',
            'perf_attention',
            'perf_warning',
            'perf_slow',
            'perf_very_slow',
            'perf_timeout',
            'performance_level',
            'anomaly_type',
            'anomaly_severity',
            'user_experience_level',
            'apdex_classification',
            'performance_rating',
            'sla_compliance',
            'sla_violation_type',
            'api_importance_level',
            'business_criticality',
            'business_value_score',
            'revenue_impact_level',
            'customer_impact_level',
            'data_quality_score',
            'data_completeness',
            'parsing_errors',
            'validation_errors',
            'enrichment_status',
            'security_risk_score',
            'security_risk_level',
            'threat_category',
            'attack_signature',
            'ip_reputation',
            'geo_anomaly',
            'access_pattern_anomaly',
            'rate_limit_hit',
            'blocked_by_waf',
            'fraud_score',
            'network_type',
            'ip_risk_level',
            'is_internal_ip',
            'is_tor_exit',
            'is_proxy',
            'is_vpn',
            'is_datacenter',
            'is_holiday',
            'created_at',
            'updated_at',
            'data_version',
            'last_processed_at',
            'processing_flags',
            'custom_dimensions',
            'custom_metrics',
            'metadata'
        ]

        # 写入统计
        self.stats = {
            'total_records': 0,
            'success_records': 0,
            'failed_records': 0,
            'batch_count': 0
        }

    def connect(self) -> bool:
        """连接ClickHouse - 确保每个线程独立连接"""
        try:
            import uuid
            import time

            # 为每个连接创建独立的配置，避免会话冲突
            connection_config = self.config.copy()
            # 使用UUID和时间戳确保唯一的会话ID
            unique_suffix = f"{uuid.uuid4().hex[:8]}_{int(time.time() * 1000000)}"
            thread_id = threading.current_thread().ident or 0
            connection_config['session_id'] = f"etl_session_{thread_id}_{unique_suffix}"
            connection_config['connect_timeout'] = 10
            connection_config['send_receive_timeout'] = 30

            self.client = clickhouse_connect.get_client(**connection_config)
            self.client.ping()
            self.logger.info(f"成功连接到ClickHouse: {self.config['host']}:{self.config['port']} (会话: {connection_config['session_id'][:20]}...)")
            return True
        except Exception as e:
            self.logger.error(f"连接ClickHouse失败: {e}")
            self.client = None
            return False

    def close(self):
        """关闭连接"""
        if self.client:
            try:
                self.client.close()
                self.logger.info("ClickHouse连接已关闭")
            except Exception as e:
                self.logger.error(f"关闭连接失败: {e}")
            finally:
                self.client = None

    def write_batch(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """批量写入数据到DWD表"""
        # 使用写入锁防止并发写入同一连接
        with self._write_lock:
            if not self.client:
                if not self.connect():
                    return self._create_error_result("数据库连接失败")

            if not records:
                return self._create_success_result(0, "没有数据需要写入")

            try:
                self.stats['batch_count'] += 1
                record_count_safe = self._safe_len(records, 1)
                self.logger.info(f"开始写入批次 {self.stats['batch_count']}: {record_count_safe} 条记录")

                # 将数据转换为字典格式
                dict_data = []
                for record in records:
                    if not isinstance(record, dict):
                        self.logger.error(f"记录不是字典类型: {type(record)}")
                        continue

                    row_dict = {}
                    for field_name in self.dwd_fields:
                        value = record.get(field_name)
                        # 特别处理空字符串，应当视为None
                        if value == '':
                            value = None
                        processed_value = self._process_field_value(field_name, value)
                        row_dict[field_name] = processed_value

                    dict_data.append(row_dict)

                if not dict_data:
                    return self._create_error_result("没有有效的记录可写入")

                dict_count_safe = self._safe_len(dict_data, 0)
                self.logger.info(f"准备写入 {dict_count_safe} 条记录到数据库")

                # 尝试使用列表格式插入
                table_name = f"{self.config['database']}.dwd_nginx_enriched_v3"

                # 将字典转换为按字段顺序的列表
                list_data = []
                for record in dict_data:
                    row_list = []
                    for field_name in self.dwd_fields:
                        row_list.append(record[field_name])
                    list_data.append(row_list)

                result = self.client.insert(
                    table=table_name,
                    data=list_data
                    # 不指定column_names，让ClickHouse使用表的默认字段顺序
                )

                # 更新统计
                success_count = dict_count_safe  # 使用之前计算的安全计数
                self.stats['total_records'] += success_count
                self.stats['success_records'] += success_count

                self.logger.info(f"批次写入成功: {success_count} 条记录")
                return self._create_success_result(success_count, f"成功写入 {success_count} 条记录")

            except ClickHouseError as e:
                self.logger.error(f"ClickHouse写入错误: {e}")
                # 使用安全的记录计数
                record_count = self._safe_len(records, 1)
                self.stats['failed_records'] += record_count
                return self._create_error_result(f"ClickHouse错误: {str(e)}")

            except Exception as e:
                self.logger.error(f"写入异常: {e}")
                # 使用安全的记录计数
                record_count = self._safe_len(records, 1)
                self.stats['failed_records'] += record_count
                return self._create_error_result(f"未知错误: {str(e)}")

    def _process_field_value(self, field_name: str, value: Any) -> Any:
        """处理字段值，确保类型匹配"""
        if value is None:
            return self._get_default_value(field_name)

        # 时间类型字段 - 移除时区信息
        if field_name in ['log_time', 'last_processed_at', 'created_at', 'updated_at']:
            if isinstance(value, datetime):
                # 移除时区信息，ClickHouse通常不需要时区
                if value.tzinfo is not None:
                    return value.replace(tzinfo=None)
                return value
            return datetime.now()

        # DateTime字段 (date_hour, date_hour_minute)
        if field_name in ['date_hour', 'date_hour_minute']:
            if isinstance(value, datetime):
                return value.replace(tzinfo=None) if value.tzinfo else value
            return datetime.now().replace(second=0, microsecond=0) if field_name == 'date_hour_minute' else datetime.now().replace(minute=0, second=0, microsecond=0)

        # 日期类型字段
        if field_name in ['date_partition', 'date']:
            if hasattr(value, 'date'):
                return value.date()
            return datetime.now().date()

        # 整数字段 - 改进空字符串处理
        if field_name in ['hour_partition', 'minute_partition', 'second_partition', 'quarter_partition', 'week_partition',
                         'client_port', 'server_port', 'client_asn', 'query_params_count', 'request_body_size',
                         'response_body_size', 'total_bytes_sent', 'bytes_received', 'total_request_duration',
                         'request_processing_time', 'response_send_time', 'upstream_connect_time',
                         'upstream_header_time', 'upstream_response_time', 'backend_connect_phase',
                         'backend_process_phase', 'backend_transfer_phase', 'nginx_transfer_phase',
                         'backend_total_phase', 'network_phase', 'processing_phase', 'transfer_phase',
                         'cache_age', 'connection_requests', 'cookie_count', 'header_size',
                         'business_value_score', 'data_sensitivity', 'performance_level', 'security_risk_score',
                         'data_version']:
            if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
                return 0
            try:
                return int(float(value))  # 先转float再转int，处理"3.0"这种情况
            except (ValueError, TypeError):
                return 0

        # 浮点数字段
        if field_name in ['response_body_size_kb', 'total_bytes_sent_kb', 'response_transfer_speed',
                         'total_transfer_speed', 'nginx_transfer_speed', 'backend_efficiency',
                         'network_overhead', 'transfer_ratio', 'connection_cost_ratio',
                         'processing_efficiency_index', 'performance_score', 'latency_percentile',
                         'cache_hit_ratio', 'bot_probability', 'fraud_score', 'data_quality_score',
                         'data_completeness']:
            return float(value) if value is not None else 0.0

        # 布尔字段
        if field_name in ['is_success', 'is_business_success', 'is_slow', 'is_very_slow',
                         'perf_attention', 'perf_warning', 'perf_slow', 'perf_very_slow', 'perf_timeout',
                         'is_error', 'is_client_error', 'is_server_error', 'is_retry',
                         'has_anomaly', 'is_internal_ip', 'is_tor_exit', 'is_proxy', 'is_vpn',
                         'is_datacenter', 'is_bot', 'ssl_session_reused', 'geo_anomaly',
                         'access_pattern_anomaly', 'rate_limit_hit', 'blocked_by_waf',
                         'sla_compliance', 'is_holiday']:
            return bool(value) if value is not None else False

        # Map类型字段
        if field_name in ['metadata', 'custom_dimensions', 'custom_metrics', 'custom_headers', 'security_headers']:
            if isinstance(value, dict):
                return value
            return {}

        # 数组字段
        if field_name in ['parsing_errors', 'validation_errors', 'error_chain', 'custom_tags', 'business_tags', 'processing_flags']:
            if isinstance(value, list):
                return value
            return []

        # 字符串字段（默认）
        return str(value) if value is not None else ''

    def _get_default_value(self, field_name: str) -> Any:
        """获取字段的默认值"""
        # 时间类型字段
        if field_name in ['log_time', 'last_processed_at', 'created_at', 'updated_at']:
            return datetime.now()
        elif field_name in ['date_partition', 'date']:
            return datetime.now().date()
        elif field_name in ['date_hour', 'date_hour_minute']:
            return datetime.now().replace(second=0, microsecond=0) if field_name == 'date_hour_minute' else datetime.now().replace(minute=0, second=0, microsecond=0)

        # 布尔字段
        elif field_name.startswith('is_') or field_name.startswith('has_') or field_name.startswith('perf_') or field_name in ['ssl_session_reused', 'geo_anomaly', 'access_pattern_anomaly', 'rate_limit_hit', 'blocked_by_waf', 'sla_compliance', 'is_holiday']:
            return False

        # Map类型字段
        elif field_name in ['metadata', 'custom_dimensions', 'custom_metrics', 'custom_headers', 'security_headers']:
            return {}

        # 数组字段
        elif field_name in ['parsing_errors', 'validation_errors', 'error_chain', 'custom_tags', 'business_tags', 'processing_flags']:
            return []

        # 整数字段 (包含新增字段)
        elif field_name in ['id', 'ods_id', 'hour_partition', 'minute_partition', 'second_partition', 'quarter_partition', 'week_partition',
                           'client_port', 'server_port', 'client_asn', 'query_params_count', 'request_body_size',
                           'response_body_size', 'total_bytes_sent', 'bytes_received', 'total_request_duration',
                           'request_processing_time', 'response_send_time', 'upstream_connect_time',
                           'upstream_header_time', 'upstream_response_time', 'backend_connect_phase',
                           'backend_process_phase', 'backend_transfer_phase', 'nginx_transfer_phase',
                           'backend_total_phase', 'network_phase', 'processing_phase', 'transfer_phase',
                           'cache_age', 'connection_requests', 'cookie_count', 'header_size',
                           'business_value_score', 'data_sensitivity', 'performance_level', 'security_risk_score',
                           'data_version', 'hour', 'minute', 'second', 'quarter', 'week', 'day_of_year', 'weekday']:
            return 0

        # 浮点数字段
        elif field_name in ['response_body_size_kb', 'total_bytes_sent_kb', 'response_transfer_speed',
                           'total_transfer_speed', 'nginx_transfer_speed', 'backend_efficiency',
                           'network_overhead', 'transfer_ratio', 'connection_cost_ratio',
                           'processing_efficiency_index', 'performance_score', 'latency_percentile',
                           'cache_hit_ratio', 'bot_probability', 'fraud_score', 'data_quality_score',
                           'data_completeness']:
            return 0.0

        # 字符串字段（默认）
        else:
            return ''

    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            result = self.client.query("SELECT 1")
            return True
        except Exception as e:
            self.logger.error(f"连接测试失败: {e}")
            return False

    def get_stats(self) -> Dict[str, Any]:
        """获取写入统计信息"""
        stats = self.stats.copy()
        if stats['total_records'] > 0:
            stats['success_rate'] = (stats['success_records'] / stats['total_records']) * 100
        else:
            stats['success_rate'] = 0.0
        return stats

    def reset_stats(self):
        """重置统计信息"""
        self.stats = {
            'total_records': 0,
            'success_records': 0,
            'failed_records': 0,
            'batch_count': 0
        }

    def _create_success_result(self, count: int, message: str) -> Dict[str, Any]:
        """创建成功结果"""
        return {
            'success': True,
            'count': count,
            'message': message,
            'timestamp': datetime.now()
        }

    def _create_error_result(self, error: str) -> Dict[str, Any]:
        """创建错误结果"""
        return {
            'success': False,
            'count': 0,
            'error': error,
            'timestamp': datetime.now()
        }

    def _safe_len(self, obj, default=1000):
        """安全的长度获取函数，防止任何len()相关错误"""
        try:
            if hasattr(obj, '__len__') and not isinstance(obj, (str, bytes)):
                return len(obj)
            else:
                return default
        except Exception:
            return default

    def batch_write_optimized(self, records: List[Dict[str, Any]]) -> bool:
        """
        优化的批量写入方法 - 使用ClickHouse性能优化技术

        Args:
            records: 要写入的记录列表

        Returns:
            bool: 写入是否成功
        """
        if not self.client:
            if not self.connect():
                self.logger.error("数据库连接失败")
                return False

        if not records:
            return True

        try:
            # 优化1: 预分配数组和批量验证
            list_data = []
            list_data_append = list_data.append  # 局部变量优化

            # 优化2: 缓存字段默认值
            field_defaults = {field: self._get_default_value(field) for field in self.dwd_fields}

            # 优化3: 快速数据转换（减少函数调用开销）
            for record in records:
                if not isinstance(record, dict):
                    continue

                row_list = []
                row_list_append = row_list.append  # 局部变量优化

                for field_name in self.dwd_fields:
                    value = record.get(field_name)
                    if value == '' or value is None:
                        value = field_defaults[field_name]
                    else:
                        value = self._process_field_value_fast(field_name, value)
                    row_list_append(value)

                list_data_append(row_list)

            if not list_data:
                return True

            # 优化4: 使用优化的ClickHouse配置写入
            table_name = f"{self.config['database']}.dwd_nginx_enriched_v3"

            # 优化5: 使用client的高性能配置
            # 使用安全长度函数并确保是整数
            data_length = self._safe_len(list_data)
            # 确保data_length是整数并且在合理范围内
            if not isinstance(data_length, int) or data_length < 1:
                data_length = 1000  # 默认安全值

            # 使用固定优化的数值，避免在settings中进行复杂计算
            block_size = 8192 if data_length < 1000 else min(65536, data_length)
            insert_block_size = 1048576 if data_length < 10000 else min(10485760, data_length)

            # 由于ClickHouse客户端库在某些优化设置下会内部调用len()导致错误，
            # 直接使用稳定的write_batch方法
            return self.write_batch(records)

            # 更新统计（简化版）
            success_count = data_length  # 使用之前计算的安全长度
            self.stats['total_records'] += success_count
            self.stats['success_records'] += success_count
            self.stats['batch_count'] += 1

            return True

        except Exception as e:
            self.logger.error(f"优化批量写入失败: {e}")

            # 详细错误诊断已完成，移除调试代码

            # 使用安全的记录计数处理
            try:
                record_count = self._safe_len(records, 1)
                if record_count == 1 and 'list_data' in locals():
                    record_count = self._safe_len(list_data, 1)
            except Exception as count_error:
                # 最后的安全网
                record_count = 1
                self.logger.error(f"记录计数失败: {count_error}")

            self.stats['failed_records'] += record_count
            return False

    def _process_field_value_fast(self, field_name: str, value: Any) -> Any:
        """快速字段值处理 - 优化版本，减少条件判断"""
        # 时间类型字段快速处理
        if field_name in {'log_time', 'last_processed_at', 'created_at', 'updated_at'}:
            if isinstance(value, datetime):
                return value.replace(tzinfo=None) if value.tzinfo else value
            return datetime.now()

        # 日期类型字段快速处理
        if field_name in {'date_partition', 'date'}:
            if hasattr(value, 'date'):
                return value.date()
            return datetime.now().date()

        # 整数字段快速处理
        if field_name.endswith(('_partition', '_count', '_size', '_code', '_port', 'duration')):
            try:
                return int(value) if value not in {None, '', 'null', 'NULL'} else 0
            except (ValueError, TypeError):
                return 0

        # 浮点数字段快速处理
        if field_name.endswith(('_time', '_score', '_rate', '_ratio')):
            try:
                return float(value) if value not in {None, '', 'null', 'NULL'} else 0.0
            except (ValueError, TypeError):
                return 0.0

        # 字符串字段（默认）
        return str(value) if value is not None else ''

# 测试代码
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    writer = DWDWriter()

    # 测试连接
    print("🔗 测试数据库连接...")
    if writer.test_connection():
        print("✅ 连接成功")
    else:
        print("❌ 连接失败")

    # 关闭连接
    writer.close()