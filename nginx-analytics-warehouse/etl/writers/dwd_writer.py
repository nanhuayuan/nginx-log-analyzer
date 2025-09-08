#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DWD层数据写入器 - 支持新表结构
DWD Writer - Supports new table structure

负责将映射后的数据写入dwd_nginx_enriched_v2表
"""

import logging
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
        
        # DWD表字段列表（排除自动生成和MATERIALIZED字段）
        # 排除：id, ods_id (自动生成), date, hour, minute, second, date_hour, date_hour_minute, 
        #       weekday, is_weekend, time_period (MATERIALIZED), created_at, updated_at (DEFAULT)
        self.dwd_fields = [
            # 基础字段
            'log_time', 'date_partition', 'hour_partition', 'minute_partition', 'second_partition',
            'client_ip', 'client_port', 'xff_ip', 'server_name', 'request_method',
            'request_uri', 'request_uri_normalized', 'request_full_uri', 'query_parameters',
            'http_protocol_version', 'response_status_code', 'response_body_size', 'response_body_size_kb',
            'total_bytes_sent', 'total_bytes_sent_kb', 'total_request_duration',
            
            # 性能字段
            'upstream_connect_time', 'upstream_header_time', 'upstream_response_time',
            'backend_connect_phase', 'backend_process_phase', 'backend_transfer_phase',
            'nginx_transfer_phase', 'backend_total_phase', 'network_phase', 'processing_phase', 
            'transfer_phase', 'response_transfer_speed', 'total_transfer_speed', 'nginx_transfer_speed',
            'backend_efficiency', 'network_overhead', 'transfer_ratio', 'connection_cost_ratio',
            'processing_efficiency_index',
            
            # 业务字段
            'platform', 'platform_version', 'app_version', 'device_type', 'browser_type',
            'os_type', 'os_version', 'sdk_type', 'sdk_version', 'bot_type',
            'entry_source', 'referer_domain', 'search_engine', 'social_media',
            'api_category', 'api_module', 'api_version', 'business_domain', 'access_type',
            'client_category', 'application_name', 'service_name', 'trace_id', 'business_sign',
            'cluster_node', 'upstream_server', 'connection_requests', 'cache_status',
            'referer_url', 'user_agent_string',
            
            # 日志源信息
            'log_source_file',
            
            # 状态字段
            'is_success', 'is_business_success', 'is_slow', 'is_very_slow', 'is_error',
            'is_client_error', 'is_server_error', 'has_anomaly', 'anomaly_type',
            'user_experience_level', 'apdex_classification', 'api_importance', 'business_value_score',
            'data_quality_score', 'parsing_errors',
            
            # 地理和风险信息
            'client_region', 'client_isp', 'ip_risk_level', 'is_internal_ip'
        ]
        
        # 写入统计
        self.stats = {
            'total_records': 0,
            'success_records': 0,
            'failed_records': 0,
            'batch_count': 0
        }
    
    def connect(self) -> bool:
        """连接ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(**self.config)
            self.client.ping()
            self.logger.info(f"成功连接到ClickHouse: {self.config['host']}:{self.config['port']}")
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
        """
        批量写入数据到DWD表
        
        Args:
            records: 要写入的记录列表
            
        Returns:
            写入结果字典
        """
        if not self.client:
            if not self.connect():
                return self._create_error_result("数据库连接失败")
        
        if not records:
            return self._create_success_result(0, "没有数据需要写入")
        
        try:
            self.stats['batch_count'] += 1
            self.logger.info(f"开始写入批次 {self.stats['batch_count']}: {len(records)} 条记录")
            
            # 准备数据
            prepared_data = self._prepare_batch_data(records)
            
            # 执行插入（排除自动生成的id和ods_id字段）
            table_name = f"{self.config['database']}.dwd_nginx_enriched_v2"
            
            # 构建INSERT语句，明确指定字段列表
            column_list = ', '.join(self.dwd_fields)
            insert_sql = f"INSERT INTO {table_name} ({column_list}) VALUES"
            
            result = self.client.insert(
                table=table_name,
                data=prepared_data,
                column_names=self.dwd_fields
            )
            
            # 更新统计
            success_count = len(records)
            self.stats['total_records'] += success_count
            self.stats['success_records'] += success_count
            
            self.logger.info(f"批次写入成功: {success_count} 条记录")
            
            return self._create_success_result(success_count, f"成功写入 {success_count} 条记录")
            
        except ClickHouseError as e:
            self.logger.error(f"ClickHouse写入错误: {e}")
            self.stats['failed_records'] += len(records)
            return self._create_error_result(f"ClickHouse错误: {str(e)}")
            
        except Exception as e:
            self.logger.error(f"写入异常: {e}")
            self.stats['failed_records'] += len(records)
            return self._create_error_result(f"未知错误: {str(e)}")
    
    def write_single(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        写入单条记录
        
        Args:
            record: 要写入的记录
            
        Returns:
            写入结果字典
        """
        return self.write_batch([record])
    
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
    
    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            
            # 执行简单查询测试连接
            result = self.client.query("SELECT 1")
            return True
            
        except Exception as e:
            self.logger.error(f"连接测试失败: {e}")
            return False
    
    def validate_table_structure(self) -> Dict[str, Any]:
        """验证目标表结构"""
        try:
            if not self.client:
                if not self.connect():
                    return {'valid': False, 'error': '无法连接数据库'}
            
            # 查询表结构
            table_name = f"{self.config['database']}.dwd_nginx_enriched_v2"
            result = self.client.query(f"DESCRIBE TABLE {table_name}")
            
            # 获取实际字段列表（排除自动生成、MATERIALIZED和DEFAULT字段）
            excluded_fields = ['id', 'ods_id', 'date', 'hour', 'minute', 'second', 
                             'date_hour', 'date_hour_minute', 'weekday', 'is_weekend', 
                             'time_period', 'created_at', 'updated_at']
            actual_fields = [row[0] for row in result.result_rows if row[0] not in excluded_fields]
            
            # 比较字段
            missing_fields = set(self.dwd_fields) - set(actual_fields)
            extra_fields = set(actual_fields) - set(self.dwd_fields)
            
            validation_result = {
                'valid': len(missing_fields) == 0,
                'total_fields': len(actual_fields),
                'expected_fields': len(self.dwd_fields),
                'missing_fields': list(missing_fields),
                'extra_fields': list(extra_fields)
            }
            
            if validation_result['valid']:
                self.logger.info(f"表结构验证通过: {validation_result['total_fields']} 个字段")
            else:
                self.logger.warning(f"表结构验证失败: 缺失 {len(missing_fields)} 个字段")
                
            return validation_result
            
        except Exception as e:
            self.logger.error(f"表结构验证失败: {e}")
            return {'valid': False, 'error': str(e)}
    
    # === 私有方法 ===
    
    def _prepare_batch_data(self, records: List[Dict[str, Any]]) -> List[List[Any]]:
        """准备批量写入数据"""
        prepared_data = []
        
        for record in records:
            row_data = []
            
            for field_name in self.dwd_fields:
                value = record.get(field_name)
                
                # 处理不同类型的字段
                processed_value = self._process_field_value(field_name, value)
                row_data.append(processed_value)
            
            prepared_data.append(row_data)
        
        return prepared_data
    
    def _process_field_value(self, field_name: str, value: Any) -> Any:
        """处理字段值，确保类型匹配"""
        # 处理None值
        if value is None:
            return self._get_default_value(field_name)
        
        # 时间类型字段
        if field_name in ['log_time', 'created_at', 'updated_at']:
            if isinstance(value, datetime):
                return value
            return datetime.now()
        
        # 日期类型字段  
        if field_name in ['date_partition']:
            if hasattr(value, 'date'):
                return value.date()
            return datetime.now().date()
        
        # 整数字段
        if field_name in ['hour_partition', 'minute_partition', 'second_partition', 
                         'client_port', 'response_body_size', 'total_bytes_sent',
                         'connection_requests', 'business_value_score']:
            return int(value) if value is not None else 0
        
        # 浮点数字段
        if field_name in ['response_body_size_kb', 'total_bytes_sent_kb', 'total_request_duration',
                         'upstream_connect_time', 'upstream_header_time', 'upstream_response_time',
                         'backend_connect_phase', 'backend_process_phase', 'backend_transfer_phase',
                         'nginx_transfer_phase', 'backend_total_phase', 'network_phase',
                         'processing_phase', 'transfer_phase', 'response_transfer_speed',
                         'total_transfer_speed', 'nginx_transfer_speed', 'backend_efficiency',
                         'network_overhead', 'transfer_ratio', 'connection_cost_ratio',
                         'processing_efficiency_index', 'data_quality_score']:
            return float(value) if value is not None else 0.0
        
        # 布尔字段
        if field_name in ['is_success', 'is_business_success', 'is_slow', 'is_very_slow',
                         'is_error', 'is_client_error', 'is_server_error', 'has_anomaly',
                         'is_internal_ip']:
            return bool(value) if value is not None else False
        
        # 数组字段
        if field_name == 'parsing_errors':
            if isinstance(value, list):
                return value
            return []
        
        # 字符串字段（默认）
        return str(value) if value is not None else ''
    
    def _get_default_value(self, field_name: str) -> Any:
        """获取字段的默认值"""
        # 时间类型
        if field_name in ['log_time']:
            return datetime.now()
        
        # 日期类型
        if field_name in ['date_partition']:
            return datetime.now().date()
        
        # 整数类型
        if field_name in ['hour_partition', 'minute_partition', 'second_partition', 
                         'client_port', 'response_body_size', 'total_bytes_sent',
                         'connection_requests', 'business_value_score']:
            return 0
        
        # 浮点数类型
        if field_name in ['response_body_size_kb', 'total_bytes_sent_kb', 'total_request_duration',
                         'upstream_connect_time', 'upstream_header_time', 'upstream_response_time'] + \
                        [f for f in self.dwd_fields if 'phase' in f or 'speed' in f or 
                         'efficiency' in f or 'overhead' in f or 'ratio' in f]:
            return 0.0
        
        # 布尔类型
        if field_name.startswith('is_') or field_name.startswith('has_'):
            return False
        
        # 数组类型
        if field_name == 'parsing_errors':
            return []
        
        # 字符串类型（默认）
        return ''
    
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


# 测试代码
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    writer = DWDWriter()
    
    # 测试连接
    print("🔗 测试数据库连接...")
    if writer.test_connection():
        print("✅ 连接成功")
        
        # 验证表结构
        print("\n📋 验证表结构...")
        validation = writer.validate_table_structure()
        print(f"验证结果: {validation}")
        
        if validation.get('valid'):
            print("✅ 表结构验证通过")
        else:
            print("❌ 表结构验证失败")
            if validation.get('missing_fields'):
                print(f"缺失字段: {validation['missing_fields'][:10]}...")  # 只显示前10个
    else:
        print("❌ 连接失败")
    
    # 关闭连接
    writer.close()