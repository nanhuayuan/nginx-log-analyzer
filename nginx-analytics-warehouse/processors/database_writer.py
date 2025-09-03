#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库写入器模块 - 专门负责数据写入
Database Writer Module - Specialized for Data Insertion

专门负责将处理后的数据写入ClickHouse数据库
支持ODS和DWD层的数据写入，包括批量写入、错误处理、状态跟踪等
"""

import json
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import logging
import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError

class DatabaseWriter:
    """数据库写入器"""
    
    def __init__(self, host: str = 'localhost', port: int = 8123, 
                 database: str = 'nginx_analytics', user: str = 'analytics_user', password: str = 'analytics_password'):
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
        
        # 批量写入配置
        self.batch_size = 1000
        self.max_retries = 3
        self.retry_delay = 1.0
        
        # 表结构映射 - 基于实际DDL
        self.ods_fields = [
            'id', 'log_time', 'server_name', 'client_ip', 'client_port', 'xff_ip',
            'remote_user', 'request_method', 'request_uri', 'request_full_uri',
            'http_protocol', 'response_status_code', 'response_body_size',
            'response_referer', 'user_agent', 'upstream_addr', 'upstream_connect_time',
            'upstream_header_time', 'upstream_response_time', 'total_request_time',
            'total_bytes_sent', 'query_string', 'connection_requests', 'trace_id',
            'business_sign', 'application_name', 'service_name', 'cache_status',
            'cluster_node', 'log_source_file', 'created_at'
        ]
        
        self.dwd_fields = [
            'id', 'ods_id', 'log_time', 'date_partition', 'hour_partition',
            'minute_partition', 'second_partition', 'client_ip', 'client_port',
            'xff_ip', 'server_name', 'request_method', 'request_uri',
            'request_uri_normalized', 'request_full_uri', 'query_parameters',
            'http_protocol_version', 'response_status_code', 'response_body_size',
            'response_body_size_kb', 'total_bytes_sent', 'total_bytes_sent_kb',
            'total_request_duration', 'upstream_connect_time', 'upstream_header_time',
            'upstream_response_time', 'backend_connect_phase', 'backend_process_phase',
            'backend_transfer_phase', 'nginx_transfer_phase', 'backend_total_phase',
            'network_phase', 'processing_phase', 'transfer_phase',
            'response_transfer_speed', 'total_transfer_speed', 'nginx_transfer_speed',
            'backend_efficiency', 'network_overhead', 'transfer_ratio',
            'connection_cost_ratio', 'processing_efficiency_index', 'platform',
            'platform_version', 'app_version', 'device_type', 'browser_type',
            'os_type', 'os_version', 'sdk_type', 'sdk_version', 'bot_type',
            'entry_source', 'referer_domain', 'search_engine', 'social_media',
            'api_category', 'api_module', 'api_version', 'business_domain',
            'access_type', 'client_category', 'application_name', 'service_name',
            'trace_id', 'business_sign', 'cluster_node', 'upstream_server',
            'connection_requests', 'cache_status', 'referer_url', 'user_agent_string',
            'log_source_file', 'is_success', 'is_business_success', 'is_slow',
            'is_very_slow', 'is_error', 'is_client_error', 'is_server_error',
            'has_anomaly', 'anomaly_type', 'user_experience_level',
            'apdex_classification', 'api_importance', 'business_value_score',
            'data_quality_score', 'parsing_errors', 'client_region', 'client_isp',
            'ip_risk_level', 'is_internal_ip', 'created_at', 'updated_at'
        ]
    
    def connect(self) -> bool:
        """连接数据库"""
        try:
            # 先连接默认数据库测试连接
            temp_config = self.config.copy()
            temp_config['database'] = 'default'
            
            self.client = clickhouse_connect.get_client(**temp_config)
            # 测试连接
            self.client.command("SELECT 1")
            self.logger.info(f"成功连接到ClickHouse: {self.config['host']}:{self.config['port']}")
            return True
            
        except Exception as e:
            self.logger.error(f"连接ClickHouse失败: {e}")
            return False
    
    def close(self):
        """关闭连接"""
        if self.client:
            try:
                self.client.close()
                self.logger.info("ClickHouse连接已关闭")
            except:
                pass
    
    def prepare_ods_data(self, processed_record: Dict[str, Any]) -> Dict[str, Any]:
        """准备ODS层数据"""
        ods_data = {}
        
        # 字段映射和转换 - 基于实际DDL字段
        field_mapping = {
            'id': processed_record.get('id'),
            'log_time': processed_record.get('log_time'),
            'server_name': processed_record.get('http_host', ''),
            'client_ip': processed_record.get('remote_addr', ''),
            'client_port': 0,  # 暂时没有端口信息
            'xff_ip': processed_record.get('xff', ''),
            'remote_user': '',  # 暂时没有远程用户信息
            'request_method': processed_record.get('method', ''),
            'request_uri': processed_record.get('uri_path', ''),
            'request_full_uri': processed_record.get('uri', ''),
            'http_protocol': processed_record.get('protocol', ''),
            'response_status_code': processed_record.get('code', ''),
            'response_body_size': processed_record.get('body_int', 0),
            'response_referer': processed_record.get('referer', ''),
            'user_agent': processed_record.get('agent', ''),
            'upstream_addr': '',  # 暂时没有上游地址
            'upstream_connect_time': processed_record.get('upstream_connect_time_float', 0.0),
            'upstream_header_time': processed_record.get('upstream_header_time_float', 0.0),
            'upstream_response_time': processed_record.get('upstream_response_time_float', 0.0),
            'total_request_time': processed_record.get('ar_time_float', 0.0),
            'total_bytes_sent': processed_record.get('body_int', 0),
            'query_string': processed_record.get('query_string', ''),
            'connection_requests': 1,  # 默认1个连接请求
            'trace_id': '',  # 暂时没有跟踪ID
            'business_sign': processed_record.get('api_category', ''),
            'application_name': processed_record.get('platform', ''),
            'service_name': processed_record.get('api_module', ''),
            'cache_status': '',  # 暂时没有缓存状态
            'cluster_node': '',  # 暂时没有集群节点
            'log_source_file': processed_record.get('source_file', ''),
            'created_at': datetime.now()
        }
        
        # 只包含ODS表需要的字段
        for field in self.ods_fields:
            ods_data[field] = field_mapping.get(field)
            
            # 处理None值
            if ods_data[field] is None:
                if field in ['id', 'response_body_size', 'total_bytes_sent', 'client_port', 'connection_requests']:
                    ods_data[field] = 0
                elif field in ['total_request_time', 'upstream_connect_time', 
                             'upstream_header_time', 'upstream_response_time']:
                    ods_data[field] = 0.0
                elif field in ['created_at']:
                    ods_data[field] = datetime.now()
                else:
                    ods_data[field] = ''
        
        return ods_data
    
    def prepare_dwd_data(self, processed_record: Dict[str, Any], ods_id: int) -> Dict[str, Any]:
        """准备DWD层数据"""
        dwd_data = {}
        
        # 基础字段赋值
        for field in self.dwd_fields:
            value = processed_record.get(field)
            
            # 特殊字段处理
            if field == 'ods_id':
                value = ods_id
            elif field == 'request_uri_normalized':
                value = processed_record.get('uri_path', '')
            elif field == 'total_bytes_sent_kb':
                value = processed_record.get('response_body_size', 0) / 1024.0
            elif field == 'parsing_errors':
                errors = processed_record.get('parsing_errors', [])
                value = errors if isinstance(errors, list) else []
            
            # 处理None值
            if value is None:
                if field in ['id', 'ods_id', 'response_body_size', 'total_bytes_sent', 
                           'client_port', 'connection_requests', 'business_value_score']:
                    value = 0
                elif field in ['total_request_duration', 'upstream_connect_time', 
                             'upstream_header_time', 'upstream_response_time',
                             'backend_connect_phase', 'backend_process_phase',
                             'backend_transfer_phase', 'nginx_transfer_phase',
                             'backend_total_phase', 'network_phase', 'processing_phase',
                             'transfer_phase', 'response_transfer_speed',
                             'total_transfer_speed', 'nginx_transfer_speed',
                             'backend_efficiency', 'network_overhead', 'transfer_ratio',
                             'connection_cost_ratio', 'processing_efficiency_index',
                             'response_body_size_kb', 'total_bytes_sent_kb',
                             'data_quality_score']:
                    value = 0.0
                elif field in ['is_success', 'is_business_success', 'is_slow',
                             'is_very_slow', 'is_error', 'is_client_error',
                             'is_server_error', 'has_anomaly', 'is_internal_ip']:
                    value = False
                elif field in ['created_at', 'updated_at']:
                    value = datetime.now()
                elif field in ['date_partition']:
                    log_time = processed_record.get('log_time')
                    value = log_time.date() if log_time else date.today()
                elif field in ['hour_partition', 'minute_partition', 'second_partition']:
                    log_time = processed_record.get('log_time')
                    if log_time:
                        if field == 'hour_partition':
                            value = log_time.hour
                        elif field == 'minute_partition':
                            value = log_time.minute
                        else:
                            value = log_time.second
                    else:
                        value = 0
                elif field == 'parsing_errors':
                    value = []
                else:
                    value = ''
            
            dwd_data[field] = value
        
        return dwd_data
    
    def insert_ods_batch(self, ods_records: List[Dict[str, Any]]) -> Tuple[bool, int, str]:
        """批量插入ODS数据"""
        if not ods_records:
            return True, 0, "没有数据需要插入"
        
        try:
            # 准备插入数据 - 转换为列表格式
            insert_data = []
            for record in ods_records:
                row_data = []
                for field in self.ods_fields:
                    value = record.get(field)
                    row_data.append(value)
                insert_data.append(row_data)
            
            # 执行插入
            start_time = time.time()
            self.client.insert(
                f'{self.database}.ods_nginx_raw',
                insert_data,
                column_names=self.ods_fields
            )
            duration = time.time() - start_time
            
            self.logger.info(f"成功插入ODS数据 {len(insert_data)} 条，耗时 {duration:.2f}s")
            return True, len(insert_data), f"插入成功，耗时 {duration:.2f}s"
            
        except Exception as e:
            error_str = str(e)
            if "filesystem error" in error_str or "Code: 1001" in error_str:
                error_msg = f"ClickHouse磁盘空间不足或权限问题，请检查服务器配置: {error_str}"
            else:
                error_msg = f"ODS数据插入失败: {error_str}"
            self.logger.error(error_msg)
            return False, 0, error_msg
    
    def insert_dwd_batch(self, dwd_records: List[Dict[str, Any]]) -> Tuple[bool, int, str]:
        """批量插入DWD数据"""
        if not dwd_records:
            return True, 0, "没有数据需要插入"
        
        try:
            # 准备插入数据
            insert_data = []
            for record in dwd_records:
                row_data = []
                for field in self.dwd_fields:
                    value = record.get(field)
                    row_data.append(value)
                insert_data.append(row_data)
            
            # 执行插入
            start_time = time.time()
            self.client.insert(
                f'{self.database}.dwd_nginx_enriched_v2',
                insert_data,
                column_names=self.dwd_fields
            )
            duration = time.time() - start_time
            
            self.logger.info(f"成功插入DWD数据 {len(insert_data)} 条，耗时 {duration:.2f}s")
            return True, len(insert_data), f"插入成功，耗时 {duration:.2f}s"
            
        except Exception as e:
            error_str = str(e)
            if "filesystem error" in error_str or "Code: 1001" in error_str:
                error_msg = f"ClickHouse磁盘空间不足或权限问题，请检查服务器配置: {error_str}"
            else:
                error_msg = f"DWD数据插入失败: {error_str}"
            self.logger.error(error_msg)
            return False, 0, error_msg
    
    def write_processed_records(self, processed_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """写入处理后的记录到ODS和DWD层"""
        if not processed_records:
            return {
                'success': False,
                'message': '没有数据需要写入',
                'ods_count': 0,
                'dwd_count': 0,
                'errors': []
            }
        
        start_time = time.time()
        errors = []
        ods_count = 0
        dwd_count = 0
        
        try:
            # 分批处理
            batches = [processed_records[i:i + self.batch_size] 
                      for i in range(0, len(processed_records), self.batch_size)]
            
            self.logger.info(f"开始写入数据，共 {len(processed_records)} 条记录，分 {len(batches)} 批处理")
            
            for batch_idx, batch in enumerate(batches, 1):
                self.logger.info(f"处理第 {batch_idx}/{len(batches)} 批，{len(batch)} 条记录")
                
                # 准备ODS数据
                ods_batch = []
                for record in batch:
                    ods_data = self.prepare_ods_data(record)
                    ods_batch.append(ods_data)
                
                # 插入ODS数据
                ods_success, ods_inserted, ods_message = self.insert_ods_batch(ods_batch)
                if not ods_success:
                    errors.append(f"批次 {batch_idx} ODS插入失败: {ods_message}")
                    continue
                
                ods_count += ods_inserted
                
                # 准备DWD数据
                dwd_batch = []
                for i, record in enumerate(batch):
                    # 使用记录ID作为ODS_ID
                    ods_id = record.get('id', i + 1)
                    dwd_data = self.prepare_dwd_data(record, ods_id)
                    dwd_batch.append(dwd_data)
                
                # 插入DWD数据
                dwd_success, dwd_inserted, dwd_message = self.insert_dwd_batch(dwd_batch)
                if not dwd_success:
                    errors.append(f"批次 {batch_idx} DWD插入失败: {dwd_message}")
                    continue
                
                dwd_count += dwd_inserted
                
                self.logger.info(f"批次 {batch_idx} 完成: ODS {ods_inserted} 条, DWD {dwd_inserted} 条")
            
            duration = time.time() - start_time
            success = len(errors) == 0
            
            result = {
                'success': success,
                'message': f"数据写入完成，耗时 {duration:.2f}s",
                'ods_count': ods_count,
                'dwd_count': dwd_count,
                'duration': duration,
                'errors': errors
            }
            
            if success:
                self.logger.info(f"数据写入成功: ODS {ods_count} 条, DWD {dwd_count} 条, 耗时 {duration:.2f}s")
            else:
                self.logger.warning(f"数据写入部分失败: {len(errors)} 个错误")
            
            return result
            
        except Exception as e:
            error_msg = f"数据写入过程发生严重错误: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'message': error_msg,
                'ods_count': ods_count,
                'dwd_count': dwd_count,
                'duration': time.time() - start_time,
                'errors': errors + [error_msg]
            }
    
    def clear_data(self, table_names: Optional[List[str]] = None, 
                   date_filter: Optional[str] = None) -> Dict[str, Any]:
        """清理数据"""
        if table_names is None:
            table_names = ['ods_nginx_raw', 'dwd_nginx_enriched_v2']
        
        results = {}
        
        for table_name in table_names:
            try:
                if date_filter:
                    # 按日期删除
                    sql = f"DELETE FROM {self.database}.{table_name} WHERE date = '{date_filter}'"
                    self.logger.info(f"清理表 {table_name} 中日期为 {date_filter} 的数据")
                else:
                    # 清空整表
                    sql = f"TRUNCATE TABLE {self.database}.{table_name}"
                    self.logger.info(f"清空表 {table_name}")
                
                start_time = time.time()
                self.client.command(sql)
                duration = time.time() - start_time
                
                results[table_name] = {
                    'success': True,
                    'message': f"清理成功，耗时 {duration:.2f}s",
                    'duration': duration
                }
                
            except Exception as e:
                error_msg = f"清理表 {table_name} 失败: {str(e)}"
                self.logger.error(error_msg)
                results[table_name] = {
                    'success': False,
                    'message': error_msg,
                    'duration': 0
                }
        
        return results
    
    def get_data_counts(self) -> Dict[str, int]:
        """获取各表数据量"""
        counts = {}
        
        tables = ['ods_nginx_raw', 'dwd_nginx_enriched_v2']
        
        for table in tables:
            try:
                result = self.client.query(f"SELECT COUNT(*) FROM {self.database}.{table}")
                counts[table] = result.result_rows[0][0] if result.result_rows else 0
            except Exception as e:
                self.logger.error(f"获取表 {table} 数据量失败: {e}")
                counts[table] = -1
        
        return counts
    
    def check_table_status(self) -> Dict[str, Any]:
        """检查表状态"""
        status = {}
        
        tables = ['ods_nginx_raw', 'dwd_nginx_enriched_v2']
        
        for table in tables:
            try:
                # 检查表是否存在
                result = self.client.query(f"EXISTS TABLE {self.database}.{table}")
                exists = result.result_rows[0][0] if result.result_rows else False
                
                if exists:
                    # 获取表信息
                    count_result = self.client.query(f"SELECT COUNT(*) FROM {self.database}.{table}")
                    count = count_result.result_rows[0][0] if count_result.result_rows else 0
                    
                    # 获取最新数据时间
                    try:
                        time_result = self.client.query(f"SELECT MAX(created_at) FROM {self.database}.{table}")
                        last_update = time_result.result_rows[0][0] if time_result.result_rows and time_result.result_rows[0][0] else None
                    except:
                        last_update = None
                    
                    status[table] = {
                        'exists': True,
                        'count': count,
                        'last_update': last_update,
                        'status': 'OK'
                    }
                else:
                    status[table] = {
                        'exists': False,
                        'count': 0,
                        'last_update': None,
                        'status': 'NOT_EXISTS'
                    }
                    
            except Exception as e:
                status[table] = {
                    'exists': False,
                    'count': -1,
                    'last_update': None,
                    'status': f'ERROR: {str(e)}'
                }
        
        return status

def test_writer():
    """测试数据库写入器"""
    writer = DatabaseWriter()
    
    print("=" * 60)
    print("数据库写入器测试")
    print("=" * 60)
    
    # 连接测试
    if not writer.connect():
        print("❌ 数据库连接失败")
        return
    
    print("✅ 数据库连接成功")
    
    # 表状态检查
    status = writer.check_table_status()
    print(f"📊 表状态:")
    for table, info in status.items():
        print(f"   {table}: {'存在' if info['exists'] else '不存在'} - {info['count']} 条记录")
    
    # 测试数据
    test_records = [
        {
            'id': 123456789,
            'log_time': datetime.now(),
            'http_host': 'test.example.com',
            'remote_addr': '192.168.1.1',
            'method': 'GET',
            'uri_path': '/api/test',
            'uri': '/api/test?param=1',
            'query_string': 'param=1',
            'protocol': 'HTTP/1.1',
            'code': '200',
            'body_int': 1024,
            'ar_time_float': 0.123,
            'agent': 'Test-Agent/1.0',
            'platform': 'Test',
            'device_type': 'Desktop',
            'api_category': 'test',
            'api_module': 'test',
            'is_success': True,
            'data_quality_score': 0.9
        }
    ]
    
    # 写入测试
    print(f"🔄 开始写入测试数据 ({len(test_records)} 条)...")
    result = writer.write_processed_records(test_records)
    
    if result['success']:
        print(f"✅ 写入成功:")
        print(f"   ODS: {result['ods_count']} 条")
        print(f"   DWD: {result['dwd_count']} 条")
        print(f"   耗时: {result['duration']:.2f}s")
    else:
        print(f"❌ 写入失败: {result['message']}")
        for error in result.get('errors', []):
            print(f"   错误: {error}")
    
    writer.close()

if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    test_writer()