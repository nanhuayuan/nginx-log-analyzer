# -*- coding: utf-8 -*-
"""
ClickHouse数据管道 - 完整的分层数据处理
支持从nginx日志到完整数据仓库的端到端处理
"""

import os
import sys
import json
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Optional

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent))

from data_pipeline.clickhouse_processor import ClickHouseProcessor
from scripts.nginx_log_processor import NginxLogProcessor
from scripts.incremental_manager import IncrementalManager
from self.self_00_02_utils import log_info

class ClickHousePipeline:
    """ClickHouse完整数据管道"""
    
    def __init__(self):
        self.ck_processor = ClickHouseProcessor()
        self.nginx_processor = NginxLogProcessor()
        self.increment_manager = IncrementalManager()
        self.ensure_advanced_tables()
    
    def ensure_advanced_tables(self):
        """确保高级表结构存在"""
        try:
            if not self.ck_processor.connect():
                raise Exception("无法连接到ClickHouse")
            
            # 读取并执行高级表创建脚本
            sql_file = Path(__file__).parent.parent / "docker" / "clickhouse_init" / "003_create_advanced_tables.sql"
            
            if sql_file.exists():
                with open(sql_file, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                
                # 分割并执行每个语句
                statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
                
                for stmt in statements:
                    if stmt.upper().startswith(('CREATE', 'ALTER')):
                        try:
                            self.ck_processor.client.command(stmt)
                        except Exception as e:
                            # 忽略已存在的表/视图错误
                            if 'already exists' not in str(e).lower():
                                log_info(f"执行SQL语句失败: {e}", level="WARN")
                
                log_info("高级表结构检查完成")
            
        except Exception as e:
            log_info(f"初始化高级表结构失败: {e}", level="ERROR")
    
    def process_nginx_logs_to_clickhouse(self, log_dir: str, target_date: date = None, mode: str = 'incremental'):
        """处理nginx日志到ClickHouse的完整流程"""
        log_info(f"开始nginx日志处理管道，模式: {mode}", show_memory=True)
        
        # 1. 收集日志文件
        log_files = self.nginx_processor.collect_log_files_by_date(log_dir, target_date)
        if not log_files:
            log_info("未找到日志文件", level="WARN")
            return {'status': 'no_files', 'processed': 0}
        
        log_info(f"发现 {len(log_files)} 个日志文件")
        
        # 2. 根据模式过滤文件
        if mode == 'incremental':
            log_files = self.increment_manager.get_unprocessed_files(log_files)
            if not log_files:
                log_info("所有文件都已处理，无需增量更新")
                return {'status': 'up_to_date', 'processed': 0}
        
        # 3. 处理日志文件
        total_processed = 0
        success_files = 0
        failed_files = 0
        
        for file_path, log_date in log_files:
            log_info(f"处理文件: {file_path} (日期: {log_date})")
            
            # 标记开始处理
            process_id = self.increment_manager.mark_file_processing(file_path, log_date)
            if not process_id:
                log_info(f"无法标记文件处理状态: {file_path}", level="ERROR")
                failed_files += 1
                continue
            
            try:
                # 解析nginx日志
                records, error = self.nginx_processor.process_log_file(file_path, log_date)
                
                if error:
                    self.increment_manager.mark_file_failed(file_path, log_date, error, process_id)
                    failed_files += 1
                    continue
                
                if not records:
                    log_info(f"文件无有效记录: {file_path}", level="WARN")
                    self.increment_manager.mark_file_completed(file_path, log_date, 0, process_id)
                    success_files += 1
                    continue
                
                # 导入ClickHouse
                success = self.import_records_to_clickhouse(records, file_path, log_date)
                
                if success:
                    self.increment_manager.mark_file_completed(file_path, log_date, len(records), process_id)
                    total_processed += len(records)
                    success_files += 1
                    log_info(f"文件处理成功: {file_path}, 记录数: {len(records)}")
                else:
                    self.increment_manager.mark_file_failed(file_path, log_date, "ClickHouse导入失败", process_id)
                    failed_files += 1
                
            except Exception as e:
                error_msg = f"处理异常: {e}"
                log_info(f"文件处理异常 {file_path}: {error_msg}", level="ERROR")
                self.increment_manager.mark_file_failed(file_path, log_date, error_msg, process_id)
                failed_files += 1
        
        # 4. 刷新物化视图和汇总表
        if total_processed > 0:
            self.refresh_aggregation_layers()
        
        log_info(f"管道处理完成 - 成功: {success_files}, 失败: {failed_files}, 总记录: {total_processed}")
        
        return {
            'status': 'completed',
            'processed': total_processed,
            'success_files': success_files,
            'failed_files': failed_files,
            'total_files': len(log_files)
        }
    
    def import_records_to_clickhouse(self, records: List[Dict], source_file: str, log_date: date) -> bool:
        """将记录导入ClickHouse"""
        try:
            if not self.ck_processor.client:
                if not self.ck_processor.connect():
                    raise Exception("无法连接到ClickHouse")
            
            # 准备ODS和DWD记录
            ods_records = []
            dwd_records = []
            
            for i, record in enumerate(records):
                record_id = int(datetime.now().timestamp() * 1000000) + i
                
                # 处理时间戳
                timestamp = self._parse_timestamp(record.get('timestamp'))
                
                # ODS记录
                ods_record = [
                    record_id,
                    timestamp,
                    self.ck_processor._safe_string(record.get('client_ip', ''), 45),
                    self.ck_processor._safe_string(record.get('request_method', ''), 10),
                    self.ck_processor._safe_string(record.get('request_full_uri', '')),
                    self.ck_processor._safe_string(record.get('request_protocol', ''), 20),
                    self.ck_processor._safe_string(record.get('response_status_code', ''), 10),
                    self.ck_processor._safe_float(record.get('response_body_size_kb', 0)),
                    self.ck_processor._safe_float(record.get('total_bytes_sent_kb', 0)),
                    self.ck_processor._safe_string(record.get('referer', '')),
                    self.ck_processor._safe_string(record.get('user_agent', '')),
                    self.ck_processor._safe_float(record.get('total_request_duration', 0)),
                    self.ck_processor._safe_float(record.get('upstream_response_time', 0)),
                    self.ck_processor._safe_float(record.get('upstream_connect_time', 0)),
                    self.ck_processor._safe_float(record.get('upstream_header_time', 0)),
                    self.ck_processor._safe_string(record.get('application_name', ''), 100),
                    self.ck_processor._safe_string(record.get('service_name', ''), 100),
                    os.path.basename(source_file),
                    datetime.now()
                ]
                ods_records.append(ods_record)
                
                # 数据富化为DWD
                enriched_data = self._enrich_record(record, timestamp)
                
                # DWD记录
                dwd_record = [
                    record_id + 1000000,  # DWD ID
                    record_id,  # ODS ID
                    timestamp,
                    enriched_data['date_partition'],
                    enriched_data['hour_partition'],
                    self.ck_processor._safe_string(record.get('client_ip', ''), 45),
                    self.ck_processor._safe_string(enriched_data.get('request_uri', ''), 500),
                    self.ck_processor._safe_string(record.get('request_method', ''), 10),
                    self.ck_processor._safe_string(record.get('response_status_code', ''), 10),
                    enriched_data['response_time'],
                    enriched_data['response_size_kb'],
                    self.ck_processor._safe_string(enriched_data.get('platform', '')),
                    self.ck_processor._safe_string(enriched_data.get('platform_version', '')),
                    self.ck_processor._safe_string(enriched_data.get('entry_source', '')),
                    self.ck_processor._safe_string(enriched_data.get('api_category', '')),
                    self.ck_processor._safe_string(record.get('application_name', ''), 100),
                    self.ck_processor._safe_string(record.get('service_name', ''), 100),
                    enriched_data['is_success'],
                    enriched_data['is_slow'],
                    enriched_data['data_quality_score'],
                    enriched_data['has_anomaly'],
                    self.ck_processor._safe_string(enriched_data.get('anomaly_type', '')),
                    datetime.now(),
                    datetime.now()
                ]
                dwd_records.append(dwd_record)
            
            # 批量插入ODS层
            if ods_records:
                self.ck_processor.client.insert('ods_nginx_log', ods_records,
                                               column_names=[
                                                   'id', 'timestamp', 'client_ip', 'request_method',
                                                   'request_full_uri', 'request_protocol', 'response_status_code',
                                                   'response_body_size_kb', 'total_bytes_sent_kb', 'referer',
                                                   'user_agent', 'total_request_duration', 'upstream_response_time',
                                                   'upstream_connect_time', 'upstream_header_time', 'application_name',
                                                   'service_name', 'source_file', 'created_at'
                                               ])
            
            # 批量插入DWD层
            if dwd_records:
                self.ck_processor.client.insert('dwd_nginx_enriched', dwd_records,
                                               column_names=[
                                                   'id', 'ods_id', 'timestamp', 'date_partition', 'hour_partition',
                                                   'client_ip', 'request_uri', 'request_method', 'response_status_code',
                                                   'response_time', 'response_size_kb', 'platform', 'platform_version',
                                                   'entry_source', 'api_category', 'application_name', 'service_name',
                                                   'is_success', 'is_slow', 'data_quality_score', 'has_anomaly',
                                                   'anomaly_type', 'created_at', 'updated_at'
                                               ])
            
            # 记录处理状态到ClickHouse
            self._record_processing_status(source_file, log_date, len(records), 'completed')
            
            log_info(f"成功导入 {len(records)} 条记录到ClickHouse")
            return True
            
        except Exception as e:
            log_info(f"导入ClickHouse失败: {e}", level="ERROR")
            self._record_processing_status(source_file, log_date, 0, 'failed', str(e))
            return False
    
    def _parse_timestamp(self, timestamp_str):
        """解析时间戳 - 保持原始时间不做时区转换"""
        if not timestamp_str:
            return datetime.now()
        
        try:
            if isinstance(timestamp_str, str):
                # 直接使用字符串格式的时间，已经在custom_log_parser中处理过时区
                return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            return timestamp_str
        except:
            return datetime.now()
    
    def _enrich_record(self, record: Dict, timestamp: datetime) -> Dict:
        """简化版数据富化"""
        # 使用现有的数据富化器
        if hasattr(self.ck_processor, 'enricher'):
            record_dict = {
                'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'client_ip': record.get('client_ip', ''),
                'request_full_uri': record.get('request_full_uri', ''),
                'response_status_code': record.get('response_status_code', ''),
                'total_request_duration': record.get('total_request_duration', 0),
                'response_body_size_kb': record.get('response_body_size_kb', 0),
                'user_agent': record.get('user_agent', ''),
                'referer': record.get('referer', ''),
                'application_name': record.get('application_name', ''),
                'service_name': record.get('service_name', ''),
                'request_method': record.get('request_method', '')
            }
            
            try:
                return self.ck_processor.enricher.enrich_record(record_dict)
            except:
                pass
        
        # 基础富化逻辑
        return {
            'date_partition': timestamp.strftime('%Y-%m-%d'),
            'hour_partition': timestamp.hour,
            'request_uri': record.get('request_full_uri', ''),
            'response_time': self.ck_processor._safe_float(record.get('total_request_duration', 0)),
            'response_size_kb': self.ck_processor._safe_float(record.get('response_body_size_kb', 0)),
            'platform': self._detect_platform(record.get('user_agent', '')),
            'platform_version': '',
            'entry_source': 'External',
            'api_category': self._classify_api(record.get('request_full_uri', '')),
            'is_success': record.get('response_status_code', '') in ['200', '201', '204'],
            'is_slow': self.ck_processor._safe_float(record.get('total_request_duration', 0)) > 3.0,
            'data_quality_score': 1.0,
            'has_anomaly': False,
            'anomaly_type': ''
        }
    
    def _detect_platform(self, user_agent: str) -> str:
        """简单的平台检测"""
        if not user_agent:
            return 'Unknown'
        
        user_agent = user_agent.lower()
        if 'android' in user_agent:
            return 'Android'
        elif 'iphone' in user_agent or 'ipad' in user_agent or 'ios' in user_agent:
            return 'iOS'
        elif 'windows' in user_agent:
            return 'Windows'
        elif 'mac' in user_agent:
            return 'macOS'
        elif 'linux' in user_agent:
            return 'Linux'
        else:
            return 'Other'
    
    def _classify_api(self, uri: str) -> str:
        """简单的API分类"""
        if not uri:
            return 'Unknown'
        
        uri = uri.lower()
        if '/api/auth' in uri or '/login' in uri or '/logout' in uri:
            return 'User_Auth'
        elif '/api/business' in uri or '/api/order' in uri or '/api/pay' in uri:
            return 'Business_Core'
        elif any(ext in uri for ext in ['.js', '.css', '.png', '.jpg', '.ico', '.woff']):
            return 'Static_Resource'
        elif '/api/' in uri:
            return 'API_Service'
        else:
            return 'Other'
    
    def _record_processing_status(self, file_path: str, log_date: date, records_count: int, status: str, error_msg: str = None):
        """记录处理状态到ClickHouse"""
        try:
            file_hash = self.nginx_processor.get_file_hash(file_path)
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            
            process_record = [
                f"{log_date.strftime('%Y%m%d')}_{os.path.basename(file_path)}_{datetime.now().strftime('%H%M%S')}",
                log_date,
                file_path,
                file_hash or '',
                file_size,
                records_count,
                datetime.now(),
                datetime.now(),
                status,
                error_msg or '',
                'v1.0.0',
                datetime.now(),
                datetime.now()
            ]
            
            self.ck_processor.client.insert('processing_status', [process_record],
                                           column_names=[
                                               'process_id', 'log_date', 'log_file_path', 'file_hash',
                                               'file_size', 'processed_records', 'processing_start_time',
                                               'processing_end_time', 'status', 'error_message',
                                               'processor_version', 'created_at', 'updated_at'
                                           ])
        except Exception as e:
            log_info(f"记录处理状态失败: {e}", level="WARN")
    
    def refresh_aggregation_layers(self):
        """刷新聚合层和物化视图"""
        try:
            log_info("开始刷新聚合层数据...")
            
            # 物化视图会自动更新，但我们可以手动刷新DWS层
            # 这里可以添加自定义的聚合逻辑
            
            log_info("聚合层数据刷新完成")
            
        except Exception as e:
            log_info(f"刷新聚合层失败: {e}", level="ERROR")
    
    def get_pipeline_status(self) -> Dict:
        """获取管道状态"""
        try:
            # 从ClickHouse获取统计信息
            stats = self.ck_processor.get_statistics()
            
            # 从增量管理器获取处理状态
            summary = self.increment_manager.get_processing_summary()
            
            return {
                'clickhouse_stats': stats,
                'processing_summary': summary,
                'pipeline_health': 'healthy' if stats.get('total_records', 0) > 0 else 'no_data',
                'last_update': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'error': str(e),
                'pipeline_health': 'error',
                'last_update': datetime.now().isoformat()
            }


def main():
    """命令行工具"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ClickHouse数据管道 v1.0.0')
    parser.add_argument('--log-dir', '-d', type=str, required=True, help='nginx日志根目录')
    parser.add_argument('--date', type=str, help='处理指定日期 (YYYY-MM-DD)')
    parser.add_argument('--mode', type=str, choices=['full', 'incremental'], default='incremental', help='处理模式')
    parser.add_argument('--status', action='store_true', help='显示管道状态')
    
    args = parser.parse_args()
    
    pipeline = ClickHousePipeline()
    
    if args.status:
        status = pipeline.get_pipeline_status()
        print(json.dumps(status, ensure_ascii=False, indent=2, default=str))
        return
    
    # 解析目标日期
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            log_info(f"日期格式错误: {args.date}", level="ERROR")
            return
    
    # 执行管道处理
    result = pipeline.process_nginx_logs_to_clickhouse(args.log_dir, target_date, args.mode)
    
    log_info(f"管道执行完成: {result}")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()