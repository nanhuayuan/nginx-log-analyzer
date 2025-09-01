#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整版nginx日志处理器 - 修复所有问题
1. 默认处理未处理过的日志
2. 完整的ODS→DWD→DWS→ADS数据流
3. 正确的记录追踪机制
"""

import os
import re
import json
import shutil
import hashlib
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import clickhouse_connect
import argparse

class NginxProcessorComplete:
    """完整版nginx日志处理器"""
    
    def __init__(self):
        self.clickhouse_config = {
            'host': 'localhost',
            'port': 8123,
            'username': 'analytics_user',
            'password': 'analytics_password',
            'database': 'nginx_analytics'
        }
        self.client = None
        self.log_base_dir = Path("D:/project/nginx-log-analyzer/nginx-analytics-warehouse/nginx_logs")
        self.processed_record_file = "processed_logs_complete.json"
        
        self.log_base_dir.mkdir(parents=True, exist_ok=True)
        self.processed_logs = self.load_processed_record()
        self.connect_clickhouse()
    
    def connect_clickhouse(self):
        """连接ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(**self.clickhouse_config)
            print("SUCCESS: ClickHouse连接成功")
            return True
        except Exception as e:
            print(f"ERROR: ClickHouse连接失败: {e}")
            return False
    
    def load_processed_record(self) -> Dict:
        """加载已处理记录"""
        if os.path.exists(self.processed_record_file):
            try:
                with open(self.processed_record_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"WARNING: 加载处理记录失败: {e}")
        return {}
    
    def save_processed_record(self):
        """保存已处理记录"""
        with open(self.processed_record_file, 'w', encoding='utf-8') as f:
            json.dump(self.processed_logs, f, indent=2, ensure_ascii=False)
    
    def get_file_hash(self, file_path: Path) -> str:
        """计算文件哈希值，用于检测文件变化"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def extract_value(self, text: str, key: str) -> Optional[str]:
        """从底座日志中提取特定键的值"""
        pattern = f'{key}:"([^"]*)"'
        match = re.search(pattern, text)
        return match.group(1) if match else None
    
    def process_time(self, time_str: str) -> str:
        """处理时间字符串格式"""
        return time_str.replace('+08:00', '').replace('T', ' ')
    
    def process_request(self, request_str: str) -> Tuple[str, str, str]:
        """处理请求字符串，分离方法、URI和协议"""
        parts = request_str.split()
        if len(parts) >= 3:
            return parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            return parts[0], parts[1], ''
        elif len(parts) == 1:
            return parts[0], '', ''
        return '', '', ''
    
    def classify_platform(self, user_agent: str) -> str:
        """分类平台类型"""
        if not user_agent:
            return 'Unknown'
        
        user_agent = user_agent.lower()
        
        # SDK类型
        if 'wst-sdk-ios' in user_agent or 'zgt-ios' in user_agent:
            return 'iOS_SDK'
        elif 'wst-sdk-android' in user_agent or 'zgt-android' in user_agent:
            return 'Android_SDK'
        elif 'wst-sdk' in user_agent:
            return 'SDK'
        
        # 移动平台
        elif 'iphone' in user_agent or 'ipad' in user_agent or 'ios' in user_agent:
            return 'iOS'
        elif 'android' in user_agent:
            return 'Android'
        
        # 浏览器
        elif any(x in user_agent for x in ['chrome', 'firefox', 'safari', 'edge']):
            return 'Web'
        
        return 'Other'
    
    def classify_api_category(self, uri: str) -> str:
        """分类API类型"""
        if not uri:
            return 'Unknown'
        
        if uri.startswith('/api/'):
            return 'API'
        elif uri.startswith('/scmp-gateway/'):
            return 'Gateway_API'
        elif uri.startswith('/static/') or uri.startswith('/css/') or uri.startswith('/js/'):
            return 'Static_Resource'
        elif uri.startswith('/group1/') or uri.startswith('/upload/'):
            return 'File_Download'
        elif uri == '/health' or uri == '/status':
            return 'Health_Check'
        elif uri.endswith(('.jpg', '.png', '.gif', '.ico', '.svg')):
            return 'Image'
        elif uri.endswith(('.css', '.js')):
            return 'Asset'
        else:
            return 'Page'
    
    def parse_base_log_line(self, line: str, source_file: str) -> Optional[Dict]:
        """解析底座格式日志行"""
        if not line.strip() or 'http_host' not in line:
            return None
        
        try:
            # 提取底座日志字段
            http_host = self.extract_value(line, 'http_host')
            remote_addr = self.extract_value(line, 'remote_addr')
            remote_port = self.extract_value(line, 'remote_port')
            time_value = self.extract_value(line, 'time')
            request_value = self.extract_value(line, 'request')
            code = self.extract_value(line, 'code')
            body = self.extract_value(line, 'body')
            http_referer = self.extract_value(line, 'http_referer')
            ar_time = self.extract_value(line, 'ar_time')
            real_ip = self.extract_value(line, 'RealIp')
            agent = self.extract_value(line, 'agent')
            
            # 处理时间
            log_datetime = None
            if time_value:
                raw_time = self.process_time(time_value)
                try:
                    log_datetime = datetime.strptime(raw_time, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    log_datetime = datetime.now()
            else:
                log_datetime = datetime.now()
            
            # 处理请求
            request_method = ''
            request_uri = ''
            request_protocol = ''
            if request_value:
                request_method, request_uri, request_protocol = self.process_request(request_value)
            
            # 数据转换和清理
            response_body_size = int(body) if body and body.isdigit() else 0
            total_request_time = float(ar_time) if ar_time else 0.0
            client_port = int(remote_port) if remote_port and remote_port.isdigit() else 0
            
            # 业务增强
            platform = self.classify_platform(agent or '')
            api_category = self.classify_api_category(request_uri)
            
            return {
                'log_time': log_datetime,
                'server_name': http_host or '',
                'client_ip': remote_addr or '',
                'client_port': client_port,
                'xff_ip': real_ip or '',
                'remote_user': '-',
                'request_method': request_method,
                'request_uri': request_uri,
                'request_full_uri': request_value or '',
                'http_protocol': request_protocol,
                'response_status_code': code or '200',
                'response_body_size': response_body_size,
                'response_referer': http_referer or '-',
                'user_agent': agent or '',
                'upstream_addr': '',
                'upstream_connect_time': 0.0,
                'upstream_header_time': 0.0,
                'upstream_response_time': 0.0,
                'total_request_time': total_request_time,
                'total_bytes_sent': response_body_size,
                'query_string': '',
                'connection_requests': 1,
                'trace_id': '',
                'business_sign': 'web_access',
                'application_name': 'nginx_app',
                'service_name': 'web_service',
                'cache_status': '',
                'cluster_node': 'node1',
                'log_source_file': source_file,
                'platform': platform,
                'api_category': api_category,
                'is_success': code in ['200', '201', '204'] if code else True,
                'is_slow': total_request_time >= 3.0,
                'is_error': not (code and code.startswith('2')) if code else False
            }
            
        except Exception as e:
            print(f"ERROR: 解析日志行失败: {e}")
            return None
    
    def insert_ods_data(self, records: List[Dict]) -> int:
        """插入ODS数据"""
        success_count = 0
        for record in records:
            try:
                insert_sql = f"""
                INSERT INTO ods_nginx_raw (
                    log_time, server_name, client_ip, client_port, xff_ip, remote_user,
                    request_method, request_uri, request_full_uri, http_protocol,
                    response_status_code, response_body_size, response_referer, user_agent,
                    upstream_addr, upstream_connect_time, upstream_header_time, upstream_response_time,
                    total_request_time, total_bytes_sent, query_string, connection_requests,
                    trace_id, business_sign, application_name, service_name, cache_status,
                    cluster_node, log_source_file
                ) VALUES (
                    '{record['log_time']}', '{record['server_name']}', '{record['client_ip']}', 
                    {record['client_port']}, '{record['xff_ip']}', '{record['remote_user']}',
                    '{record['request_method']}', '{record['request_uri']}', '{record['request_full_uri']}', 
                    '{record['http_protocol']}', '{record['response_status_code']}', {record['response_body_size']},
                    '{record['response_referer']}', '{record['user_agent']}', '{record['upstream_addr']}',
                    {record['upstream_connect_time']}, {record['upstream_header_time']}, {record['upstream_response_time']},
                    {record['total_request_time']}, {record['total_bytes_sent']}, '{record['query_string']}',
                    {record['connection_requests']}, '{record['trace_id']}', '{record['business_sign']}',
                    '{record['application_name']}', '{record['service_name']}', '{record['cache_status']}',
                    '{record['cluster_node']}', '{record['log_source_file']}'
                )
                """
                self.client.command(insert_sql)
                success_count += 1
            except Exception as e:
                print(f"ERROR: ODS插入失败: {e}")
        
        return success_count
    
    def insert_dwd_data(self, records: List[Dict]) -> int:
        """插入DWD数据"""
        success_count = 0
        for record in records:
            try:
                # 处理可能的SQL注入和特殊字符
                user_agent_clean = (record['user_agent'] or '').replace("'", "''")
                referer_url_clean = (record['response_referer'] or '').replace("'", "''")
                request_uri_clean = (record['request_uri'] or '').replace("'", "''")
                
                insert_sql = f"""
                INSERT INTO dwd_nginx_enriched (
                    log_time, date_partition, hour_partition, minute_partition, second_partition,
                    client_ip, client_port, xff_ip, server_name, request_method, request_uri, 
                    request_uri_normalized, request_full_uri, query_parameters, http_protocol_version,
                    response_status_code, response_body_size, response_body_size_kb, 
                    total_bytes_sent, total_bytes_sent_kb, total_request_duration,
                    upstream_connect_time, upstream_header_time, upstream_response_time,
                    platform, api_category, application_name, service_name, trace_id, business_sign,
                    cluster_node, upstream_server, connection_requests, cache_status,
                    referer_url, user_agent_string, log_source_file,
                    is_success, is_slow, is_error, has_anomaly, anomaly_type,
                    data_quality_score, client_region, client_isp, ip_risk_level, is_internal_ip
                ) VALUES (
                    '{record['log_time']}', '{record['log_time'].date()}', {record['log_time'].hour}, 
                    {record['log_time'].minute}, {record['log_time'].second}, '{record['client_ip']}', 
                    {record['client_port']}, '{record['xff_ip']}', '{record['server_name']}', 
                    '{record['request_method']}', '{request_uri_clean}', '{request_uri_clean}',
                    '{record['request_full_uri']}', '{record['query_string']}', '{record['http_protocol']}',
                    '{record['response_status_code']}', {record['response_body_size']}, {record['response_body_size'] / 1024.0},
                    {record['total_bytes_sent']}, {record['total_bytes_sent'] / 1024.0}, {record['total_request_time']},
                    {record['upstream_connect_time']}, {record['upstream_header_time']}, {record['upstream_response_time']},
                    '{record['platform']}', '{record['api_category']}', '{record['application_name']}', 
                    '{record['service_name']}', '{record['trace_id']}', '{record['business_sign']}',
                    '{record['cluster_node']}', '{record['upstream_addr']}', {record['connection_requests']},
                    '{record['cache_status']}', '{referer_url_clean}', '{user_agent_clean}', '{record['log_source_file']}',
                    {record['is_success']}, {record['is_slow']}, {record['is_error']}, false,
                    'None', 1.0, 'Unknown', 'Unknown', 'Low', 
                    {record['client_ip'].startswith(('192.168.', '10.', '172.')) if record['client_ip'] else False}
                )
                """
                self.client.command(insert_sql)
                success_count += 1
            except Exception as e:
                print(f"ERROR: DWD插入失败: {e}")
        
        return success_count
    
    def generate_dws_data(self) -> bool:
        """生成DWS聚合数据"""
        try:
            print("INFO: 生成DWS聚合数据...")
            
            # 清空现有DWS数据
            dws_tables = [
                'dws_api_performance_percentiles',
                'dws_client_behavior_analysis', 
                'dws_error_monitoring'
            ]
            
            for table in dws_tables:
                try:
                    self.client.command(f'TRUNCATE TABLE {table}')
                except:
                    pass  # 表可能不存在
            
            # 1. API性能百分位统计
            api_perf_sql = """
            INSERT INTO dws_api_performance_percentiles
            SELECT 
                toDate(log_time) as stat_date,
                toHour(log_time) as stat_hour,
                api_category,
                platform,
                count() as request_count,
                countIf(is_success) as success_count,
                round(avg(total_request_duration), 3) as avg_response_time,
                round(quantile(0.5)(total_request_duration), 3) as p50_response_time,
                round(quantile(0.95)(total_request_duration), 3) as p95_response_time,
                round(quantile(0.99)(total_request_duration), 3) as p99_response_time,
                sum(response_body_size) as total_bytes
            FROM dwd_nginx_enriched
            GROUP BY stat_date, stat_hour, api_category, platform
            """
            
            self.client.command(api_perf_sql)
            
            # 2. 客户端行为分析
            behavior_sql = """
            INSERT INTO dws_client_behavior_analysis
            SELECT 
                toDate(log_time) as stat_date,
                client_ip,
                platform,
                count() as request_count,
                uniq(request_uri) as unique_apis,
                countIf(is_error) as error_count,
                round(avg(total_request_duration), 3) as avg_response_time,
                max(total_request_duration) as max_response_time,
                sum(response_body_size) as total_bytes_consumed
            FROM dwd_nginx_enriched
            GROUP BY stat_date, client_ip, platform
            HAVING request_count >= 2  -- 过滤单次访问
            """
            
            self.client.command(behavior_sql)
            
            # 3. 错误监控
            error_sql = """
            INSERT INTO dws_error_monitoring
            SELECT 
                toDate(log_time) as stat_date,
                toHour(log_time) as stat_hour,
                response_status_code,
                api_category,
                count() as error_count,
                uniq(client_ip) as affected_users,
                groupArray(request_uri) as error_apis
            FROM dwd_nginx_enriched
            WHERE NOT is_success
            GROUP BY stat_date, stat_hour, response_status_code, api_category
            """
            
            self.client.command(error_sql)
            
            return True
            
        except Exception as e:
            print(f"ERROR: DWS数据生成失败: {e}")
            return False
    
    def generate_ads_data(self) -> bool:
        """生成ADS业务洞察数据"""
        try:
            print("INFO: 生成ADS业务洞察数据...")
            
            # 清空现有ADS数据
            ads_tables = [
                'ads_top_hot_apis',
                'ads_top_slow_apis',
                'ads_cache_hit_analysis',
                'ads_cluster_performance_comparison'
            ]
            
            for table in ads_tables:
                try:
                    self.client.command(f'TRUNCATE TABLE {table}')
                except:
                    pass
            
            # 1. 热门API排行
            hot_api_sql = """
            INSERT INTO ads_top_hot_apis
            SELECT 
                toDate(log_time) as stat_date,
                request_uri,
                platform,
                count() as request_count,
                countIf(is_success) as success_count,
                round(avg(total_request_duration), 3) as avg_response_time,
                sum(response_body_size) as total_bytes,
                uniq(client_ip) as unique_visitors
            FROM dwd_nginx_enriched
            GROUP BY stat_date, request_uri, platform
            ORDER BY request_count DESC
            """
            
            self.client.command(hot_api_sql)
            
            # 2. 慢API排行
            slow_api_sql = """
            INSERT INTO ads_top_slow_apis
            SELECT 
                toDate(log_time) as stat_date,
                request_uri,
                platform,
                count() as request_count,
                round(avg(total_request_duration), 3) as avg_response_time,
                round(quantile(0.95)(total_request_duration), 3) as p95_response_time,
                countIf(is_slow) as slow_request_count
            FROM dwd_nginx_enriched
            WHERE total_request_duration >= 1.0  -- 只统计较慢的请求
            GROUP BY stat_date, request_uri, platform
            HAVING avg_response_time >= 1.0
            ORDER BY avg_response_time DESC
            """
            
            self.client.command(slow_api_sql)
            
            # 3. 缓存命中分析（基于现有数据模拟）
            cache_sql = """
            INSERT INTO ads_cache_hit_analysis
            SELECT 
                toDate(log_time) as stat_date,
                api_category,
                count() as total_requests,
                -- 模拟缓存命中：响应时间快的可能是缓存命中
                countIf(total_request_duration < 0.1) as cache_hits,
                countIf(total_request_duration >= 0.1) as cache_misses,
                round(countIf(total_request_duration < 0.1) * 100.0 / count(), 2) as hit_rate
            FROM dwd_nginx_enriched
            WHERE api_category IN ('API', 'Gateway_API', 'Static_Resource')
            GROUP BY stat_date, api_category
            """
            
            self.client.command(cache_sql)
            
            # 4. 集群性能对比（基于节点模拟）
            cluster_sql = """
            INSERT INTO ads_cluster_performance_comparison
            SELECT 
                toDate(log_time) as stat_date,
                cluster_node,
                count() as request_count,
                round(avg(total_request_duration), 3) as avg_response_time,
                round(quantile(0.95)(total_request_duration), 3) as p95_response_time,
                countIf(is_success) as success_count,
                round(countIf(is_success) * 100.0 / count(), 2) as success_rate
            FROM dwd_nginx_enriched
            GROUP BY stat_date, cluster_node
            """
            
            self.client.command(cluster_sql)
            
            return True
            
        except Exception as e:
            print(f"ERROR: ADS数据生成失败: {e}")
            return False
    
    def process_single_log_file(self, log_file: Path) -> Dict:
        """处理单个日志文件"""
        print(f"PROCESS: 处理日志文件: {log_file.name}")
        
        # 检查文件是否已处理
        file_key = str(log_file)
        file_hash = self.get_file_hash(log_file)
        
        if file_key in self.processed_logs:
            stored_hash = self.processed_logs[file_key].get('file_hash')
            if stored_hash == file_hash:
                print(f"SKIP: {log_file.name} (已处理，文件未变化)")
                return {'success': True, 'skipped': True, 'ods_count': 0, 'dwd_count': 0}
        
        records = []
        stats = {'total_lines': 0, 'parsed_lines': 0, 'error_lines': 0}
        
        try:
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, 1):
                    stats['total_lines'] += 1
                    
                    parsed = self.parse_base_log_line(line.strip(), log_file.name)
                    if parsed:
                        records.append(parsed)
                        stats['parsed_lines'] += 1
                    else:
                        stats['error_lines'] += 1
                        if stats['error_lines'] <= 3:  # 只显示前3个错误
                            print(f"  WARNING: 第{line_num}行解析失败")
            
            print(f"  统计: 总行数{stats['total_lines']}, 解析成功{stats['parsed_lines']}, 解析失败{stats['error_lines']}")
            
            # 插入数据
            success_ods = self.insert_ods_data(records)
            success_dwd = self.insert_dwd_data(records)
            
            print(f"SUCCESS: ODS插入{success_ods}条, DWD插入{success_dwd}条")
            
            # 记录处理状态
            self.processed_logs[file_key] = {
                'processed_at': datetime.now().isoformat(),
                'file_hash': file_hash,
                'stats': stats,
                'ods_records': success_ods,
                'dwd_records': success_dwd
            }
            
            return {
                'success': True,
                'skipped': False,
                'ods_count': success_ods,
                'dwd_count': success_dwd,
                'stats': stats
            }
            
        except Exception as e:
            print(f"ERROR: 处理文件失败: {e}")
            return {'success': False, 'error': str(e)}
    
    def process_all_unprocessed_logs(self) -> Dict:
        """处理所有未处理的日志文件（默认模式）"""
        print("START: 处理所有未处理的nginx日志")
        print("=" * 60)
        
        # 扫描所有日志文件
        all_log_files = []
        for date_dir in self.log_base_dir.glob("*/"):
            if date_dir.is_dir():
                log_files = list(date_dir.glob("*.log"))
                all_log_files.extend(log_files)
        
        if not all_log_files:
            print("INFO: 未找到任何日志文件")
            return {'success': True, 'message': '未找到日志文件'}
        
        print(f"INFO: 总共发现 {len(all_log_files)} 个日志文件")
        
        # 处理文件
        total_ods = 0
        total_dwd = 0
        processed_files = 0
        skipped_files = 0
        start_time = datetime.now()
        
        for log_file in all_log_files:
            result = self.process_single_log_file(log_file)
            if result['success']:
                if result.get('skipped'):
                    skipped_files += 1
                else:
                    total_ods += result['ods_count']
                    total_dwd += result['dwd_count']
                    processed_files += 1
        
        # 生成DWS和ADS数据
        if processed_files > 0:
            print("\n=== 生成聚合数据 ===")
            dws_success = self.generate_dws_data()
            ads_success = self.generate_ads_data()
            
            if dws_success and ads_success:
                print("SUCCESS: DWS和ADS数据生成完成")
            else:
                print("WARNING: 聚合数据生成部分失败")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # 保存处理记录
        self.save_processed_record()
        
        print(f"\nSUMMARY: 处理完成结果:")
        print(f"   新处理文件: {processed_files}")
        print(f"   跳过文件: {skipped_files}")
        print(f"   ODS记录: {total_ods}")
        print(f"   DWD记录: {total_dwd}")
        print(f"   处理时间: {duration:.2f}秒")
        
        return {
            'success': True,
            'processed_files': processed_files,
            'skipped_files': skipped_files,
            'ods_count': total_ods,
            'dwd_count': total_dwd,
            'duration': duration
        }
    
    def process_specific_date(self, target_date: str, force_reprocess: bool = False) -> Dict:
        """处理指定日期的日志（特殊情况）"""
        print(f"START: 处理指定日期 {target_date} 的nginx日志")
        print("=" * 60)
        
        # 获取日志文件
        date_dir = self.log_base_dir / target_date
        if not date_dir.exists():
            error_msg = f"日志目录不存在: {date_dir}"
            print(f"ERROR: {error_msg}")
            return {'success': False, 'error': error_msg}
        
        log_files = list(date_dir.glob("*.log"))
        if not log_files:
            error_msg = f"在 {date_dir} 中未找到 .log 文件"
            print(f"ERROR: {error_msg}")
            return {'success': False, 'error': error_msg}
        
        print(f"INFO: 发现 {len(log_files)} 个日志文件在 {date_dir}")
        
        # 过滤需要处理的文件
        files_to_process = []
        for log_file in log_files:
            file_key = str(log_file)
            if force_reprocess:
                files_to_process.append(log_file)
            elif file_key not in self.processed_logs:
                files_to_process.append(log_file)
            else:
                file_hash = self.get_file_hash(log_file)
                stored_hash = self.processed_logs[file_key].get('file_hash')
                if stored_hash != file_hash:
                    files_to_process.append(log_file)
                else:
                    print(f"SKIP: {log_file.name} (已处理)")
        
        if not files_to_process:
            print("INFO: 所有文件都已处理")
            return {'success': True, 'message': '所有文件都已处理'}
        
        print(f"INFO: 需要处理 {len(files_to_process)} 个文件")
        
        # 处理文件
        total_ods = 0
        total_dwd = 0
        start_time = datetime.now()
        
        for log_file in files_to_process:
            result = self.process_single_log_file(log_file)
            if result['success'] and not result.get('skipped'):
                total_ods += result['ods_count']
                total_dwd += result['dwd_count']
        
        # 生成聚合数据
        if total_ods > 0 or total_dwd > 0:
            print("\n=== 生成聚合数据 ===")
            dws_success = self.generate_dws_data()
            ads_success = self.generate_ads_data()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # 保存处理记录
        self.save_processed_record()
        
        print(f"\nSUMMARY: 处理 {target_date} 完成结果:")
        print(f"   ODS记录: {total_ods}")
        print(f"   DWD记录: {total_dwd}")
        print(f"   处理时间: {duration:.2f}秒")
        
        return {
            'success': True,
            'ods_count': total_ods,
            'dwd_count': total_dwd,
            'duration': duration,
            'files_processed': len(files_to_process)
        }
    
    def clear_all_data(self):
        """清空所有数据"""
        try:
            tables = [
                'ods_nginx_raw', 'dwd_nginx_enriched',
                'dws_api_performance_percentiles', 'dws_client_behavior_analysis', 'dws_error_monitoring',
                'ads_top_hot_apis', 'ads_top_slow_apis', 'ads_cache_hit_analysis', 'ads_cluster_performance_comparison'
            ]
            for table in tables:
                try:
                    self.client.command(f'TRUNCATE TABLE {table}')
                    print(f"SUCCESS: 清空表 {table}")
                except:
                    print(f"WARNING: 表 {table} 可能不存在")
            
            # 清空处理记录
            self.processed_logs = {}
            self.save_processed_record()
            print("SUCCESS: 清空处理记录")
            
        except Exception as e:
            print(f"ERROR: 清空数据失败: {e}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='完整版nginx日志处理器')
    parser.add_argument('--date', help='处理特定日期 (YYYYMMDD)')
    parser.add_argument('--force', action='store_true', help='强制重新处理')
    parser.add_argument('--clear-all', action='store_true', help='清空所有数据')
    
    args = parser.parse_args()
    
    processor = NginxProcessorComplete()
    
    if args.clear_all:
        processor.clear_all_data()
    elif args.date:
        # 特殊情况：处理指定日期
        processor.process_specific_date(args.date, args.force)
    else:
        # 默认模式：处理所有未处理的日志
        processor.process_all_unprocessed_logs()

if __name__ == "__main__":
    main()