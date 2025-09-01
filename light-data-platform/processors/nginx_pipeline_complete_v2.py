# -*- coding: utf-8 -*-
"""
Self功能完整支持的Nginx日志处理管道 V2.0
支持65字段DWD表结构和所有Self分析器需求

设计理念：
1. 完整的字段计算和enrichment
2. 支持所有Self分析器需求
3. 高性能的阶段时间计算
4. 业务维度智能识别
"""

import os
import re
import json
import math
import hashlib
import ipaddress
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import clickhouse_connect
from pathlib import Path


class NginxLogCompleteProcessorV2:
    """完整的Nginx日志处理器V2 - 支持Self全功能"""
    
    def __init__(self, clickhouse_config: Dict[str, Any]):
        self.clickhouse_config = clickhouse_config
        self.client = None
        self.connect()
        
        # Self功能平台识别规则（来自self_00_01_constants.py）
        self.platform_patterns = {
            'iOS_SDK': [
                r'wst-sdk-ios', r'zgt-ios/', r'iOS.*SDK',
                r'CFNetwork.*Darwin', r'iOS/\d+\.\d+'
            ],
            'Android_SDK': [
                r'wst-sdk-android', r'zgt-android/', r'Android.*SDK',
                r'okhttp', r'Retrofit'
            ],
            'Java_SDK': [
                r'wst-sdk-java', r'Java.*SDK', r'Apache-HttpClient',
                r'Java/\d+\.\d+'
            ],
            'iOS': [
                r'iPhone', r'iPad', r'iPod', r'iOS \d+\.\d+',
                r'Mobile/\w+', r'Version/\d+\.\d+ Mobile.*Safari'
            ],
            'Android': [
                r'Android \d+\.\d+', r'Android;', r'Linux.*Android',
                r'Mobile.*Chrome.*Android', r'Version.*Mobile Safari.*Android'
            ],
            'macOS': [
                r'Macintosh.*Mac OS X', r'macOS', r'Darwin.*Safari',
                r'Version.*Safari.*Macintosh'
            ],
            'Windows': [
                r'Windows NT \d+\.\d+', r'Windows \d+', r'Win64',
                r'Trident', r'MSIE', r'Edge'
            ],
            'Web': [
                r'Chrome/\d+', r'Firefox/\d+', r'Safari/\d+', r'Edge/\d+',
                r'Opera/\d+'
            ],
            'Bot': [
                r'bot', r'spider', r'crawler', r'curl', r'wget',
                r'facebookexternalhit', r'Twitterbot', r'LinkedInBot'
            ]
        }
        
        # Self功能API分类规则
        self.api_categories = {
            'User_Auth': [
                r'/api/.*/auth', r'/api/.*/login', r'/api/.*/register',
                r'/api/.*/logout', r'/oauth', r'/sso'
            ],
            'Business_Core': [
                r'/api/.*/order', r'/api/.*/payment', r'/api/.*/business',
                r'/api/.*/transaction', r'/api/.*/core'
            ],
            'System_Config': [
                r'/api/.*/config', r'/api/.*/settings', r'/api/.*/admin',
                r'/api/.*/system', r'/api/.*/management'
            ],
            'Data_Query': [
                r'/api/.*/query', r'/api/.*/search', r'/api/.*/list',
                r'/api/.*/get', r'/api/.*/find'
            ],
            'File_Upload': [
                r'/api/.*/upload', r'/api/.*/file', r'/api/.*/attachment',
                r'/api/.*/media', r'/api/.*/document'
            ],
            'Static_Resource': [
                r'\.(css|js|png|jpg|jpeg|gif|ico|woff|woff2|ttf|svg|mp4|pdf)$',
                r'/static/', r'/assets/', r'/resources/'
            ],
            'Health_Check': [
                r'/health', r'/ping', r'/status', r'/heartbeat',
                r'/actuator', r'/metrics'
            ]
        }
        
        # 地理位置映射（简化版，实际应使用GeoIP数据库）
        self.region_mapping = {
            '北京': ['10.', '172.16.', '192.168.', '110.', '111.'],
            '上海': ['121.', '180.', '221.', '222.'],
            '广州': ['113.', '14.', '223.', '59.'],
            '深圳': ['119.', '183.', '224.', '58.'],
            '杭州': ['115.', '60.', '101.', '125.'],
            '成都': ['118.', '61.', '171.', '182.']
        }
        
        # ISP映射
        self.isp_mapping = {
            '电信': ['202.', '218.', '222.', '61.', '180.'],
            '联通': ['210.', '221.', '123.', '125.', '140.'],
            '移动': ['211.', '223.', '183.', '117.', '139.'],
            '教育网': ['202.112.', '166.111.', '219.'],
            '广电网': ['117.', '183.']
        }
    
    def connect(self):
        """连接ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(**self.clickhouse_config)
            self.client.command("SET session_timezone = 'Asia/Shanghai'")
            print("ClickHouse连接成功")
            return True
        except Exception as e:
            print(f"ClickHouse连接失败: {e}")
            return False
    
    def parse_nginx_log_line(self, line: str, source_file: str) -> Optional[Dict[str, Any]]:
        """
        解析单行nginx日志，支持JSON格式和自定义格式
        完整支持所有Self分析器需要的字段
        """
        try:
            # 1. 尝试JSON格式解析
            try:
                data = json.loads(line.strip())
                return self._process_json_format(data, source_file)
            except (json.JSONDecodeError, ValueError):
                pass
            
            # 2. 尝试自定义key-value格式解析
            if ':' in line and '=' not in line:
                return self._process_custom_format(line, source_file)
                
            # 3. 尝试Combined格式解析
            if '"' in line and ' - ' in line:
                return self._process_combined_format(line, source_file)
                
        except Exception as e:
            print(f"日志解析失败: {e}, 内容: {line[:100]}...")
            
        return None
    
    def _process_json_format(self, data: Dict[str, Any], source_file: str) -> Dict[str, Any]:
        """处理JSON格式日志"""
        try:
            # 提取基础时间信息
            time_str = data.get('time', data.get('timestamp', data.get('log_time', '')))
            log_time = self._parse_time(time_str)
            
            # 构建统一数据结构
            unified_data = {
                # 基础信息
                'log_time': log_time,
                'server_name': data.get('server_name', data.get('host', 'default')),
                'client_ip': data.get('remote_addr', data.get('client_ip', '')),
                'client_port': int(data.get('remote_port', 0)),
                'xff_ip': data.get('http_x_forwarded_for', ''),
                
                # 请求信息
                'request_method': data.get('request_method', data.get('method', 'GET')),
                'request_uri': data.get('request_uri', data.get('uri', '')),
                'request_full_uri': data.get('request', ''),
                'http_protocol': data.get('server_protocol', 'HTTP/1.1'),
                'query_string': data.get('query_string', ''),
                
                # 响应信息
                'response_status_code': str(data.get('status', data.get('response_status', 200))),
                'response_body_size': int(data.get('body_bytes_sent', data.get('response_size', 0))),
                'total_bytes_sent': int(data.get('bytes_sent', data.get('body_bytes_sent', 0))),
                
                # 时间信息（原始）
                'total_request_time': float(data.get('request_time', data.get('response_time', 0.0))),
                'upstream_connect_time': float(data.get('upstream_connect_time', 0.0)),
                'upstream_header_time': float(data.get('upstream_header_time', 0.0)),  
                'upstream_response_time': float(data.get('upstream_response_time', 0.0)),
                
                # 上游信息
                'upstream_addr': data.get('upstream_addr', ''),
                'upstream_status': str(data.get('upstream_status', '')),
                'cache_status': data.get('upstream_cache_status', ''),
                
                # 链路追踪
                'trace_id': data.get('trace_id', data.get('x_trace_id', '')),
                'business_sign': data.get('business_sign', ''),
                
                # 连接信息
                'connection_requests': int(data.get('connection_requests', 1)),
                
                # 客户端信息  
                'user_agent': data.get('http_user_agent', data.get('user_agent', '')),
                'referer': data.get('http_referer', data.get('referer', '')),
                
                # 应用信息
                'application_name': data.get('app_name', data.get('application', '')),
                'service_name': data.get('service_name', data.get('service', '')),
                'cluster_name': data.get('cluster', 'default'),
                
                # 元数据
                'source_file': source_file
            }
            
            return self._enrich_data(unified_data)
            
        except Exception as e:
            print(f"JSON格式处理失败: {e}")
            return None
    
    def _process_custom_format(self, line: str, source_file: str) -> Dict[str, Any]:
        """处理自定义key:value格式日志（来自样例数据）"""
        try:
            # 解析key:value格式
            data = {}
            
            # 使用正则表达式解析key:"value"格式
            pattern = r'(\w+):"([^"]*)"'
            matches = re.findall(pattern, line)
            
            for key, value in matches:
                data[key] = value
            
            # 解析没有引号的key:value格式  
            remaining = re.sub(pattern, '', line)
            simple_pattern = r'(\w+):([^\s]+)'
            simple_matches = re.findall(simple_pattern, remaining)
            
            for key, value in simple_matches:
                if key not in data:  # 避免覆盖已有数据
                    data[key] = value
            
            # 提取时间信息
            time_str = data.get('time', data.get('timestamp', ''))
            log_time = self._parse_time(time_str)
            
            # 构建统一数据结构
            unified_data = {
                # 基础信息
                'log_time': log_time,
                'server_name': data.get('http_host', 'default'),
                'client_ip': data.get('remote_addr', ''),
                'client_port': int(data.get('remote_port', 0)),
                'xff_ip': data.get('http_x_forwarded_for', ''),
                
                # 请求信息（解析完整请求字符串）
            }
            
            # 解析request字段（如"GET /path HTTP/1.1"）
            request_full = data.get('request', '')
            unified_data.update({
                'request_method': self._extract_method_from_request(request_full),
                'request_uri': self._extract_uri_from_request(request_full),
                'request_full_uri': request_full,
                'http_protocol': self._extract_protocol_from_request(request_full) or data.get('server_protocol', 'HTTP/1.1'),
                'query_string': data.get('args', ''),
                
                # 响应信息
                'response_status_code': data.get('code', '200'),
                'response_body_size': int(float(data.get('body', 0))),
                'total_bytes_sent': int(float(data.get('body', 0))),  # 使用body作为发送字节数
                
                # 时间信息（原始）
                'total_request_time': float(data.get('ar_time', 0.0)),
                'upstream_connect_time': float(data.get('upstream_connect_time', 0.0)),
                'upstream_header_time': float(data.get('upstream_header_time', 0.0)),
                'upstream_response_time': float(data.get('upstream_response_time', 0.0)),
                
                # 上游信息
                'upstream_addr': data.get('upstream_addr', ''),
                'upstream_status': data.get('upstream_status', ''),
                'cache_status': data.get('upstream_cache_status', ''),
                
                # 链路追踪
                'trace_id': data.get('trace_id', data.get('http_x_trace_id', '')),
                'business_sign': data.get('business_sign', ''),
                
                # 连接信息
                'connection_requests': int(data.get('connection_requests', 1)),
                
                # 客户端信息
                'user_agent': data.get('agent', ''),
                'referer': data.get('http_referer', ''),
                
                # 应用信息
                'application_name': '',
                'service_name': '',
                'cluster_name': 'default',
                
                # 元数据
                'source_file': source_file
            })
            
            return self._enrich_data(unified_data)
            
        except Exception as e:
            print(f"自定义格式处理失败: {e}")
            return None
    
    def _process_combined_format(self, line: str, source_file: str) -> Dict[str, Any]:
        """处理Combined格式日志"""
        # Combined格式正则表达式
        pattern = r'^([^\s]+)\s+-\s+([^\s]*)\s+\[([^\]]+)\]\s+"([^"]+)"\s+(\d+)\s+(\d+)\s+"([^"]*)"\s+"([^"]*)"'
        
        match = re.match(pattern, line)
        if not match:
            return None
        
        groups = match.groups()
        
        # 解析请求行
        request_parts = groups[3].split(' ', 2)
        method = request_parts[0] if len(request_parts) > 0 else 'GET'
        uri = request_parts[1] if len(request_parts) > 1 else '/'
        protocol = request_parts[2] if len(request_parts) > 2 else 'HTTP/1.1'
        
        unified_data = {
            'log_time': self._parse_time(groups[2]),
            'server_name': 'default',
            'client_ip': groups[0],
            'client_port': 0,
            'xff_ip': '',
            'request_method': method,
            'request_uri': uri,
            'request_full_uri': groups[3],
            'http_protocol': protocol,
            'query_string': '',
            'response_status_code': groups[4],
            'response_body_size': int(groups[5]),
            'total_bytes_sent': int(groups[5]),
            'total_request_time': 0.0,
            'upstream_connect_time': 0.0,
            'upstream_header_time': 0.0,
            'upstream_response_time': 0.0,
            'upstream_addr': '',
            'upstream_status': '',
            'cache_status': '',
            'trace_id': '',
            'business_sign': '',
            'connection_requests': 1,
            'user_agent': groups[7],
            'referer': groups[6],
            'application_name': '',
            'service_name': '',
            'cluster_name': 'default',
            'source_file': source_file
        }
        
        return self._enrich_data(unified_data)
    
    def _parse_time(self, time_str: str) -> datetime:
        """解析时间字符串（保持Asia/Shanghai时区）"""
        if not time_str:
            return datetime.now()
        
        try:
            # ISO格式: 2025-04-23T00:00:02+08:00
            if 'T' in time_str:
                # 移除时区信息，保持本地时间
                if '+' in time_str:
                    time_str = time_str.split('+')[0]
                elif 'Z' in time_str:
                    time_str = time_str.replace('Z', '')
                return datetime.fromisoformat(time_str.replace('T', ' '))
            
            # 标准格式: 2025-04-23 00:00:02
            elif ' ' in time_str and len(time_str) >= 19:
                return datetime.strptime(time_str[:19], '%Y-%m-%d %H:%M:%S')
            
            # Apache格式: 23/Apr/2025:00:00:02 +0800  
            elif '/' in time_str and ':' in time_str:
                clean_time = time_str.split(' +')[0]
                return datetime.strptime(clean_time, '%d/%b/%Y:%H:%M:%S')
                
        except Exception as e:
            print(f"时间解析失败: {e}, 时间字符串: {time_str}")
        
        return datetime.now()
    
    def _enrich_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        数据enrichment - 完整的65字段支持
        支持Self全部12个分析器需求
        """
        
        enriched = data.copy()
        
        # ============================================================
        # 1. 基础时间维度字段
        # ============================================================
        log_time = enriched['log_time']
        enriched['date_partition'] = log_time.date()
        enriched['hour_partition'] = log_time.hour
        enriched['minute_partition'] = log_time.minute
        enriched['second_partition'] = log_time.second
        
        # ============================================================
        # 2. URI处理和规范化（支持Self 01,02,03,05分析器）
        # ============================================================
        uri = enriched.get('request_uri', '')
        if uri:
            parsed_uri = urlparse(uri)
            enriched['request_uri_normalized'] = self._normalize_uri(parsed_uri.path)
            enriched['query_parameters'] = parsed_uri.query or ''
        else:
            enriched['request_uri_normalized'] = ''
            enriched['query_parameters'] = ''
        
        enriched['request_full_uri'] = enriched.get('request_full_uri', uri)
        enriched['http_protocol_version'] = enriched.get('http_protocol', 'HTTP/1.1')
        
        # ============================================================
        # 3. 响应大小处理（KB单位，支持Self分析器）
        # ============================================================
        body_size = enriched.get('response_body_size', 0)
        bytes_sent = enriched.get('total_bytes_sent', 0)
        
        enriched['response_body_size_kb'] = body_size / 1024.0
        enriched['total_bytes_sent_kb'] = bytes_sent / 1024.0
        
        # ============================================================
        # 4. ★★★ 核心阶段时间计算（Self 01,02,03分析器核心需求）★★★
        # ============================================================
        total_time = enriched.get('total_request_time', 0.0)
        upstream_connect = enriched.get('upstream_connect_time', 0.0) 
        upstream_header = enriched.get('upstream_header_time', 0.0)
        upstream_response = enriched.get('upstream_response_time', 0.0)
        
        # 修复负数时间（nginx有时会返回负数）
        upstream_connect = max(0, upstream_connect) if upstream_connect > 0 else 0
        upstream_header = max(0, upstream_header) if upstream_header > 0 else 0  
        upstream_response = max(0, upstream_response) if upstream_response > 0 else 0
        
        # Self核心阶段时间计算（参考self_02_service_analyzer.py逻辑）
        enriched['backend_connect_phase'] = upstream_connect
        enriched['backend_process_phase'] = max(0, upstream_header - upstream_connect)
        enriched['backend_transfer_phase'] = max(0, upstream_response - upstream_header)
        enriched['nginx_transfer_phase'] = max(0, total_time - upstream_response)
        
        # 组合阶段时间
        enriched['backend_total_phase'] = upstream_response
        enriched['network_phase'] = enriched['backend_connect_phase'] + enriched['nginx_transfer_phase']
        enriched['processing_phase'] = enriched['backend_process_phase']
        enriched['transfer_phase'] = enriched['backend_transfer_phase'] + enriched['nginx_transfer_phase']
        
        # 总时长统一
        enriched['total_request_duration'] = total_time
        
        # ============================================================
        # 5. ★★★ 性能效率指标计算（Self 01,02,03分析器核心需求）★★★
        # ============================================================
        if total_time > 0:
            # 传输速度计算(KB/s)
            if enriched['backend_transfer_phase'] > 0:
                enriched['response_transfer_speed'] = enriched['response_body_size_kb'] / enriched['backend_transfer_phase']
            else:
                enriched['response_transfer_speed'] = 0.0
                
            if enriched['nginx_transfer_phase'] > 0:
                enriched['nginx_transfer_speed'] = enriched['total_bytes_sent_kb'] / enriched['nginx_transfer_phase'] 
            else:
                enriched['nginx_transfer_speed'] = 0.0
                
            enriched['total_transfer_speed'] = enriched['total_bytes_sent_kb'] / total_time
            
            # 效率指标计算(%)
            enriched['backend_efficiency'] = (enriched['backend_process_phase'] / total_time) * 100
            enriched['network_overhead'] = (enriched['network_phase'] / total_time) * 100
            enriched['transfer_ratio'] = (enriched['transfer_phase'] / total_time) * 100
            enriched['connection_cost_ratio'] = (enriched['backend_connect_phase'] / total_time) * 100
            
            # 处理效率指数（Self核心指标）
            if enriched['backend_process_phase'] > 0:
                enriched['processing_efficiency_index'] = (enriched['response_body_size_kb'] / enriched['backend_process_phase']) * (100 - enriched['network_overhead']) / 100
            else:
                enriched['processing_efficiency_index'] = 0.0
        else:
            # 时间为0时的默认值
            for field in ['response_transfer_speed', 'nginx_transfer_speed', 'total_transfer_speed',
                         'backend_efficiency', 'network_overhead', 'transfer_ratio', 
                         'connection_cost_ratio', 'processing_efficiency_index']:
                enriched[field] = 0.0
        
        # ============================================================
        # 6. 平台和设备识别（支持Self 08,10,11分析器）
        # ============================================================  
        user_agent = enriched.get('user_agent', '')
        enriched['platform'] = self._classify_platform(user_agent)
        enriched['platform_version'] = self._extract_platform_version(user_agent)
        enriched['device_type'] = self._classify_device_type(user_agent)
        enriched['browser_type'] = self._classify_browser(user_agent)
        enriched['os_type'] = self._classify_os(user_agent)
        enriched['bot_type'] = self._classify_bot(user_agent)
        
        # ============================================================
        # 7. 来源和入口分析（支持Self 10,11分析器）
        # ============================================================
        referer = enriched.get('referer', '')
        enriched['entry_source'] = self._classify_entry_source(referer, user_agent)
        enriched['referer_domain'] = self._extract_referer_domain(referer)
        enriched['search_engine'] = self._classify_search_engine(referer)
        enriched['social_media'] = self._classify_social_media(referer)
        
        # ============================================================
        # 8. API分类（支持Self 01,02,03分析器）
        # ============================================================
        enriched['api_category'] = self._classify_api_category(uri)
        
        # ============================================================
        # 9. 地理和网络信息（支持Self 08分析器）
        # ============================================================
        client_ip = enriched.get('client_ip', '')
        enriched['client_region'] = self._classify_region(client_ip)
        enriched['client_isp'] = self._classify_isp(client_ip)  
        enriched['ip_risk_level'] = self._assess_ip_risk(client_ip, enriched)
        enriched['is_internal_ip'] = self._is_internal_ip(client_ip)
        
        # ============================================================
        # 10. 业务和链路信息
        # ============================================================
        enriched['trace_id'] = enriched.get('trace_id', '')
        enriched['business_sign'] = enriched.get('business_sign', '')
        enriched['cluster_node'] = enriched.get('cluster_name', 'default')
        enriched['upstream_server'] = enriched.get('upstream_addr', '')
        enriched['connection_requests'] = int(enriched.get('connection_requests', 1))
        enriched['cache_status'] = enriched.get('cache_status', '')
        
        # ============================================================
        # 11. 质量和状态标识（支持Self分析器判断）
        # ============================================================
        status_code = str(enriched.get('response_status_code', '200'))
        enriched['is_success'] = status_code.startswith('2')
        enriched['is_slow'] = total_time > 3.0  # 慢请求阈值3秒
        enriched['is_error'] = not enriched['is_success']
        
        # 异常检测
        enriched['has_anomaly'] = self._detect_anomaly(enriched)
        enriched['anomaly_type'] = self._classify_anomaly_type(enriched)
        
        # 数据质量评分
        enriched['data_quality_score'] = self._calculate_data_quality_score(enriched)
        
        # ============================================================
        # 12. 原始字段保留
        # ============================================================
        enriched['referer_url'] = referer
        enriched['user_agent_string'] = user_agent
        enriched['log_source_file'] = enriched.get('source_file', '')
        
        # ============================================================
        # 13. 应用信息处理
        # ============================================================
        enriched['application_name'] = enriched.get('application_name', '') or self._infer_application_name(uri)
        enriched['service_name'] = enriched.get('service_name', '') or self._infer_service_name(uri)
        
        return enriched
    
    def _normalize_uri(self, uri_path: str) -> str:
        """URI规范化，用于聚合分析"""
        if not uri_path:
            return ''
        
        # 移除查询参数
        path = uri_path.split('?')[0]
        
        # ID参数规范化
        path = re.sub(r'/\d+', '/{id}', path)
        path = re.sub(r'/[a-f0-9]{8,}', '/{uuid}', path)
        
        # 版本号规范化
        path = re.sub(r'/v\d+/', '/v{n}/', path)
        
        return path
    
    def _classify_platform(self, user_agent: str) -> str:
        """平台分类（Self核心功能）"""
        if not user_agent:
            return 'Unknown'
        
        user_agent_lower = user_agent.lower()
        
        # 按优先级匹配
        for platform, patterns in self.platform_patterns.items():
            for pattern in patterns:
                if re.search(pattern.lower(), user_agent_lower):
                    return platform
        
        return 'Other'
    
    def _extract_platform_version(self, user_agent: str) -> str:
        """提取平台版本"""
        if not user_agent:
            return ''
        
        # iOS版本
        ios_match = re.search(r'OS (\d+[_\.]\d+)', user_agent)
        if ios_match:
            return ios_match.group(1).replace('_', '.')
            
        # Android版本
        android_match = re.search(r'Android (\d+\.\d+)', user_agent)
        if android_match:
            return android_match.group(1)
            
        # Chrome版本
        chrome_match = re.search(r'Chrome/(\d+\.\d+)', user_agent)
        if chrome_match:
            return chrome_match.group(1)
            
        return ''
    
    def _classify_device_type(self, user_agent: str) -> str:
        """设备类型分类"""
        if not user_agent:
            return 'Unknown'
            
        user_agent_lower = user_agent.lower()
        
        if any(x in user_agent_lower for x in ['iphone', 'android', 'mobile']):
            return 'Mobile'
        elif any(x in user_agent_lower for x in ['ipad', 'tablet']):
            return 'Tablet'
        elif any(x in user_agent_lower for x in ['tv', 'smart-tv']):
            return 'TV'
        elif any(x in user_agent_lower for x in ['bot', 'crawler', 'spider']):
            return 'Bot'
        else:
            return 'Desktop'
    
    def _classify_browser(self, user_agent: str) -> str:
        """浏览器分类"""
        if not user_agent:
            return 'Unknown'
        
        user_agent_lower = user_agent.lower()
        
        if 'chrome' in user_agent_lower and 'edg' not in user_agent_lower:
            return 'Chrome'
        elif 'firefox' in user_agent_lower:
            return 'Firefox'
        elif 'safari' in user_agent_lower and 'chrome' not in user_agent_lower:
            return 'Safari'
        elif 'edg' in user_agent_lower:
            return 'Edge'
        elif 'opera' in user_agent_lower:
            return 'Opera'
        else:
            return 'Other'
    
    def _classify_os(self, user_agent: str) -> str:
        """操作系统分类"""
        if not user_agent:
            return 'Unknown'
        
        user_agent_lower = user_agent.lower()
        
        if 'windows' in user_agent_lower:
            return 'Windows'
        elif 'mac os' in user_agent_lower:
            return 'macOS'
        elif 'android' in user_agent_lower:
            return 'Android'  
        elif 'iphone' in user_agent_lower or 'ipad' in user_agent_lower:
            return 'iOS'
        elif 'linux' in user_agent_lower:
            return 'Linux'
        else:
            return 'Other'
    
    def _classify_bot(self, user_agent: str) -> str:
        """机器人类型分类"""
        if not user_agent:
            return ''
        
        user_agent_lower = user_agent.lower()
        
        bot_patterns = {
            'Search Engine': ['googlebot', 'bingbot', 'baiduspider', 'yandexbot'],
            'Social Media': ['facebookexternalhit', 'twitterbot', 'linkedinbot'],
            'Monitoring': ['pingdom', 'uptimerobot', 'nagios'],
            'Generic': ['bot', 'spider', 'crawler']
        }
        
        for bot_type, patterns in bot_patterns.items():
            if any(pattern in user_agent_lower for pattern in patterns):
                return bot_type
        
        return ''
    
    def _classify_entry_source(self, referer: str, user_agent: str) -> str:
        """入口来源分类"""
        if not referer or referer == '-':
            if self._classify_bot(user_agent):
                return 'Bot'
            return 'Direct'
        
        referer_lower = referer.lower()
        
        # 搜索引擎
        if any(engine in referer_lower for engine in ['google', 'baidu', 'bing', 'yahoo']):
            return 'Search_Engine'
        
        # 社交媒体
        if any(social in referer_lower for social in ['weibo', 'qq', 'wechat', 'douyin', 'facebook', 'twitter']):
            return 'Social_Media'
        
        # 内部链接
        if any(internal in referer_lower for internal in ['wechat', 'weixin']):
            return 'Internal'
        
        return 'External'
    
    def _extract_referer_domain(self, referer: str) -> str:
        """提取来源域名"""
        if not referer or referer == '-':
            return ''
        
        try:
            parsed = urlparse(referer)
            return parsed.netloc
        except:
            return ''
    
    def _classify_search_engine(self, referer: str) -> str:
        """搜索引擎分类"""
        if not referer:
            return ''
        
        referer_lower = referer.lower()
        
        if 'google' in referer_lower:
            return 'Google'
        elif 'baidu' in referer_lower:
            return 'Baidu'
        elif 'bing' in referer_lower:
            return 'Bing'
        elif 'yahoo' in referer_lower:
            return 'Yahoo'
        
        return ''
    
    def _classify_social_media(self, referer: str) -> str:
        """社交媒体分类"""
        if not referer:
            return ''
        
        referer_lower = referer.lower()
        
        social_patterns = {
            'WeChat': ['weixin', 'wechat'],
            'Weibo': ['weibo'],
            'QQ': ['qq.com', 'qzone'],
            'Douyin': ['douyin', 'tiktok'],
            'Facebook': ['facebook'],
            'Twitter': ['twitter', 't.co']
        }
        
        for social, patterns in social_patterns.items():
            if any(pattern in referer_lower for pattern in patterns):
                return social
        
        return ''
    
    def _classify_api_category(self, uri: str) -> str:
        """API分类（Self核心功能）"""
        if not uri:
            return 'Unknown'
        
        uri_lower = uri.lower()
        
        for category, patterns in self.api_categories.items():
            for pattern in patterns:
                if re.search(pattern.lower(), uri_lower):
                    return category
        
        return 'Other'
    
    def _classify_region(self, client_ip: str) -> str:
        """地理区域分类"""
        if not client_ip:
            return 'Unknown'
        
        for region, prefixes in self.region_mapping.items():
            if any(client_ip.startswith(prefix) for prefix in prefixes):
                return region
        
        return 'Other'
    
    def _classify_isp(self, client_ip: str) -> str:
        """ISP分类"""
        if not client_ip:
            return 'Unknown'
        
        for isp, prefixes in self.isp_mapping.items():
            if any(client_ip.startswith(prefix) for prefix in prefixes):
                return isp
        
        return 'Other'
    
    def _assess_ip_risk(self, client_ip: str, enriched_data: Dict[str, Any]) -> str:
        """IP风险评估"""
        if not client_ip:
            return 'Unknown'
        
        risk_score = 0
        
        # 机器人检测
        if enriched_data.get('bot_type', ''):
            risk_score += 30
        
        # 异常请求频率（暂时简单判断）
        if enriched_data.get('connection_requests', 1) > 100:
            risk_score += 40
        
        # 错误请求
        if enriched_data.get('is_error', False):
            risk_score += 20
        
        # 内网IP
        if self._is_internal_ip(client_ip):
            risk_score = max(0, risk_score - 20)
        
        if risk_score >= 70:
            return 'High'
        elif risk_score >= 40:
            return 'Medium'
        else:
            return 'Low'
    
    def _is_internal_ip(self, client_ip: str) -> bool:
        """判断是否为内网IP"""
        if not client_ip:
            return False
        
        try:
            ip_obj = ipaddress.ip_address(client_ip)
            return ip_obj.is_private
        except:
            return False
    
    def _detect_anomaly(self, enriched_data: Dict[str, Any]) -> bool:
        """异常检测"""
        # 简单异常检测规则
        if enriched_data.get('total_request_duration', 0) > 30:  # 超过30秒
            return True
        
        if enriched_data.get('response_body_size_kb', 0) > 10240:  # 超过10MB
            return True
        
        if enriched_data.get('is_error', False) and enriched_data.get('total_request_duration', 0) > 10:
            return True
        
        return False
    
    def _classify_anomaly_type(self, enriched_data: Dict[str, Any]) -> str:
        """异常类型分类"""
        if not enriched_data.get('has_anomaly', False):
            return ''
        
        if enriched_data.get('total_request_duration', 0) > 30:
            return 'timeout'
        
        if enriched_data.get('response_body_size_kb', 0) > 10240:
            return 'large_response'
        
        if enriched_data.get('is_error', False):
            return 'error_response'
        
        return 'unknown'
    
    def _calculate_data_quality_score(self, enriched_data: Dict[str, Any]) -> float:
        """数据质量评分"""
        score = 100.0
        
        # 必需字段缺失扣分
        required_fields = ['log_time', 'client_ip', 'request_uri', 'response_status_code']
        for field in required_fields:
            if not enriched_data.get(field):
                score -= 10
        
        # 时间字段异常扣分
        if enriched_data.get('total_request_duration', 0) < 0:
            score -= 20
        
        # 数据一致性检查
        if enriched_data.get('response_body_size', 0) < 0:
            score -= 10
        
        return max(0.0, score)
    
    def _infer_application_name(self, uri: str) -> str:
        """从URI推断应用名称"""
        if not uri:
            return ''
        
        # 提取路径第一段作为应用名
        path_parts = uri.strip('/').split('/')
        if path_parts and path_parts[0]:
            # 常见应用标识
            if path_parts[0] in ['api', 'v1', 'v2']:
                return path_parts[1] if len(path_parts) > 1 else ''
            else:
                return path_parts[0]
        
        return ''
    
    def _infer_service_name(self, uri: str) -> str:
        """从URI推断服务名称"""
        if not uri:
            return ''
        
        # 从路径提取服务标识
        path_parts = uri.strip('/').split('/')
        
        # 查找服务标识关键词
        service_keywords = ['gateway', 'service', 'api', 'rest']
        for part in path_parts:
            if any(keyword in part.lower() for keyword in service_keywords):
                return part
        
        return ''
    
    def insert_to_clickhouse(self, records: List[Dict[str, Any]]) -> bool:
        """插入数据到ClickHouse（支持65字段DWD表）"""
        if not records:
            return True
        
        try:
            # ODS表插入（原始数据）
            ods_records = []
            for record in records:
                ods_record = {
                    'id': hash(f"{record['log_time']}{record['client_ip']}{record['request_uri']}") & 0x7FFFFFFFFFFFFFFF,
                    'log_time': record['log_time'],
                    'server_name': record.get('server_name', ''),
                    'client_ip': record['client_ip'],
                    'client_port': record.get('client_port', 0),
                    'xff_ip': record.get('xff_ip', ''),
                    'remote_user': '',
                    'request_method': record['request_method'],
                    'request_uri': record['request_uri'],
                    'request_full_uri': record.get('request_full_uri', ''),
                    'http_protocol': record.get('http_protocol_version', ''),
                    'response_status_code': record['response_status_code'],
                    'response_body_size': record.get('response_body_size', 0),
                    'response_referer': record.get('referer_url', ''),
                    'user_agent': record.get('user_agent_string', ''),
                    'upstream_addr': record.get('upstream_server', ''),
                    'upstream_connect_time': record.get('upstream_connect_time', 0),
                    'upstream_header_time': record.get('upstream_header_time', 0),
                    'upstream_response_time': record.get('upstream_response_time', 0),
                    'total_request_time': record.get('total_request_duration', 0),
                    'total_bytes_sent': record.get('total_bytes_sent', 0),
                    'query_string': record.get('query_parameters', ''),
                    'connection_requests': record.get('connection_requests', 1),
                    'trace_id': record.get('trace_id', ''),
                    'business_sign': record.get('business_sign', ''),
                    'application_name': record.get('application_name', ''),
                    'service_name': record.get('service_name', ''),
                    'cache_status': record.get('cache_status', ''),
                    'cluster_node': record.get('cluster_node', ''),
                    'log_source_file': record.get('log_source_file', ''),
                    # 时间分区字段
                    'date_partition': record.get('date_partition', record['log_time'].date() if record.get('log_time') else None),
                    'hour_partition': record.get('hour_partition', record['log_time'].hour if record.get('log_time') else 0),
                    # 元数据字段
                    'created_at': datetime.now()
                }
                ods_records.append(ods_record)
            
            # DWD表插入（enriched数据，不包括MATERIALIZED和DEFAULT字段）
            dwd_records = []
            for record in records:
                dwd_record = {
                    # 主键和基础信息
                    'id': hash(f"{record['log_time']}{record['client_ip']}{record['request_uri']}") & 0x7FFFFFFFFFFFFFFF,
                    'ods_id': hash(f"{record['log_time']}{record['client_ip']}{record['request_uri']}") & 0x7FFFFFFFFFFFFFFF,
                    'log_time': record['log_time'],
                    'date_partition': record['date_partition'],
                    'hour_partition': record['hour_partition'],
                    'minute_partition': record['minute_partition'],
                    'second_partition': record['second_partition'],
                    
                    # 请求基础信息
                    'client_ip': record['client_ip'],
                    'client_port': record.get('client_port', 0),
                    'xff_ip': record.get('xff_ip', ''),
                    'server_name': record.get('server_name', ''),
                    'request_method': record['request_method'],
                    'request_uri': record['request_uri'],
                    'request_uri_normalized': record['request_uri_normalized'],
                    'request_full_uri': record.get('request_full_uri', ''),
                    'query_parameters': record['query_parameters'],
                    'http_protocol_version': record.get('http_protocol_version', ''),
                    
                    # 响应信息
                    'response_status_code': record['response_status_code'],
                    'response_body_size': record.get('response_body_size', 0),
                    'response_body_size_kb': record['response_body_size_kb'],
                    'total_bytes_sent': record.get('total_bytes_sent', 0),
                    'total_bytes_sent_kb': record['total_bytes_sent_kb'],
                    
                    # ★★★ 核心时间字段
                    'total_request_duration': record['total_request_duration'],
                    'upstream_connect_time': record.get('upstream_connect_time', 0),
                    'upstream_header_time': record.get('upstream_header_time', 0),
                    'upstream_response_time': record.get('upstream_response_time', 0),
                    
                    # ★★★ 阶段时间字段
                    'backend_connect_phase': record['backend_connect_phase'],
                    'backend_process_phase': record['backend_process_phase'], 
                    'backend_transfer_phase': record['backend_transfer_phase'],
                    'nginx_transfer_phase': record['nginx_transfer_phase'],
                    'backend_total_phase': record['backend_total_phase'],
                    'network_phase': record['network_phase'],
                    'processing_phase': record['processing_phase'],
                    'transfer_phase': record['transfer_phase'],
                    
                    # ★★★ 性能效率指标
                    'response_transfer_speed': record['response_transfer_speed'],
                    'total_transfer_speed': record['total_transfer_speed'],
                    'nginx_transfer_speed': record['nginx_transfer_speed'],
                    'backend_efficiency': record['backend_efficiency'],
                    'network_overhead': record['network_overhead'],
                    'transfer_ratio': record['transfer_ratio'],
                    'connection_cost_ratio': record['connection_cost_ratio'],
                    'processing_efficiency_index': record['processing_efficiency_index'],
                    
                    # 业务维度
                    'platform': record['platform'],
                    'platform_version': record['platform_version'],
                    'device_type': record['device_type'],
                    'browser_type': record['browser_type'],
                    'os_type': record['os_type'],
                    'bot_type': record['bot_type'],
                    'entry_source': record['entry_source'],
                    'referer_domain': record['referer_domain'],
                    'search_engine': record['search_engine'],
                    'social_media': record['social_media'],
                    'api_category': record['api_category'],
                    'application_name': record['application_name'],
                    'service_name': record['service_name'],
                    
                    # 链路和集群
                    'trace_id': record['trace_id'],
                    'business_sign': record['business_sign'],
                    'cluster_node': record['cluster_node'],
                    'upstream_server': record.get('upstream_server', ''),
                    'connection_requests': record['connection_requests'],
                    'cache_status': record.get('cache_status', ''),
                    
                    # 原始字段
                    'referer_url': record['referer_url'],
                    'user_agent_string': record['user_agent_string'],
                    'log_source_file': record['log_source_file'],
                    
                    # 状态标识
                    'is_success': record['is_success'],
                    'is_slow': record['is_slow'],
                    'is_error': record['is_error'],
                    'has_anomaly': record['has_anomaly'],
                    'anomaly_type': record['anomaly_type'],
                    'data_quality_score': record['data_quality_score'],
                    
                    # 地理网络
                    'client_region': record['client_region'],
                    'client_isp': record['client_isp'],
                    'ip_risk_level': record['ip_risk_level'],
                    'is_internal_ip': record['is_internal_ip']
                }
                
                dwd_records.append(dwd_record)
            
            # 批量插入ODS表（使用SQL方式，包含完整33字段）
            if ods_records:
                ods_values = []
                for record in ods_records:
                    # 构建完整的ODS插入
                    values = f"""({record['id']}, '{record['log_time']}', '{record['server_name']}', 
                    '{record['client_ip']}', {record['client_port']}, '{record.get('xff_ip', '')}', 
                    '{record.get('remote_user', '')}', '{record['request_method']}', '{record['request_uri']}', 
                    '{record.get('request_full_uri', '')}', '{record.get('http_protocol', '')}', 
                    '{record['response_status_code']}', {record.get('response_body_size', 0)}, 
                    '{record.get('response_referer', '')}', '{record.get('user_agent', '')}', 
                    '{record.get('upstream_addr', '')}', {record.get('upstream_connect_time', 0)}, 
                    {record.get('upstream_header_time', 0)}, {record.get('upstream_response_time', 0)}, 
                    {record.get('total_request_time', 0)}, {record.get('total_bytes_sent', 0)}, 
                    '{record.get('query_string', '')}', {record.get('connection_requests', 1)}, 
                    '{record.get('trace_id', '')}', '{record.get('business_sign', '')}', 
                    '{record.get('application_name', '')}', '{record.get('service_name', '')}', 
                    '{record.get('cache_status', '')}', '{record.get('cluster_node', '')}', 
                    '{record.get('log_source_file', '')}', '{record['date_partition']}', 
                    {record['hour_partition']}, '{record['created_at']}')"""
                    ods_values.append(values)
                
                if ods_values:
                    ods_sql = f"""INSERT INTO ods_nginx_raw (
                        id, log_time, server_name, client_ip, client_port, xff_ip, remote_user,
                        request_method, request_uri, request_full_uri, http_protocol, response_status_code,
                        response_body_size, response_referer, user_agent, upstream_addr, upstream_connect_time,
                        upstream_header_time, upstream_response_time, total_request_time, total_bytes_sent,
                        query_string, connection_requests, trace_id, business_sign, application_name,
                        service_name, cache_status, cluster_node, log_source_file, date_partition,
                        hour_partition, created_at
                    ) VALUES {', '.join(ods_values)}"""
                    self.client.command(ods_sql)
            
            # 批量插入DWD表（使用SQL方式，包含所有关键字段）
            if dwd_records:
                dwd_values = []
                for record in dwd_records:
                    # 构建完整的DWD插入，包含enrichment数据
                    values = f"""({record['id']}, {record['ods_id']}, '{record['log_time']}', '{record['date_partition']}', {record['hour_partition']}, {record['minute_partition']}, {record['second_partition']}, 
                    '{record['client_ip']}', {record.get('client_port', 0)}, '{record.get('xff_ip', '')}', 
                    '{record.get('server_name', '')}', '{record['request_method']}', '{record['request_uri']}', 
                    '{record.get('request_uri_normalized', '')}', '{record.get('request_full_uri', '')}', '{record.get('query_parameters', '')}', 
                    '{record.get('http_protocol_version', '')}', '{record['response_status_code']}', {record.get('response_body_size', 0)}, 
                    {record.get('response_body_size_kb', 0)}, {record.get('total_bytes_sent', 0)}, {record.get('total_bytes_sent_kb', 0)}, 
                    {record['total_request_duration']}, {record.get('upstream_connect_time', 0)}, {record.get('upstream_header_time', 0)}, 
                    {record.get('upstream_response_time', 0)}, {record['backend_connect_phase']}, {record['backend_process_phase']}, 
                    {record['backend_transfer_phase']}, {record['nginx_transfer_phase']}, {record['backend_total_phase']}, 
                    {record['network_phase']}, {record['processing_phase']}, {record['transfer_phase']}, 
                    {record['response_transfer_speed']}, {record['total_transfer_speed']}, {record['nginx_transfer_speed']}, 
                    {record['backend_efficiency']}, {record['network_overhead']}, {record['transfer_ratio']}, 
                    {record['connection_cost_ratio']}, {record['processing_efficiency_index']}, '{record['platform']}', 
                    '{record['platform_version']}', '{record['device_type']}', '{record['browser_type']}', '{record['os_type']}', 
                    '{record['bot_type']}', '{record['entry_source']}', '{record['referer_domain']}', '{record['search_engine']}', 
                    '{record['social_media']}', '{record['api_category']}', '{record['application_name']}', '{record['service_name']}', 
                    '{record['trace_id']}', '{record['business_sign']}', '{record['cluster_node']}', '{record.get('upstream_server', '')}', 
                    {record['connection_requests']}, '{record.get('cache_status', '')}', '{record['referer_url']}', 
                    '{record['user_agent_string']}', '{record['log_source_file']}', {record['is_success']}, 
                    {record['is_slow']}, {record['is_error']}, {record['has_anomaly']}, '{record['anomaly_type']}', 
                    {record['data_quality_score']}, '{record['client_region']}', '{record['client_isp']}', 
                    '{record['ip_risk_level']}', {record['is_internal_ip']})"""
                    dwd_values.append(values)
                
                if dwd_values:
                    # 构建完整的INSERT语句
                    dwd_sql = f"""INSERT INTO dwd_nginx_enriched (
                        id, ods_id, log_time, date_partition, hour_partition, minute_partition, second_partition,
                        client_ip, client_port, xff_ip, server_name, request_method, request_uri, request_uri_normalized,
                        request_full_uri, query_parameters, http_protocol_version, response_status_code, response_body_size,
                        response_body_size_kb, total_bytes_sent, total_bytes_sent_kb, total_request_duration, upstream_connect_time,
                        upstream_header_time, upstream_response_time, backend_connect_phase, backend_process_phase,
                        backend_transfer_phase, nginx_transfer_phase, backend_total_phase, network_phase, processing_phase,
                        transfer_phase, response_transfer_speed, total_transfer_speed, nginx_transfer_speed, backend_efficiency,
                        network_overhead, transfer_ratio, connection_cost_ratio, processing_efficiency_index, platform,
                        platform_version, device_type, browser_type, os_type, bot_type, entry_source, referer_domain,
                        search_engine, social_media, api_category, application_name, service_name, trace_id, business_sign,
                        cluster_node, upstream_server, connection_requests, cache_status, referer_url, user_agent_string,
                        log_source_file, is_success, is_slow, is_error, has_anomaly, anomaly_type, data_quality_score,
                        client_region, client_isp, ip_risk_level, is_internal_ip
                    ) VALUES {', '.join(dwd_values)}"""
                    self.client.command(dwd_sql)
            
            print(f"成功插入 {len(records)} 条记录到ClickHouse")
            return True
            
        except Exception as e:
            print(f"ClickHouse插入失败: {e}")
            return False
    
    def process_log_file(self, file_path: str, batch_size: int = 1000) -> Dict[str, Any]:
        """处理单个日志文件（支持65字段enrichment）"""
        print(f"开始处理日志文件: {file_path}")
        
        source_file = os.path.basename(file_path)
        total_lines = 0
        success_lines = 0
        error_lines = 0
        
        batch_records = []
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, 1):
                    total_lines += 1
                    
                    # 解析日志行
                    record = self.parse_nginx_log_line(line.strip(), source_file)
                    if record:
                        batch_records.append(record)
                        success_lines += 1
                        
                        # 批量处理
                        if len(batch_records) >= batch_size:
                            if self.insert_to_clickhouse(batch_records):
                                pass
                            else:
                                error_lines += len(batch_records)
                                success_lines -= len(batch_records)
                            batch_records = []
                    else:
                        error_lines += 1
                    
                    # 进度报告
                    if total_lines % 10000 == 0:
                        print(f"  处理进度: {total_lines} 行, 成功: {success_lines}, 失败: {error_lines}")
                
                # 处理最后一批
                if batch_records:
                    if self.insert_to_clickhouse(batch_records):
                        pass
                    else:
                        error_lines += len(batch_records)
                        success_lines -= len(batch_records)
            
        except Exception as e:
            print(f"文件处理失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'total_lines': total_lines,
                'success_lines': success_lines,
                'error_lines': error_lines
            }
        
        success_rate = (success_lines / total_lines * 100) if total_lines > 0 else 0
        
        print(f"文件处理完成: {source_file}")
        print(f"   总行数: {total_lines}, 成功: {success_lines}, 失败: {error_lines}")
        print(f"   成功率: {success_rate:.1f}%")
        
        return {
            'success': True,
            'total_lines': total_lines,
            'success_lines': success_lines,
            'error_lines': error_lines,
            'success_rate': success_rate
        }
    
    def _extract_method_from_request(self, request_full: str) -> str:
        """从完整请求字符串中提取HTTP方法"""
        if not request_full:
            return 'GET'
        
        parts = request_full.strip().split()
        return parts[0] if parts else 'GET'
    
    def _extract_uri_from_request(self, request_full: str) -> str:
        """从完整请求字符串中提取URI"""
        if not request_full:
            return ''
        
        parts = request_full.strip().split()
        return parts[1] if len(parts) > 1 else ''
    
    def _extract_protocol_from_request(self, request_full: str) -> str:
        """从完整请求字符串中提取HTTP协议版本"""
        if not request_full:
            return ''
        
        parts = request_full.strip().split()
        return parts[2] if len(parts) > 2 else ''