# -*- coding: utf-8 -*-
"""
全新Nginx日志处理管道
支持Self功能 + 核心监控指标 + 实时分析

设计理念：
1. 全量数据存储，保证信息完整性
2. 智能字段解析和enrichment  
3. 实时物化视图计算
4. 高效的分层存储架构
"""

import os
import re
import json
import hashlib
import ipaddress
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import clickhouse_connect
from pathlib import Path


class NginxLogCompleteProcessor:
    """完整的Nginx日志处理器"""
    
    def __init__(self, clickhouse_config: Dict[str, Any]):
        self.clickhouse_config = clickhouse_config
        self.client = None
        self.connect()
        
        # 业务规则配置
        self.platform_patterns = {
            'iOS_SDK': [r'wst-sdk-ios', r'zgt-ios/', r'iOS.*SDK'],
            'Android_SDK': [r'wst-sdk-android', r'zgt-android/', r'Android.*SDK'],  
            'Java_SDK': [r'wst-sdk-java', r'Java.*SDK'],
            'iOS': [r'iPhone', r'iPad', r'iPod', r'iOS'],
            'Android': [r'Android'],
            'Web': [r'Chrome', r'Firefox', r'Safari', r'Edge'],
            'Bot': [r'bot', r'spider', r'crawler', r'curl', r'wget']
        }
        
        self.api_categories = {
            'User_Auth': [r'/api/.*/auth', r'/api/.*/login', r'/api/.*/register'],
            'Business_Core': [r'/api/.*/order', r'/api/.*/payment', r'/api/.*/business'],
            'System_Config': [r'/api/.*/config', r'/api/.*/settings'],
            'Data_Query': [r'/api/.*/query', r'/api/.*/search', r'/api/.*/list'],
            'File_Upload': [r'/api/.*/upload', r'/api/.*/file'],
            'Static_Resource': [r'\.(css|js|png|jpg|gif|ico|woff|ttf)$']
        }
        
        # 地理位置映射（简化版）
        self.region_mapping = {
            '北京': ['10.', '172.16.', '192.168.'],
            '上海': ['221.', '222.'],
            '广州': ['113.', '223.'],  
            '深圳': ['119.', '224.']
        }
    
    def connect(self):
        """连接ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(**self.clickhouse_config)
            # 设置时区
            self.client.command("SET session_timezone = 'Asia/Shanghai'")
            print("ClickHouse连接成功")
            return True
        except Exception as e:
            print(f"ClickHouse连接失败: {e}")
            return False
    
    def parse_nginx_log_line(self, line: str, source_file: str) -> Optional[Dict[str, Any]]:
        """
        解析单行nginx日志
        支持JSON格式和自定义格式
        """
        try:
            # 1. 尝试JSON格式解析
            if line.strip().startswith('{'):
                return self._parse_json_log(line, source_file)
            
            # 2. 尝试自定义key:value格式解析
            elif 'http_host:' in line:
                return self._parse_custom_log(line, source_file)
            
            # 3. 尝试标准nginx combined格式
            else:
                return self._parse_combined_log(line, source_file)
                
        except Exception as e:
            print(f"日志解析失败: {e}, 内容: {line[:100]}...")
            return None
    
    def _parse_json_log(self, line: str, source_file: str) -> Dict[str, Any]:
        """解析JSON格式日志"""
        data = json.loads(line)
        
        # 统一字段名映射
        unified_data = {
            'log_time': self._parse_time(data.get('@timestamp', data.get('timestamp', ''))),
            'server_name': data.get('server_name', data.get('host', '')),
            'client_ip': data.get('remote_addr', data.get('client_ip', '')),
            'client_port': int(data.get('remote_port', 0)),
            'xff_ip': data.get('http_x_forwarded_for', data.get('x_forwarded_for', '')),
            'request_method': data.get('request_method', ''),
            'request_uri': data.get('request_uri', data.get('uri', '')),
            'http_protocol': data.get('server_protocol', 'HTTP/1.1'),
            'http_host': data.get('http_host', data.get('host', '')),
            'response_status_code': int(data.get('status', data.get('response_status', 0))),
            'response_body_size': int(data.get('body_bytes_sent', data.get('response_size', 0))),
            'request_time': float(data.get('request_time', data.get('response_time', 0))),
            'upstream_response_time': float(data.get('upstream_response_time', 0)),
            'upstream_connect_time': float(data.get('upstream_connect_time', 0)),
            'upstream_header_time': float(data.get('upstream_header_time', 0)),
            'upstream_status_code': int(data.get('upstream_status', 0)),
            'upstream_cache_status': data.get('upstream_cache_status', ''),
            'upstream_addr': data.get('upstream_addr', ''),
            'user_agent': data.get('http_user_agent', data.get('user_agent', '')),
            'referer': data.get('http_referer', data.get('referer', '')),
            'connection_requests': int(data.get('connection_requests', 1)),
            'request_length': int(data.get('request_length', 0)),
            'bytes_sent': int(data.get('bytes_sent', data.get('response_size', 0))),
            'source_file': source_file
        }
        
        # 添加业务字段
        unified_data.update({
            'trace_id': data.get('trace_id', data.get('x_trace_id', '')),
            'business_sign': data.get('business_sign', ''),
            'cluster_name': data.get('cluster_name', 'default')
        })
        
        return self._enrich_data(unified_data)
    
    def _parse_custom_log(self, line: str, source_file: str) -> Dict[str, Any]:
        """解析自定义key:value格式日志"""
        def extract_value(text: str, key: str) -> str:
            pattern = f'{key}:"([^"]*)"'
            match = re.search(pattern, text)
            return match.group(1) if match else ''
        
        # 提取基础字段
        raw_time = extract_value(line, 'time')
        if '+08:00' in raw_time:
            raw_time = raw_time.replace('+08:00', '')
        
        request_str = extract_value(line, 'request')
        method, uri, protocol = '', '', 'HTTP/1.1'
        if request_str:
            parts = request_str.split()
            if len(parts) >= 3:
                method, uri, protocol = parts[0], parts[1], parts[2]
            elif len(parts) >= 2:
                method, uri = parts[0], parts[1]
        
        unified_data = {
            'log_time': self._parse_time(raw_time),
            'server_name': extract_value(line, 'http_host'),
            'client_ip': extract_value(line, 'remote_addr'),
            'client_port': int(extract_value(line, 'remote_port') or 0),
            'xff_ip': extract_value(line, 'RealIp'),
            'request_method': method,
            'request_uri': uri,
            'http_protocol': protocol,
            'http_host': extract_value(line, 'http_host'),
            'response_status_code': int(extract_value(line, 'code') or 0),
            'response_body_size': int(extract_value(line, 'body') or 0),
            'request_time': float(extract_value(line, 'ar_time') or 0),
            'upstream_response_time': float(extract_value(line, 'ar_time') or 0),  # 底座日志用ar_time
            'upstream_connect_time': 0.0,
            'upstream_header_time': 0.0,
            'upstream_status_code': int(extract_value(line, 'code') or 0),
            'upstream_cache_status': '',
            'upstream_addr': '',
            'user_agent': extract_value(line, 'agent'),
            'referer': extract_value(line, 'http_referer'),
            'connection_requests': 1,
            'request_length': 0,
            'bytes_sent': int(extract_value(line, 'body') or 0),
            'trace_id': '',
            'business_sign': '',
            'cluster_name': 'default',
            'source_file': source_file
        }
        
        return self._enrich_data(unified_data)
    
    def _parse_combined_log(self, line: str, source_file: str) -> Dict[str, Any]:
        """解析标准nginx combined格式日志"""
        # 标准combined格式正则表达式
        pattern = r'(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d+) (\d+) "([^"]*)" "([^"]*)"'
        match = re.match(pattern, line)
        
        if not match:
            return None
            
        groups = match.groups()
        
        unified_data = {
            'log_time': self._parse_time(groups[1]),
            'server_name': 'default',
            'client_ip': groups[0],
            'client_port': 0,
            'xff_ip': '',
            'request_method': groups[2],
            'request_uri': groups[3],
            'http_protocol': groups[4],
            'http_host': 'default',
            'response_status_code': int(groups[5]),
            'response_body_size': int(groups[6]),
            'request_time': 0.0,  # combined格式没有时间信息
            'upstream_response_time': 0.0,
            'upstream_connect_time': 0.0,
            'upstream_header_time': 0.0,
            'upstream_status_code': 0,
            'upstream_cache_status': '',
            'upstream_addr': '',
            'user_agent': groups[8],
            'referer': groups[7],
            'connection_requests': 1,
            'request_length': 0,
            'bytes_sent': int(groups[6]),
            'trace_id': '',
            'business_sign': '',
            'cluster_name': 'default',
            'source_file': source_file
        }
        
        return self._enrich_data(unified_data)
    
    def _parse_time(self, time_str: str) -> datetime:
        """解析时间字符串"""
        if not time_str:
            return datetime.now()
            
        try:
            # ISO格式: 2025-04-23T00:00:02
            if 'T' in time_str:
                return datetime.fromisoformat(time_str.replace('T', ' '))
            
            # 标准格式: 2025-04-23 00:00:02  
            elif ' ' in time_str and len(time_str) >= 19:
                return datetime.strptime(time_str[:19], '%Y-%m-%d %H:%M:%S')
                
            # Apache格式: 23/Apr/2025:00:00:02 +0800
            elif '/' in time_str and ':' in time_str:
                clean_time = time_str.split(' +')[0]  # 移除时区
                return datetime.strptime(clean_time, '%d/%b/%Y:%H:%M:%S')
                
        except Exception as e:
            print(f"时间解析失败: {e}, 时间字符串: {time_str}")
            
        return datetime.now()
    
    def _enrich_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """数据enrichment - 添加业务维度和计算字段"""
        
        # 1. URI解析和参数提取
        uri = data['request_uri']
        if uri:
            parsed_uri = urlparse(uri)
            data['request_uri_path'] = parsed_uri.path
            data['request_query_string'] = parsed_uri.query
            
            # 参数统计
            if parsed_uri.query:
                query_params = parse_qs(parsed_uri.query)
                data['query_param_count'] = len(query_params)
                # 检测敏感参数
                sensitive_params = {'password', 'token', 'key', 'secret', 'pwd'}
                data['has_sensitive_params'] = any(param.lower() in sensitive_params 
                                                 for param in query_params.keys())
            else:
                data['query_param_count'] = 0
                data['has_sensitive_params'] = False
        else:
            data['request_uri_path'] = ''
            data['request_query_string'] = ''
            data['query_param_count'] = 0
            data['has_sensitive_params'] = False
        
        # 2. 平台识别
        user_agent = data.get('user_agent', '')
        data['platform'] = self._classify_platform(user_agent)
        
        # 3. User-Agent解析（简化版）
        if user_agent:
            user_agent_lower = user_agent.lower()
            
            # 简单的浏览器识别
            if 'chrome' in user_agent_lower:
                data['browser_name'] = 'Chrome'
            elif 'firefox' in user_agent_lower:
                data['browser_name'] = 'Firefox'  
            elif 'safari' in user_agent_lower:
                data['browser_name'] = 'Safari'
            elif 'edge' in user_agent_lower:
                data['browser_name'] = 'Edge'
            else:
                data['browser_name'] = 'Other'
            
            data['browser_version'] = ''  # 简化版不解析版本
            
            # 简单的OS识别
            if 'windows' in user_agent_lower:
                data['os_name'] = 'Windows'
            elif 'mac os' in user_agent_lower or 'macos' in user_agent_lower:
                data['os_name'] = 'macOS'
            elif 'linux' in user_agent_lower:
                data['os_name'] = 'Linux'
            elif 'android' in user_agent_lower:
                data['os_name'] = 'Android'
            elif 'ios' in user_agent_lower or 'iphone' in user_agent_lower:
                data['os_name'] = 'iOS'
            else:
                data['os_name'] = 'Other'
                
            data['os_version'] = ''  # 简化版不解析版本
            
            # 设备类型识别
            if any(keyword in user_agent_lower for keyword in ['mobile', 'phone']):
                data['device_type'] = 'mobile'
            elif 'tablet' in user_agent_lower or 'ipad' in user_agent_lower:
                data['device_type'] = 'tablet'
            else:
                data['device_type'] = 'desktop'
            
            # Bot识别
            bot_keywords = ['bot', 'spider', 'crawler', 'curl', 'wget', 'python']
            data['is_bot'] = any(keyword in user_agent_lower for keyword in bot_keywords)
            data['bot_name'] = 'Bot' if data['is_bot'] else ''
            
        else:
            data.update({
                'browser_name': 'Unknown', 'browser_version': '',
                'os_name': 'Unknown', 'os_version': '',
                'device_type': 'desktop', 'is_bot': False, 'bot_name': ''
            })
        
        # 4. API分类
        data['api_category'] = self._classify_api(data['request_uri_path'])
        
        # 5. 地理位置和ISP识别（简化版）
        client_ip = data.get('client_ip', '')
        data['client_region'] = self._get_client_region(client_ip)
        data['client_isp'] = 'Unknown'  # 需要IP库支持
        
        # 6. URI标准化和模式识别
        data['request_uri_normalized'] = self._normalize_uri(data['request_uri_path'])
        data['request_uri_pattern'] = self._extract_uri_pattern(data['request_uri_path'])
        
        # 7. 业务维度
        data['business_category'] = self._classify_business(data['request_uri_path'])
        data['entry_source'] = self._classify_entry_source(data.get('referer', ''))
        
        # 8. 连接和会话
        data['connection_reused'] = data.get('connection_requests', 1) > 1
        data['connection_id'] = hash(f"{client_ip}_{data.get('client_port', 0)}") & 0xFFFFFFFF
        
        # 9. 生成会话和用户ID（脱敏）
        data['session_id'] = hashlib.md5(f"{client_ip}_{user_agent}_{data['log_time'].date()}".encode()).hexdigest()[:16]
        data['user_id'] = hashlib.md5(f"{client_ip}_{user_agent}".encode()).hexdigest()[:12]
        
        # 10. 性能分类
        request_time = data.get('request_time', 0)
        data['response_size_kb'] = data.get('response_body_size', 0) / 1024.0
        
        # 11. 上游服务信息
        data['upstream_cluster'] = 'default'  # 可以从upstream_addr解析
        
        # 12. 平台版本
        data['platform_version'] = self._extract_platform_version(user_agent, data['platform'])
        
        return data
    
    def _classify_platform(self, user_agent: str) -> str:
        """平台分类"""
        if not user_agent:
            return 'Unknown'
            
        user_agent_lower = user_agent.lower()
        
        for platform, patterns in self.platform_patterns.items():
            for pattern in patterns:
                if re.search(pattern.lower(), user_agent_lower):
                    return platform
        
        return 'Other'
    
    def _classify_api(self, uri_path: str) -> str:
        """API分类"""
        if not uri_path:
            return 'Other'
            
        for category, patterns in self.api_categories.items():
            for pattern in patterns:
                if re.search(pattern, uri_path, re.IGNORECASE):
                    return category
        
        return 'Other'
    
    def _get_client_region(self, client_ip: str) -> str:
        """获取客户端地理位置（简化版）"""
        if not client_ip:
            return 'Unknown'
            
        # 内网IP
        try:
            ip_obj = ipaddress.ip_address(client_ip)
            if ip_obj.is_private:
                return 'Internal'
        except:
            pass
        
        # 简单的地理位置映射
        for region, prefixes in self.region_mapping.items():
            if any(client_ip.startswith(prefix) for prefix in prefixes):
                return region
        
        return 'External'
    
    def _normalize_uri(self, uri_path: str) -> str:
        """URI标准化"""
        if not uri_path:
            return ''
        
        # 移除末尾斜杠
        normalized = uri_path.rstrip('/')
        
        # 转小写
        normalized = normalized.lower()
        
        return normalized or '/'
    
    def _extract_uri_pattern(self, uri_path: str) -> str:
        """提取URI模式"""
        if not uri_path:
            return ''
        
        # 替换数字ID为占位符
        pattern = re.sub(r'/\d+', '/{id}', uri_path)
        
        # 替换UUID为占位符
        pattern = re.sub(r'/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '/{uuid}', pattern, re.IGNORECASE)
        
        # 替换哈希值为占位符
        pattern = re.sub(r'/[a-f0-9]{32,}', '/{hash}', pattern, re.IGNORECASE)
        
        return pattern
    
    def _classify_business(self, uri_path: str) -> str:
        """业务分类"""
        business_patterns = {
            'User_Management': [r'/user', r'/profile', r'/account'],
            'Order_Management': [r'/order', r'/purchase', r'/buy'],
            'Payment': [r'/pay', r'/payment', r'/billing'],
            'Content': [r'/content', r'/article', r'/news'],
            'System': [r'/system', r'/admin', r'/config'],
        }
        
        if not uri_path:
            return 'Other'
        
        for category, patterns in business_patterns.items():
            for pattern in patterns:
                if re.search(pattern, uri_path, re.IGNORECASE):
                    return category
        
        return 'Other'
    
    def _classify_entry_source(self, referer: str) -> str:
        """入口来源分类"""
        if not referer or referer == '-':
            return 'Direct'
        
        referer_lower = referer.lower()
        
        if any(search_engine in referer_lower for search_engine in ['google', 'baidu', 'bing']):
            return 'Search_Engine'
        elif any(social in referer_lower for social in ['weibo', 'qq', 'wechat', 'douyin']):
            return 'Social_Media'
        elif referer_lower.startswith('http'):
            return 'External'
        else:
            return 'Unknown'
    
    def _extract_platform_version(self, user_agent: str, platform: str) -> str:
        """提取平台版本"""
        if not user_agent:
            return ''
        
        # iOS版本
        if platform == 'iOS':
            match = re.search(r'OS (\d+_\d+)', user_agent)
            if match:
                return match.group(1).replace('_', '.')
        
        # Android版本
        elif platform == 'Android':
            match = re.search(r'Android (\d+\.?\d*)', user_agent)
            if match:
                return match.group(1)
        
        return ''
    
    def insert_to_clickhouse(self, records: List[Dict[str, Any]]) -> bool:
        """插入数据到ClickHouse"""
        if not records:
            return True
        
        try:
            # 转换为ODS格式
            ods_data = []
            dwd_data = []
            
            for record in records:
                # 生成ID
                record_id = int(f"{int(record['log_time'].timestamp())}{hash(record['request_uri']) & 0xFFFF}")
                
                # ODS记录
                ods_record = [
                    record_id,
                    record['log_time'],
                    record.get('server_name', ''),
                    record.get('client_ip', ''),
                    record.get('client_port', 0),
                    record.get('xff_ip', ''),
                    record.get('request_method', ''),
                    record.get('request_uri', ''),
                    record.get('request_uri_path', ''),
                    record.get('request_query_string', ''),
                    record.get('http_protocol', ''),
                    record.get('http_host', ''),
                    record.get('response_status_code', 0),
                    record.get('response_body_size', 0),
                    record.get('response_content_type', ''),
                    record.get('request_time', 0.0),
                    record.get('upstream_response_time', 0.0),
                    record.get('upstream_connect_time', 0.0),
                    record.get('upstream_header_time', 0.0),
                    record.get('upstream_status_code', 0),
                    record.get('upstream_cache_status', ''),
                    record.get('upstream_addr', ''),
                    record.get('connection_requests', 1),
                    record.get('connection_id', 0),
                    record.get('request_length', 0),
                    record.get('bytes_sent', 0),
                    record.get('user_agent', ''),
                    record.get('referer', ''),
                    record.get('trace_id', ''),
                    record.get('business_sign', ''),
                    record.get('cluster_name', ''),
                    record.get('source_file', ''),
                    datetime.now()
                ]
                ods_data.append(ods_record)
                
                # DWD记录
                dwd_record = [
                    record_id + 1000000,  # DWD ID
                    record_id,            # ODS ID
                    record['log_time'],
                    record.get('platform', ''),
                    record.get('platform_version', ''),
                    record.get('entry_source', ''),
                    record.get('api_category', ''),
                    record.get('business_category', ''),
                    record.get('client_ip', ''),
                    record.get('client_region', ''),
                    record.get('client_isp', ''),
                    record.get('request_method', ''),
                    record.get('request_uri_normalized', ''),
                    record.get('request_uri_pattern', ''),
                    record.get('query_param_count', 0),
                    record.get('has_sensitive_params', False),
                    record.get('request_time', 0.0),
                    record.get('response_status_code', 0),
                    record.get('response_size_kb', 0.0),
                    record.get('upstream_response_time', 0.0),
                    record.get('upstream_status_code', 0),
                    record.get('upstream_cache_status', ''),
                    record.get('upstream_cluster', ''),
                    record.get('connection_reused', False),
                    record.get('connection_requests', 1),
                    record.get('browser_name', ''),
                    record.get('browser_version', ''),
                    record.get('os_name', ''),
                    record.get('os_version', ''),
                    record.get('device_type', ''),
                    record.get('is_bot', False),
                    record.get('bot_name', ''),
                    record.get('trace_id', ''),
                    record.get('business_sign', ''),
                    record.get('user_id', ''),
                    record.get('session_id', ''),
                    False,  # has_anomaly
                    0.0,    # anomaly_score
                    '',     # anomaly_type
                    1.0,    # data_quality_score
                    datetime.now(),
                    datetime.now()
                ]
                dwd_data.append(dwd_record)
            
            # 插入ODS
            self.client.insert('ods_nginx_raw', ods_data, 
                             column_names=['id', 'log_time', 'server_name', 'client_ip', 'client_port', 
                                         'xff_ip', 'request_method', 'request_uri', 'request_uri_path',
                                         'request_query_string', 'http_protocol', 'http_host', 
                                         'response_status_code', 'response_body_size', 'response_content_type',
                                         'request_time', 'upstream_response_time', 'upstream_connect_time',
                                         'upstream_header_time', 'upstream_status_code', 'upstream_cache_status',
                                         'upstream_addr', 'connection_requests', 'connection_id',
                                         'request_length', 'bytes_sent', 'user_agent', 'referer',
                                         'trace_id', 'business_sign', 'cluster_name', 'source_file', 'created_at'])
            
            # 插入DWD
            self.client.insert('dwd_nginx_enriched', dwd_data,
                             column_names=['id', 'ods_id', 'log_time', 'platform', 'platform_version',
                                         'entry_source', 'api_category', 'business_category', 'client_ip',
                                         'client_region', 'client_isp', 'request_method', 'request_uri_normalized',
                                         'request_uri_pattern', 'query_param_count', 'has_sensitive_params',
                                         'request_time', 'response_status_code', 'response_size_kb',
                                         'upstream_response_time', 'upstream_status_code', 'upstream_cache_status',
                                         'upstream_cluster', 'connection_reused', 'connection_requests',
                                         'browser_name', 'browser_version', 'os_name', 'os_version',
                                         'device_type', 'is_bot', 'bot_name', 'trace_id', 'business_sign',
                                         'user_id', 'session_id', 'has_anomaly', 'anomaly_score',
                                         'anomaly_type', 'data_quality_score', 'created_at', 'updated_at'])
            
            print(f"成功插入 {len(records)} 条记录到ClickHouse")
            return True
            
        except Exception as e:
            print(f"ClickHouse插入失败: {e}")
            return False
    
    def process_log_file(self, file_path: str, batch_size: int = 1000) -> Dict[str, Any]:
        """处理单个日志文件"""
        print(f"开始处理日志文件: {file_path}")
        
        source_file = os.path.basename(file_path)
        total_lines = 0
        success_lines = 0
        error_lines = 0
        batch_records = []
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    total_lines += 1
                    
                    # 解析单行日志
                    parsed_data = self.parse_nginx_log_line(line, source_file)
                    
                    if parsed_data:
                        batch_records.append(parsed_data)
                        success_lines += 1
                        
                        # 批量插入
                        if len(batch_records) >= batch_size:
                            if self.insert_to_clickhouse(batch_records):
                                print(f"  ✓ 已处理 {success_lines} 行 ({success_lines/total_lines*100:.1f}%)")
                            batch_records = []
                    else:
                        error_lines += 1
                    
                    # 进度显示
                    if total_lines % 10000 == 0:
                        print(f"  处理进度: {total_lines} 行, 成功: {success_lines}, 失败: {error_lines}")
                
                # 处理剩余记录
                if batch_records:
                    self.insert_to_clickhouse(batch_records)
                    
        except Exception as e:
            print(f"文件处理失败: {e}")
            return {'success': False, 'error': str(e)}
        
        result = {
            'success': True,
            'total_lines': total_lines,
            'success_lines': success_lines,
            'error_lines': error_lines,
            'success_rate': (success_lines / total_lines * 100) if total_lines > 0 else 0
        }
        
        print(f"文件处理完成: {source_file}")
        print(f"   总行数: {total_lines}, 成功: {success_lines}, 失败: {error_lines}")
        print(f"   成功率: {result['success_rate']:.1f}%")
        
        return result