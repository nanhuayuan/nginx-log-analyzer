#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
字段映射器 - 将底座格式日志映射到DWD表结构
Field Mapper - Maps base format logs to DWD table structure

负责将解析后的底座格式数据映射到dwd_nginx_enriched_v2表的128个字段
"""

import re
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, Union
from urllib.parse import urlparse, parse_qs

class FieldMapper:
    """字段映射器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # 初始化用户代理解析器
        self.user_agent_patterns = self._init_user_agent_patterns()
        
    def _init_user_agent_patterns(self) -> Dict[str, str]:
        """初始化用户代理解析模式"""
        return {
            # 移动端检测
            'mobile': r'Mobile|Android|iPhone|iPad|Windows Phone',
            'ios': r'iPhone|iPad|iOS',
            'android': r'Android',
            'wechat': r'MicroMessenger|WeChat',
            'alipay': r'AlipayClient|AliApp',
            
            # 浏览器检测
            'chrome': r'Chrome/([0-9.]+)',
            'safari': r'Safari/([0-9.]+)',
            'firefox': r'Firefox/([0-9.]+)',
            
            # 设备类型
            'bot': r'bot|crawler|spider|scraper|slurp',
            'sdk': r'SDK|API|Client'
        }
    
    def map_to_dwd(self, parsed_data: Dict[str, Any], source_file: str = '') -> Dict[str, Any]:
        """
        将解析后的数据映射到DWD表结构
        
        Args:
            parsed_data: 解析后的原始数据
            source_file: 源文件名
            
        Returns:
            映射后的DWD结构数据
        """
        try:
            # 创建DWD记录
            dwd_record = {}
            
            # === 基础字段映射 ===
            self._map_basic_fields(dwd_record, parsed_data, source_file)
            
            # === 时间字段映射 ===
            self._map_time_fields(dwd_record, parsed_data)
            
            # === 请求字段映射 ===
            self._map_request_fields(dwd_record, parsed_data)
            
            # === 响应字段映射 ===
            self._map_response_fields(dwd_record, parsed_data)
            
            # === 性能字段映射 ===
            self._map_performance_fields(dwd_record, parsed_data)
            
            # === 业务字段映射 ===
            self._map_business_fields(dwd_record, parsed_data)
            
            # === 计算字段生成 ===
            self._generate_derived_fields(dwd_record)
            
            # === 数据质量字段 ===
            self._add_quality_fields(dwd_record, parsed_data)
            
            return dwd_record
            
        except Exception as e:
            self.logger.error(f"字段映射失败: {e}")
            raise
    
    def _map_basic_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any], source_file: str):
        """映射基础字段"""
        # ID字段（由数据库自动生成）
        # dwd_record['id'] = 自动生成
        # dwd_record['ods_id'] = 将在写入时关联
        
        # 网络字段
        dwd_record['client_ip'] = parsed_data.get('remote_addr', '')
        dwd_record['client_port'] = self._safe_int(parsed_data.get('remote_port'), 0)
        dwd_record['xff_ip'] = parsed_data.get('RealIp', parsed_data.get('remote_addr', ''))
        dwd_record['server_name'] = parsed_data.get('http_host', '')
        
        # HTTP协议相关
        request_info = self._parse_request(parsed_data.get('request', ''))
        dwd_record['request_method'] = request_info['method']
        dwd_record['request_uri'] = request_info['uri']
        dwd_record['request_uri_normalized'] = self._normalize_uri(request_info['uri'])
        dwd_record['request_full_uri'] = parsed_data.get('request', '')
        dwd_record['http_protocol_version'] = request_info['protocol']
        
        # 查询参数
        dwd_record['query_parameters'] = self._extract_query_params(request_info['uri'])
        
        # 用户代理和引用页
        dwd_record['user_agent_string'] = parsed_data.get('agent', '')
        dwd_record['referer_url'] = parsed_data.get('http_referer', '')
        dwd_record['referer_domain'] = self._extract_domain(parsed_data.get('http_referer', ''))
        
        # 源文件信息
        dwd_record['log_source_file'] = source_file
        
        # 连接信息（底座格式中可能不包含，使用默认值）
        dwd_record['connection_requests'] = 1
        dwd_record['cache_status'] = 'unknown'
        dwd_record['upstream_server'] = ''
        dwd_record['cluster_node'] = 'unknown'
        dwd_record['trace_id'] = ''
        
    def _map_time_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射时间字段"""
        # 解析时间字符串
        time_str = parsed_data.get('time', '')
        log_time = self._parse_time(time_str)
        
        if log_time:
            dwd_record['log_time'] = log_time
            
            # 日期分区字段
            dwd_record['date_partition'] = log_time.date()
            dwd_record['date'] = log_time.date()
            
            # 时间分区字段
            dwd_record['hour_partition'] = log_time.hour
            dwd_record['minute_partition'] = log_time.minute
            dwd_record['second_partition'] = log_time.second
            dwd_record['hour'] = log_time.hour
            dwd_record['minute'] = log_time.minute
            dwd_record['second'] = log_time.second
            
            # 时间字符串字段
            dwd_record['date_hour'] = log_time.strftime('%Y%m%d%H')
            dwd_record['date_hour_minute'] = log_time.strftime('%Y%m%d%H%M')
            
            # 星期和周末判断
            dwd_record['weekday'] = log_time.weekday() + 1  # 1=周一, 7=周日
            dwd_record['is_weekend'] = log_time.weekday() >= 5
            
            # 时间段分类
            hour = log_time.hour
            if 6 <= hour < 12:
                time_period = 'morning'
            elif 12 <= hour < 18:
                time_period = 'afternoon'
            elif 18 <= hour < 22:
                time_period = 'evening'
            else:
                time_period = 'night'
            dwd_record['time_period'] = time_period
        else:
            # 时间解析失败时的默认值
            now = datetime.now()
            dwd_record['log_time'] = now
            dwd_record['date_partition'] = now.date()
            dwd_record['date'] = now.date()
            dwd_record['hour_partition'] = now.hour
            dwd_record['minute_partition'] = now.minute
            dwd_record['second_partition'] = now.second
            dwd_record['hour'] = now.hour
            dwd_record['minute'] = now.minute
            dwd_record['second'] = now.second
            dwd_record['date_hour'] = now.strftime('%Y%m%d%H')
            dwd_record['date_hour_minute'] = now.strftime('%Y%m%d%H%M')
            dwd_record['weekday'] = now.weekday() + 1
            dwd_record['is_weekend'] = now.weekday() >= 5
            dwd_record['time_period'] = 'unknown'
            
        # 上游时间字段（底座格式中通常没有，使用默认值）
        dwd_record['upstream_connect_time'] = 0.0
        dwd_record['upstream_header_time'] = 0.0
        dwd_record['upstream_response_time'] = 0.0
        
        # 审计时间
        dwd_record['created_at'] = datetime.now()
        dwd_record['updated_at'] = datetime.now()
    
    def _map_request_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射请求相关字段"""
        # 基础请求信息已在_map_basic_fields中处理
        
        # 请求持续时间
        ar_time = self._safe_float(parsed_data.get('ar_time'), 0.0)
        dwd_record['total_request_duration'] = ar_time
        
    def _map_response_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射响应相关字段"""
        # 状态码
        dwd_record['response_status_code'] = str(parsed_data.get('code', '0'))
        
        # 响应体大小
        body_size = self._safe_int(parsed_data.get('body'), 0)
        dwd_record['response_body_size'] = body_size
        dwd_record['response_body_size_kb'] = body_size / 1024.0
        
        # 传输相关
        dwd_record['total_bytes_sent'] = body_size  # 简化处理
        dwd_record['total_bytes_sent_kb'] = body_size / 1024.0
        
    def _map_performance_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射性能相关字段"""
        # 总请求时间
        total_time = self._safe_float(parsed_data.get('ar_time'), 0.0)
        
        # 由于底座格式缺少上游时间信息，我们做简化处理
        # 假设所有时间都在nginx处理阶段
        dwd_record['backend_connect_phase'] = 0.0
        dwd_record['backend_process_phase'] = 0.0
        dwd_record['backend_transfer_phase'] = 0.0
        dwd_record['nginx_transfer_phase'] = total_time
        dwd_record['backend_total_phase'] = 0.0
        dwd_record['network_phase'] = total_time * 0.1  # 估算网络时间为总时间的10%
        dwd_record['processing_phase'] = total_time * 0.9  # 估算处理时间为总时间的90%
        dwd_record['transfer_phase'] = total_time * 0.1
        
        # 传输速度计算
        body_size_kb = dwd_record['response_body_size_kb']
        if total_time > 0 and body_size_kb > 0:
            dwd_record['response_transfer_speed'] = body_size_kb / total_time
            dwd_record['total_transfer_speed'] = body_size_kb / total_time
            dwd_record['nginx_transfer_speed'] = body_size_kb / total_time
        else:
            dwd_record['response_transfer_speed'] = 0.0
            dwd_record['total_transfer_speed'] = 0.0
            dwd_record['nginx_transfer_speed'] = 0.0
        
        # 效率指标计算
        if total_time > 0:
            dwd_record['backend_efficiency'] = 0.0  # 没有后端时间
            dwd_record['network_overhead'] = (dwd_record['network_phase'] / total_time) * 100
            dwd_record['transfer_ratio'] = (dwd_record['transfer_phase'] / total_time) * 100
            dwd_record['connection_cost_ratio'] = 0.0  # 没有连接时间
            dwd_record['processing_efficiency_index'] = min(100.0, (1.0 / total_time) * 100)
        else:
            dwd_record['backend_efficiency'] = 0.0
            dwd_record['network_overhead'] = 0.0
            dwd_record['transfer_ratio'] = 0.0
            dwd_record['connection_cost_ratio'] = 0.0
            dwd_record['processing_efficiency_index'] = 100.0
    
    def _map_business_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射业务相关字段"""
        # 解析User-Agent
        user_agent = parsed_data.get('agent', '')
        ua_info = self._parse_user_agent(user_agent)
        
        dwd_record['platform'] = ua_info['platform']
        dwd_record['platform_version'] = ua_info['platform_version']
        dwd_record['app_version'] = ua_info['app_version']
        dwd_record['device_type'] = ua_info['device_type']
        dwd_record['browser_type'] = ua_info['browser_type']
        dwd_record['os_type'] = ua_info['os_type']
        dwd_record['os_version'] = ua_info['os_version']
        dwd_record['sdk_type'] = ua_info['sdk_type']
        dwd_record['sdk_version'] = ua_info['sdk_version']
        dwd_record['bot_type'] = ua_info['bot_type']
        
        # 来源分析
        referer = parsed_data.get('http_referer', '')
        dwd_record['entry_source'] = self._classify_entry_source(referer)
        dwd_record['search_engine'] = self._detect_search_engine(referer)
        dwd_record['social_media'] = self._detect_social_media(referer)
        
        # API分类
        uri = dwd_record['request_uri']
        dwd_record['api_category'] = self._classify_api_category(uri)
        dwd_record['api_module'] = self._extract_api_module(uri)
        dwd_record['api_version'] = self._extract_api_version(uri)
        
        # 业务域和访问类型
        dwd_record['business_domain'] = self._classify_business_domain(uri)
        dwd_record['access_type'] = self._classify_access_type(user_agent, uri)
        dwd_record['client_category'] = self._classify_client_category(user_agent)
        dwd_record['application_name'] = self._extract_application_name(user_agent, uri)
        dwd_record['service_name'] = self._extract_service_name(uri)
        dwd_record['business_sign'] = ''  # 底座格式中通常没有
        
        # IP风险级别和地理信息
        dwd_record['client_region'] = 'unknown'  # 需要IP地理库
        dwd_record['client_isp'] = 'unknown'     # 需要IP地理库
        dwd_record['ip_risk_level'] = self._assess_ip_risk(dwd_record['client_ip'])
        dwd_record['is_internal_ip'] = self._is_internal_ip(dwd_record['client_ip'])
        
        # API重要性评分
        dwd_record['api_importance'] = self._assess_api_importance(uri)
        dwd_record['business_value_score'] = self._calculate_business_value_score(dwd_record)
    
    def _generate_derived_fields(self, dwd_record: Dict[str, Any]):
        """生成计算字段"""
        # 成功状态判断
        status_code = dwd_record['response_status_code']
        dwd_record['is_success'] = status_code.startswith('2')  # 2xx为成功
        dwd_record['is_error'] = not dwd_record['is_success']
        dwd_record['is_client_error'] = status_code.startswith('4')  # 4xx为客户端错误
        dwd_record['is_server_error'] = status_code.startswith('5')  # 5xx为服务端错误
        
        # 业务成功（简化判断：HTTP成功且有响应体）
        dwd_record['is_business_success'] = (dwd_record['is_success'] and 
                                           dwd_record['response_body_size'] > 0)
        
        # 慢请求判断
        duration = dwd_record['total_request_duration']
        dwd_record['is_slow'] = duration > 3.0  # 超过3秒认为是慢请求
        dwd_record['is_very_slow'] = duration > 10.0  # 超过10秒认为是非常慢请求
        
        # 异常检测（简化版）
        dwd_record['has_anomaly'] = (dwd_record['is_server_error'] or 
                                   dwd_record['is_very_slow'] or
                                   dwd_record['response_body_size'] > 10 * 1024 * 1024)  # 超过10MB
        dwd_record['anomaly_type'] = self._classify_anomaly_type(dwd_record)
        
        # 用户体验级别
        dwd_record['user_experience_level'] = self._classify_user_experience(duration, dwd_record['is_success'])
        
        # APDEX分类
        if duration <= 1.5:
            apdex = 'satisfied'
        elif duration <= 6.0:
            apdex = 'tolerated'
        else:
            apdex = 'frustrated'
        dwd_record['apdex_classification'] = apdex
    
    def _add_quality_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """添加数据质量字段"""
        # 解析错误记录
        parsing_errors = parsed_data.get('parsing_errors', [])
        dwd_record['parsing_errors'] = parsing_errors
        
        # 数据质量评分（基于字段完整性）
        total_fields = len(dwd_record)
        missing_fields = sum(1 for v in dwd_record.values() if v is None or v == '')
        quality_score = max(0.0, (total_fields - missing_fields) / total_fields * 100)
        dwd_record['data_quality_score'] = quality_score
    
    # === 辅助方法 ===
    
    def _parse_time(self, time_str: str) -> Optional[datetime]:
        """解析时间字符串"""
        if not time_str:
            return None
        
        formats = [
            '%Y-%m-%dT%H:%M:%S%z',
            '%Y-%m-%dT%H:%M:%S+08:00',
            '%Y-%m-%d %H:%M:%S',
        ]
        
        # 处理时区后缀
        time_str = time_str.replace('+08:00', '+0800')
        
        for fmt in formats:
            try:
                return datetime.strptime(time_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def _parse_request(self, request_str: str) -> Dict[str, str]:
        """解析请求字符串"""
        if not request_str:
            return {'method': '', 'uri': '', 'protocol': ''}
        
        parts = request_str.split(' ', 2)
        if len(parts) >= 3:
            return {
                'method': parts[0],
                'uri': parts[1],
                'protocol': parts[2]
            }
        elif len(parts) == 2:
            return {
                'method': parts[0],
                'uri': parts[1], 
                'protocol': 'HTTP/1.1'
            }
        else:
            return {
                'method': 'GET',
                'uri': request_str,
                'protocol': 'HTTP/1.1'
            }
    
    def _normalize_uri(self, uri: str) -> str:
        """标准化URI"""
        if not uri:
            return uri
        
        # 移除查询参数
        if '?' in uri:
            uri = uri.split('?')[0]
        
        # 移除末尾斜杠
        if uri.endswith('/') and len(uri) > 1:
            uri = uri[:-1]
        
        return uri
    
    def _extract_query_params(self, uri: str) -> str:
        """提取查询参数"""
        if '?' not in uri:
            return ''
        
        return uri.split('?', 1)[1]
    
    def _extract_domain(self, url: str) -> str:
        """提取域名"""
        if not url or url == '-':
            return ''
        
        try:
            parsed = urlparse(url)
            return parsed.netloc
        except:
            return ''
    
    def _parse_user_agent(self, user_agent: str) -> Dict[str, str]:
        """解析User-Agent"""
        if not user_agent:
            return self._empty_ua_info()
        
        ua_info = {
            'platform': 'unknown',
            'platform_version': '',
            'app_version': '',
            'device_type': 'unknown',
            'browser_type': 'unknown',
            'os_type': 'unknown',
            'os_version': '',
            'sdk_type': '',
            'sdk_version': '',
            'bot_type': ''
        }
        
        # 检测设备类型和平台
        if re.search(self.user_agent_patterns['ios'], user_agent, re.I):
            ua_info['platform'] = 'iOS'
            ua_info['device_type'] = 'Mobile'
            ua_info['os_type'] = 'iOS'
        elif re.search(self.user_agent_patterns['android'], user_agent, re.I):
            ua_info['platform'] = 'Android'
            ua_info['device_type'] = 'Mobile'
            ua_info['os_type'] = 'Android'
        elif re.search(self.user_agent_patterns['mobile'], user_agent, re.I):
            ua_info['device_type'] = 'Mobile'
        else:
            ua_info['device_type'] = 'Desktop'
        
        # 检测特殊应用
        if 'zgt-ios' in user_agent.lower():
            ua_info['platform'] = 'iOS'
            ua_info['app_version'] = 'zgt-ios'
        elif 'wst-sdk' in user_agent.lower():
            ua_info['sdk_type'] = 'WST-SDK'
        
        # 检测浏览器
        if 'Chrome' in user_agent:
            ua_info['browser_type'] = 'Chrome'
        elif 'Safari' in user_agent:
            ua_info['browser_type'] = 'Safari'
        elif 'Firefox' in user_agent:
            ua_info['browser_type'] = 'Firefox'
        
        # 检测机器人
        if re.search(self.user_agent_patterns['bot'], user_agent, re.I):
            ua_info['bot_type'] = 'bot'
            ua_info['device_type'] = 'Bot'
        
        return ua_info
    
    def _empty_ua_info(self) -> Dict[str, str]:
        """空的User-Agent信息"""
        return {
            'platform': 'unknown',
            'platform_version': '',
            'app_version': '',
            'device_type': 'unknown',
            'browser_type': 'unknown',
            'os_type': 'unknown',
            'os_version': '',
            'sdk_type': '',
            'sdk_version': '',
            'bot_type': ''
        }
    
    def _classify_api_category(self, uri: str) -> str:
        """API分类"""
        if not uri:
            return 'unknown'
        
        if '/api/' in uri:
            return 'api'
        elif uri.endswith(('.jpg', '.png', '.gif', '.css', '.js')):
            return 'static'
        elif '/gateway/' in uri:
            return 'gateway'
        else:
            return 'page'
    
    def _extract_api_module(self, uri: str) -> str:
        """提取API模块"""
        if not uri:
            return ''
        
        # 简单的模块提取逻辑
        parts = uri.split('/')
        if len(parts) > 2:
            return parts[1]  # 第一个路径段作为模块名
        
        return ''
    
    def _extract_api_version(self, uri: str) -> str:
        """提取API版本"""
        # 简化处理，寻找v1、v2等版本标识
        if re.search(r'/v\d+/', uri):
            match = re.search(r'/(v\d+)/', uri)
            return match.group(1) if match else ''
        
        return ''
    
    def _classify_business_domain(self, uri: str) -> str:
        """分类业务域"""
        if not uri:
            return 'unknown'
        
        if '/scmp-gateway/' in uri:
            return 'scmp'
        elif '/zgt-h5/' in uri:
            return 'h5'
        elif '/group1/' in uri:
            return 'file'
        else:
            return 'other'
    
    def _classify_access_type(self, user_agent: str, uri: str) -> str:
        """分类访问类型"""
        if 'SDK' in user_agent:
            return 'API'
        elif '/api/' in uri:
            return 'API'
        elif '/h5/' in uri:
            return 'H5'
        else:
            return 'App'
    
    def _classify_client_category(self, user_agent: str) -> str:
        """分类客户端类别"""
        if 'bot' in user_agent.lower():
            return 'bot'
        elif 'SDK' in user_agent:
            return 'sdk'
        else:
            return 'user'
    
    def _extract_application_name(self, user_agent: str, uri: str) -> str:
        """提取应用名称"""
        if 'zgt-ios' in user_agent.lower():
            return 'zgt-ios'
        elif 'WST-SDK' in user_agent:
            return 'WST-SDK'
        else:
            return 'unknown'
    
    def _extract_service_name(self, uri: str) -> str:
        """提取服务名称"""
        if '/scmp-gateway/' in uri:
            return 'scmp-gateway'
        elif '/zgt-h5/' in uri:
            return 'zgt-h5'
        else:
            return 'default'
    
    def _classify_entry_source(self, referer: str) -> str:
        """分类入口来源"""
        if not referer or referer == '-':
            return 'direct'
        
        if 'google' in referer.lower():
            return 'search'
        elif 'baidu' in referer.lower():
            return 'search'
        else:
            return 'referral'
    
    def _detect_search_engine(self, referer: str) -> str:
        """检测搜索引擎"""
        if not referer or referer == '-':
            return ''
        
        if 'google' in referer.lower():
            return 'google'
        elif 'baidu' in referer.lower():
            return 'baidu'
        
        return ''
    
    def _detect_social_media(self, referer: str) -> str:
        """检测社交媒体"""
        if not referer or referer == '-':
            return ''
        
        if 'weibo' in referer.lower():
            return 'weibo'
        elif 'wechat' in referer.lower():
            return 'wechat'
        
        return ''
    
    def _is_internal_ip(self, ip: str) -> bool:
        """判断是否内网IP"""
        if not ip:
            return False
        
        # 简单的内网IP判断
        internal_prefixes = ['10.', '192.168.', '172.16.', '127.']
        return any(ip.startswith(prefix) for prefix in internal_prefixes)
    
    def _assess_ip_risk(self, ip: str) -> str:
        """评估IP风险级别"""
        if self._is_internal_ip(ip):
            return 'Low'
        
        # 简化的风险评估
        return 'Medium'
    
    def _assess_api_importance(self, uri: str) -> str:
        """评估API重要性"""
        if not uri:
            return 'low'
        
        if '/login' in uri or '/auth' in uri:
            return 'critical'
        elif '/pay' in uri or '/order' in uri:
            return 'high'
        elif '/user' in uri:
            return 'medium'
        else:
            return 'low'
    
    def _calculate_business_value_score(self, dwd_record: Dict[str, Any]) -> int:
        """计算业务价值评分"""
        score = 50  # 基础分
        
        # 根据API重要性调整
        importance = dwd_record.get('api_importance', 'low')
        if importance == 'critical':
            score += 30
        elif importance == 'high':
            score += 20
        elif importance == 'medium':
            score += 10
        
        # 根据成功状态调整
        if dwd_record.get('is_business_success', False):
            score += 20
        
        return min(100, score)
    
    def _classify_anomaly_type(self, dwd_record: Dict[str, Any]) -> str:
        """分类异常类型"""
        if not dwd_record.get('has_anomaly', False):
            return ''
        
        if dwd_record.get('is_server_error', False):
            return 'server_error'
        elif dwd_record.get('is_very_slow', False):
            return 'performance'
        elif dwd_record.get('response_body_size', 0) > 10 * 1024 * 1024:
            return 'large_response'
        else:
            return 'other'
    
    def _classify_user_experience(self, duration: float, is_success: bool) -> str:
        """分类用户体验级别"""
        if not is_success:
            return 'poor'
        
        if duration <= 1.0:
            return 'excellent'
        elif duration <= 3.0:
            return 'good'
        elif duration <= 10.0:
            return 'fair'
        else:
            return 'poor'
    
    def _safe_int(self, value: Any, default: int = 0) -> int:
        """安全的整数转换"""
        if value is None:
            return default
        
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        """安全的浮点数转换"""
        if value is None:
            return default
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return default