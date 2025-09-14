#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
字段映射器 - 将底座格式日志映射到DWD表结构
Field Mapper - Maps base format logs to DWD table structure

负责将解析后的底座格式数据映射到dwd_nginx_enriched_v3表的200+个字段
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
            
            # === v3.0 新增维度字段映射 ===
            self._map_permission_control_fields(dwd_record, parsed_data)
            self._map_platform_entry_fields(dwd_record, parsed_data) 
            self._map_error_analysis_fields(dwd_record, parsed_data)
            self._map_security_fields(dwd_record, parsed_data)
            self._map_business_process_fields(dwd_record, parsed_data)
            
            # === 计算字段生成 ===
            self._generate_derived_fields(dwd_record)
            
            # === 数据质量字段 ===
            self._add_quality_fields(dwd_record, parsed_data)
            
            return dwd_record
            
        except Exception as e:
            # 记录详细错误信息和上下文
            error_context = {
                'source_file': source_file,
                'raw_line': parsed_data.get('raw_line', '')[:200],  # 限制长度
                'line_number': parsed_data.get('line_number', 'unknown'),
                'error_type': type(e).__name__,
                'error_message': str(e),
                'key_fields': {
                    'time': parsed_data.get('time'),
                    'remote_addr': parsed_data.get('remote_addr'),
                    'request': parsed_data.get('request', '')[:100],
                    'agent': parsed_data.get('agent', '')[:50]
                }
            }
            
            self.logger.error(f"字段映射失败: {error_context}")
            
            # 返回缺省值记录而不是引发异常，实现容错处理
            return self._create_fallback_record(parsed_data, source_file, str(e))
    
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
        dwd_record['user_agent_string'] = parsed_data.get('agent', '') or ''
        dwd_record['referer_url'] = parsed_data.get('http_referer', '')
        dwd_record['referer_domain'] = self._extract_domain(parsed_data.get('http_referer', ''))
        
        # 源文件信息
        dwd_record['log_source_file'] = source_file
        
        # 基础设施字段提取 - 增强版
        self._extract_infrastructure_fields(dwd_record, parsed_data)
        
    def _map_time_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射时间字段"""
        # 解析时间字符串
        time_str = parsed_data.get('time', '')
        log_time = self._parse_time(time_str)
        
        if log_time:
            dwd_record['log_time'] = log_time
            
            # 日期分区字段 (只包含非MATERIALIZED字段)
            dwd_record['date_partition'] = log_time.date()
            
            # 时间分区字段 (只包含非MATERIALIZED字段)
            dwd_record['hour_partition'] = log_time.hour
            dwd_record['minute_partition'] = log_time.minute
            dwd_record['second_partition'] = log_time.second
            
            # 移除MATERIALIZED字段的生成:
            # - date, hour, minute, second (从log_time自动计算)
            # - date_hour, date_hour_minute (从date和hour自动计算)
            # - weekday, is_weekend, time_period (从log_time自动计算)
        else:
            # 时间解析失败时的默认值 (只包含非MATERIALIZED字段)
            now = datetime.now()
            dwd_record['log_time'] = now
            dwd_record['date_partition'] = now.date()
            dwd_record['hour_partition'] = now.hour
            dwd_record['minute_partition'] = now.minute
            dwd_record['second_partition'] = now.second
            
            # 移除MATERIALIZED字段的生成:
            # - date, hour, minute, second, date_hour, date_hour_minute
            # - weekday, is_weekend, time_period
            
        # 上游时间字段（底座格式中通常没有，使用默认值）
        dwd_record['upstream_connect_time'] = 0.0
        dwd_record['upstream_header_time'] = 0.0
        dwd_record['upstream_response_time'] = 0.0
        
        # 移除审计时间字段 (created_at, updated_at 有DEFAULT值，会自动生成)
        # 这些字段虽然不是MATERIALIZED，但有DEFAULT值，不应该手动设置
    
    def _map_request_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射请求相关字段"""
        # 基础请求信息已在_map_basic_fields中处理

        # 请求持续时间 - 单位标准化：秒转换为毫秒 (2025-09-14)
        # 原始日志ar_time是秒(如0.325)，转换为毫秒存储(如325)提高性能和直观性
        ar_time = self._safe_float(parsed_data.get('ar_time'), 0.0)
        dwd_record['total_request_duration'] = int(ar_time * 1000)  # 秒 -> 毫秒
        
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
        """映射性能相关字段 - 基于HTTP生命周期的标准计算"""
        # 基础时间参数 - 单位标准化：统一转换为毫秒 (2025-09-14)
        # 原始日志均为秒单位，转换为毫秒便于Grafana使用和整数运算
        total_time = self._safe_float(parsed_data.get('ar_time'), 0.0)
        upstream_connect_time_raw = self._safe_float(parsed_data.get('upstream_connect_time', '0'), 0.0)
        upstream_header_time_raw = self._safe_float(parsed_data.get('upstream_header_time', '0'), 0.0)
        upstream_response_time_raw = self._safe_float(parsed_data.get('upstream_response_time', '0'), 0.0)

        # 转换为毫秒
        total_time_ms = int(total_time * 1000)
        upstream_connect_time = int(upstream_connect_time_raw * 1000)
        upstream_header_time = int(upstream_header_time_raw * 1000)
        upstream_response_time = int(upstream_response_time_raw * 1000)
        
        # 如果底座格式没有上游时间，基于总时间进行智能估算 (保持毫秒单位)
        if upstream_response_time == 0 and total_time_ms > 0:
            # 智能估算：基于HTTP生命周期阶段的合理比例
            estimated_backend_ratio = 0.7  # 后端处理占70%
            estimated_nginx_ratio = 0.3    # nginx处理占30%

            # 估算上游时间 (直接用毫秒计算)
            upstream_response_time = int(total_time_ms * estimated_backend_ratio)
            upstream_header_time = int(upstream_response_time * 0.8)  # 处理时间占后端时间80%
            upstream_connect_time = int(upstream_response_time * 0.1)  # 连接时间占后端时间10%
        
        # 按照HTTP生命周期标准计算各阶段 - 全部使用毫秒单位 (2025-09-14)
        dwd_record['upstream_connect_time'] = upstream_connect_time
        dwd_record['upstream_header_time'] = upstream_header_time
        dwd_record['upstream_response_time'] = upstream_response_time
        dwd_record['backend_connect_phase'] = upstream_connect_time
        dwd_record['backend_process_phase'] = max(0, upstream_header_time - upstream_connect_time)
        dwd_record['backend_transfer_phase'] = max(0, upstream_response_time - upstream_header_time)
        dwd_record['nginx_transfer_phase'] = max(0, total_time_ms - upstream_response_time)

        # 计算复合阶段
        dwd_record['backend_total_phase'] = upstream_response_time
        dwd_record['network_phase'] = upstream_connect_time + (total_time_ms - upstream_response_time)
        dwd_record['processing_phase'] = upstream_header_time - upstream_connect_time
        dwd_record['transfer_phase'] = (upstream_response_time - upstream_header_time) + (total_time_ms - upstream_response_time)
        
        # 性能效率指标计算
        body_size_kb = dwd_record['response_body_size_kb']
        total_bytes_kb = dwd_record['total_bytes_sent_kb']
        
        # 传输速度计算 - 时间单位改为毫秒，需要转换为秒计算速度 (2025-09-14)
        # 速度单位保持为KB/s，所以毫秒需要除以1000转换为秒
        backend_transfer_time_s = dwd_record['backend_transfer_phase'] / 1000.0  # 毫秒->秒
        if backend_transfer_time_s > 0 and body_size_kb > 0:
            dwd_record['response_transfer_speed'] = body_size_kb / backend_transfer_time_s  # KB/s
        else:
            dwd_record['response_transfer_speed'] = 0.0

        total_time_s = total_time_ms / 1000.0  # 毫秒->秒
        if total_time_s > 0 and total_bytes_kb > 0:
            dwd_record['total_transfer_speed'] = total_bytes_kb / total_time_s  # KB/s
        else:
            dwd_record['total_transfer_speed'] = 0.0

        nginx_transfer_time_s = dwd_record['nginx_transfer_phase'] / 1000.0  # 毫秒->秒
        if nginx_transfer_time_s > 0 and body_size_kb > 0:
            dwd_record['nginx_transfer_speed'] = body_size_kb / nginx_transfer_time_s  # KB/s
        else:
            dwd_record['nginx_transfer_speed'] = 0.0
        
        # 效率比率计算 - 参考HTTP生命周期可视化的性能指标
        if upstream_response_time > 0:
            dwd_record['backend_efficiency'] = (dwd_record['backend_process_phase'] / upstream_response_time) * 100
        else:
            dwd_record['backend_efficiency'] = 0.0
            
        if total_time_ms > 0:
            dwd_record['network_overhead'] = (dwd_record['network_phase'] / total_time_ms) * 100
            dwd_record['transfer_ratio'] = (dwd_record['transfer_phase'] / total_time_ms) * 100
            dwd_record['connection_cost_ratio'] = (upstream_connect_time / total_time_ms) * 100
        else:
            dwd_record['network_overhead'] = 0.0
            dwd_record['transfer_ratio'] = 0.0
            dwd_record['connection_cost_ratio'] = 0.0
            
        # 处理效率指数 - 综合评分
        if total_time_ms > 0 and upstream_response_time > 0:
            # 基于后端效率、网络开销、传输比率的综合评分
            efficiency_score = (
                min(100, dwd_record['backend_efficiency']) * 0.4 +
                max(0, 100 - dwd_record['network_overhead']) * 0.3 +
                max(0, 100 - dwd_record['transfer_ratio']) * 0.3
            )
            dwd_record['processing_efficiency_index'] = efficiency_score
        else:
            dwd_record['processing_efficiency_index'] = 0.0
    
    def _map_business_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射业务相关字段"""
        # 解析User-Agent
        user_agent = parsed_data.get('agent', '') or ''
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
        
        # 使用新的URI解析策略获取核心信息
        uri = dwd_record['request_uri']
        parsed_uri = self._parse_uri_structure(uri)
        
        # 核心字段映射
        dwd_record['application_name'] = self._extract_application_name(user_agent, uri)
        dwd_record['service_name'] = self._extract_service_name(uri)
        dwd_record['api_module'] = self._extract_api_module(uri)
        
        # 分类字段
        dwd_record['api_category'] = self._classify_api_category(uri)
        dwd_record['api_version'] = self._extract_api_version(uri)
        dwd_record['business_domain'] = self._classify_business_domain(uri)
        dwd_record['access_type'] = self._classify_access_type_old(user_agent, uri)
        dwd_record['client_category'] = self._classify_client_category(user_agent)
        # 业务标识提取 - 基于多源信息
        dwd_record['business_sign'] = self._extract_business_sign(parsed_data, uri)
        
        # IP风险级别和地理信息
        client_ip = dwd_record['client_ip']  # 从已设置的字段获取IP
        dwd_record['client_region'] = self._infer_client_region(client_ip)
        dwd_record['client_isp'] = self._infer_client_isp(client_ip)
        dwd_record['ip_risk_level'] = self._assess_ip_risk_old(dwd_record['client_ip'])
        dwd_record['is_internal_ip'] = self._is_internal_ip(dwd_record['client_ip'])
        
        # IP类型检测 - 增强版
        dwd_record['is_tor_exit'] = self._detect_tor_exit(client_ip)
        dwd_record['is_proxy'] = self._detect_proxy(client_ip, parsed_data)
        dwd_record['is_vpn'] = self._detect_vpn(client_ip)
        dwd_record['is_datacenter'] = self._detect_datacenter_ip(client_ip)
        dwd_record['is_bot'] = self._detect_bot_request(ua_info, parsed_data)
        
        # API重要性评分 - 基于新的解析结果
        dwd_record['api_importance_level'] = self._assess_api_importance_v2(parsed_uri)
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
        
        # 性能分级体系 - 基于行业APM最佳实践 (2025-09-14)
        duration = dwd_record['total_request_duration']  # 已转换为毫秒

        # 布尔分级字段 (添加perf_前缀避免命名冲突)
        dwd_record['perf_attention'] = duration > 500    # 0.5秒关注阈值 - 开始影响用户体验
        dwd_record['perf_warning'] = duration > 1000     # 1秒预警阈值 - 明显性能问题
        dwd_record['perf_slow'] = duration > 3000        # 3秒慢请求阈值 - 严重性能问题
        dwd_record['perf_very_slow'] = duration > 10000  # 10秒严重阈值 - 系统异常
        dwd_record['perf_timeout'] = duration > 30000    # 30秒超时阈值 - 系统故障级别

        # 保留原有字段以兼容现有逻辑
        dwd_record['is_slow'] = dwd_record['perf_slow']
        dwd_record['is_very_slow'] = dwd_record['perf_very_slow']

        # 性能等级数值字段 (便于计算和聚合分析)
        if duration <= 200:
            dwd_record['performance_level'] = 1  # excellent (0-200ms)
        elif duration <= 500:
            dwd_record['performance_level'] = 2  # good (200-500ms)
        elif duration <= 1000:
            dwd_record['performance_level'] = 3  # acceptable (500ms-1s)
        elif duration <= 3000:
            dwd_record['performance_level'] = 4  # slow (1-3s)
        elif duration <= 30000:
            dwd_record['performance_level'] = 5  # critical (3-30s)
        else:
            dwd_record['performance_level'] = 6  # timeout (>30s)
        
        # 增强超时判断 (结合时间阈值和状态码)
        if not dwd_record.get('is_timeout'):  # 如果还没设置为超时
            dwd_record['is_timeout'] = status_code in ['408', '504', '524']  # 特定状态码也算超时
        dwd_record['is_retry'] = self._detect_retry_request(dwd_record)
        
        # 异常检测（简化版）
        dwd_record['has_anomaly'] = (dwd_record['is_server_error'] or 
                                   dwd_record['is_very_slow'] or
                                   dwd_record['response_body_size'] > 10 * 1024 * 1024)  # 超过10MB
        dwd_record['anomaly_type'] = self._classify_anomaly_type(dwd_record)
        
        # 用户体验级别
        dwd_record['user_experience_level'] = self._classify_user_experience(duration, dwd_record['is_success'])
        
        # APDEX分类 (基于毫秒阈值)
        if duration <= 1500:      # 1.5秒 = 1500毫秒
            apdex = 'satisfied'
        elif duration <= 6000:    # 6秒 = 6000毫秒
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
        try:
            dwd_record['data_quality_score'] = quality_score
        except Exception as e:
            self.logger.warning(f"数据质量计算失败: {e}")
            dwd_record['data_quality_score'] = 0.0
            
    def _create_fallback_record(self, parsed_data: Dict[str, Any], source_file: str, error_msg: str) -> Dict[str, Any]:
        """创建容错备用记录 - 当字段映射失败时使用"""
        try:
            fallback_record = {
                # === 基础必需字段（使用安全的默认值） ===
                'client_ip': parsed_data.get('remote_addr', '0.0.0.0'),
                'client_port': 0,
                'xff_ip': parsed_data.get('RealIp', parsed_data.get('remote_addr', '0.0.0.0')),
                'server_name': parsed_data.get('http_host', 'unknown'),
                'request_method': 'UNKNOWN',
                'request_uri': '/',
                'request_uri_normalized': '/',
                'request_full_uri': parsed_data.get('request', ''),
                'http_protocol_version': 'HTTP/1.1',
                'query_parameters': '',
                'user_agent_string': parsed_data.get('agent', '') or '',
                'referer_url': parsed_data.get('http_referer', ''),
                'referer_domain': '',
                'log_source_file': source_file,
                
                # === 时间字段（使用当前时间作为备用） ===
                'log_time': datetime.now(),
                'date_partition': datetime.now().date(),
                'hour_partition': datetime.now().hour,
                'minute_partition': datetime.now().minute,
                'second_partition': datetime.now().second,
                
                # === 响应字段 ===
                'response_status_code': str(parsed_data.get('code', '0')),
                'response_size_bytes': int(parsed_data.get('body', 0)) if parsed_data.get('body', '').isdigit() else 0,
                'response_time_ms': 0.0,
                'upstream_response_time_ms': 0.0,
                'total_request_time_ms': 0.0,
                
                # === 业务字段（使用默认值） ===
                'platform': 'unknown',
                'platform_version': '',
                'app_version': '',
                'device_type': 'unknown',
                'browser_type': 'unknown',
                'os_type': 'unknown',
                'os_version': '',
                'sdk_type': '',
                'sdk_version': '',
                'bot_type': '',
                'entry_source': 'unknown',
                'search_engine': '',
                'social_media': '',
                'application_name': 'unknown',
                'service_name': 'unknown',
                'api_module': 'unknown',
                'api_category': 'unknown',
                'api_version': '',
                'business_domain': 'unknown',
                'access_type': 'unknown',
                'client_category': 'unknown',
                'business_sign': 'error.fallback',
                'client_region': 'unknown',
                'client_isp': 'unknown',
                'ip_risk_level': 'unknown',
                'is_internal_ip': False,
                'api_importance_level': 'low',
                'business_value_score': 0,
                'upstream_server': '',
                'cluster_node': 'unknown',
                'load_balancer_node': '',
                'cdn_pop': '',
                'trace_id': '',
                'span_id': '',
                'parent_span_id': '',
                'service_mesh_info': '',
                'kubernetes_info': '',
                
                # === 错误标记字段 ===
                'parsing_errors': [f'字段映射失败: {error_msg}'],
                'data_quality_score': 0.0,
                'is_fallback_record': True,
                'fallback_reason': error_msg,
                'fallback_timestamp': datetime.now().isoformat()
            }
            
            return fallback_record
            
        except Exception as fallback_error:
            # 如果连备用记录都创建失败，返回最小化记录
            self.logger.error(f"创建备用记录失败: {fallback_error}")
            return {
                'client_ip': '0.0.0.0',
                'client_port': 0,
                'log_time': datetime.now(),
                'request_method': 'ERROR',
                'request_uri': '/error',
                'response_status_code': '0',
                'response_size_bytes': 0,
                'is_fallback_record': True,
                'fallback_reason': f'完全失败: {error_msg}',
                'parsing_errors': [error_msg, str(fallback_error)]
            }
    
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
        """解析User-Agent - 增强版，提取更多详细信息"""
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
        
        # 1. SDK优先检测（最高优先级）
        sdk_match = re.search(r'WST-SDK-(iOS|Android|ANDROID)(?:/([0-9.]+))?', user_agent, re.I)
        if sdk_match:
            platform = sdk_match.group(1).upper()
            version = sdk_match.group(2) if sdk_match.group(2) else 'unknown'
            ua_info['sdk_type'] = f'WST-SDK-{platform}'
            ua_info['sdk_version'] = version
            ua_info['platform'] = 'iOS' if platform == 'IOS' else 'Android'
            ua_info['os_type'] = ua_info['platform']
            ua_info['device_type'] = 'Mobile'
            return ua_info
        
        # 2. iOS平台详细解析
        ios_match = re.search(r'iPhone.*?OS ([0-9_]+).*?like Mac OS X', user_agent)
        if ios_match:
            ua_info['platform'] = 'iOS'
            ua_info['os_type'] = 'iOS'
            ua_info['device_type'] = 'Mobile'
            ua_info['os_version'] = ios_match.group(1).replace('_', '.')
            
            # 检测Safari版本
            safari_match = re.search(r'Version/([0-9.]+).*?Safari', user_agent)
            if safari_match:
                ua_info['browser_type'] = 'Safari'
                ua_info['platform_version'] = safari_match.group(1)
            
            # 检测WebKit版本
            webkit_match = re.search(r'AppleWebKit/([0-9.]+)', user_agent)
            if webkit_match and not safari_match:
                ua_info['browser_type'] = 'WebView'
                ua_info['platform_version'] = webkit_match.group(1)
            
            # 检测支付宝小程序
            if 'Ariver' in user_agent and 'AliApp' in user_agent:
                alipay_match = re.search(r'AliApp\(AP/([0-9.]+)', user_agent)
                if alipay_match:
                    ua_info['app_version'] = alipay_match.group(1)
                    ua_info['sdk_type'] = 'Alipay-MiniApp'
                    
            return ua_info
        
        # 3. Android平台详细解析
        android_match = re.search(r'Android ([0-9.]+)', user_agent)
        if android_match:
            ua_info['platform'] = 'Android'
            ua_info['os_type'] = 'Android'
            ua_info['device_type'] = 'Mobile'
            ua_info['os_version'] = android_match.group(1)
            
            # 检测Chrome版本
            chrome_match = re.search(r'Chrome/([0-9.]+)', user_agent)
            if chrome_match:
                ua_info['browser_type'] = 'Chrome'
                ua_info['platform_version'] = chrome_match.group(1)
            
            # 检测WebView
            elif 'wv' in user_agent.lower():
                ua_info['browser_type'] = 'WebView'
            
            return ua_info
        
        # 4. 桌面浏览器解析
        if 'Windows' in user_agent or 'Macintosh' in user_agent or 'X11' in user_agent:
            ua_info['device_type'] = 'Desktop'
            
            # Windows版本检测
            win_match = re.search(r'Windows NT ([0-9.]+)', user_agent)
            if win_match:
                ua_info['os_type'] = 'Windows'
                ua_info['os_version'] = win_match.group(1)
            
            # Mac版本检测
            mac_match = re.search(r'Mac OS X ([0-9_]+)', user_agent)
            if mac_match:
                ua_info['os_type'] = 'macOS'
                ua_info['os_version'] = mac_match.group(1).replace('_', '.')
            
            # 浏览器版本检测 - 增强版
            browser_patterns = {
                'Chrome': r'Chrome/([0-9.]+)',
                'Edge': r'Edg(?:e)?/([0-9.]+)',  # 支持新旧Edge
                'Firefox': r'Firefox/([0-9.]+)',
                'Safari': r'Version/([0-9.]+).*Safari',
                'Opera': r'(?:Opera|OPR)/([0-9.]+)',
                'Internet Explorer': r'MSIE ([0-9.]+)',
                'Vivaldi': r'Vivaldi/([0-9.]+)',
                'Brave': r'Brave/([0-9.]+)'
            }
            
            for browser, pattern in browser_patterns.items():
                match = re.search(pattern, user_agent)
                if match:
                    ua_info['browser_type'] = browser
                    ua_info['platform_version'] = match.group(1)
                    break
            
            return ua_info
        
        # 5. 机器人/爬虫检测 - 增强版
        bot_patterns = {
            'Baiduspider': r'Baiduspider(?:/([0-9.]+))?',
            'Googlebot': r'Googlebot(?:/([0-9.]+))?',
            'YisouSpider': r'YisouSpider(?:/([0-9.]+))?',
            'Bingbot': r'bingbot(?:/([0-9.]+))?',
            'Sogou Spider': r'Sogou.*spider(?:/([0-9.]+))?',
            'DuckDuckBot': r'DuckDuckBot(?:/([0-9.]+))?',
            'Twitterbot': r'Twitterbot(?:/([0-9.]+))?',
            'Facebookexternalhit': r'facebookexternalhit(?:/([0-9.]+))?',
            'LinkedInBot': r'LinkedInBot(?:/([0-9.]+))?',
            'Applebot': r'Applebot(?:/([0-9.]+))?',
            'AhrefsBot': r'AhrefsBot(?:/([0-9.]+))?'
        }
        
        for bot_name, pattern in bot_patterns.items():
            match = re.search(pattern, user_agent, re.I)
            if match:
                ua_info['bot_type'] = bot_name
                ua_info['device_type'] = 'Bot'
                ua_info['platform_version'] = match.group(1) if match.group(1) else 'unknown'
                return ua_info
        
        # 6. 通用机器人检测
        if re.search(self.user_agent_patterns['bot'], user_agent, re.I):
            ua_info['bot_type'] = 'unknown_bot'
            ua_info['device_type'] = 'Bot'
        
        # 7. 平板设备检测
        elif re.search(r'iPad|Tablet|PlayBook|Kindle', user_agent, re.I):
            ua_info['device_type'] = 'Tablet'
            if 'iPad' in user_agent:
                ua_info['platform'] = 'iOS'
                ua_info['os_type'] = 'iPadOS'
                ipad_os_match = re.search(r'OS ([0-9_]+)', user_agent)
                if ipad_os_match:
                    ua_info['os_version'] = ipad_os_match.group(1).replace('_', '.')
        
        # 8. 移动设备通用检测
        elif re.search(self.user_agent_patterns['mobile'], user_agent, re.I):
            ua_info['device_type'] = 'Mobile'
        
        # 8. 特殊应用检测
        if user_agent and 'zgt-ios' in user_agent.lower():
            zgt_match = re.search(r'zgt-ios[/\s]?([0-9.]+)', user_agent, re.I)
            if zgt_match:
                ua_info['app_version'] = zgt_match.group(1)
            ua_info['sdk_type'] = 'ZGT-iOS'
        
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
        """分类API分类 - 基于新的URI解析策略"""
        if not uri:
            return 'unknown'
        
        parsed = self._parse_uri_structure(uri)
        
        # 静态资源优先判断
        if parsed['is_static_resource']:
            return 'static'
        
        app_name = parsed['application_name']
        service_name = parsed['service_name']
        
        # 基于应用类型的精确分类
        if app_name == 'scmp-gateway':
            return 'gateway-api'
        elif app_name == 'zgt-h5':
            if service_name in ('js', 'css', 'images'):
                return 'static'
            else:
                return 'h5-page'
        elif app_name == 'group1':
            return 'file-service'
        elif service_name == 'api' or 'api' in uri:
            return 'rest-api'
        elif app_name in ('admin', 'manage', 'system'):
            return 'admin-api'
        elif app_name in ('upload', 'download'):
            return 'file-api'
        else:
            return 'service-api'
    
    def _parse_uri_structure(self, uri: str) -> dict:
        """
        全新URI解析策略 - 基于实际日志数据分析
        
        分析原则：
        1. /scmp-gateway/gxrz-rest/newUser/queryUserBindAesV2
           - application_name: scmp-gateway (网关应用)
           - service_name: gxrz-rest (具体服务)
           - api_module: newUser (功能模块)
           - api_endpoint: queryUserBindAesV2 (具体接口)
           
        2. /zgt-h5/js/transferPlatformTool.min.js
           - application_name: zgt-h5 (H5应用)
           - service_name: js (静态资源服务)
           - api_module: transferPlatformTool (具体模块)
           
        3. /group1/M00/05/39/file.png
           - application_name: group1 (文件系统)
           - service_name: M00 (存储服务)
           - api_module: files (文件模块)
        
        Args:
            uri: 请求URI
            
        Returns:
            dict: 包含 application_name, service_name, api_module 等信息
        """
        if not uri or not uri.startswith('/'):
            return {
                'application_name': 'unknown',
                'service_name': 'unknown', 
                'api_module': 'unknown',
                'api_endpoint': '',
                'path_depth': 0,
                'is_static_resource': False,
                'resource_type': 'unknown'
            }
        
        # 清理URI并分割路径
        clean_uri = uri.split('?')[0].strip('/')  # 移除查询参数
        parts = clean_uri.split('/') if clean_uri else []
        depth = len(parts)
        
        # 初始化结果
        result = {
            'application_name': parts[0] if depth > 0 else 'unknown',
            'service_name': 'unknown',
            'api_module': 'unknown', 
            'api_endpoint': '',
            'path_depth': depth,
            'is_static_resource': False,
            'resource_type': 'unknown'
        }
        
        # 静态资源检测
        if depth > 0:
            last_part = parts[-1].lower()
            static_extensions = {
                '.js': 'javascript',
                '.css': 'stylesheet', 
                '.png': 'image',
                '.jpg': 'image',
                '.jpeg': 'image',
                '.gif': 'image',
                '.svg': 'image',
                '.ico': 'icon',
                '.woff': 'font',
                '.woff2': 'font',
                '.ttf': 'font'
            }
            
            for ext, res_type in static_extensions.items():
                if last_part.endswith(ext):
                    result['is_static_resource'] = True
                    result['resource_type'] = res_type
                    break
        
        # 根据不同应用类型解析
        if depth >= 2:
            app_name = parts[0]
            
            # scmp-gateway 网关类型: /scmp-gateway/service/module/endpoint
            if app_name == 'scmp-gateway':
                result['service_name'] = parts[1]  # gxrz-rest, alipay, column, thirdSpecial, zww
                if depth >= 3:
                    result['api_module'] = parts[2]  # newUser, api, weixinJsSdkSign, appKind
                if depth >= 4:
                    result['api_endpoint'] = parts[3]  # 具体接口
                    
            # zgt-h5 H5应用类型: /zgt-h5/service/resource
            elif app_name == 'zgt-h5':
                result['service_name'] = parts[1] if depth >= 2 else 'unknown'  # js, css, pages
                if result['is_static_resource']:
                    # 静态资源: /zgt-h5/js/transferPlatformTool.min.js
                    filename = parts[-1].split('.')[0]  # transferPlatformTool.min -> transferPlatformTool
                    result['api_module'] = filename.split('.')[0]  # 移除.min后缀
                else:
                    result['api_module'] = parts[2] if depth >= 3 else 'unknown'
                    
            # group1 文件系统类型: /group1/M00/path/to/file
            elif app_name == 'group1':
                result['service_name'] = parts[1] if depth >= 2 else 'unknown'  # M00, M01
                result['api_module'] = 'files'  # 统一为文件模块
                
            # 其他应用类型的通用解析
            else:
                result['service_name'] = parts[1]
                if depth >= 3:
                    result['api_module'] = parts[2]
                if depth >= 4:
                    result['api_endpoint'] = parts[3]
        
        return result
    
    def _extract_uri_levels(self, uri: str) -> tuple:
        """提取URI的第一层和第二层 - 保持向后兼容"""
        if not uri or not uri.startswith('/'):
            return '', ''
        
        parts = uri.strip('/').split('/')
        first_level = parts[0] if len(parts) > 0 else ''
        second_level = parts[1] if len(parts) > 1 else ''
        return first_level, second_level
    
    def _extract_api_module(self, uri: str) -> str:
        """提取API模块 - 基于新的URI解析策略"""
        if not uri:
            return ''
        
        parsed = self._parse_uri_structure(uri)
        return parsed['api_module']
    
    def _extract_api_version(self, uri: str) -> str:
        """提取API版本 - 增强版本检测"""
        if not uri:
            return ''
        
        # 常见的版本模式
        version_patterns = [
            r'/v(\d+(?:\.\d+)?)',           # v1, v1.0, v2.1
            r'/version(\d+)',               # version1
            r'/api/v(\d+)',                 # api/v1
            r'(\d+\.\d+\.\d+)',            # 1.0.0 格式
            r'(\d+\.\d+)',                 # 1.0 格式
        ]
        
        for pattern in version_patterns:
            match = re.search(pattern, uri, re.IGNORECASE)
            if match:
                return f"v{match.group(1)}"
        
        return ''
    
    def _classify_business_domain(self, uri: str) -> str:
        """分类业务域 - 基于新的URI解析策略的精确分类"""
        if not uri:
            return 'unknown'
        
        parsed = self._parse_uri_structure(uri)
        app_name = parsed['application_name']
        service_name = parsed['service_name']
        api_module = parsed['api_module']
        
        # 基于应用和服务的精确业务域分类
        if app_name == 'scmp-gateway':
            # 根据服务名进一步细分
            service_domain_mapping = {
                'gxrz-rest': 'user-auth',      # 用户认证
                'alipay': 'payment',           # 支付服务
                'column': 'content',           # 内容服务
                'thirdSpecial': 'third-party', # 第三方服务
                'zww': 'government',           # 政务服务
            }
            return service_domain_mapping.get(service_name, 'gateway')
            
        elif app_name == 'zgt-h5':
            return 'h5-frontend'
            
        elif app_name == 'group1':
            return 'file-storage'
            
        else:
            # 通用映射
            domain_mapping = {
                'api': 'api-service',
                'admin': 'admin-service',
                'user': 'user-service',
                'auth': 'auth-service',
                'upload': 'file-service',
                'download': 'file-service',
                'search': 'search-service',
                'notification': 'message-service'
            }
            return domain_mapping.get(app_name, app_name)
    
    def _classify_access_type_old(self, user_agent: str, uri: str) -> str:
        """分类访问类型 - 基于User-Agent和URI的综合判断，更精确的分类"""
        if not user_agent:
            user_agent = ''
            
        ua_lower = user_agent.lower() if user_agent else ''
        parsed = self._parse_uri_structure(uri)
        
        # 第一优先级：爬虫/机器人检测
        if any(bot in ua_lower for bot in ['spider', 'bot', 'crawler']):
            spider_types = {
                'baiduspider': 'Baidu_Spider',
                'googlebot': 'Google_Bot',
                'yisou': 'Yisou_Spider',
                'bingbot': 'Bing_Bot'
            }
            for bot_name, bot_type in spider_types.items():
                if bot_name in ua_lower:
                    return bot_type
            return 'Web_Crawler'
        
        # 第二优先级：SDK/API工具
        if any(keyword in ua_lower for keyword in ['wst-sdk', 'sdk-ios', 'sdk-android']):
            return 'Native_SDK'
        elif any(keyword in ua_lower for keyword in ['curl', 'wget', 'postman', 'httpie']):
            return 'API_Tool'
            
        # 第三优先级：第三方平台判断
        elif 'alipay' in ua_lower and 'ariver' in ua_lower:
            return 'Alipay_MiniApp'
        elif 'micromessenger' in ua_lower or 'wechat' in ua_lower:
            return 'WeChat_MiniApp'
            
        # 第四优先级：移动端细分
        elif 'iphone' in ua_lower or 'ios' in ua_lower:
            if 'safari' in ua_lower and 'version' in ua_lower:
                return 'iOS_Safari'
            elif 'webkit' in ua_lower:
                return 'iOS_WebView'
            else:
                return 'iOS_Native'
                
        elif 'android' in ua_lower:
            if 'chrome' in ua_lower and 'version' in ua_lower:
                return 'Android_Chrome'
            elif 'webkit' in ua_lower:
                return 'Android_WebView'
            else:
                return 'Android_Native'
        
        # 第五优先级：桌面浏览器分类
        elif 'chrome' in ua_lower and 'mozilla' in ua_lower:
            return 'Desktop_Chrome'
        elif 'firefox' in ua_lower:
            return 'Desktop_Firefox'
        elif 'safari' in ua_lower and 'mac' in ua_lower:
            return 'Desktop_Safari'
        elif 'edge' in ua_lower:
            return 'Desktop_Edge'
        
        # 第六优先级：按照URI类型分类
        elif parsed['application_name'] == 'zgt-h5':
            return 'H5_WebApp'
        elif parsed['application_name'] == 'scmp-gateway':
            return 'Gateway_API'
        elif parsed['is_static_resource']:
            return 'Static_Resource'
            
        else:
            return 'Unknown_Client'
    
    def _classify_client_category(self, user_agent: str) -> str:
        """分类客户端类别"""
        if user_agent and 'bot' in user_agent.lower():
            return 'bot'
        elif 'SDK' in user_agent:
            return 'sdk'
        else:
            return 'user'
    
    def _extract_application_name(self, user_agent: str, uri: str) -> str:
        """提取应用名称 - 优先从URI第一层获取，补充User-Agent信息"""
        # 优先使用URI第一层作为应用名
        first_level, _ = self._extract_uri_levels(uri)
        if first_level:
            return first_level
            
        # 如果URI无法提供信息，从User-Agent推断
        if user_agent and 'zgt-ios' in user_agent.lower():
            return 'zgt-ios'
        elif 'WST-SDK' in user_agent:
            return 'WST-SDK'
        elif 'Android' in user_agent:
            return 'android-app'
        elif 'iPhone' in user_agent or 'iOS' in user_agent:
            return 'ios-app'
        elif 'Mozilla' in user_agent and 'Chrome' in user_agent:
            return 'web-browser'
        else:
            return 'unknown'
    
    def _extract_service_name(self, uri: str) -> str:
        """提取服务名称 - 使用第二层路径"""
        if not uri:
            return ''
        
        _, second_level = self._extract_uri_levels(uri)
        return second_level if second_level else 'unknown'
    
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
    
    def _assess_ip_risk_old(self, ip: str) -> str:
        """评估IP风险级别"""
        if self._is_internal_ip(ip):
            return 'Low'
        
        # 简化的风险评估
        return 'Medium'
    
    def _infer_client_region(self, ip: str) -> str:
        """推断客户端地理区域"""
        if not ip:
            return 'unknown'
        
        if self._is_internal_ip(ip):
            return 'internal'
        
        # 基于IP段的简单地理推断（生产环境应使用专业IP地理库）
        if ip.startswith('202.'):
            return 'Beijing'
        elif ip.startswith('218.'):
            return 'Shanghai' 
        elif ip.startswith('210.'):
            return 'Guangdong'
        elif ip.startswith('61.'):
            return 'Jiangsu'
        elif ip.startswith('58.'):
            return 'Sichuan'
        else:
            return 'other'
    
    def _infer_client_isp(self, ip: str) -> str:
        """推断客户端ISP运营商"""
        if not ip:
            return 'unknown'
            
        if self._is_internal_ip(ip):
            return 'internal'
        
        # 基于IP段的简单ISP推断（生产环境应使用专业IP库）
        if ip.startswith(('202.', '218.', '222.')):
            return 'China_Telecom'
        elif ip.startswith(('61.', '210.', '211.')):
            return 'China_Unicom'
        elif ip.startswith(('58.', '120.', '125.')):
            return 'China_Mobile'
        elif ip.startswith('59.'):
            return 'China_Education'
        else:
            return 'other_isp'
    
    def _detect_retry_request(self, dwd_record: Dict[str, Any]) -> bool:
        """检测是否为重试请求"""
        # 简单的重试检测逻辑
        user_agent = dwd_record.get('user_agent_string', '')
        uri = dwd_record.get('request_uri', '')
        
        # 检测重试标识
        retry_indicators = ['retry', 'again', 'resend']
        return any(indicator in uri.lower() for indicator in retry_indicators)
    
    def _detect_tor_exit(self, ip: str) -> bool:
        """检测是否为Tor出口节点"""
        if not ip or self._is_internal_ip(ip):
            return False
        
        # 简化实现：真实环境应查询Tor出口节点列表
        # 这里仅提供框架，生产环境需要接入Tor出口节点数据库
        tor_known_ranges = ['185.220.', '199.87.', '204.11.']
        return any(ip.startswith(prefix) for prefix in tor_known_ranges)
    
    def _detect_proxy(self, ip: str, parsed_data: Dict[str, Any]) -> bool:
        """检测是否通过代理访问"""
        if not ip:
            return False
        
        # 检测代理头部标识
        proxy_headers = ['HTTP_X_FORWARDED_FOR', 'HTTP_X_REAL_IP', 'HTTP_X_PROXY_ID']
        for header in proxy_headers:
            if parsed_data.get(header):
                return True
        
        # 检测已知代理IP段
        proxy_ranges = ['103.21.', '173.245.', '108.162.']  # Cloudflare等CDN
        return any(ip.startswith(prefix) for prefix in proxy_ranges)
    
    def _detect_vpn(self, ip: str) -> bool:
        """检测是否为VPN"""
        if not ip or self._is_internal_ip(ip):
            return False
        
        # 简化实现：检测已知VPN服务商IP段
        vpn_ranges = ['45.132.', '194.39.', '91.200.']  # 常见VPN提供商IP段
        return any(ip.startswith(prefix) for prefix in vpn_ranges)
    
    def _detect_datacenter_ip(self, ip: str) -> bool:
        """检测是否为数据中心IP"""
        if not ip or self._is_internal_ip(ip):
            return False
        
        # 检测已知数据中心IP段
        datacenter_ranges = [
            '104.16.',   # Cloudflare
            '13.107.',   # Microsoft Azure
            '52.84.',    # AWS CloudFront
            '142.250.',  # Google
            '157.240.'   # Facebook
        ]
        return any(ip.startswith(prefix) for prefix in datacenter_ranges)
    
    def _detect_bot_request(self, ua_info: Dict[str, str], parsed_data: Dict[str, Any]) -> bool:
        """检测是否为机器人请求"""
        # 1. User-Agent中已检测到bot
        if ua_info.get('bot_type') or ua_info.get('device_type') == 'Bot':
            return True
        
        # 2. 检测请求模式（快速连续请求等）
        uri = parsed_data.get('request', '')
        if '/robots.txt' in uri or '/sitemap' in uri:
            return True
        
        # 3. 缺少常见浏览器特征
        user_agent = parsed_data.get('agent', '')
        if user_agent and len(user_agent) < 20:  # User-Agent过短
            return True
        
        return False
    
    def _assess_api_importance(self, uri: str) -> str:
        """评估API重要性 - 保持向后兼容"""
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
            
    def _assess_api_importance_v2(self, parsed_uri: dict) -> str:
        """评估API重要性 - 基于新的URI解析策略"""
        app_name = parsed_uri['application_name']
        service_name = parsed_uri['service_name'] 
        api_module = parsed_uri['api_module']
        
        # 关键业务API
        critical_patterns = [
            (app_name == 'scmp-gateway' and service_name == 'gxrz-rest' and api_module in ['login', 'auth', 'token']),
            (service_name == 'alipay' and 'payment' in api_module.lower()),
            ('auth' in api_module.lower()),
            ('login' in api_module.lower()),
        ]
        
        if any(critical_patterns):
            return 'critical'
            
        # 高重要性API  
        high_patterns = [
            (app_name == 'scmp-gateway' and service_name == 'alipay'),
            (service_name == 'gxrz-rest' and api_module in ['newUser', 'userInfo']),
            (service_name == 'zww' and 'query' in api_module.lower()),
            ('user' in api_module.lower()),
            ('payment' in api_module.lower()),
        ]
        
        if any(high_patterns):
            return 'high'
            
        # 中等重要性API
        medium_patterns = [
            (app_name == 'scmp-gateway'),
            (service_name in ['column', 'thirdSpecial']),
            (parsed_uri['is_static_resource'] == False and parsed_uri['path_depth'] >= 3),
        ]
        
        if any(medium_patterns):
            return 'medium'
            
        return 'low'
    
    def _calculate_business_value_score(self, dwd_record: Dict[str, Any]) -> int:
        """计算业务价值评分"""
        score = 50  # 基础分
        
        # 根据API重要性调整
        importance = dwd_record.get('api_importance_level', 'low')
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
    
    def _extract_infrastructure_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """提取基础设施字段 - 增强版"""
        # 上游服务器信息提取
        upstream_addr = parsed_data.get('upstream_addr', '')
        if upstream_addr and upstream_addr != '-':
            # 处理多个上游地址的情况，取第一个
            if ',' in upstream_addr:
                upstream_addr = upstream_addr.split(',')[0].strip()
            dwd_record['upstream_server'] = upstream_addr
        else:
            dwd_record['upstream_server'] = ''
        
        # 集群节点信息 - 基于多种来源推断
        cluster_node = ''
        
        # 1. 从upstream_addr推断
        if upstream_addr:
            # 尝试从IP:端口提取节点信息
            import re
            node_match = re.search(r'(\d+\.\d+\.\d+\.\d+):(\d+)', upstream_addr)
            if node_match:
                ip, port = node_match.groups()
                cluster_node = f"node-{ip.split('.')[-1]}-{port}"
        
        # 2. 从HTTP头中提取（如果有的话）
        if not cluster_node:
            server_name = parsed_data.get('server_name', '')
            if server_name and server_name != '_':
                cluster_node = f"node-{server_name.split('.')[0]}"
        
        # 3. 从请求特征推断
        if not cluster_node:
            user_agent = parsed_data.get('agent', '') or ''
            if 'WST-SDK' in user_agent:
                cluster_node = 'api-cluster'
            elif '/scmp-gateway/' in parsed_data.get('request', ''):
                cluster_node = 'gateway-cluster'
            else:
                cluster_node = 'web-cluster'
        
        dwd_record['cluster_node'] = cluster_node
        
        # 链路跟踪ID提取
        trace_id = ''
        
        # 1. 从HTTP头中提取
        request_id = parsed_data.get('request_id', '')
        if request_id and request_id != '-':
            trace_id = request_id
        
        # 2. 从请求体或查询参数中提取（简化）
        if not trace_id:
            request_str = parsed_data.get('request', '')
            import re
            trace_match = re.search(r'traceId=([a-f0-9-]+)', request_str, re.I)
            if trace_match:
                trace_id = trace_match.group(1)
        
        # 3. 生成简化的trace_id（基于时间和IP）
        if not trace_id:
            import hashlib
            import time
            content = f"{parsed_data.get('time', '')}-{parsed_data.get('remote_addr', '')}"
            trace_id = hashlib.md5(content.encode()).hexdigest()[:16]
        
        dwd_record['trace_id'] = trace_id
        
        # 连接信息
        connection_requests = parsed_data.get('connection_requests', '1')
        dwd_record['connection_requests'] = self._safe_int(connection_requests, 1)
        
        # 缓存状态 - 基于多种指标判断
        cache_status = 'unknown'
        
        # 1. 直接从日志中获取
        cache_field = parsed_data.get('cache_status', parsed_data.get('upstream_cache_status', ''))
        if cache_field and cache_field != '-':
            cache_status = cache_field.lower()
        else:
            # 2. 基于响应时间推断
            response_time = self._safe_float(parsed_data.get('ar_time', '0'))
            if response_time < 0.1:  # 小于100ms很可能是缓存命中
                cache_status = 'hit'
            elif response_time > 3.0:  # 大于3秒很可能是缓存失效
                cache_status = 'miss'
            else:
                cache_status = 'unknown'
        
        dwd_record['cache_status'] = cache_status
    
    def _extract_business_sign(self, parsed_data: Dict[str, Any], uri: str) -> str:
        """提取业务标识 - 基于多源信息"""
        business_sign = ''
        
        # 1. 从URI路径提取业务标识
        if '/scmp-gateway/' in uri:
            # 提取服务标识
            import re
            service_match = re.search(r'/scmp-gateway/([^/]+)', uri)
            if service_match:
                service = service_match.group(1)
                business_sign = f"gateway.{service}"
        
        # 2. 从User-Agent提取应用标识
        if not business_sign:
            user_agent = parsed_data.get('agent', '') or ''
            if 'WST-SDK-iOS' in user_agent:
                business_sign = 'mobile.ios'
            elif 'WST-SDK-Android' in user_agent:
                business_sign = 'mobile.android'
            elif 'zgt-ios' in user_agent:
                business_sign = 'app.zgt'
        
        # 3. 从请求特征推断
        if not business_sign:
            if '/api/' in uri:
                business_sign = 'api.service'
            elif '/h5/' in uri:
                business_sign = 'h5.webapp'
            elif uri.endswith(('.js', '.css', '.png', '.jpg')):
                business_sign = 'static.resource'
            else:
                business_sign = 'web.default'
        
        return business_sign

    # ================================
    # v3.0 新增维度字段映射方法
    # ================================
    
    def _map_permission_control_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射权限控制维度字段"""
        server_name = parsed_data.get('http_host', '')
        
        # 租户代码推断（基于域名）
        dwd_record['tenant_code'] = self._infer_tenant_code(server_name)
        dwd_record['team_code'] = self._infer_team_code(server_name)
        dwd_record['environment'] = self._infer_environment(server_name)
        dwd_record['data_sensitivity'] = 2  # internal级别(DDL中定义为Enum8: 1=public, 2=internal, 3=confidential, 4=restricted)
        dwd_record['cost_center'] = 'default'
        dwd_record['business_unit'] = 'default'
        dwd_record['region_code'] = self._infer_region_code(server_name)
        dwd_record['compliance_zone'] = 'default'
        
    def _map_platform_entry_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射平台入口下钻维度字段（核心功能）"""
        user_agent = parsed_data.get('agent', '') or ''
        server_name = parsed_data.get('http_host', '')
        
        # 访问入口点分析
        dwd_record['access_entry_point'] = self._infer_access_entry_point(server_name)
        dwd_record['client_channel'] = self._infer_client_channel(user_agent)
        dwd_record['client_type'] = self._classify_client_type(user_agent)
        dwd_record['traffic_source'] = self._analyze_traffic_source(parsed_data)
        dwd_record['access_type'] = self._classify_access_type(user_agent)
        
        # 平台分类字段
        platform_info = self._parse_user_agent(user_agent)
        dwd_record['platform_category'] = self._classify_platform_category(platform_info['platform'])
        dwd_record['platform_version'] = platform_info.get('platform_version', '')
        
    def _map_error_analysis_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射错误分析维度字段（工作介绍重点）"""
        status_code = parsed_data.get('code', '0')
        uri = parsed_data.get('request', '').split(' ', 2)[1] if parsed_data.get('request') else ''
        
        # 错误分组和严重程度
        dwd_record['error_code_group'] = self._classify_error_group(status_code)
        dwd_record['error_severity_level'] = self._assess_error_severity(status_code, uri)
        dwd_record['error_propagation_path'] = self._analyze_error_propagation(parsed_data)
        dwd_record['error_classification'] = self._classify_error_type(status_code)
        dwd_record['is_timeout_error'] = self._is_timeout_error(parsed_data)
        dwd_record['is_rate_limit_error'] = self._is_rate_limit_error(status_code)
        
    def _map_security_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射安全分析字段"""
        client_ip = parsed_data.get('remote_addr', '')
        user_agent = parsed_data.get('agent', '') or ''
        
        # IP安全分析
        dwd_record['client_ip_type'] = self._classify_ip_type(client_ip)
        dwd_record['client_ip_classification'] = self._classify_ip_security(client_ip)
        dwd_record['ip_risk_level'] = self._assess_ip_risk(client_ip, parsed_data)
        dwd_record['is_internal_ip'] = self._is_internal_ip(client_ip)
        
        # 威胁检测
        dwd_record['threat_level'] = self._assess_threat_level(parsed_data)
        dwd_record['attack_signature'] = self._detect_attack_signature(parsed_data)
        dwd_record['is_suspicious_request'] = self._is_suspicious_request(parsed_data)
        dwd_record['bot_detection_score'] = self._calculate_bot_score(user_agent)
        
    def _map_business_process_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射业务流程分析字段"""
        uri = parsed_data.get('request', '').split(' ', 2)[1] if parsed_data.get('request') else ''
        
        # 业务流程字段
        dwd_record['business_operation_type'] = self._classify_business_operation(uri)
        dwd_record['business_value_score'] = self._calculate_business_value(uri, parsed_data)
        dwd_record['user_journey_stage'] = self._identify_journey_stage(uri)
        dwd_record['process_step_type'] = self._classify_process_step(uri)
        dwd_record['conversion_funnel_stage'] = self._identify_funnel_stage(uri)
        dwd_record['business_priority'] = self._assess_business_priority(uri)
        
    # ================================
    # v3.0 辅助推断方法
    # ================================
    
    def _infer_tenant_code(self, server_name: str) -> str:
        """推断租户代码"""
        if not server_name:
            return 'default_tenant'
        if 'company-a' in server_name.lower():
            return 'company_a'
        elif 'company-b' in server_name.lower():
            return 'company_b'
        elif 'test' in server_name.lower():
            return 'test_tenant'
        return 'default_tenant'
        
    def _infer_team_code(self, server_name: str) -> str:
        """推断团队代码"""
        if not server_name:
            return 'default_team'
        if 'api' in server_name.lower():
            return 'backend_team'
        elif 'web' in server_name.lower():
            return 'frontend_team'
        return 'default_team'
        
    def _infer_environment(self, server_name: str) -> str:
        """推断环境标识"""
        if not server_name:
            return 'unknown'
        server_lower = server_name.lower()
        if any(env in server_lower for env in ['dev', 'develop']):
            return 'dev'
        elif any(env in server_lower for env in ['test', 'testing', 'qa']):
            return 'test'
        elif any(env in server_lower for env in ['staging', 'stage']):
            return 'staging'
        elif any(env in server_lower for env in ['prod', 'production']):
            return 'prod'
        return 'prod'  # 默认生产环境
        
    def _infer_region_code(self, server_name: str) -> str:
        """推断区域代码"""
        if not server_name:
            return 'default'
        if 'cn' in server_name.lower():
            return 'cn-north'
        elif 'us' in server_name.lower():
            return 'us-west'
        return 'default'
        
    def _infer_access_entry_point(self, server_name: str) -> str:
        """推断访问入口点（核心下钻维度）"""
        if not server_name:
            return 'direct'
        server_lower = server_name.lower()
        if 'gateway' in server_lower:
            return 'gateway'
        elif 'cdn' in server_lower:
            return 'cdn'
        elif 'proxy' in server_lower:
            return 'proxy'
        elif 'lb' in server_lower or 'load' in server_lower:
            return 'load_balancer'
        return 'direct'
        
    def _infer_client_channel(self, user_agent: str) -> str:
        """推断客户端渠道"""
        if not user_agent:
            return 'unknown'
        ua_lower = user_agent.lower()
        if 'official' in ua_lower:
            return 'official'
        elif 'partner' in ua_lower:
            return 'partner'
        elif any(third in ua_lower for third in ['third', 'external']):
            return 'third_party'
        return 'official'  # 默认官方
        
    def _classify_client_type(self, user_agent: str) -> str:
        """分类客户端类型"""
        if not user_agent:
            return 'unknown'
        ua_lower = user_agent.lower()
        if any(mobile in ua_lower for mobile in ['mobile', 'android', 'iphone']):
            return 'mobile_app'
        elif 'sdk' in ua_lower:
            return 'sdk_client'
        elif any(browser in ua_lower for browser in ['chrome', 'firefox', 'safari']):
            return 'web_browser'
        elif any(bot in ua_lower for bot in ['bot', 'crawler', 'spider']):
            return 'bot_crawler'
        return 'web_browser'
        
    def _analyze_traffic_source(self, parsed_data: Dict[str, Any]) -> str:
        """分析流量来源"""
        referer = parsed_data.get('referer', '') or ''
        if not referer or referer == '-':
            return 'direct'
        elif 'google.com' in referer.lower():
            return 'search_engine'
        elif 'baidu.com' in referer.lower():
            return 'search_engine'
        elif any(social in referer.lower() for social in ['wechat', 'weibo', 'qq']):
            return 'social_media'
        return 'referral'
        
    def _classify_access_type(self, user_agent: str) -> str:
        """分类接入方式"""
        if not user_agent:
            return 'Unknown'
        ua_lower = user_agent.lower()
        if 'android' in ua_lower:
            return 'APP_Native'
        elif 'iphone' in ua_lower or 'ios' in ua_lower:
            return 'APP_Native'
        elif 'webview' in ua_lower:
            return 'H5_WebView'
        elif any(browser in ua_lower for browser in ['chrome', 'firefox', 'safari']):
            return 'Browser'
        elif 'api' in ua_lower or 'sdk' in ua_lower:
            return 'API'
        return 'Browser'
        
    def _classify_platform_category(self, platform: str) -> str:
        """分类平台类别"""
        if not platform or platform == 'unknown':
            return 'unknown'
        platform_lower = platform.lower()
        if platform_lower in ['android', 'ios', 'harmonyos']:
            return 'mobile'
        elif platform_lower == 'web':
            return 'desktop'
        elif platform_lower in ['tablet', 'ipad']:
            return 'tablet'
        return 'unknown'
        
    def _classify_error_group(self, status_code: str) -> str:
        """分类错误组"""
        try:
            code = int(status_code)
            if 400 <= code < 500:
                return '4xx_client'
            elif 500 <= code < 600:
                return '5xx_server'
            elif 300 <= code < 400:
                return '3xx_redirect'
            return 'success'
        except (ValueError, TypeError):
            return 'unknown'
            
    def _assess_error_severity(self, status_code: str, uri: str) -> str:
        """评估错误严重程度"""
        try:
            code = int(status_code)
            if code >= 500:
                if any(critical in uri for critical in ['/payment', '/order', '/login']):
                    return 'critical'
                return 'high'
            elif code >= 400:
                if code in [401, 403, 404]:
                    return 'medium'
                return 'low'
            return 'info'
        except (ValueError, TypeError):
            return 'unknown'
            
    def _analyze_error_propagation(self, parsed_data: Dict[str, Any]) -> str:
        """分析错误传播路径"""
        # 简化实现，实际应该根据具体的架构来分析
        status_code = parsed_data.get('code', '0')
        try:
            code = int(status_code)
            if 500 <= code < 600:
                return 'client->gateway->service->db'
            elif 400 <= code < 500:
                return 'client->gateway'
            return ''
        except (ValueError, TypeError):
            return ''
            
    def _classify_error_type(self, status_code: str) -> str:
        """分类错误类型"""
        try:
            code = int(status_code)
            if code == 400:
                return 'bad_request'
            elif code == 401:
                return 'unauthorized'
            elif code == 403:
                return 'forbidden'
            elif code == 404:
                return 'not_found'
            elif code == 429:
                return 'rate_limit'
            elif code == 500:
                return 'internal_error'
            elif code == 502:
                return 'gateway_error'
            elif code == 503:
                return 'service_unavailable'
            elif code == 504:
                return 'gateway_timeout'
            return 'other'
        except (ValueError, TypeError):
            return 'unknown'
            
    def _is_timeout_error(self, parsed_data: Dict[str, Any]) -> bool:
        """判断是否超时错误"""
        status_code = parsed_data.get('code', '0')
        request_time = self._safe_float(parsed_data.get('ar_time'), 0.0)
        return status_code == '504' or request_time > 30.0
        
    def _is_rate_limit_error(self, status_code: str) -> bool:
        """判断是否限流错误"""
        return status_code == '429'
        
    def _classify_ip_type(self, client_ip: str) -> str:
        """分类IP类型"""
        if not client_ip:
            return 'unknown'
        if self._is_internal_ip(client_ip):
            return 'internal'
        elif client_ip.startswith('127.') or client_ip == 'localhost':
            return 'loopback'
        return 'external'
        
    def _classify_ip_security(self, client_ip: str) -> str:
        """分类IP安全级别"""
        if not client_ip:
            return 'unknown'
        if self._is_internal_ip(client_ip):
            return 'trusted'
        # 简化实现，实际应该查询威胁情报库
        return 'untrusted'
        
    def _assess_ip_risk(self, client_ip: str, parsed_data: Dict[str, Any]) -> str:
        """评估IP风险等级"""
        if self._is_internal_ip(client_ip):
            return 'safe'
        # 简化实现
        status_code = parsed_data.get('code', '0')
        if int(status_code) >= 400:
            return 'medium'
        return 'low'
        
    def _is_internal_ip(self, client_ip: str) -> bool:
        """判断是否内网IP"""
        if not client_ip:
            return False
        return (client_ip.startswith('10.') or 
                client_ip.startswith('192.168.') or 
                client_ip.startswith('172.'))
                
    def _assess_threat_level(self, parsed_data: Dict[str, Any]) -> str:
        """评估威胁等级"""
        # 简化实现
        uri = parsed_data.get('request', '')
        if any(suspicious in uri.lower() for suspicious in ['admin', 'config', 'backup']):
            return 'high'
        return 'low'
        
    def _detect_attack_signature(self, parsed_data: Dict[str, Any]) -> str:
        """检测攻击特征"""
        uri = parsed_data.get('request', '')
        if 'sql' in uri.lower() or 'script' in uri.lower():
            return 'injection_attempt'
        return 'normal'
        
    def _is_suspicious_request(self, parsed_data: Dict[str, Any]) -> bool:
        """判断是否可疑请求"""
        user_agent = parsed_data.get('agent', '') or ''
        return len(user_agent) < 10 or 'bot' in user_agent.lower()
        
    def _calculate_bot_score(self, user_agent: str) -> float:
        """计算机器人评分"""
        if not user_agent:
            return 0.8
        ua_lower = user_agent.lower()
        if any(bot in ua_lower for bot in ['bot', 'crawler', 'spider']):
            return 0.9
        elif len(user_agent) < 20:
            return 0.6
        return 0.1
        
    def _classify_business_operation(self, uri: str) -> str:
        """分类业务操作类型"""
        if not uri:
            return 'unknown'
        uri_lower = uri.lower()
        if '/payment' in uri_lower or '/pay' in uri_lower:
            return 'payment'
        elif '/order' in uri_lower:
            return 'order'
        elif '/login' in uri_lower or '/auth' in uri_lower:
            return 'authentication'
        elif '/register' in uri_lower:
            return 'registration'
        elif '/search' in uri_lower:
            return 'search'
        elif '/api/' in uri_lower:
            return 'api_call'
        return 'page_view'
        
    def _calculate_business_value(self, uri: str, parsed_data: Dict[str, Any]) -> int:
        """计算业务价值评分(UInt8: 0-255)"""
        if not uri:
            return 0
        uri_lower = uri.lower()
        if '/payment' in uri_lower:
            return 95  # 支付相关最高价值
        elif '/order' in uri_lower:
            return 85  # 订单相关高价值
        elif '/login' in uri_lower:
            return 70  # 登录认证中等价值
        elif '/api/' in uri_lower:
            return 50  # API接口基础价值
        return 30  # 默认基础价值
        
    def _identify_journey_stage(self, uri: str) -> str:
        """识别用户旅程阶段"""
        if not uri:
            return 'unknown'
        uri_lower = uri.lower()
        if uri_lower in ['/', '/home', '/index']:
            return 'landing'
        elif '/login' in uri_lower:
            return 'authentication'
        elif '/product' in uri_lower or '/item' in uri_lower:
            return 'browsing'
        elif '/cart' in uri_lower:
            return 'consideration'
        elif '/order' in uri_lower or '/checkout' in uri_lower:
            return 'purchase'
        elif '/payment' in uri_lower:
            return 'payment'
        elif '/success' in uri_lower or '/complete' in uri_lower:
            return 'completion'
        return 'browsing'
        
    def _classify_process_step(self, uri: str) -> str:
        """分类流程步骤类型"""
        if not uri:
            return 'unknown'
        uri_lower = uri.lower()
        if any(start in uri_lower for start in ['/start', '/begin', '/init']):
            return 'initiation'
        elif any(proc in uri_lower for proc in ['/process', '/handle', '/execute']):
            return 'processing'
        elif any(val in uri_lower for val in ['/validate', '/verify', '/check']):
            return 'validation'
        elif any(end in uri_lower for end in ['/complete', '/finish', '/done']):
            return 'completion'
        return 'processing'
        
    def _identify_funnel_stage(self, uri: str) -> str:
        """识别转化漏斗阶段"""
        if not uri:
            return 'unknown'
        uri_lower = uri.lower()
        if uri_lower in ['/', '/home']:
            return 'awareness'
        elif '/product' in uri_lower or '/search' in uri_lower:
            return 'interest'
        elif '/cart' in uri_lower or '/wishlist' in uri_lower:
            return 'consideration'
        elif '/checkout' in uri_lower:
            return 'intent'
        elif '/payment' in uri_lower or '/order' in uri_lower:
            return 'purchase'
        elif '/success' in uri_lower:
            return 'retention'
        return 'interest'
        
    def _assess_business_priority(self, uri: str) -> str:
        """评估业务优先级"""
        if not uri:
            return 'low'
        uri_lower = uri.lower()
        if any(critical in uri_lower for critical in ['/payment', '/order', '/checkout']):
            return 'critical'
        elif any(high in uri_lower for high in ['/login', '/register', '/api/']):
            return 'high'
        elif any(medium in uri_lower for medium in ['/product', '/search']):
            return 'medium'
        return 'low'