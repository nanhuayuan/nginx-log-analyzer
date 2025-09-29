#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
字段映射器 - 将底座格式日志映射到DWD表结构 (重构版)
Field Mapper - Maps base format logs to DWD table structure (Refactored)

主要改进：
1. 使用user-agents库进行专业UA解析
2. 使用GeoIP2进行准确地理定位
3. 基于Web Vitals标准的性能评级
4. LRU缓存优化重复计算
5. 更科学的业务价值评分算法
"""

import re
import logging
import hashlib
import json
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, Optional, Union, List, Tuple
from urllib.parse import urlparse, parse_qs
from functools import lru_cache
from ipaddress import ip_address, ip_network

# 第三方库
try:
    from user_agents import parse as ua_parse
    HAS_UA_PARSER = True
except ImportError:
    HAS_UA_PARSER = False
    logging.warning("user-agents库未安装，将使用备用解析方法")

try:
    import geoip2.database
    import geoip2.errors
    HAS_GEOIP = True
except ImportError:
    HAS_GEOIP = False
    logging.warning("geoip2库未安装，将使用简化IP解析")


class FieldMapper:
    """字段映射器 - 重构版"""
    
    # 性能阈值常量（毫秒）- 基于Google Web Vitals
    PERF_EXCELLENT = 200    # 极优
    PERF_GOOD = 500        # 良好  
    PERF_ACCEPTABLE = 1000  # 可接受
    PERF_SLOW = 3000       # 慢
    PERF_VERY_SLOW = 10000  # 非常慢
    PERF_TIMEOUT = 30000    # 超时
    
    # Apdex标准阈值
    APDEX_T = 500   # 满意阈值（毫秒）
    APDEX_F = 2000  # 容忍阈值（毫秒）
    
    def __init__(self, geoip_db_path: str = None):
        self.logger = logging.getLogger(__name__)

        # 初始化GeoIP
        self.geoip_reader = None
        if HAS_GEOIP:
            # 确定GeoIP数据库路径
            if not geoip_db_path:
                # 使用默认路径：../data/GeoLite2-City.mmdb
                geoip_db_path = Path(__file__).parent.parent / 'data' / 'GeoLite2-City.mmdb'
                self.logger.info(f"使用默认GeoIP数据库路径: {geoip_db_path}")

            # 尝试加载GeoIP数据库
            if geoip_db_path and Path(geoip_db_path).exists():
                try:
                    self.geoip_reader = geoip2.database.Reader(str(geoip_db_path))
                    self.logger.info(f"✅ GeoIP数据库加载成功: {geoip_db_path}")
                except Exception as e:
                    self.logger.warning(f"❌ GeoIP数据库加载失败: {e}")
            else:
                self.logger.info(f"📍 GeoIP数据库文件不存在: {geoip_db_path}")
                self.logger.info("💡 提示: 下载GeoLite2-City.mmdb到data目录可启用精确地理定位")
        
        # 预编译正则表达式
        self._compile_patterns()
        
        # 初始化缓存统计
        self.cache_stats = {
            'ua_hits': 0,
            'ua_misses': 0,
            'ip_hits': 0,
            'ip_misses': 0,
        }
    
    def _compile_patterns(self):
        """预编译常用正则表达式"""
        self.patterns = {
            # URI模式
            'api_version': re.compile(r'/v(\d+(?:\.\d+)?)', re.IGNORECASE),
            'static_resource': re.compile(r'\.(js|css|png|jpg|jpeg|gif|svg|ico|woff2?|ttf)$', re.IGNORECASE),
            
            # SDK检测
            'wst_sdk': re.compile(r'WST-SDK-(iOS|Android|ANDROID)(?:/([0-9.]+))?', re.IGNORECASE),
            'zgt_app': re.compile(r'zgt-(ios|android)[/\s]?([0-9.]+)?', re.IGNORECASE),
            
            # 小程序检测
            'alipay_miniapp': re.compile(r'AlipayClient.*miniprogram|AliApp.*miniprogram', re.IGNORECASE),
            'wechat_miniapp': re.compile(r'MicroMessenger.*miniprogram', re.IGNORECASE),
            
            # IP模式
            'internal_ip': re.compile(r'^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.|127\.)'),
            
            # 安全检测
            'sql_injection': re.compile(r'(\bunion\b.*\bselect\b|\bselect\b.*\bfrom\b|;.*\bdrop\b|\bor\b.*=.*\bor\b)', re.IGNORECASE),
            'xss_attack': re.compile(r'(<script|javascript:|onerror=|onload=)', re.IGNORECASE),
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
            dwd_record = {}
            
            # === 基础字段映射 ===
            self._map_basic_fields(dwd_record, parsed_data, source_file)
            
            # === 时间字段映射 ===
            self._map_time_fields(dwd_record, parsed_data)
            
            # === 网络和地理信息 ===
            self._map_network_geo_fields(dwd_record, parsed_data)
            
            # === 请求响应字段 ===
            self._map_request_response_fields(dwd_record, parsed_data)
            
            # === 性能字段映射 ===
            self._map_performance_fields(dwd_record, parsed_data)
            
            # === User-Agent解析 ===
            self._map_user_agent_fields(dwd_record, parsed_data)
            
            # === 业务字段映射 ===
            self._map_business_fields(dwd_record, parsed_data)
            
            # === 权限控制维度 ===
            self._map_permission_fields(dwd_record, parsed_data)
            
            # === 错误分析维度 ===
            self._map_error_fields(dwd_record, parsed_data)
            
            # === 安全分析维度 ===
            self._map_security_fields(dwd_record, parsed_data)
            
            # === 计算衍生字段 ===
            self._generate_derived_fields(dwd_record)
            
            # === 数据质量评估 ===
            self._assess_data_quality(dwd_record, parsed_data)
            
            return dwd_record
            
        except Exception as e:
            self.logger.error(f"字段映射失败: {e}")
            return self._create_fallback_record(parsed_data, source_file, str(e))
    
    def _map_basic_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any], source_file: str):
        """映射基础字段"""
        # 网络基础字段
        dwd_record['client_ip'] = parsed_data.get('RealIp', parsed_data.get('remote_addr', ''))
        dwd_record['client_port'] = self._safe_int(parsed_data.get('remote_port'), 0)
        dwd_record['xff_ip'] = parsed_data.get('RealIp', '')
        dwd_record['client_real_ip'] = parsed_data.get('RealIp', parsed_data.get('remote_addr', ''))
        
        # 服务器信息
        dwd_record['server_name'] = parsed_data.get('http_host', '')
        dwd_record['server_port'] = 443 if 'https' in parsed_data.get('http_host', '') else 80
        dwd_record['server_protocol'] = 'HTTP/1.1'  # 从request字段提取
        
        # 请求信息解析
        request_str = parsed_data.get('request', '')
        request_info = self._parse_request_line(request_str)
        
        dwd_record['request_method'] = request_info['method']
        dwd_record['request_uri'] = request_info['uri']
        dwd_record['request_uri_normalized'] = self._normalize_uri(request_info['uri'])
        dwd_record['request_full_uri'] = request_str
        dwd_record['request_path'] = request_info['path']
        dwd_record['query_parameters'] = request_info['query_string']
        dwd_record['query_params_count'] = len(request_info['query_params'])
        dwd_record['http_protocol_version'] = request_info['protocol']
        
        # 请求体大小（从日志推断）
        dwd_record['request_body_size'] = 0  # POST请求可能有body，但日志中没有
        
        # User-Agent和Referer
        dwd_record['user_agent_string'] = parsed_data.get('agent', '')
        dwd_record['referer_url'] = parsed_data.get('http_referer', '')
        dwd_record['referer_domain'] = self._extract_domain(parsed_data.get('http_referer', ''))
        
        # 日志元信息
        dwd_record['log_source_file'] = source_file
        dwd_record['log_format_version'] = '1.0'
        dwd_record['raw_log_entry'] = str(parsed_data)[:1000]  # 截断避免过长
    
    def _parse_request_line(self, request_str: str) -> Dict[str, Any]:
        """解析HTTP请求行"""
        result = {
            'method': 'UNKNOWN',
            'uri': '/',
            'path': '/',
            'query_string': '',
            'query_params': {},
            'protocol': 'HTTP/1.1'
        }
        
        if not request_str:
            return result
        
        # 解析 "GET /path?query HTTP/1.1" 格式
        parts = request_str.split(' ', 2)
        if len(parts) >= 2:
            result['method'] = parts[0]
            full_uri = parts[1]
            result['uri'] = full_uri
            
            # 分离路径和查询参数
            if '?' in full_uri:
                path, query_string = full_uri.split('?', 1)
                result['path'] = path
                result['query_string'] = query_string
                result['query_params'] = parse_qs(query_string)
            else:
                result['path'] = full_uri
                
        if len(parts) >= 3:
            result['protocol'] = parts[2]
            
        return result
    
    def _map_time_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射时间字段"""
        time_str = parsed_data.get('time', '')
        log_time = self._parse_log_time(time_str)
        
        if not log_time:
            log_time = datetime.now()
            
        dwd_record['log_time'] = log_time
        dwd_record['date_partition'] = log_time.date()
        dwd_record['hour_partition'] = log_time.hour
        dwd_record['minute_partition'] = log_time.minute
        dwd_record['second_partition'] = log_time.second
        dwd_record['quarter_partition'] = (log_time.month - 1) // 3 + 1
        dwd_record['week_partition'] = log_time.isocalendar()[1]
    
    @lru_cache(maxsize=10000)
    def _parse_log_time(self, time_str: str) -> Optional[datetime]:
        """解析日志时间（带缓存）"""
        if not time_str:
            return None
            
        # 处理 "2025-04-23T00:00:04+08:00" 格式
        time_str = time_str.replace('+08:00', '+0800')
        
        formats = [
            '%Y-%m-%dT%H:%M:%S%z',
            '%Y-%m-%d %H:%M:%S',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(time_str, fmt)
            except ValueError:
                continue
                
        return None
    
    def _map_network_geo_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射网络和地理位置字段"""
        client_ip = dwd_record['client_ip']
        
        # IP类型分类
        dwd_record['client_ip_type'] = self._classify_ip_type(client_ip)
        dwd_record['is_internal_ip'] = self._is_internal_ip(client_ip)
        
        # 地理位置解析
        geo_info = self._resolve_geo_location(client_ip)
        dwd_record.update(geo_info)
        
        # IP风险评估
        dwd_record['client_ip_classification'] = self._classify_ip_reputation(client_ip)
        dwd_record['ip_reputation'] = dwd_record['client_ip_classification']
        
        # 特殊IP检测
        dwd_record['is_tor_exit'] = self._detect_tor_exit(client_ip)
        dwd_record['is_proxy'] = self._detect_proxy(client_ip, parsed_data)
        dwd_record['is_vpn'] = self._detect_vpn(client_ip)
        dwd_record['is_datacenter'] = self._detect_datacenter(client_ip)
    
    @lru_cache(maxsize=50000)
    def _resolve_geo_location(self, ip: str) -> Dict[str, Any]:
        """解析IP地理位置（带缓存）"""
        default_geo = {
            'client_country': 'CN',
            'client_region': 'unknown',
            'client_city': 'unknown',
            'client_isp': 'unknown',
            'client_org': 'unknown',
            'client_asn': 0,
        }
        
        if not ip or self._is_internal_ip(ip):
            default_geo['client_region'] = 'internal'
            return default_geo
            
        # 使用GeoIP2
        if self.geoip_reader:
            try:
                response = self.geoip_reader.city(ip)
                return {
                    'client_country': response.country.iso_code or 'unknown',
                    'client_region': response.subdivisions.most_specific.name or 'unknown',
                    'client_city': response.city.name or 'unknown',
                    'client_isp': response.traits.isp or 'unknown',
                    'client_org': response.traits.organization or 'unknown',
                    'client_asn': response.traits.autonomous_system_number or 0,
                }
            except geoip2.errors.AddressNotFoundError:
                pass
            except Exception as e:
                self.logger.debug(f"GeoIP查询失败: {e}")
        
        # 备用方案：基于IP段的简单推断
        return self._infer_geo_from_ip(ip, default_geo)
    
    def _infer_geo_from_ip(self, ip: str, default_geo: Dict) -> Dict[str, Any]:
        """基于IP段推断地理位置（备用方案）"""
        geo = default_geo.copy()
        
        # 中国主要运营商IP段示例
        if ip.startswith(('111.', '112.', '113.', '114.', '115.', '116.', '117.', '118.', '119.', '120.')):
            geo['client_isp'] = 'China Mobile'
            geo['client_region'] = 'China'
        elif ip.startswith(('221.', '222.', '223.')):
            geo['client_isp'] = 'China Telecom'
            geo['client_region'] = 'China'
        elif ip.startswith(('58.', '59.', '60.', '61.')):
            geo['client_isp'] = 'China Unicom'
            geo['client_region'] = 'China'
            
        return geo
    
    def _map_request_response_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射请求响应字段"""
        # 响应状态码
        status_code = str(parsed_data.get('code', '0'))
        dwd_record['response_status_code'] = status_code
        dwd_record['response_status_class'] = self._get_status_class(status_code)
        
        # 响应体大小
        body_size = self._safe_int(parsed_data.get('body'), 0)
        dwd_record['response_body_size'] = body_size
        dwd_record['response_body_size_kb'] = round(body_size / 1024.0, 2)
        dwd_record['total_bytes_sent'] = body_size
        dwd_record['total_bytes_sent_kb'] = round(body_size / 1024.0, 2)
        dwd_record['bytes_received'] = dwd_record.get('request_body_size', 0)
        
        # 推断内容类型
        uri = dwd_record['request_path']
        dwd_record['response_content_type'] = self._infer_content_type(uri, status_code)
        dwd_record['content_type'] = dwd_record['response_content_type']
    
    def _map_performance_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射性能字段 - 基于Web Vitals标准"""
        # ar_time是秒，转换为毫秒
        ar_time_seconds = self._safe_float(parsed_data.get('ar_time'), 0.0)
        ar_time_ms = int(ar_time_seconds * 1000)
        
        dwd_record['total_request_duration'] = ar_time_ms
        
        # 由于日志中没有upstream时间，使用智能估算
        # 基于经验：后端处理约占70%，网络传输占20%，Nginx处理占10%
        if ar_time_ms > 0:
            backend_ratio = 0.7
            network_ratio = 0.2
            nginx_ratio = 0.1
            
            dwd_record['upstream_response_time'] = int(ar_time_ms * backend_ratio)
            dwd_record['upstream_connect_time'] = int(ar_time_ms * 0.05)  # 连接时间约5%
            dwd_record['upstream_header_time'] = int(ar_time_ms * 0.15)   # 头部处理约15%
            
            dwd_record['backend_connect_phase'] = dwd_record['upstream_connect_time']
            dwd_record['backend_process_phase'] = dwd_record['upstream_header_time'] - dwd_record['upstream_connect_time']
            dwd_record['backend_transfer_phase'] = dwd_record['upstream_response_time'] - dwd_record['upstream_header_time']
            dwd_record['nginx_transfer_phase'] = ar_time_ms - dwd_record['upstream_response_time']
            
            dwd_record['backend_total_phase'] = dwd_record['upstream_response_time']
            dwd_record['network_phase'] = int(ar_time_ms * network_ratio)
            dwd_record['processing_phase'] = dwd_record['backend_process_phase']
            dwd_record['transfer_phase'] = dwd_record['backend_transfer_phase'] + dwd_record['nginx_transfer_phase']
        else:
            # 时间为0的情况（静态缓存）
            for field in ['upstream_response_time', 'upstream_connect_time', 'upstream_header_time',
                         'backend_connect_phase', 'backend_process_phase', 'backend_transfer_phase',
                         'nginx_transfer_phase', 'backend_total_phase', 'network_phase',
                         'processing_phase', 'transfer_phase']:
                dwd_record[field] = 0
        
        # 其他未使用的性能字段设为0
        dwd_record['request_processing_time'] = 0
        dwd_record['response_send_time'] = ar_time_ms
        
        # 性能评分和分级
        self._calculate_performance_metrics(dwd_record, ar_time_ms)


    
    def _calculate_performance_metrics(self, dwd_record: Dict[str, Any], response_time_ms: int):
        """计算性能指标和评分"""
        # 性能分级（基于Google Web Vitals）
        if response_time_ms <= self.PERF_EXCELLENT:
            dwd_record['performance_level'] = 1  # excellent
            dwd_record['user_experience_level'] = 'Excellent'
            dwd_record['performance_rating'] = 'A'
        elif response_time_ms <= self.PERF_GOOD:
            dwd_record['performance_level'] = 2  # good
            dwd_record['user_experience_level'] = 'Good'
            dwd_record['performance_rating'] = 'B'
        elif response_time_ms <= self.PERF_ACCEPTABLE:
            dwd_record['performance_level'] = 3  # acceptable
            dwd_record['user_experience_level'] = 'Fair'
            dwd_record['performance_rating'] = 'C'
        elif response_time_ms <= self.PERF_SLOW:
            dwd_record['performance_level'] = 4  # slow
            dwd_record['user_experience_level'] = 'Poor'
            dwd_record['performance_rating'] = 'D'
        elif response_time_ms <= self.PERF_VERY_SLOW:
            dwd_record['performance_level'] = 5  # critical
            dwd_record['user_experience_level'] = 'Unacceptable'
            dwd_record['performance_rating'] = 'F'
        else:
            dwd_record['performance_level'] = 6  # timeout
            dwd_record['user_experience_level'] = 'Timeout'
            dwd_record['performance_rating'] = 'F'
        
        # 性能布尔标记
        dwd_record['perf_attention'] = response_time_ms > 500
        dwd_record['perf_warning'] = response_time_ms > 1000
        dwd_record['perf_slow'] = response_time_ms > 3000
        dwd_record['perf_very_slow'] = response_time_ms > 10000
        dwd_record['perf_timeout'] = response_time_ms > 30000
        
        # 兼容旧字段
        dwd_record['is_slow'] = dwd_record['perf_slow']
        dwd_record['is_very_slow'] = dwd_record['perf_very_slow']
        
        # Apdex分类
        if response_time_ms <= self.APDEX_T:
            dwd_record['apdex_classification'] = 'Satisfied'
            apdex_score = 1.0
        elif response_time_ms <= self.APDEX_F:
            dwd_record['apdex_classification'] = 'Tolerating'
            apdex_score = 0.5
        else:
            dwd_record['apdex_classification'] = 'Frustrated'
            apdex_score = 0.0
        
        # 综合性能评分（0-100）
        if response_time_ms == 0:
            performance_score = 100.0  # 缓存命中
        elif response_time_ms <= 100:
            performance_score = 95.0
        elif response_time_ms <= 500:
            performance_score = 85.0
        elif response_time_ms <= 1000:
            performance_score = 70.0
        elif response_time_ms <= 3000:
            performance_score = 50.0
        elif response_time_ms <= 10000:
            performance_score = 30.0
        else:
            performance_score = 10.0
            
        dwd_record['performance_score'] = performance_score
        
        # 传输速度计算
        if response_time_ms > 0 and dwd_record['response_body_size_kb'] > 0:
            dwd_record['response_transfer_speed'] = dwd_record['response_body_size_kb'] / (response_time_ms / 1000.0)
            dwd_record['total_transfer_speed'] = dwd_record['response_transfer_speed']
        else:
            dwd_record['response_transfer_speed'] = 0.0
            dwd_record['total_transfer_speed'] = 0.0
        
        dwd_record['nginx_transfer_speed'] = 0.0
        
        # 效率指标
        if response_time_ms > 0:
            dwd_record['backend_efficiency'] = (1.0 - (dwd_record['backend_total_phase'] / response_time_ms)) * 100
            dwd_record['network_overhead'] = (dwd_record['network_phase'] / response_time_ms) * 100
            dwd_record['transfer_ratio'] = (dwd_record['transfer_phase'] / response_time_ms) * 100
            dwd_record['connection_cost_ratio'] = (dwd_record['upstream_connect_time'] / response_time_ms) * 100
        else:
            dwd_record['backend_efficiency'] = 100.0
            dwd_record['network_overhead'] = 0.0
            dwd_record['transfer_ratio'] = 0.0
            dwd_record['connection_cost_ratio'] = 0.0
        
        dwd_record['processing_efficiency_index'] = performance_score
        dwd_record['latency_percentile'] = 0.0  # 需要批量计算才能得出

    def _map_user_agent_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射User-Agent相关字段 - 增强政务应用识别"""
        user_agent = parsed_data.get('agent', '')

        # 优先检测政务SDK和应用
        gov_app_info = self._detect_government_app(user_agent)
        if gov_app_info['is_government_app']:
            # 政务应用优先处理
            dwd_record.update({
                'platform': gov_app_info['platform'],
                'platform_version': gov_app_info['platform_version'],
                'platform_category': 'government',
                'app_version': gov_app_info['app_version'],
                'device_type': 'Mobile',
                'browser_type': 'Native',
                'browser_version': '',
                'os_type': gov_app_info['os_type'],
                'os_version': gov_app_info['os_version'],
                'sdk_type': gov_app_info['sdk_type'],
                'sdk_version': gov_app_info['sdk_version'],
                'integration_type': 'government_sdk',
                'is_bot': False,
                'bot_type': '',
                'bot_name': '',
                'bot_probability': 0.0,
                'crawler_category': '',
            })
            return

        # 如果不是政务应用，使用通用解析
        if HAS_UA_PARSER:
            ua_info = self._parse_ua_with_library(user_agent)
        else:
            ua_info = self._parse_ua_fallback(user_agent)

        # 映射解析结果
        dwd_record['platform'] = ua_info['platform']
        dwd_record['platform_version'] = ua_info['platform_version']
        dwd_record['platform_category'] = ua_info['platform_category']
        dwd_record['app_version'] = ua_info['app_version']
        dwd_record['app_build_number'] = ''
        dwd_record['device_type'] = ua_info['device_type']
        dwd_record['device_model'] = ua_info.get('device_model', '')
        dwd_record['device_manufacturer'] = ua_info.get('device_manufacturer', '')
        dwd_record['screen_resolution'] = ''
        dwd_record['browser_type'] = ua_info['browser_type']
        dwd_record['browser_version'] = ua_info['browser_version']
        dwd_record['browser_engine'] = ua_info.get('browser_engine', '')
        dwd_record['os_type'] = ua_info['os_type']
        dwd_record['os_version'] = ua_info['os_version']
        dwd_record['os_architecture'] = ua_info.get('os_architecture', '')

        # SDK和框架
        dwd_record['sdk_type'] = ua_info.get('sdk_type', '')
        dwd_record['sdk_version'] = ua_info.get('sdk_version', '')
        dwd_record['integration_type'] = ua_info.get('integration_type', 'native')
        dwd_record['framework_type'] = ua_info.get('framework_type', '')
        dwd_record['framework_version'] = ''

        # Bot检测
        dwd_record['is_bot'] = ua_info.get('is_bot', False)
        dwd_record['bot_type'] = ua_info.get('bot_type', '')
        dwd_record['bot_name'] = ua_info.get('bot_name', '')
        dwd_record['bot_probability'] = ua_info.get('bot_probability', 0.0)
        dwd_record['crawler_category'] = ua_info.get('crawler_category', '')

    @lru_cache(maxsize=5000)
    def _detect_government_app(self, user_agent: str) -> Dict[str, Any]:
        """检测政务应用 - 专门优化"""
        result = {
            'is_government_app': False,
            'platform': 'Unknown',
            'platform_version': '',
            'os_type': 'Unknown',
            'os_version': '',
            'app_version': '',
            'sdk_type': '',
            'sdk_version': ''
        }

        if not user_agent:
            return result

        # WST-SDK检测（政务技术栈）
        wst_match = re.search(r'WST-SDK-(iOS|Android|ANDROID)(?:/([0-9.]+))?', user_agent, re.IGNORECASE)
        if wst_match:
            platform = wst_match.group(1).upper()
            version = wst_match.group(2) if wst_match.group(2) else ''
            result.update({
                'is_government_app': True,
                'platform': 'iOS' if platform == 'IOS' else 'Android',
                'os_type': 'iOS' if platform == 'IOS' else 'Android',
                'sdk_type': f'WST-SDK-{platform}',
                'sdk_version': version,
            })

            # 提取iOS版本
            if platform == 'IOS':
                ios_match = re.search(r'iOS\s+([0-9.]+)', user_agent)
                if ios_match:
                    result['os_version'] = ios_match.group(1)

            return result

        # ZGT应用检测（政务通）
        zgt_match = re.search(r'zgt-(ios|android)[/\s]?([0-9.]+)?', user_agent, re.IGNORECASE)
        if zgt_match:
            platform = zgt_match.group(1).lower()
            version = zgt_match.group(2) if zgt_match.group(2) else ''
            result.update({
                'is_government_app': True,
                'platform': 'iOS' if platform == 'ios' else 'Android',
                'os_type': 'iOS' if platform == 'ios' else 'Android',
                'app_version': version,
                'sdk_type': f'ZGT-{platform.upper()}',
            })

            # 提取OS版本
            if platform == 'ios':
                ios_match = re.search(r'iOS\s+([0-9.]+)', user_agent)
                if ios_match:
                    result['os_version'] = ios_match.group(1)
            else:
                android_match = re.search(r'Android\s+([0-9.]+)', user_agent)
                if android_match:
                    result['os_version'] = android_match.group(1)

            return result

        # 其他政务应用关键词检测
        gov_apps = ['iGXRZ', 'gxrz', 'zwfw', '政务通', '市民云', 'i深圳', '随申办']
        for app in gov_apps:
            if app.lower() in user_agent.lower():
                result.update({
                    'is_government_app': True,
                    'platform': 'Mobile',
                    'sdk_type': f'GOV-{app}',
                })
                return result

        return result


    @lru_cache(maxsize=10000)
    def _parse_ua_with_library(self, user_agent: str) -> Dict[str, Any]:
        """使用user-agents库解析UA"""
        if not user_agent:
            return self._get_empty_ua_info()
        
        try:
            ua = ua_parse(user_agent)
            
            # 基础解析
            result = {
                'platform': self._get_platform_from_ua(ua),
                'platform_version': str(ua.os.version_string) if ua.os.version_string else '',
                'platform_category': self._get_platform_category(ua),
                'device_type': self._get_device_type_from_ua(ua),
                'device_model': ua.device.model or '',
                'device_manufacturer': ua.device.brand or '',
                'browser_type': ua.browser.family or 'unknown',
                'browser_version': str(ua.browser.version_string) if ua.browser.version_string else '',
                'os_type': ua.os.family or 'unknown',
                'os_version': str(ua.os.version_string) if ua.os.version_string else '',
                'is_bot': ua.is_bot,
                'is_mobile': ua.is_mobile,
                'is_tablet': ua.is_tablet,
                'is_pc': ua.is_pc,
                'app_version': '',
                'sdk_type': '',
                'sdk_version': '',
                'integration_type': 'native',
                'bot_type': 'crawler' if ua.is_bot else '',
                'bot_name': '',
                'bot_probability': 1.0 if ua.is_bot else 0.0,
                'crawler_category': 'bot' if ua.is_bot else '',
            }
            
            # 特殊应用检测
            self._detect_special_apps(user_agent, result)
            
            # 浏览器引擎
            if 'webkit' in user_agent.lower():
                result['browser_engine'] = 'WebKit'
            elif 'gecko' in user_agent.lower():
                result['browser_engine'] = 'Gecko'
            elif 'trident' in user_agent.lower():
                result['browser_engine'] = 'Trident'
            
            return result
            
        except Exception as e:
            self.logger.debug(f"UA解析失败，使用备用方案: {e}")
            return self._parse_ua_fallback(user_agent)
    
    def _parse_ua_fallback(self, user_agent: str) -> Dict[str, Any]:
        """备用UA解析方案"""
        if not user_agent:
            return self._get_empty_ua_info()
        
        result = self._get_empty_ua_info()
        ua_lower = user_agent.lower()
        
        # 特殊SDK检测（优先级最高）
        if match := self.patterns['wst_sdk'].search(user_agent):
            result['sdk_type'] = f'WST-SDK-{match.group(1).upper()}'
            result['sdk_version'] = match.group(2) if match.group(2) else ''
            result['platform'] = 'Android' if 'android' in match.group(1).lower() else 'iOS'
            result['os_type'] = result['platform']
            result['device_type'] = 'Mobile'
            result['integration_type'] = 'sdk'
            return result
        
        # zgt应用检测
        if match := self.patterns['zgt_app'].search(user_agent):
            result['app_version'] = match.group(2) if match.group(2) else ''
            result['platform'] = 'iOS' if match.group(1).lower() == 'ios' else 'Android'
            result['os_type'] = result['platform']
            result['device_type'] = 'Mobile'
            result['sdk_type'] = f'ZGT-{match.group(1).upper()}'
            return result
        
        # 小程序检测
        if self.patterns['alipay_miniapp'].search(user_agent):
            result['platform'] = 'Alipay'
            result['platform_category'] = 'miniprogram'
            result['integration_type'] = 'miniprogram'
            result['device_type'] = 'Mobile'
            
        # 移动设备检测
        if 'android' in ua_lower:
            result['platform'] = 'Android'
            result['os_type'] = 'Android'
            result['device_type'] = 'Mobile'
            result['platform_category'] = 'mobile'
            
            # Android版本提取
            if match := re.search(r'android\s+([0-9.]+)', ua_lower):
                result['os_version'] = match.group(1)
                
        elif 'iphone' in ua_lower or 'ipad' in ua_lower:
            result['platform'] = 'iOS'
            result['os_type'] = 'iOS'
            result['device_type'] = 'Mobile' if 'iphone' in ua_lower else 'Tablet'
            result['platform_category'] = 'mobile' if 'iphone' in ua_lower else 'tablet'
            
            # iOS版本提取
            if match := re.search(r'os\s+([0-9_]+)', ua_lower):
                result['os_version'] = match.group(1).replace('_', '.')
        
        # 浏览器检测
        if 'chrome' in ua_lower:
            result['browser_type'] = 'Chrome'
            if match := re.search(r'chrome/([0-9.]+)', ua_lower):
                result['browser_version'] = match.group(1)
        elif 'safari' in ua_lower:
            result['browser_type'] = 'Safari'
            if match := re.search(r'version/([0-9.]+)', ua_lower):
                result['browser_version'] = match.group(1)
        elif 'firefox' in ua_lower:
            result['browser_type'] = 'Firefox'
            if match := re.search(r'firefox/([0-9.]+)', ua_lower):
                result['browser_version'] = match.group(1)
        
        # Bot检测
        bot_keywords = ['bot', 'crawler', 'spider', 'scraper']
        if any(keyword in ua_lower for keyword in bot_keywords):
            result['is_bot'] = True
            result['bot_type'] = 'crawler'
            result['bot_probability'] = 0.9
            result['device_type'] = 'Bot'
        
        # 特殊应用检测
        self._detect_special_apps(user_agent, result)
        
        return result
    
    def _detect_special_apps(self, user_agent: str, result: Dict[str, Any]):
        """检测特殊应用"""
        ua_lower = user_agent.lower()
        
        # 支付宝
        if 'alipayclient' in ua_lower:
            if match := re.search(r'alipayclient/([0-9.]+)', ua_lower):
                result['app_version'] = match.group(1)
            
            # 检测是否小程序
            if 'miniprogram' in ua_lower or 'ariver' in ua_lower:
                result['platform_category'] = 'miniprogram'
                result['integration_type'] = 'miniprogram'
        
        # 微信
        elif 'micromessenger' in ua_lower:
            if match := re.search(r'micromessenger/([0-9.]+)', ua_lower):
                result['app_version'] = match.group(1)
            
            if 'miniprogram' in ua_lower:
                result['platform_category'] = 'miniprogram'
                result['integration_type'] = 'miniprogram'
    
    def _get_platform_from_ua(self, ua) -> str:
        """从UA对象获取平台"""
        if ua.is_bot:
            return 'Bot'
        elif ua.os.family:
            return ua.os.family
        return 'Unknown'
    
    def _get_platform_category(self, ua) -> str:
        """获取平台分类"""
        if ua.is_mobile:
            return 'mobile'
        elif ua.is_tablet:
            return 'tablet'
        elif ua.is_pc:
            return 'desktop'
        elif ua.is_bot:
            return 'bot'
        return 'unknown'
    
    def _get_device_type_from_ua(self, ua) -> str:
        """从UA对象获取设备类型"""
        if ua.is_bot:
            return 'Bot'
        elif ua.is_mobile:
            return 'Mobile'
        elif ua.is_tablet:
            return 'Tablet'
        elif ua.is_pc:
            return 'Desktop'
        return 'Unknown'
    
    def _map_business_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射业务字段"""
        uri = dwd_record['request_uri']
        
        # URI结构化解析
        uri_structure = self._parse_uri_structure(uri)
        
        # API和服务信息
        dwd_record['application_name'] = uri_structure['application_name']
        dwd_record['application_version'] = ''
        dwd_record['service_name'] = uri_structure['service_name']
        dwd_record['service_version'] = ''
        dwd_record['microservice_name'] = uri_structure['service_name']
        dwd_record['service_mesh_name'] = ''
        dwd_record['upstream_server'] = ''
        dwd_record['upstream_service'] = uri_structure['service_name']
        dwd_record['downstream_service'] = ''
        
        # API分类
        dwd_record['api_module'] = uri_structure['api_module']
        dwd_record['api_submodule'] = ''
        dwd_record['api_category'] = self._classify_api_category(uri_structure)
        dwd_record['api_subcategory'] = ''
        dwd_record['api_version'] = self._extract_api_version(uri)
        dwd_record['api_endpoint_type'] = self._get_endpoint_type(dwd_record['request_method'])
        
        # 业务域分类
        dwd_record['business_domain'] = self._classify_business_domain(uri_structure)
        dwd_record['business_subdomain'] = ''
        dwd_record['functional_area'] = self._get_functional_area(uri_structure)
        dwd_record['service_tier'] = self._get_service_tier(uri_structure)
        
        # 业务操作分类
        dwd_record['business_operation_type'] = self._classify_business_operation(uri)
        dwd_record['business_operation_subtype'] = ''
        dwd_record['transaction_type'] = self._get_transaction_type(dwd_record['request_method'])
        dwd_record['workflow_step'] = ''
        dwd_record['process_stage'] = ''
        
        # 用户旅程
        dwd_record['user_journey_stage'] = self._identify_user_journey_stage(uri)
        dwd_record['user_session_stage'] = ''
        
        # 访问入口和来源
        referer = dwd_record['referer_url']
        dwd_record['access_entry_point'] = self._identify_access_entry_point(dwd_record['server_name'])
        dwd_record['entry_source'] = self._classify_entry_source(referer)
        dwd_record['entry_source_detail'] = referer[:200] if referer and referer != '-' else ''
        dwd_record['client_channel'] = self._identify_client_channel(dwd_record['platform'])
        dwd_record['traffic_source'] = self._analyze_traffic_source(referer)
        
        # 搜索引擎和社交媒体
        dwd_record['search_engine'] = self._detect_search_engine(referer)
        dwd_record['search_keywords'] = self._extract_search_keywords(referer)
        dwd_record['social_media'] = self._detect_social_media(referer)
        dwd_record['social_media_type'] = self._get_social_media_type(dwd_record['social_media'])
        
        # Referer域名分类
        dwd_record['referer_domain_type'] = self._classify_domain_type(dwd_record['referer_domain'])
        
        # 访问分类
        dwd_record['access_type'] = self._classify_access_type(dwd_record)
        dwd_record['access_method'] = 'sync'  # 默认同步
        dwd_record['client_category'] = self._classify_client_category(dwd_record)
        dwd_record['client_type'] = self._classify_client_type(dwd_record)
        dwd_record['client_classification'] = self._classify_client_classification(dwd_record)
        dwd_record['integration_pattern'] = self._identify_integration_pattern(uri_structure)
        
        # 业务价值评估
        dwd_record['api_importance_level'] = self._assess_api_importance(uri_structure)
        dwd_record['business_criticality'] = self._assess_business_criticality(uri_structure)
        dwd_record['business_value_score'] = self._calculate_business_value_score(uri_structure, dwd_record)
        dwd_record['revenue_impact_level'] = self._assess_revenue_impact(uri_structure)
        dwd_record['customer_impact_level'] = self._assess_customer_impact(uri_structure)
        
        # 业务标识
        dwd_record['business_sign'] = self._generate_business_sign(uri_structure)
        
        # 用户信息（从日志中无法获取，设置默认值）
        dwd_record['user_id'] = ''
        dwd_record['session_id'] = ''
        dwd_record['user_type'] = 'guest'
        dwd_record['user_tier'] = 'free'
        dwd_record['user_segment'] = 'consumer'
        dwd_record['authentication_method'] = 'none'
        dwd_record['authorization_level'] = 'public'
        
        # 链路追踪（生成简单的ID）
        dwd_record['trace_id'] = self._generate_trace_id(parsed_data)
        dwd_record['span_id'] = ''
        dwd_record['parent_span_id'] = ''
        dwd_record['correlation_id'] = dwd_record['trace_id']
        dwd_record['request_id'] = dwd_record['trace_id']
        dwd_record['transaction_id'] = ''
        dwd_record['business_transaction_id'] = ''
        dwd_record['batch_id'] = ''
        
        # 缓存信息
        dwd_record['cache_status'] = self._infer_cache_status(dwd_record['total_request_duration'])
        dwd_record['cache_layer'] = 'L1' if dwd_record['cache_status'] == 'HIT' else ''
        dwd_record['cache_key'] = ''
        dwd_record['cache_age'] = 0
        dwd_record['cache_hit_ratio'] = 0.0
        
        # 连接信息
        dwd_record['connection_requests'] = 1
        dwd_record['connection_id'] = ''
        dwd_record['connection_type'] = 'keep_alive'
        dwd_record['ssl_session_reused'] = False
        
        # 标签和元数据
        dwd_record['feature_flag'] = ''
        dwd_record['ab_test_group'] = ''
        dwd_record['experiment_id'] = ''
        dwd_record['custom_tags'] = []
        dwd_record['business_tags'] = []
        dwd_record['custom_headers'] = {}
        dwd_record['security_headers'] = {}
        dwd_record['cookie_count'] = 0
        dwd_record['header_size'] = 0
        dwd_record['custom_dimensions'] = {}
        dwd_record['custom_metrics'] = {}
        dwd_record['metadata'] = {}
        
        # UTM参数提取
        query_params = parse_qs(dwd_record['query_parameters'])
        dwd_record['campaign_id'] = ''
        dwd_record['utm_source'] = query_params.get('utm_source', [''])[0]
        dwd_record['utm_medium'] = query_params.get('utm_medium', [''])[0]
        dwd_record['utm_campaign'] = query_params.get('utm_campaign', [''])[0]
        dwd_record['utm_content'] = query_params.get('utm_content', [''])[0]
        dwd_record['utm_term'] = query_params.get('utm_term', [''])[0]
        
        # 网络类型（无法从日志判断）
        dwd_record['network_type'] = 'unknown'
        
        # 基础设施信息
        dwd_record['load_balancer_node'] = ''
        dwd_record['edge_location'] = ''
        dwd_record['datacenter'] = ''
        dwd_record['availability_zone'] = ''
        dwd_record['cluster_node'] = ''
        dwd_record['instance_id'] = ''
        dwd_record['pod_name'] = ''
        dwd_record['container_id'] = ''
    
    @lru_cache(maxsize=10000)
    def _parse_uri_structure(self, uri: str) -> Dict[str, Any]:
        """解析URI结构（带缓存）"""
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
        
        # 清理URI
        clean_uri = uri.split('?')[0].strip('/')
        parts = clean_uri.split('/') if clean_uri else []
        depth = len(parts)
        
        result = {
            'application_name': parts[0] if depth > 0 else 'unknown',
            'service_name': 'unknown',
            'api_module': 'unknown',
            'api_endpoint': '',
            'path_depth': depth,
            'is_static_resource': False,
            'resource_type': 'unknown'
        }
        
        # 检测静态资源
        if self.patterns['static_resource'].search(uri):
            result['is_static_resource'] = True
            result['resource_type'] = self._get_resource_type(uri)
        
        # 解析不同应用类型
        if depth >= 2:
            app_name = parts[0]
            
            if app_name == 'scmp-gateway':
                # 网关格式: /scmp-gateway/service/module/endpoint
                result['service_name'] = parts[1] if depth >= 2 else 'unknown'
                result['api_module'] = parts[2] if depth >= 3 else 'unknown'
                result['api_endpoint'] = parts[3] if depth >= 4 else ''
                
            elif app_name == 'zgt-h5':
                # H5应用格式: /zgt-h5/type/resource
                result['service_name'] = parts[1] if depth >= 2 else 'unknown'
                if result['is_static_resource'] and depth >= 3:
                    filename = parts[-1].split('.')[0]
                    result['api_module'] = filename.replace('.min', '')
                else:
                    result['api_module'] = parts[2] if depth >= 3 else 'unknown'
                    
            elif app_name == 'group1':
                # 文件系统格式: /group1/M00/path/to/file
                result['service_name'] = parts[1] if depth >= 2 else 'unknown'
                result['api_module'] = 'files'
                result['is_static_resource'] = True
                result['resource_type'] = 'file'
            else:
                # 通用格式
                result['service_name'] = parts[1] if depth >= 2 else 'unknown'
                result['api_module'] = parts[2] if depth >= 3 else 'unknown'
                result['api_endpoint'] = parts[3] if depth >= 4 else ''
        
        return result
    
    def _classify_api_category(self, uri_structure: Dict[str, Any]) -> str:
        """分类API类别"""
        if uri_structure['is_static_resource']:
            return 'static'
        
        app_name = uri_structure['application_name']
        service_name = uri_structure['service_name']
        
        if app_name == 'scmp-gateway':
            if service_name == 'gxrz-rest':
                return 'auth'
            elif service_name == 'alipay':
                return 'payment'
            elif service_name == 'zww':
                return 'government'
            return 'gateway'
        elif app_name == 'zgt-h5':
            return 'webapp'
        elif app_name == 'group1':
            return 'storage'
        
        return 'service'

    def _calculate_business_value_score(self, uri_structure: Dict[str, Any], dwd_record: Dict[str, Any]) -> int:
        """计算业务价值评分 - 政务服务加权"""
        base_score = 30

        # 政务服务基础加分
        business_domain = dwd_record.get('business_domain', '')
        if 'government' in business_domain:
            base_score = 60  # 政务服务基础分更高

            # 核心政务服务额外加分
            critical_gov_services = [
                'government-authentication',  # 认证服务 +30
                'government-certificate-service',  # 证照服务 +25
                'government-payment-service',  # 支付服务 +25
                'government-social-security',  # 社保服务 +20
                'government-taxation-service',  # 税务服务 +20
            ]

            for service in critical_gov_services:
                if business_domain == service:
                    if 'authentication' in service:
                        base_score += 30
                    elif 'certificate' in service or 'payment' in service:
                        base_score += 25
                    else:
                        base_score += 20
                    break

        # 基于API类别的权重
        else:
            category_weights = {
                'auth': 85,
                'payment': 95,
                'gateway': 60,
                'webapp': 50,
                'storage': 40,
                'static': 20,
                'service': 50,
            }

            api_category = dwd_record.get('api_category', 'service')
            base_score = category_weights.get(api_category, 50)

        # 基于服务名称的重要性评分
        service_name = uri_structure.get('service_name', '')
        if service_name in ['gxrz-rest', 'zww']:
            base_score = min(100, base_score + 15)

        # 基于状态码调整
        status_code = dwd_record.get('response_status_code', '200')
        if status_code.startswith('2'):
            base_score = int(base_score * 1.1)
        elif status_code.startswith('4'):
            base_score = int(base_score * 0.7)
        elif status_code.startswith('5'):
            base_score = int(base_score * 0.5)

        # 基于响应时间调整
        response_time = dwd_record.get('total_request_duration', 0)
        if response_time < 500:
            base_score = int(base_score * 1.2)
        elif response_time > 5000:
            base_score = int(base_score * 0.8)

        return min(100, max(1, base_score))

    def _map_permission_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射权限控制字段 - 政务场景增强"""
        server_name = dwd_record.get('server_name', '')
        business_domain = dwd_record.get('business_domain', '')

        # 政务租户识别
        if '.gov.cn' in server_name or 'government' in business_domain:
            dwd_record['tenant_code'] = self._identify_government_tenant(server_name)
            dwd_record['environment'] = 'prod'  # 政务系统默认生产环境
            dwd_record['data_sensitivity'] = 3  # confidential - 政务数据敏感级别高
            dwd_record['compliance_zone'] = 'government'  # 政务合规区

            # 基于域名识别具体政府部门
            if 'gx' in server_name or 'guangxi' in server_name:
                dwd_record['team_code'] = 'guangxi_gov'
                dwd_record['business_unit'] = 'guangxi_government'
                dwd_record['region_code'] = 'cn-south'
            elif 'zj' in server_name or 'zhejiang' in server_name:
                dwd_record['team_code'] = 'zhejiang_gov'
                dwd_record['business_unit'] = 'zhejiang_government'
                dwd_record['region_code'] = 'cn-east'
            else:
                dwd_record['team_code'] = 'central_gov'
                dwd_record['business_unit'] = 'central_government'
                dwd_record['region_code'] = 'cn-north'
        else:
            # 非政务系统
            dwd_record['tenant_code'] = 'default'
            dwd_record['environment'] = self._infer_environment(server_name)
            dwd_record['team_code'] = 'default'
            dwd_record['data_sensitivity'] = 2  # internal
            dwd_record['business_unit'] = 'default'
            dwd_record['region_code'] = 'cn-north'
            dwd_record['compliance_zone'] = 'default'

        dwd_record['cost_center'] = dwd_record['business_unit']

    def _identify_government_tenant(self, server_name: str) -> str:
        """识别政府租户"""
        if not server_name:
            return 'government'

        server_lower = server_name.lower()

        # 省级政府
        provinces = {
            'beijing': 'beijing_gov',
            'shanghai': 'shanghai_gov',
            'guangdong': 'guangdong_gov',
            'guangxi': 'guangxi_gov',
            'gx': 'guangxi_gov',
            'zhejiang': 'zhejiang_gov',
            'zj': 'zhejiang_gov',
            'jiangsu': 'jiangsu_gov',
            'shandong': 'shandong_gov',
            'sichuan': 'sichuan_gov',
        }

        for keyword, tenant in provinces.items():
            if keyword in server_lower:
                return tenant

        # 中央部委
        if any(ministry in server_lower for ministry in ['mof', 'moe', 'moh', 'mps']):
            return 'central_ministry'

        return 'government'


    def _map_error_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射错误分析字段"""
        status_code = dwd_record['response_status_code']
        
        # 错误分类
        dwd_record['error_code_group'] = self._classify_error_group(status_code)
        dwd_record['http_error_class'] = self._classify_http_error_class(status_code)
        dwd_record['error_severity_level'] = self._assess_error_severity(status_code)
        dwd_record['error_category'] = self._classify_error_category(status_code)
        dwd_record['error_subcategory'] = ''
        dwd_record['error_source'] = self._identify_error_source(status_code)
        dwd_record['error_propagation_path'] = ''
        dwd_record['upstream_status_code'] = ''
        dwd_record['error_correlation_id'] = dwd_record.get('trace_id', '')
        dwd_record['error_chain'] = []
        dwd_record['root_cause_analysis'] = ''
    
    def _map_security_fields(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """映射安全分析字段"""
        client_ip = dwd_record['client_ip']
        uri = dwd_record['request_uri']
        
        # 安全风险评分
        risk_score = 0
        
        # IP风险评估
        if dwd_record.get('is_tor_exit'):
            risk_score += 30
        if dwd_record.get('is_vpn'):
            risk_score += 20
        if dwd_record.get('is_datacenter'):
            risk_score += 10
        
        # URI风险检测
        if self._detect_sql_injection(uri):
            risk_score += 50
            dwd_record['threat_category'] = 'sql_injection'
        elif self._detect_xss(uri):
            risk_score += 40
            dwd_record['threat_category'] = 'xss'
        else:
            dwd_record['threat_category'] = ''
        
        dwd_record['security_risk_score'] = min(100, risk_score)
        dwd_record['security_risk_level'] = self._get_risk_level(risk_score)
        
        dwd_record['attack_signature'] = ''
        dwd_record['ip_risk_level'] = dwd_record.get('security_risk_level', 'low')
        dwd_record['geo_anomaly'] = False
        dwd_record['access_pattern_anomaly'] = False
        dwd_record['rate_limit_hit'] = False
        dwd_record['blocked_by_waf'] = False
        dwd_record['fraud_score'] = risk_score / 100.0
    
    def _generate_derived_fields(self, dwd_record: Dict[str, Any]):
        """生成衍生字段"""
        status_code = dwd_record['response_status_code']
        
        # 成功状态判断
        dwd_record['is_success'] = status_code.startswith('2')
        dwd_record['is_business_success'] = dwd_record['is_success'] and dwd_record['response_body_size'] > 0
        dwd_record['is_error'] = not dwd_record['is_success']
        dwd_record['is_client_error'] = status_code.startswith('4')
        dwd_record['is_server_error'] = status_code.startswith('5')
        
        # 重试检测
        dwd_record['is_retry'] = False  # 需要会话级别的分析才能准确判断
        
        # 异常检测
        dwd_record['has_anomaly'] = (
            dwd_record['is_server_error'] or
            dwd_record['perf_very_slow'] or
            dwd_record['response_body_size'] > 10 * 1024 * 1024  # 10MB
        )
        
        if dwd_record['has_anomaly']:
            if dwd_record['is_server_error']:
                dwd_record['anomaly_type'] = 'error'
            elif dwd_record['perf_very_slow']:
                dwd_record['anomaly_type'] = 'performance'
            else:
                dwd_record['anomaly_type'] = 'data'
        else:
            dwd_record['anomaly_type'] = ''
        
        dwd_record['anomaly_severity'] = 'high' if dwd_record['has_anomaly'] else ''
        
        # SLA合规性
        dwd_record['sla_compliance'] = not dwd_record['perf_slow'] and dwd_record['is_success']
        dwd_record['sla_violation_type'] = ''
        if not dwd_record['sla_compliance']:
            if dwd_record['perf_slow']:
                dwd_record['sla_violation_type'] = 'performance'
            elif not dwd_record['is_success']:
                dwd_record['sla_violation_type'] = 'availability'
        
        # 时间字段（这些是MATERIALIZED字段，不需要设置）
        # date, hour, minute等会自动从log_time计算
        
        # 节假日标记（需要节假日数据库）
        dwd_record['is_holiday'] = False
        
        # 系统字段（有默认值的不设置）
        # created_at, updated_at, data_version等会使用默认值
        
        # 扩展字段
        dwd_record['enrichment_status'] = 'complete'
        dwd_record['validation_errors'] = []
        dwd_record['processing_flags'] = []
    
    def _assess_data_quality(self, dwd_record: Dict[str, Any], parsed_data: Dict[str, Any]):
        """评估数据质量"""
        # 计算完整性
        total_fields = 0
        filled_fields = 0
        
        for key, value in dwd_record.items():
            total_fields += 1
            if value not in [None, '', 0, False, [], {}]:
                filled_fields += 1
        
        completeness = filled_fields / total_fields if total_fields > 0 else 0
        
        dwd_record['data_completeness'] = completeness
        dwd_record['data_quality_score'] = completeness * 100
        
        # 解析错误记录
        dwd_record['parsing_errors'] = parsed_data.get('parsing_errors', [])
    
    # ========== 辅助方法 ==========
    
    def _safe_int(self, value: Any, default: int = 0) -> int:
        """安全转换为整数"""
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        """安全转换为浮点数"""
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def _normalize_uri(self, uri: str) -> str:
        """标准化URI"""
        if not uri:
            return uri
        
        # 移除查询参数
        uri = uri.split('?')[0]
        
        # 移除末尾斜杠
        if uri.endswith('/') and len(uri) > 1:
            uri = uri[:-1]
            
        return uri
    
    def _extract_domain(self, url: str) -> str:
        """提取域名"""
        if not url or url == '-':
            return ''
        
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc
        except:
            return ''
    
    def _is_internal_ip(self, ip: str) -> bool:
        """判断是否内网IP"""
        if not ip:
            return False
        return self.patterns['internal_ip'].match(ip) is not None
    
    def _classify_ip_type(self, ip: str) -> str:
        """分类IP类型"""
        if not ip:
            return 'unknown'
        if self._is_internal_ip(ip):
            return 'internal'
        elif ip.startswith('127.') or ip == 'localhost':
            return 'loopback'
        return 'external'
    
    def _classify_ip_reputation(self, ip: str) -> str:
        """评估IP信誉"""
        if self._is_internal_ip(ip):
            return 'trusted'
        
        # 检测已知恶意IP段（示例）
        suspicious_ranges = ['185.220.', '199.87.', '204.11.']  # Tor节点示例
        if any(ip.startswith(prefix) for prefix in suspicious_ranges):
            return 'suspicious'
            
        return 'neutral'
    
    def _detect_tor_exit(self, ip: str) -> bool:
        """检测Tor出口节点"""
        if not ip or self._is_internal_ip(ip):
            return False
        
        # 已知Tor出口节点IP段（示例）
        tor_ranges = ['185.220.', '199.87.', '204.11.', '109.70.', '176.10.']
        return any(ip.startswith(prefix) for prefix in tor_ranges)
    
    def _detect_proxy(self, ip: str, parsed_data: Dict[str, Any]) -> bool:
        """检测代理访问"""
        if not ip:
            return False
        
        # 检测X-Forwarded-For等代理头
        if parsed_data.get('RealIp') != parsed_data.get('remote_addr'):
            return True
        
        # 已知代理IP段
        proxy_ranges = ['104.16.', '172.64.', '173.245.', '103.21.', '103.22.']  # Cloudflare
        return any(ip.startswith(prefix) for prefix in proxy_ranges)
    
    def _detect_vpn(self, ip: str) -> bool:
        """检测VPN"""
        if not ip or self._is_internal_ip(ip):
            return False
        
        # 已知VPN提供商IP段（示例）
        vpn_ranges = ['45.', '91.', '194.', '195.']
        return any(ip.startswith(prefix) for prefix in vpn_ranges)
    
    def _detect_datacenter(self, ip: str) -> bool:
        """检测数据中心IP"""
        if not ip or self._is_internal_ip(ip):
            return False
        
        # 已知数据中心IP段
        datacenter_ranges = [
            '13.',      # AWS
            '52.',      # AWS
            '104.',     # Cloudflare
            '142.250.', # Google
            '157.240.', # Facebook
        ]
        return any(ip.startswith(prefix) for prefix in datacenter_ranges)
    
    def _get_status_class(self, status_code: str) -> str:
        """获取状态码类别"""
        if status_code.startswith('2'):
            return '2xx'
        elif status_code.startswith('3'):
            return '3xx'
        elif status_code.startswith('4'):
            return '4xx'
        elif status_code.startswith('5'):
            return '5xx'
        return 'unknown'
    
    def _infer_content_type(self, uri: str, status_code: str) -> str:
        """推断内容类型"""
        if not uri:
            return 'unknown'
        
        uri_lower = uri.lower()
        
        # 静态资源
        if uri_lower.endswith('.js'):
            return 'application/javascript'
        elif uri_lower.endswith('.css'):
            return 'text/css'
        elif uri_lower.endswith(('.jpg', '.jpeg')):
            return 'image/jpeg'
        elif uri_lower.endswith('.png'):
            return 'image/png'
        elif uri_lower.endswith('.gif'):
            return 'image/gif'
        elif uri_lower.endswith('.svg'):
            return 'image/svg+xml'
        elif uri_lower.endswith('.json'):
            return 'application/json'
        
        # API接口
        elif '/api/' in uri_lower or 'gateway' in uri_lower:
            return 'application/json'
        
        # HTML页面
        elif uri_lower.endswith(('.html', '.htm')) or uri == '/':
            return 'text/html'
        
        return 'application/octet-stream'
    
    def _get_resource_type(self, uri: str) -> str:
        """获取资源类型"""
        if not uri:
            return 'unknown'
        
        uri_lower = uri.lower()
        
        if uri_lower.endswith(('.js', '.mjs')):
            return 'javascript'
        elif uri_lower.endswith('.css'):
            return 'stylesheet'
        elif uri_lower.endswith(('.jpg', '.jpeg', '.png', '.gif', '.svg', '.ico', '.webp')):
            return 'image'
        elif uri_lower.endswith(('.woff', '.woff2', '.ttf', '.eot', '.otf')):
            return 'font'
        elif uri_lower.endswith(('.mp4', '.webm', '.ogg', '.mp3', '.wav')):
            return 'media'
        elif uri_lower.endswith(('.pdf', '.doc', '.docx', '.xls', '.xlsx')):
            return 'document'
        
        return 'other'
    
    def _extract_api_version(self, uri: str) -> str:
        """提取API版本"""
        if not uri:
            return ''
        
        # 使用预编译的正则
        if match := self.patterns['api_version'].search(uri):
            return f'v{match.group(1)}'
        
        return ''
    
    def _get_endpoint_type(self, method: str) -> str:
        """获取端点类型"""
        method_upper = method.upper()
        
        if method_upper == 'GET':
            return 'read'
        elif method_upper == 'POST':
            return 'create'
        elif method_upper == 'PUT':
            return 'update'
        elif method_upper == 'DELETE':
            return 'delete'
        elif method_upper == 'PATCH':
            return 'patch'
        elif method_upper == 'HEAD':
            return 'head'
        elif method_upper == 'OPTIONS':
            return 'options'
        
        return 'unknown'
    
    def _get_transaction_type(self, method: str) -> str:
        """获取事务类型"""
        method_upper = method.upper()
        
        if method_upper in ['GET', 'HEAD', 'OPTIONS']:
            return 'query'
        elif method_upper in ['POST', 'PUT', 'PATCH', 'DELETE']:
            return 'mutation'
        
        return 'unknown'

    def _classify_business_domain(self, uri_structure: Dict[str, Any]) -> str:
        """分类业务域 - 增强政务服务识别"""
        app_name = uri_structure['application_name']
        service_name = uri_structure['service_name']
        api_module = uri_structure['api_module']

        # 政务网关服务精确分类
        if app_name == 'scmp-gateway':
            # 广西认证服务
            if service_name == 'gxrz-rest':
                if any(auth in api_module.lower() for auth in ['login', 'auth', 'token', 'verify']):
                    return 'government-authentication'
                elif 'user' in api_module.lower():
                    return 'government-user-management'
                elif 'bind' in api_module.lower():
                    return 'government-account-binding'
                return 'government-identity-service'

            # 政务网服务
            elif service_name == 'zww':
                if 'query' in api_module.lower():
                    return 'government-query-service'
                elif 'apply' in api_module.lower():
                    return 'government-application-service'
                return 'government-portal-service'

            # 支付宝政务服务
            elif service_name == 'alipay':
                return 'government-payment-service'

            # 栏目服务
            elif service_name == 'column':
                return 'government-content-service'

            # 第三方专项服务
            elif service_name == 'thirdSpecial':
                return 'government-integration-service'

            # 微信集成
            elif service_name in ['weixinJsSdkSign', 'wechat']:
                return 'government-wechat-service'

            # 应用配置
            elif service_name == 'appKind':
                return 'government-app-config'

            # 政务通服务
            elif service_name == 'zgt-rest':
                return 'government-zgt-service'

            return 'government-gateway-service'

        # 政务H5前端
        elif app_name == 'zgt-h5':
            if service_name in ['js', 'css', 'images', 'fonts']:
                return 'government-static-assets'
            elif service_name == 'pages':
                return 'government-h5-pages'
            return 'government-h5-service'

        # 文件存储服务
        elif app_name == 'group1':
            return 'government-file-storage'

        # 基于URI关键词的政务服务识别
        uri_combined = f"{app_name}/{service_name}/{api_module}".lower()

        # 政务关键业务分类
        if any(cert in uri_combined for cert in ['certificate', 'license', 'permit', '证照', '许可']):
            return 'government-certificate-service'
        elif any(social in uri_combined for social in ['social', 'insurance', '社保', '医保', '养老']):
            return 'government-social-security'
        elif any(tax in uri_combined for tax in ['tax', 'revenue', '税务', '纳税', '发票']):
            return 'government-taxation-service'
        elif any(house in uri_combined for house in ['household', 'residence', '户口', '户籍', '居住']):
            return 'government-household-service'
        elif any(fund in uri_combined for fund in ['fund', 'provident', '公积金', '住房']):
            return 'government-housing-fund'
        elif any(traffic in uri_combined for traffic in ['traffic', 'vehicle', '交通', '车辆', '驾驶']):
            return 'government-transportation'

        # 通用业务域分类（非政务）
        if 'user' in uri_combined or 'account' in uri_combined:
            return 'user-service'
        elif 'auth' in uri_combined or 'login' in uri_combined:
            return 'auth-service'
        elif 'pay' in uri_combined or 'order' in uri_combined:
            return 'payment-service'
        elif 'content' in uri_combined or 'article' in uri_combined:
            return 'content-service'
        elif 'file' in uri_combined or 'upload' in uri_combined:
            return 'file-service'

        return 'general-service'

    def _get_functional_area(self, uri_structure: Dict[str, Any]) -> str:
        """获取功能区域"""
        if uri_structure['is_static_resource']:
            return 'frontend'
        elif 'gateway' in uri_structure['application_name']:
            return 'middleware'
        elif 'api' in uri_structure['application_name']:
            return 'backend'
        
        return 'service'
    
    def _get_service_tier(self, uri_structure: Dict[str, Any]) -> str:
        """获取服务层级"""
        if uri_structure['is_static_resource']:
            return 'web'
        elif 'gateway' in uri_structure['application_name']:
            return 'api'
        elif 'service' in uri_structure['service_name']:
            return 'service'
        
        return 'data'
    
    def _classify_business_operation(self, uri: str) -> str:
        """分类业务操作"""
        if not uri:
            return 'unknown'
        
        uri_lower = uri.lower()
        
        if 'login' in uri_lower or 'auth' in uri_lower:
            return 'login'
        elif 'register' in uri_lower or 'signup' in uri_lower:
            return 'register'
        elif 'payment' in uri_lower or 'pay' in uri_lower:
            return 'payment'
        elif 'search' in uri_lower or 'query' in uri_lower:
            return 'search'
        elif 'upload' in uri_lower:
            return 'upload'
        elif 'download' in uri_lower:
            return 'download'
        elif 'create' in uri_lower or 'add' in uri_lower:
            return 'create'
        elif 'update' in uri_lower or 'edit' in uri_lower:
            return 'update'
        elif 'delete' in uri_lower or 'remove' in uri_lower:
            return 'delete'
        elif 'view' in uri_lower or 'get' in uri_lower:
            return 'view'
        
        return 'other'
    
    def _identify_user_journey_stage(self, uri: str) -> str:
        """识别用户旅程阶段"""
        if not uri:
            return 'unknown'
        
        uri_lower = uri.lower()
        
        if uri == '/' or 'home' in uri_lower:
            return 'discovery'
        elif 'register' in uri_lower or 'signup' in uri_lower:
            return 'onboarding'
        elif 'login' in uri_lower or 'auth' in uri_lower:
            return 'authentication'
        elif 'search' in uri_lower or 'browse' in uri_lower:
            return 'exploration'
        elif 'product' in uri_lower or 'detail' in uri_lower:
            return 'consideration'
        elif 'cart' in uri_lower or 'checkout' in uri_lower:
            return 'conversion'
        elif 'payment' in uri_lower or 'pay' in uri_lower:
            return 'transaction'
        elif 'success' in uri_lower or 'complete' in uri_lower:
            return 'completion'
        elif 'profile' in uri_lower or 'account' in uri_lower:
            return 'retention'
        
        return 'active'
    
    def _identify_access_entry_point(self, server_name: str) -> str:
        """识别访问入口点"""
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
    
    def _classify_entry_source(self, referer: str) -> str:
        """分类入口来源"""
        if not referer or referer == '-':
            return 'Direct'
        
        domain = self._extract_domain(referer).lower()
        
        if self._detect_search_engine(referer):
            return 'Search'
        elif self._detect_social_media(referer):
            return 'Social'
        elif 'gov.cn' in domain:
            return 'Government'
        
        return 'Referral'
    
    def _identify_client_channel(self, platform: str) -> str:
        """识别客户端渠道"""
        if platform in ['iOS', 'Android']:
            return 'official'
        elif platform == 'Bot':
            return 'automated'
        
        return 'web'
    
    def _analyze_traffic_source(self, referer: str) -> str:
        """分析流量来源"""
        if not referer or referer == '-':
            return 'direct'
        
        if self._detect_search_engine(referer):
            return 'organic'
        elif self._detect_social_media(referer):
            return 'social'
        elif 'utm_' in referer:
            return 'paid'
        
        return 'referral'
    
    @lru_cache(maxsize=1000)
    def _detect_search_engine(self, referer: str) -> str:
        """检测搜索引擎"""
        if not referer or referer == '-':
            return ''
        
        domain = self._extract_domain(referer).lower()
        
        search_engines = {
            'baidu.com': 'Baidu',
            'google.com': 'Google',
            'bing.com': 'Bing',
            'sogou.com': 'Sogou',
            'so.com': '360',
            'sm.cn': 'Shenma',
        }
        
        for engine_domain, engine_name in search_engines.items():
            if engine_domain in domain:
                return engine_name
        
        return ''
    
    def _extract_search_keywords(self, referer: str) -> str:
        """提取搜索关键词"""
        if not referer or referer == '-':
            return ''
        
        # 解析查询参数
        parsed = urlparse(referer)
        params = parse_qs(parsed.query)
        
        # 不同搜索引擎的关键词参数
        keyword_params = ['q', 'query', 'wd', 'keyword', 'search']
        
        for param in keyword_params:
            if param in params:
                return params[param][0]
        
        return ''
    
    @lru_cache(maxsize=1000)
    def _detect_social_media(self, referer: str) -> str:
        """检测社交媒体"""
        if not referer or referer == '-':
            return ''
        
        domain = self._extract_domain(referer).lower()
        
        social_platforms = {
            'weixin.qq.com': 'WeChat',
            'weibo.com': 'Weibo',
            'qq.com': 'QQ',
            'douyin.com': 'Douyin',
            'xiaohongshu.com': 'XiaoHongShu',
            'zhihu.com': 'Zhihu',
        }
        
        for platform_domain, platform_name in social_platforms.items():
            if platform_domain in domain:
                return platform_name
        
        return ''
    
    def _get_social_media_type(self, social_media: str) -> str:
        """获取社交媒体类型"""
        if not social_media:
            return ''
        
        types = {
            'WeChat': 'instant_message',
            'QQ': 'instant_message',
            'Weibo': 'social_network',
            'Douyin': 'short_video',
            'XiaoHongShu': 'social_commerce',
            'Zhihu': 'knowledge_sharing',
        }
        
        return types.get(social_media, 'social_network')
    
    def _classify_domain_type(self, domain: str) -> str:
        """分类域名类型"""
        if not domain:
            return 'unknown'
        
        domain_lower = domain.lower()
        
        if 'gov.cn' in domain_lower:
            return 'government'
        elif any(edu in domain_lower for edu in ['edu.cn', '.edu']):
            return 'education'
        elif self._detect_search_engine(f'http://{domain}'):
            return 'search'
        elif self._detect_social_media(f'http://{domain}'):
            return 'social'
        elif any(news in domain_lower for news in ['news', 'xinhua', 'people', 'cctv']):
            return 'news'
        
        return 'general'
    
    def _classify_access_type(self, dwd_record: Dict[str, Any]) -> str:
        """分类访问类型"""
        platform = dwd_record.get('platform', '')
        browser = dwd_record.get('browser_type', '')
        sdk_type = dwd_record.get('sdk_type', '')
        
        if sdk_type:
            return 'APP_Native'
        elif platform in ['iOS', 'Android']:
            if browser and browser != 'unknown':
                return 'H5_WebView'
            return 'APP_Native'
        elif browser and browser != 'unknown':
            return 'Browser'
        elif dwd_record.get('is_bot'):
            return 'Bot'
        
        return 'API'
    
    def _classify_client_category(self, dwd_record: Dict[str, Any]) -> str:
        """分类客户端类别"""
        if dwd_record.get('is_bot'):
            return 'Bot'
        elif dwd_record.get('device_type') == 'Mobile':
            return 'Mobile_App'
        elif dwd_record.get('device_type') == 'Desktop':
            return 'Desktop_Web'
        elif dwd_record.get('sdk_type'):
            return 'SDK_Client'
        
        return 'Unknown'
    
    def _classify_client_type(self, dwd_record: Dict[str, Any]) -> str:
        """分类客户端类型"""
        if dwd_record.get('sdk_type'):
            return 'official'
        elif dwd_record.get('is_bot'):
            return 'automated'
        
        return 'standard'
    
    def _classify_client_classification(self, dwd_record: Dict[str, Any]) -> str:
        """分类客户端分级"""
        if dwd_record.get('is_internal_ip'):
            return 'trusted'
        elif dwd_record.get('is_bot'):
            return 'unverified'
        elif dwd_record.get('sdk_type'):
            return 'verified'
        
        return 'standard'
    
    def _identify_integration_pattern(self, uri_structure: Dict[str, Any]) -> str:
        """识别集成模式"""
        if 'gateway' in uri_structure['application_name']:
            return 'gateway'
        elif uri_structure['is_static_resource']:
            return 'direct'
        
        return 'mesh'
    
    def _assess_api_importance(self, uri_structure: Dict[str, Any]) -> str:
        """评估API重要性"""
        service_name = uri_structure['service_name']
        api_module = uri_structure['api_module']
        
        # 关键服务
        critical_services = ['gxrz-rest', 'alipay', 'payment']
        if any(s in service_name for s in critical_services):
            return 'critical'
        
        # 重要模块
        important_modules = ['user', 'auth', 'order']
        if any(m in api_module.lower() for m in important_modules):
            return 'important'
        
        # 静态资源
        if uri_structure['is_static_resource']:
            return 'optional'
        
        return 'normal'
    
    def _assess_business_criticality(self, uri_structure: Dict[str, Any]) -> str:
        """评估业务关键性"""
        importance = self._assess_api_importance(uri_structure)
        
        if importance == 'critical':
            return 'mission_critical'
        elif importance == 'important':
            return 'business_critical'
        elif importance == 'normal':
            return 'important'
        
        return 'standard'
    
    def _assess_revenue_impact(self, uri_structure: Dict[str, Any]) -> str:
        """评估收入影响"""
        service_name = uri_structure['service_name']
        
        if 'alipay' in service_name or 'payment' in service_name:
            return 'high'
        elif 'order' in service_name:
            return 'medium'
        
        return 'low'
    
    def _assess_customer_impact(self, uri_structure: Dict[str, Any]) -> str:
        """评估客户影响"""
        if uri_structure['is_static_resource']:
            return 'low'
        
        service_name = uri_structure['service_name']
        if 'user' in service_name or 'auth' in service_name:
            return 'high'
        
        return 'medium'
    
    def _generate_business_sign(self, uri_structure: Dict[str, Any]) -> str:
        """生成业务标识"""
        return f"{uri_structure['application_name']}.{uri_structure['service_name']}.{uri_structure['api_module']}"
    
    def _generate_trace_id(self, parsed_data: Dict[str, Any]) -> str:
        """生成链路追踪ID"""
        # 基于时间和IP生成唯一ID
        content = f"{parsed_data.get('time', '')}-{parsed_data.get('remote_addr', '')}"
        return hashlib.md5(content.encode()).hexdigest()[:16]
    
    def _infer_cache_status(self, response_time_ms: int) -> str:
        """推断缓存状态"""
        if response_time_ms == 0:
            return 'HIT'
        elif response_time_ms < 10:
            return 'HIT'
        elif response_time_ms > 3000:
            return 'MISS'
        
        return 'BYPASS'
    
    def _infer_environment(self, server_name: str) -> str:
        """推断环境"""
        if not server_name:
            return 'prod'
        
        server_lower = server_name.lower()
        
        if 'dev' in server_lower or 'develop' in server_lower:
            return 'dev'
        elif 'test' in server_lower or 'qa' in server_lower:
            return 'test'
        elif 'staging' in server_lower or 'stage' in server_lower:
            return 'staging'
        
        return 'prod'
    
    def _classify_error_group(self, status_code: str) -> str:
        """分类错误组"""
        try:
            code = int(status_code)
            if 400 <= code < 500:
                return '4xx_client'
            elif 500 <= code < 600:
                return '5xx_server'
            elif code == 0:
                return 'network_error'
        except:
            pass
        
        return ''
    
    def _classify_http_error_class(self, status_code: str) -> str:
        """分类HTTP错误类"""
        if status_code.startswith('2'):
            return 'success'
        elif status_code.startswith('3'):
            return 'redirection'
        elif status_code.startswith('4'):
            return 'client_error'
        elif status_code.startswith('5'):
            return 'server_error'
        
        return 'unknown'
    
    def _assess_error_severity(self, status_code: str) -> str:
        """评估错误严重程度"""
        try:
            code = int(status_code)
            if code >= 500:
                return 'critical'
            elif code >= 400:
                return 'medium'
        except:
            pass
        
        return 'low'
    
    def _classify_error_category(self, status_code: str) -> str:
        """分类错误类别"""
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
        except:
            pass
        
        return 'other'
    
    def _identify_error_source(self, status_code: str) -> str:
        """识别错误来源"""
        if status_code.startswith('4'):
            return 'client'
        elif status_code.startswith('5'):
            if status_code in ['502', '504']:
                return 'gateway'
            return 'service'
        
        return ''
    
    def _detect_sql_injection(self, uri: str) -> bool:
        """检测SQL注入"""
        if not uri:
            return False
        return self.patterns['sql_injection'].search(uri) is not None
    
    def _detect_xss(self, uri: str) -> bool:
        """检测XSS攻击"""
        if not uri:
            return False
        return self.patterns['xss_attack'].search(uri) is not None
    
    def _get_risk_level(self, risk_score: int) -> str:
        """获取风险级别"""
        if risk_score >= 80:
            return 'critical'
        elif risk_score >= 60:
            return 'high'
        elif risk_score >= 40:
            return 'medium'
        elif risk_score >= 20:
            return 'low'
        
        return 'none'
    
    def _get_empty_ua_info(self) -> Dict[str, Any]:
        """获取空UA信息"""
        return {
            'platform': 'Unknown',
            'platform_version': '',
            'platform_category': 'unknown',
            'device_type': 'Unknown',
            'device_model': '',
            'device_manufacturer': '',
            'browser_type': 'Unknown',
            'browser_version': '',
            'browser_engine': '',
            'os_type': 'Unknown',
            'os_version': '',
            'os_architecture': '',
            'app_version': '',
            'sdk_type': '',
            'sdk_version': '',
            'integration_type': 'unknown',
            'is_bot': False,
            'bot_type': '',
            'bot_name': '',
            'bot_probability': 0.0,
            'crawler_category': '',
        }
    
    def _create_fallback_record(self, parsed_data: Dict[str, Any], source_file: str, error_msg: str) -> Dict[str, Any]:
        """创建容错记录"""
        # 返回最小可用记录
        return {
            'log_time': datetime.now(),
            'client_ip': parsed_data.get('remote_addr', '0.0.0.0'),
            'request_method': 'UNKNOWN',
            'request_uri': '/',
            'response_status_code': '0',
            'response_body_size': 0,
            'total_request_duration': 0,
            'platform': 'Unknown',
            'device_type': 'Unknown',
            'parsing_errors': [error_msg],
            'data_quality_score': 0.0,
        }
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        return {
            'ua_cache_hit_rate': self.cache_stats['ua_hits'] / (self.cache_stats['ua_hits'] + self.cache_stats['ua_misses'])
            if (self.cache_stats['ua_hits'] + self.cache_stats['ua_misses']) > 0 else 0,
            'ip_cache_hit_rate': self.cache_stats['ip_hits'] / (self.cache_stats['ip_hits'] + self.cache_stats['ip_misses'])
            if (self.cache_stats['ip_hits'] + self.cache_stats['ip_misses']) > 0 else 0,
            'total_cache_hits': self.cache_stats['ua_hits'] + self.cache_stats['ip_hits'],
            'total_cache_misses': self.cache_stats['ua_misses'] + self.cache_stats['ip_misses'],
        }
    
    def clear_caches(self):
        """清空所有缓存"""
        self._parse_ua_with_library.cache_clear()
        self._parse_ua_fallback.cache_clear()
        self._resolve_geo_location.cache_clear()
        self._parse_uri_structure.cache_clear()
        self._parse_log_time.cache_clear()
        self._detect_search_engine.cache_clear()
        self._detect_social_media.cache_clear()
        
        # 重置缓存统计
        self.cache_stats = {
            'ua_hits': 0,
            'ua_misses': 0,
            'ip_hits': 0,
            'ip_misses': 0,
        }


# ========== 批处理优化类 ==========

class BatchFieldMapper(FieldMapper):
    """批处理优化的字段映射器"""
    
    def __init__(self, geoip_db_path: str = None, batch_size: int = 1000):
        super().__init__(geoip_db_path)
        self.batch_size = batch_size
        self.processed_count = 0
        # 添加政务相关模式编译
        self._compile_government_patterns()

    def _compile_government_patterns(self):
        """编译政务相关正则表达式"""
        gov_patterns = {
            # 政务SDK
            'wst_sdk': re.compile(r'WST-SDK-(iOS|Android|ANDROID)(?:/([0-9.]+))?', re.IGNORECASE),
            'zgt_app': re.compile(r'zgt-(ios|android)[/\s]?([0-9.]+)?', re.IGNORECASE),

            # 政务域名
            'gov_domain': re.compile(r'\.gov\.cn|zwfw\.|zzrs\.|rsj\.', re.IGNORECASE),

            # 政务关键词
            'gov_keywords': re.compile(r'政务|政府|办事|一网通办|电子政务|市民云|i深圳|随申办', re.IGNORECASE),
        }

        # 合并到主patterns字典
        self.patterns.update(gov_patterns)
        
    def process_batch(self, records: List[Dict[str, Any]], source_file: str = '') -> List[Dict[str, Any]]:
        """
        批量处理日志记录
        
        Args:
            records: 解析后的日志记录列表
            source_file: 源文件名
            
        Returns:
            映射后的DWD记录列表
        """
        results = []
        
        # 预热缓存
        self._preheat_caches(records)
        
        # 分批处理
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            
            for record in batch:
                try:
                    dwd_record = self.map_to_dwd(record, source_file)
                    results.append(dwd_record)
                    self.processed_count += 1
                except Exception as e:
                    self.logger.error(f"处理记录失败: {e}")
                    results.append(self._create_fallback_record(record, source_file, str(e)))
            
            # 定期清理缓存，避免内存溢出
            if self.processed_count % (self.batch_size * 10) == 0:
                self._cleanup_old_cache_entries()
                self.logger.info(f"已处理 {self.processed_count} 条记录，缓存命中率: UA={self._get_ua_cache_hit_rate():.2%}, IP={self._get_ip_cache_hit_rate():.2%}")
        
        return results
    
    def _preheat_caches(self, records: List[Dict[str, Any]]):
        """预热缓存，提取常见模式"""
        # 统计最常见的UA和IP
        ua_counter = {}
        ip_counter = {}
        
        sample_size = min(1000, len(records))
        sample = records[:sample_size]
        
        for record in sample:
            ua = record.get('agent', '')
            if ua:
                ua_counter[ua] = ua_counter.get(ua, 0) + 1
            
            ip = record.get('RealIp', record.get('remote_addr', ''))
            if ip:
                ip_counter[ip] = ip_counter.get(ip, 0) + 1
        
        # 预解析最常见的UA
        top_uas = sorted(ua_counter.items(), key=lambda x: x[1], reverse=True)[:100]
        for ua, _ in top_uas:
            if HAS_UA_PARSER:
                self._parse_ua_with_library(ua)
            else:
                self._parse_ua_fallback(ua)
        
        # 预解析最常见的IP
        top_ips = sorted(ip_counter.items(), key=lambda x: x[1], reverse=True)[:100]
        for ip, _ in top_ips:
            self._resolve_geo_location(ip)
        
        self.logger.info(f"缓存预热完成: 预解析了 {len(top_uas)} 个UA和 {len(top_ips)} 个IP")
    
    def _cleanup_old_cache_entries(self):
        """清理旧缓存条目，保留最近使用的"""
        # LRU缓存会自动管理，但可以根据需要手动清理
        cache_info = self._parse_ua_with_library.cache_info() if HAS_UA_PARSER else self._parse_ua_fallback.cache_info()
        
        if cache_info.currsize > cache_info.maxsize * 0.9:
            # 缓存接近满，可以考虑增大缓存大小或清理
            self.logger.warning(f"UA缓存接近满: {cache_info.currsize}/{cache_info.maxsize}")
    
    def _get_ua_cache_hit_rate(self) -> float:
        """获取UA缓存命中率"""
        if HAS_UA_PARSER:
            cache_info = self._parse_ua_with_library.cache_info()
        else:
            cache_info = self._parse_ua_fallback.cache_info()
        
        total = cache_info.hits + cache_info.misses
        return cache_info.hits / total if total > 0 else 0
    
    def _get_ip_cache_hit_rate(self) -> float:
        """获取IP缓存命中率"""
        cache_info = self._resolve_geo_location.cache_info()
        total = cache_info.hits + cache_info.misses
        return cache_info.hits / total if total > 0 else 0
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """获取处理统计信息"""
        return {
            'processed_count': self.processed_count,
            'ua_cache_hit_rate': self._get_ua_cache_hit_rate(),
            'ip_cache_hit_rate': self._get_ip_cache_hit_rate(),
            'cache_stats': self.get_cache_stats(),
        }


# ========== 使用示例 ==========

if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 示例日志数据
    sample_logs = [
        {
            'http_host': 'aa1.bbb.ccc.gov.cn',
            'remote_addr': '100.100.8.44',
            'remote_port': '10305',
            'time': '2025-04-23T00:00:04+08:00',
            'request': 'GET /group1/M00/06/59/rBAWN2bRGW-AXqJSAALv84fWgSo873.jpg HTTP/1.1',
            'code': '200',
            'body': '192499',
            'http_referer': '-',
            'ar_time': '0.004',
            'RealIp': '100.100.8.44',
            'agent': 'zgt-ios/1.4.1 (iPhone; iOS 15.4.1; Scale/3.00)'
        },
        {
            'http_host': 'aa1.bbb.ccc.gov.cn',
            'remote_addr': '100.100.8.45',
            'remote_port': '17113',
            'time': '2025-04-23T00:00:06+08:00',
            'request': 'POST /scmp-gateway/gxrz-rest/newUser/queryAllUserInfoAes HTTP/1.1',
            'code': '200',
            'body': '1063',
            'http_referer': '-',
            'ar_time': '0.370',
            'RealIp': '100.100.8.45',
            'agent': 'WST-SDK-ANDROID'
        }
    ]
    
    # 创建映射器
    # 注意：需要下载GeoLite2-City.mmdb文件
    # 下载地址：https://dev.maxmind.com/geoip/geolite2-free-geolocation-data
    mapper = BatchFieldMapper(geoip_db_path='/path/to/GeoLite2-City.mmdb')
    
    # 处理日志
    results = mapper.process_batch(sample_logs, source_file='sample.log')
    
    # 输出结果
    for i, result in enumerate(results, 1):
        print(f"\n===== 记录 {i} =====")
        print(f"时间: {result.get('log_time')}")
        print(f"客户端IP: {result.get('client_ip')}")
        print(f"请求: {result.get('request_method')} {result.get('request_uri')}")
        print(f"响应: {result.get('response_status_code')} ({result.get('response_body_size_kb')} KB)")
        print(f"耗时: {result.get('total_request_duration')} ms")
        print(f"性能等级: {result.get('performance_level')} - {result.get('user_experience_level')}")
        print(f"平台: {result.get('platform')} ({result.get('device_type')})")
        print(f"业务域: {result.get('business_domain')}")
        print(f"业务价值: {result.get('business_value_score')}")
        print(f"数据质量: {result.get('data_quality_score'):.1f}%")
    
    # 输出统计信息
    stats = mapper.get_processing_stats()
    print(f"\n===== 处理统计 =====")
    print(f"处理记录数: {stats['processed_count']}")
    print(f"UA缓存命中率: {stats['ua_cache_hit_rate']:.2%}")
    print(f"IP缓存命中率: {stats['ip_cache_hit_rate']:.2%}")