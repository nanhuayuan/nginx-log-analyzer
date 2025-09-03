#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nginx日志数据处理器模块 - 业务逻辑处理
Nginx Log Data Processor Module - Business Logic Processing

专门负责对解析后的原始数据进行业务逻辑处理和增强
包括平台识别、API分类、性能指标计算、异常检测等
"""

import re
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import urlparse, parse_qs
import logging

class DataProcessor:
    """数据处理器 - 业务逻辑增强"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # 平台识别规则
        self.platform_patterns = {
            'WST-SDK-iOS': 'iOS',
            'WST-SDK-ANDROID': 'Android',
            'WST-SDK-HarmonyOS': 'HarmonyOS',
            'okhttp': 'Android',
            'CFNetwork': 'iOS',
            'Darwin': 'iOS',
            'iPhone': 'iOS',
            'iPad': 'iOS',
            'Android': 'Android',
            'Mobile': 'Mobile',
            'Chrome': 'Web',
            'Firefox': 'Web',
            'Safari': 'Web',
            'Edge': 'Web'
        }
        
        # API分类规则
        self.api_categories = {
            'login': {'category': 'auth', 'module': 'user', 'importance': 'Critical'},
            'logout': {'category': 'auth', 'module': 'user', 'importance': 'High'},
            'register': {'category': 'auth', 'module': 'user', 'importance': 'Critical'},
            'search': {'category': 'search', 'module': 'search', 'importance': 'High'},
            'calendar': {'category': 'calendar', 'module': 'calendar', 'importance': 'Medium'},
            'upload': {'category': 'file', 'module': 'upload', 'importance': 'High'},
            'download': {'category': 'file', 'module': 'download', 'importance': 'Medium'},
            'gxrz': {'category': 'gxrz', 'module': 'gxrz', 'importance': 'High'},
            'zgt': {'category': 'zgt', 'module': 'zgt', 'importance': 'High'},
            'api': {'category': 'api', 'module': 'general', 'importance': 'Medium'},
            'group': {'category': 'static', 'module': 'cdn', 'importance': 'Low'}
        }
        
        # 响应时间阈值(秒)
        self.performance_thresholds = {
            'fast': 0.1,      # 快速响应
            'normal': 1.0,    # 正常响应
            'slow': 3.0,      # 慢响应
            'very_slow': 10.0 # 超慢响应
        }
        
        # 状态码分类
        self.status_categories = {
            'success': ['200', '201', '202', '204'],
            'redirect': ['301', '302', '303', '304', '307', '308'],
            'client_error': ['400', '401', '403', '404', '405', '408', '409', '410', '413', '429'],
            'server_error': ['500', '501', '502', '503', '504', '505']
        }
        
        # 机器人识别
        self.bot_patterns = [
            'bot', 'crawl', 'spider', 'scraper', 'wget', 'curl', 'python', 'java', 'go-http'
        ]
    
    def extract_platform_info(self, user_agent: str) -> Dict[str, str]:
        """提取平台信息"""
        if not user_agent:
            return {
                'platform': 'Unknown',
                'platform_version': '',
                'device_type': 'Unknown',
                'browser_type': '',
                'os_type': 'Unknown',
                'os_version': ''
            }
        
        ua_lower = user_agent.lower()
        
        # 平台识别
        platform = 'Unknown'
        for pattern, platform_name in self.platform_patterns.items():
            if pattern.lower() in ua_lower:
                platform = platform_name
                break
        
        # 设备类型识别
        device_type = 'Unknown'
        if any(x in ua_lower for x in ['mobile', 'android', 'iphone']):
            device_type = 'Mobile'
        elif any(x in ua_lower for x in ['ipad', 'tablet']):
            device_type = 'Tablet'
        elif any(x in ua_lower for x in ['windows', 'mac', 'linux', 'chrome', 'firefox']):
            device_type = 'Desktop'
        elif any(x in ua_lower for x in self.bot_patterns):
            device_type = 'Bot'
        
        # 浏览器类型
        browser_type = ''
        if 'chrome' in ua_lower:
            browser_type = 'Chrome'
        elif 'firefox' in ua_lower:
            browser_type = 'Firefox'
        elif 'safari' in ua_lower and 'chrome' not in ua_lower:
            browser_type = 'Safari'
        elif 'edge' in ua_lower:
            browser_type = 'Edge'
        elif any(x in ua_lower for x in ['okhttp', 'wst-sdk']):
            browser_type = 'NativeApp'
        
        # 操作系统
        os_type = 'Unknown'
        if 'android' in ua_lower:
            os_type = 'Android'
        elif any(x in ua_lower for x in ['iphone', 'ipad', 'ios']):
            os_type = 'iOS'
        elif 'harmonyos' in ua_lower:
            os_type = 'HarmonyOS'
        elif 'windows' in ua_lower:
            os_type = 'Windows'
        elif 'mac' in ua_lower:
            os_type = 'macOS'
        elif 'linux' in ua_lower:
            os_type = 'Linux'
        
        return {
            'platform': platform,
            'platform_version': self.extract_version(user_agent),
            'device_type': device_type,
            'browser_type': browser_type,
            'os_type': os_type,
            'os_version': '',
            'is_bot': device_type == 'Bot'
        }
    
    def extract_version(self, text: str) -> str:
        """提取版本号"""
        if not text:
            return ''
        
        # 匹配版本号模式
        version_patterns = [
            r'(\d+\.\d+\.\d+)',
            r'(\d+\.\d+)',
            r'/(\d+\.\d+\.\d+)',
            r'/(\d+\.\d+)'
        ]
        
        for pattern in version_patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        
        return ''
    
    def classify_api(self, uri_path: str) -> Dict[str, str]:
        """API分类"""
        if not uri_path:
            return {
                'api_category': 'unknown',
                'api_module': 'unknown',
                'business_domain': 'unknown',
                'api_version': '',
                'api_importance': 'Low'
            }
        
        uri_lower = uri_path.lower()
        
        # 查找匹配的API分类
        for keyword, info in self.api_categories.items():
            if keyword in uri_lower:
                return {
                    'api_category': info['category'],
                    'api_module': info['module'],
                    'business_domain': info['category'],
                    'api_version': self.extract_api_version(uri_path),
                    'api_importance': info['importance']
                }
        
        # 默认分类
        return {
            'api_category': 'general',
            'api_module': 'unknown',
            'business_domain': 'general',
            'api_version': self.extract_api_version(uri_path),
            'api_importance': 'Medium'
        }
    
    def extract_api_version(self, uri_path: str) -> str:
        """提取API版本"""
        if not uri_path:
            return ''
        
        # 匹配版本模式
        version_patterns = [
            r'/v(\d+)',
            r'/api/v(\d+)',
            r'version=(\d+)',
            r'ver=(\d+)'
        ]
        
        for pattern in version_patterns:
            match = re.search(pattern, uri_path.lower())
            if match:
                return f"v{match.group(1)}"
        
        if '/rest/' in uri_path.lower():
            return 'rest'
        
        return 'v1'  # 默认版本
    
    def calculate_performance_metrics(self, parsed_data: Dict[str, Any]) -> Dict[str, Any]:
        """计算性能指标"""
        metrics = {}
        
        # 基础响应时间
        response_time = parsed_data.get('ar_time_float', 0.0)
        metrics['total_request_duration'] = response_time
        
        # 上游时间指标
        upstream_response_time = parsed_data.get('upstream_response_time_float', 0.0)
        upstream_connect_time = parsed_data.get('upstream_connect_time_float', 0.0)
        upstream_header_time = parsed_data.get('upstream_header_time_float', 0.0)
        
        metrics['upstream_response_time'] = upstream_response_time
        metrics['upstream_connect_time'] = upstream_connect_time
        metrics['upstream_header_time'] = upstream_header_time
        
        # 计算各阶段耗时
        if upstream_response_time > 0:
            metrics['backend_connect_phase'] = upstream_connect_time
            metrics['backend_process_phase'] = max(0, upstream_header_time - upstream_connect_time)
            metrics['backend_transfer_phase'] = max(0, upstream_response_time - upstream_header_time)
            metrics['nginx_transfer_phase'] = max(0, response_time - upstream_response_time)
            metrics['backend_total_phase'] = upstream_response_time
        else:
            # 没有上游数据时的默认值
            metrics['backend_connect_phase'] = 0.0
            metrics['backend_process_phase'] = 0.0
            metrics['backend_transfer_phase'] = 0.0
            metrics['nginx_transfer_phase'] = response_time
            metrics['backend_total_phase'] = 0.0
        
        # 网络和处理阶段
        metrics['network_phase'] = metrics['backend_connect_phase']
        metrics['processing_phase'] = metrics['backend_process_phase']
        metrics['transfer_phase'] = metrics['backend_transfer_phase'] + metrics['nginx_transfer_phase']
        
        # 响应大小和传输速度
        response_size = parsed_data.get('body_int', 0)
        metrics['response_body_size'] = response_size
        metrics['response_body_size_kb'] = response_size / 1024.0
        
        if response_time > 0 and response_size > 0:
            metrics['response_transfer_speed'] = response_size / response_time  # bytes/sec
            metrics['total_transfer_speed'] = metrics['response_transfer_speed']
            metrics['nginx_transfer_speed'] = response_size / max(0.001, metrics['nginx_transfer_phase'])
        else:
            metrics['response_transfer_speed'] = 0.0
            metrics['total_transfer_speed'] = 0.0
            metrics['nginx_transfer_speed'] = 0.0
        
        # 效率指标
        if response_time > 0:
            metrics['backend_efficiency'] = metrics['backend_total_phase'] / response_time
            metrics['network_overhead'] = metrics['network_phase'] / response_time
            metrics['transfer_ratio'] = metrics['transfer_phase'] / response_time
            metrics['connection_cost_ratio'] = metrics['backend_connect_phase'] / response_time
        else:
            metrics['backend_efficiency'] = 0.0
            metrics['network_overhead'] = 0.0
            metrics['transfer_ratio'] = 0.0
            metrics['connection_cost_ratio'] = 0.0
        
        # 处理效率指数 (综合指标)
        processing_efficiency = 0.0
        if response_time > 0:
            # 考虑响应时间、传输速度、后端效率
            time_score = max(0, 1 - response_time / 10.0)  # 10秒为最差
            speed_score = min(1.0, metrics['response_transfer_speed'] / 1000000)  # 1MB/s为满分
            backend_score = metrics['backend_efficiency']
            processing_efficiency = (time_score * 0.4 + speed_score * 0.3 + backend_score * 0.3)
        
        metrics['processing_efficiency_index'] = processing_efficiency
        
        return metrics
    
    def classify_performance(self, response_time: float) -> Dict[str, Any]:
        """性能分类"""
        result = {
            'is_slow': response_time > self.performance_thresholds['slow'],
            'is_very_slow': response_time > self.performance_thresholds['very_slow'],
            'performance_level': 'Unknown'
        }
        
        if response_time <= self.performance_thresholds['fast']:
            result['performance_level'] = 'Excellent'
            result['user_experience_level'] = 'Excellent'
            result['apdex_classification'] = 'Satisfied'
        elif response_time <= self.performance_thresholds['normal']:
            result['performance_level'] = 'Good'
            result['user_experience_level'] = 'Good'
            result['apdex_classification'] = 'Satisfied'
        elif response_time <= self.performance_thresholds['slow']:
            result['performance_level'] = 'Fair'
            result['user_experience_level'] = 'Fair'
            result['apdex_classification'] = 'Tolerating'
        elif response_time <= self.performance_thresholds['very_slow']:
            result['performance_level'] = 'Poor'
            result['user_experience_level'] = 'Poor'
            result['apdex_classification'] = 'Frustrated'
        else:
            result['performance_level'] = 'Unacceptable'
            result['user_experience_level'] = 'Unacceptable'
            result['apdex_classification'] = 'Frustrated'
        
        return result
    
    def classify_status_code(self, status_code: str) -> Dict[str, bool]:
        """状态码分类"""
        if not status_code:
            return {
                'is_success': False,
                'is_error': True,
                'is_client_error': False,
                'is_server_error': False,
                'is_business_success': False
            }
        
        result = {
            'is_success': status_code in self.status_categories['success'],
            'is_error': False,
            'is_client_error': status_code in self.status_categories['client_error'],
            'is_server_error': status_code in self.status_categories['server_error'],
            'is_business_success': status_code in self.status_categories['success']
        }
        
        result['is_error'] = result['is_client_error'] or result['is_server_error']
        
        return result
    
    def detect_anomalies(self, processed_data: Dict[str, Any]) -> Dict[str, Any]:
        """异常检测"""
        anomalies = []
        
        response_time = processed_data.get('total_request_duration', 0.0)
        response_size = processed_data.get('response_body_size', 0)
        status_code = processed_data.get('code', '')
        
        # 响应时间异常
        if response_time > 30:
            anomalies.append('超长响应时间')
        elif response_time > 10:
            anomalies.append('异常响应时间')
        
        # 响应大小异常
        if response_size > 50 * 1024 * 1024:  # 50MB
            anomalies.append('超大响应体')
        elif response_size > 10 * 1024 * 1024:  # 10MB
            anomalies.append('大响应体')
        
        # 状态码异常
        if status_code.startswith('5'):
            anomalies.append('服务器错误')
        elif status_code in ['404', '403']:
            anomalies.append('资源访问异常')
        elif status_code == '429':
            anomalies.append('频率限制')
        
        # 性能异常
        backend_efficiency = processed_data.get('backend_efficiency', 1.0)
        if backend_efficiency > 0.9:  # 后端占用过多时间
            anomalies.append('后端性能异常')
        
        return {
            'has_anomaly': len(anomalies) > 0,
            'anomaly_type': ','.join(anomalies) if anomalies else '',
            'anomaly_count': len(anomalies)
        }
    
    def calculate_business_value_score(self, api_info: Dict[str, str], performance_data: Dict[str, Any]) -> int:
        """计算业务价值评分 (1-10)"""
        base_score = 5  # 默认分数
        
        # 根据重要性调整
        importance = api_info.get('api_importance', 'Medium')
        if importance == 'Critical':
            base_score = 9
        elif importance == 'High':
            base_score = 7
        elif importance == 'Medium':
            base_score = 5
        else:
            base_score = 3
        
        # 根据性能表现微调
        if performance_data.get('user_experience_level') == 'Excellent':
            base_score += 1
        elif performance_data.get('user_experience_level') == 'Unacceptable':
            base_score -= 1
        
        return max(1, min(10, base_score))
    
    def process_single_record(self, parsed_data: Dict[str, Any]) -> Dict[str, Any]:
        """处理单条记录"""
        try:
            # 创建输出结构
            processed = parsed_data.copy()
            
            # 生成唯一ID
            raw_line = parsed_data.get('raw_line', '')
            unique_str = f"{parsed_data.get('log_time', '')}{raw_line}"
            processed['id'] = int(hashlib.md5(unique_str.encode()).hexdigest()[:15], 16)
            
            # 提取基础信息
            user_agent = parsed_data.get('agent', '')
            uri_path = parsed_data.get('uri_path', '')
            status_code = parsed_data.get('code', '')
            
            # 平台信息处理
            platform_info = self.extract_platform_info(user_agent)
            processed.update(platform_info)
            
            # API分类
            api_info = self.classify_api(uri_path)
            processed.update(api_info)
            
            # 性能指标计算
            performance_metrics = self.calculate_performance_metrics(parsed_data)
            processed.update(performance_metrics)
            
            # 性能分类
            response_time = performance_metrics.get('total_request_duration', 0.0)
            performance_classification = self.classify_performance(response_time)
            processed.update(performance_classification)
            
            # 状态码分类
            status_classification = self.classify_status_code(status_code)
            processed.update(status_classification)
            
            # 异常检测
            anomaly_info = self.detect_anomalies(processed)
            processed.update(anomaly_info)
            
            # 业务价值评分
            business_score = self.calculate_business_value_score(api_info, performance_classification)
            processed['business_value_score'] = business_score
            
            # 时间维度处理
            if parsed_data.get('log_time'):
                log_time = parsed_data['log_time']
                processed.update({
                    'date_partition': log_time.date(),
                    'hour_partition': log_time.hour,
                    'minute_partition': log_time.minute,
                    'second_partition': log_time.second,
                    'weekday': log_time.weekday() + 1,
                    'is_weekend': log_time.weekday() >= 5
                })
            
            # 访问类型分类
            processed['access_type'] = self.classify_access_type(platform_info, uri_path)
            processed['client_category'] = self.classify_client_category(platform_info)
            
            # 链路跟踪和服务信息
            processed.update({
                'trace_id': '',
                'business_sign': api_info.get('api_category', ''),
                'cluster_node': '',
                'upstream_server': '',
                'connection_requests': 1,
                'cache_status': '',
                'service_name': api_info.get('api_module', ''),
                'application_name': platform_info.get('platform', 'Unknown')
            })
            
            # IP风险评估 (简单版本)
            client_ip = parsed_data.get('remote_addr', '')
            processed.update(self.assess_ip_risk(client_ip))
            
            # 数据质量重新评估
            processed['data_quality_score'] = self.recalculate_quality_score(processed)
            
            # 设置创建时间
            processed.update({
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            })
            
            return processed
            
        except Exception as e:
            self.logger.error(f"处理记录时发生错误: {e}")
            # 返回原始数据加上错误信息
            error_data = parsed_data.copy()
            error_data['processing_errors'] = [str(e)]
            error_data['data_quality_score'] = 0.1
            return error_data
    
    def classify_access_type(self, platform_info: Dict[str, str], uri_path: str) -> str:
        """分类访问类型"""
        device_type = platform_info.get('device_type', 'Unknown')
        browser_type = platform_info.get('browser_type', '')
        
        if device_type == 'Bot':
            return 'Bot_Crawler'
        elif browser_type == 'NativeApp':
            return 'APP_Native'
        elif device_type == 'Mobile' and browser_type in ['Chrome', 'Safari', 'Firefox']:
            return 'H5_WebView'
        elif device_type == 'Desktop':
            return 'Browser_Desktop'
        elif uri_path and '/api/' in uri_path:
            return 'API_Direct'
        else:
            return 'Unknown'
    
    def classify_client_category(self, platform_info: Dict[str, str]) -> str:
        """客户端分类"""
        device_type = platform_info.get('device_type', 'Unknown')
        platform = platform_info.get('platform', 'Unknown')
        
        if device_type in ['Mobile', 'Tablet']:
            return f'Mobile_{platform}'
        elif device_type == 'Desktop':
            return 'Desktop_Web'
        elif device_type == 'Bot':
            return 'Bot_Crawler'
        else:
            return 'Unknown'
    
    def assess_ip_risk(self, client_ip: str) -> Dict[str, Any]:
        """IP风险评估"""
        if not client_ip:
            return {
                'client_region': 'Unknown',
                'client_isp': 'Unknown',
                'ip_risk_level': 'Unknown',
                'is_internal_ip': False
            }
        
        # 简单的内网IP判断
        is_internal = any([
            client_ip.startswith('192.168.'),
            client_ip.startswith('10.'),
            client_ip.startswith('172.'),
            client_ip.startswith('127.'),
            client_ip == 'localhost'
        ])
        
        # 风险等级评估 (简化版本)
        risk_level = 'Low'
        if is_internal:
            risk_level = 'Internal'
        elif client_ip.startswith('100.100.'):
            risk_level = 'Low'  # 阿里云内网
        
        return {
            'client_region': 'Unknown',  # 需要IP库支持
            'client_isp': 'Unknown',     # 需要IP库支持
            'ip_risk_level': risk_level,
            'is_internal_ip': is_internal
        }
    
    def recalculate_quality_score(self, processed_data: Dict[str, Any]) -> float:
        """重新计算数据质量评分"""
        # 基础质量分数
        base_score = processed_data.get('data_quality_score', 0.5)
        
        # 加分项
        bonus = 0.0
        if processed_data.get('platform') != 'Unknown':
            bonus += 0.1
        if processed_data.get('api_category') != 'unknown':
            bonus += 0.1
        if processed_data.get('total_request_duration', 0) > 0:
            bonus += 0.1
        
        # 扣分项
        penalty = 0.0
        if processed_data.get('processing_errors'):
            penalty += 0.2
        if processed_data.get('has_anomaly'):
            penalty += 0.1
        
        final_score = base_score + bonus - penalty
        return max(0.0, min(1.0, final_score))

def test_processor():
    """测试数据处理器"""
    processor = DataProcessor()
    
    # 测试数据
    test_data = {
        'raw_line': 'test line',
        'log_time': datetime.now(),
        'http_host': 'zgtapp.zwfw.gxzf.gov.cn',
        'remote_addr': '100.100.8.44',
        'method': 'GET',
        'uri_path': '/api/v1/user/login',
        'code': '200',
        'body_int': 234,
        'ar_time_float': 0.156,
        'agent': 'WST-SDK-iOS/2.1.0',
        'data_quality_score': 0.8
    }
    
    print("=" * 60)
    print("数据处理器测试")
    print("=" * 60)
    
    result = processor.process_single_record(test_data)
    
    print(f"原始数据: {test_data.get('agent')}")
    print(f"平台识别: {result.get('platform')}")
    print(f"设备类型: {result.get('device_type')}")
    print(f"API分类: {result.get('api_category')}")
    print(f"业务模块: {result.get('api_module')}")
    print(f"响应时间: {result.get('total_request_duration'):.3f}s")
    print(f"性能等级: {result.get('user_experience_level')}")
    print(f"是否成功: {result.get('is_success')}")
    print(f"业务价值: {result.get('business_value_score')}/10")
    print(f"质量评分: {result.get('data_quality_score'):.2f}")

if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    test_processor()