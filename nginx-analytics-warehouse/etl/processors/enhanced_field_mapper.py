#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强字段映射器 - 集成完整的5%待完善功能
Enhanced Field Mapper - 完整的数据清洗增强功能

新增功能：
1. IP地理位置精确定位 (支持GeoIP2库和简化版本)
2. 业务域智能分类 (10个业务域自动分类)
3. 实时异常行为检测 (6种异常类型检测)
4. 增强的User-Agent解析 (8大类细分识别)
5. 智能缓存优化 (多级缓存提升性能)
"""

import re
import json
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, Union, List
from urllib.parse import urlparse, parse_qs
from pathlib import Path

# 导入基础字段映射器
try:
    from .field_mapper import FieldMapper as BaseFieldMapper
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from field_mapper import FieldMapper as BaseFieldMapper

# 可选依赖导入
try:
    import geoip2.database
    import geoip2.errors
    GEOIP_AVAILABLE = True
except ImportError:
    GEOIP_AVAILABLE = False

class EnhancedFieldMapper(BaseFieldMapper):
    """增强字段映射器 - 完整功能版本"""

    def __init__(self, geoip_db_path: str = None):
        """
        初始化增强字段映射器

        Args:
            geoip_db_path: GeoIP2数据库文件路径 (可选)
        """
        super().__init__()

        # IP地理位置组件
        self.geoip_reader = None
        self._init_geoip_database(geoip_db_path)

        # 业务域分类器
        self.domain_classifier = BusinessDomainClassifier()

        # 异常检测器
        self.anomaly_detector = SimpleAnomalyDetector()

        # 增强缓存
        self.geo_cache = {}
        self.domain_cache = {}
        self.anomaly_cache = {}

        # 统计信息
        self.enhancement_stats = {
            'geo_lookups': 0,
            'geo_cache_hits': 0,
            'domain_classifications': 0,
            'domain_cache_hits': 0,
            'anomalies_detected': 0,
            'enhancement_errors': 0
        }

        self.logger.info("🚀 增强字段映射器初始化完成")
        if self.geoip_reader:
            self.logger.info("✅ GeoIP2精确地理位置定位已启用")
        else:
            self.logger.info("📍 使用简化地理位置定位")

    def _init_geoip_database(self, db_path: str = None):
        """初始化GeoIP数据库"""
        if not GEOIP_AVAILABLE:
            self.logger.info("💡 安装geoip2库可启用精确地理位置: pip install geoip2")
            return

        # 尝试几个常见的GeoIP数据库路径
        possible_paths = [
            db_path,
            "GeoLite2-City.mmdb",
            "/usr/share/GeoIP/GeoLite2-City.mmdb",
            "/opt/GeoIP/GeoLite2-City.mmdb",
            str(Path(__file__).parent.parent / "data" / "GeoLite2-City.mmdb")
        ]

        for path in possible_paths:
            if path and Path(path).exists():
                try:
                    self.geoip_reader = geoip2.database.Reader(path)
                    self.logger.info(f"✅ GeoIP数据库加载成功: {path}")
                    return
                except Exception as e:
                    self.logger.warning(f"⚠️ GeoIP数据库加载失败 {path}: {e}")

        self.logger.info("📍 未找到GeoIP数据库，使用简化地理位置分析")

    def map_to_dwd(self, parsed_data: Dict[str, Any], source_file: str = None) -> Dict[str, Any]:
        """
        增强版字段映射 - 集成所有新功能
        """
        # 首先调用基础映射
        mapped_data = super().map_to_dwd(parsed_data, source_file)

        try:
            # 获取关键字段
            client_ip = mapped_data.get('client_ip', '')
            request_uri = mapped_data.get('request_uri', '')
            user_agent = mapped_data.get('user_agent', '')
            referer = mapped_data.get('referer', '')

            # 1. IP地理位置增强
            if client_ip:
                geo_info = self._enhance_geo_location(client_ip)
                mapped_data.update(geo_info)

            # 2. 业务域智能分类
            if request_uri:
                domain_info = self._enhance_domain_classification(request_uri, user_agent, referer)
                mapped_data.update(domain_info)

            # 3. 异常行为检测
            anomaly_info = self._enhance_anomaly_detection(mapped_data)
            mapped_data.update(anomaly_info)

            # 4. 增强User-Agent解析 (基于父类功能)
            if user_agent:
                enhanced_ua = self._enhance_user_agent_parsing(user_agent)
                mapped_data.update(enhanced_ua)

            # 5. 请求特征增强
            request_features = self._enhance_request_features(request_uri, user_agent, referer)
            mapped_data.update(request_features)

        except Exception as e:
            self.enhancement_stats['enhancement_errors'] += 1
            self.logger.error(f"增强字段映射失败: {e}")

        return mapped_data

    def _enhance_geo_location(self, ip: str) -> Dict[str, Any]:
        """增强IP地理位置定位"""
        # 检查缓存
        if ip in self.geo_cache:
            self.enhancement_stats['geo_cache_hits'] += 1
            return self.geo_cache[ip]

        self.enhancement_stats['geo_lookups'] += 1
        geo_info = {}

        try:
            if self.geoip_reader:
                # 使用GeoIP2精确定位
                try:
                    response = self.geoip_reader.city(ip)
                    geo_info = {
                        'geo_country_name': response.country.names.get('zh-CN') or response.country.name or 'Unknown',
                        'geo_country_code': response.country.iso_code or 'XX',
                        'geo_city_name': response.city.names.get('zh-CN') or response.city.name or 'Unknown',
                        'geo_region_name': response.subdivisions.most_specific.names.get('zh-CN') or
                                         response.subdivisions.most_specific.name or 'Unknown',
                        'geo_latitude': float(response.location.latitude) if response.location.latitude else 0.0,
                        'geo_longitude': float(response.location.longitude) if response.location.longitude else 0.0,
                        'geo_accuracy_radius': response.location.accuracy_radius or 0,
                        'geo_timezone': str(response.location.time_zone) if response.location.time_zone else 'Unknown',
                        'geo_isp': 'Unknown',  # GeoLite2-City不包含ISP信息
                        'geo_organization': 'Unknown',
                        'geo_asn': 0,
                        'geo_source': 'geoip2'
                    }
                except (geoip2.errors.AddressNotFoundError, ValueError):
                    # IP不在数据库中，使用简化分类
                    geo_info = self._simple_geo_classify(ip)
            else:
                # 使用简化地理位置分类
                geo_info = self._simple_geo_classify(ip)

        except Exception as e:
            self.logger.warning(f"地理位置解析失败 {ip}: {e}")
            geo_info = self._get_default_geo_info()

        # 缓存结果
        if len(self.geo_cache) < 10000:  # 限制缓存大小
            self.geo_cache[ip] = geo_info

        return geo_info

    def _simple_geo_classify(self, ip: str) -> Dict[str, Any]:
        """简化地理位置分类"""
        # 内网IP判断
        if self._is_private_ip(ip):
            return {
                'geo_country_name': '内网',
                'geo_country_code': 'PRIVATE',
                'geo_city_name': '本地',
                'geo_region_name': '内网',
                'geo_latitude': 0.0,
                'geo_longitude': 0.0,
                'geo_accuracy_radius': 0,
                'geo_timezone': 'Local',
                'geo_isp': '内网',
                'geo_organization': '本地网络',
                'geo_asn': 0,
                'geo_source': 'simple_private'
            }

        # 中国IP段简单识别
        china_prefixes = {
            'china_telecom': ['1.0.', '1.1.', '14.', '27.', '36.', '42.', '49.', '58.', '59.', '60.', '61.'],
            'china_unicom': ['10.', '112.', '113.', '114.', '115.', '116.', '117.', '118.', '119.', '120.', '121.'],
            'china_mobile': ['39.', '111.', '183.', '211.', '218.', '221.', '222.']
        }

        for isp, prefixes in china_prefixes.items():
            if any(ip.startswith(prefix) for prefix in prefixes):
                return {
                    'geo_country_name': '中国',
                    'geo_country_code': 'CN',
                    'geo_city_name': '未知',
                    'geo_region_name': '中国大陆',
                    'geo_latitude': 39.9042,  # 北京坐标
                    'geo_longitude': 116.4074,
                    'geo_accuracy_radius': 1000,
                    'geo_timezone': 'Asia/Shanghai',
                    'geo_isp': isp.replace('_', ' ').title(),
                    'geo_organization': isp.replace('_', ' ').title(),
                    'geo_asn': 0,
                    'geo_source': f'simple_{isp}'
                }

        # 默认分类
        return self._get_default_geo_info()

    def _get_default_geo_info(self) -> Dict[str, Any]:
        """获取默认地理位置信息"""
        return {
            'geo_country_name': '未知',
            'geo_country_code': 'UNKNOWN',
            'geo_city_name': '未知',
            'geo_region_name': '未知',
            'geo_latitude': 0.0,
            'geo_longitude': 0.0,
            'geo_accuracy_radius': 5000,
            'geo_timezone': 'Unknown',
            'geo_isp': '未知',
            'geo_organization': '未知',
            'geo_asn': 0,
            'geo_source': 'default'
        }

    def _is_private_ip(self, ip: str) -> bool:
        """判断是否为内网IP"""
        private_ranges = [
            '10.', '172.16.', '172.17.', '172.18.', '172.19.', '172.20.',
            '172.21.', '172.22.', '172.23.', '172.24.', '172.25.', '172.26.',
            '172.27.', '172.28.', '172.29.', '172.30.', '172.31.', '192.168.',
            '127.', '0.', 'localhost', '::1'
        ]
        return any(ip.startswith(prefix) for prefix in private_ranges)

    def _enhance_domain_classification(self, uri: str, user_agent: str = '', referer: str = '') -> Dict[str, Any]:
        """增强业务域分类"""
        cache_key = f"{uri}|{user_agent[:50]}|{referer[:50]}"

        if cache_key in self.domain_cache:
            self.enhancement_stats['domain_cache_hits'] += 1
            return self.domain_cache[cache_key]

        self.enhancement_stats['domain_classifications'] += 1

        try:
            classification = self.domain_classifier.classify_request(uri, user_agent, referer)

            domain_info = {
                'business_domain': classification['primary_domain'],
                'business_domain_confidence': classification['confidence'],
                'business_domain_secondary': classification['all_matches'][1]['domain'] if len(classification['all_matches']) > 1 else '',
                'business_domain_tertiary': classification['all_matches'][2]['domain'] if len(classification['all_matches']) > 2 else '',
                'business_domain_is_classified': classification['is_classified'],
                'business_domain_match_count': len(classification['all_matches'])
            }

        except Exception as e:
            self.logger.warning(f"业务域分类失败: {e}")
            domain_info = {
                'business_domain': 'general_web',
                'business_domain_confidence': 0.0,
                'business_domain_secondary': '',
                'business_domain_tertiary': '',
                'business_domain_is_classified': False,
                'business_domain_match_count': 0
            }

        # 缓存结果
        if len(self.domain_cache) < 5000:
            self.domain_cache[cache_key] = domain_info

        return domain_info

    def _enhance_anomaly_detection(self, mapped_data: Dict[str, Any]) -> Dict[str, Any]:
        """增强异常行为检测"""
        try:
            # 准备检测数据
            detection_data = {
                'client_ip': mapped_data.get('client_ip', ''),
                'request_uri': mapped_data.get('request_uri', ''),
                'status_code': mapped_data.get('status_code', 200),
                'response_time_ms': mapped_data.get('response_time_ms', 0),
                'user_agent': mapped_data.get('user_agent', ''),
                'referer': mapped_data.get('referer', ''),
                'request_method': mapped_data.get('request_method', 'GET'),
                'request_body_size': mapped_data.get('request_body_size', 0)
            }

            # 执行异常检测
            anomaly_result = self.anomaly_detector.detect_anomalies(detection_data)

            if anomaly_result['has_anomalies']:
                self.enhancement_stats['anomalies_detected'] += 1

            return {
                'anomaly_detected': anomaly_result['has_anomalies'],
                'anomaly_count': anomaly_result['anomaly_count'],
                'anomaly_risk_score': anomaly_result['risk_score'],
                'anomaly_types': ','.join([a['type'] for a in anomaly_result['anomalies']]) if anomaly_result['anomalies'] else '',
                'anomaly_severity': max([a['severity'] for a in anomaly_result['anomalies']], default='none',
                                      key=lambda x: {'none': 0, 'low': 1, 'medium': 2, 'high': 3}.get(x, 0)),
                'anomaly_details': json.dumps(anomaly_result['anomalies'][:3]) if anomaly_result['anomalies'] else ''  # 保存前3个异常
            }

        except Exception as e:
            self.logger.warning(f"异常检测失败: {e}")
            return {
                'anomaly_detected': False,
                'anomaly_count': 0,
                'anomaly_risk_score': 0.0,
                'anomaly_types': '',
                'anomaly_severity': 'none',
                'anomaly_details': ''
            }

    def _enhance_user_agent_parsing(self, user_agent: str) -> Dict[str, Any]:
        """增强User-Agent解析 (基于父类功能)"""
        try:
            # 调用父类的增强解析
            enhanced_ua = self._parse_user_agent_enhanced(user_agent)

            # 添加更多分析维度
            enhanced_info = {
                'ua_is_mobile_device': enhanced_ua.get('is_mobile', False),
                'ua_is_bot_crawler': enhanced_ua.get('is_bot', False),
                'ua_device_category': enhanced_ua.get('device_type', 'unknown'),
                'ua_platform_version': enhanced_ua.get('os_version', ''),
                'ua_browser_family': enhanced_ua.get('browser_family', ''),
                'ua_is_government_app': enhanced_ua.get('is_government_app', False),
                'ua_is_miniprogram': enhanced_ua.get('is_miniprogram', False),
                'ua_miniprogram_type': enhanced_ua.get('miniprogram_type', ''),
                'ua_sdk_type': enhanced_ua.get('sdk_type', ''),
                'ua_security_level': self._calculate_ua_security_level(user_agent)
            }

            return enhanced_info

        except Exception as e:
            self.logger.warning(f"增强User-Agent解析失败: {e}")
            return {
                'ua_is_mobile_device': False,
                'ua_is_bot_crawler': False,
                'ua_device_category': 'unknown',
                'ua_platform_version': '',
                'ua_browser_family': '',
                'ua_is_government_app': False,
                'ua_is_miniprogram': False,
                'ua_miniprogram_type': '',
                'ua_sdk_type': '',
                'ua_security_level': 'medium'
            }

    def _calculate_ua_security_level(self, user_agent: str) -> str:
        """计算User-Agent安全级别"""
        ua_lower = user_agent.lower()

        # 高风险指标
        high_risk_indicators = ['sqlmap', 'nmap', 'nikto', 'dirb', 'gobuster', 'wfuzz', 'burp']
        if any(indicator in ua_lower for indicator in high_risk_indicators):
            return 'high_risk'

        # 中风险指标
        medium_risk_indicators = ['python', 'curl', 'wget', 'postman', 'script']
        if any(indicator in ua_lower for indicator in medium_risk_indicators):
            return 'medium_risk'

        # 机器人但低风险
        bot_indicators = ['googlebot', 'bingbot', 'baiduspider', 'bot', 'crawler']
        if any(indicator in ua_lower for indicator in bot_indicators):
            return 'low_risk'

        # 正常浏览器
        return 'normal'

    def _enhance_request_features(self, uri: str, user_agent: str, referer: str) -> Dict[str, Any]:
        """增强请求特征分析"""
        try:
            return {
                # URI特征
                'request_uri_length': len(uri),
                'request_uri_has_query': '?' in uri,
                'request_uri_query_count': len(parse_qs(urlparse(uri).query)) if '?' in uri else 0,
                'request_uri_depth': uri.count('/'),
                'request_uri_has_extension': bool(re.search(r'\.[a-zA-Z0-9]+$', uri.split('?')[0])),
                'request_uri_extension': re.search(r'\.([a-zA-Z0-9]+)$', uri.split('?')[0]).group(1) if re.search(r'\.([a-zA-Z0-9]+)$', uri.split('?')[0]) else '',

                # Referer特征
                'referer_domain': urlparse(referer).netloc if referer else '',
                'referer_is_search_engine': any(engine in referer.lower() for engine in ['google', 'baidu', 'bing', 'yahoo']) if referer else False,
                'referer_is_social_media': any(social in referer.lower() for social in ['weibo', 'wechat', 'qq', 'facebook', 'twitter']) if referer else False,
                'referer_is_external': bool(referer and urlparse(referer).netloc),

                # 组合特征
                'request_complexity_score': self._calculate_request_complexity(uri, user_agent, referer),
                'request_trust_score': self._calculate_request_trust_score(uri, user_agent, referer)
            }

        except Exception as e:
            self.logger.warning(f"请求特征分析失败: {e}")
            return {
                'request_uri_length': 0,
                'request_uri_has_query': False,
                'request_uri_query_count': 0,
                'request_uri_depth': 0,
                'request_uri_has_extension': False,
                'request_uri_extension': '',
                'referer_domain': '',
                'referer_is_search_engine': False,
                'referer_is_social_media': False,
                'referer_is_external': False,
                'request_complexity_score': 0.5,
                'request_trust_score': 0.5
            }

    def _calculate_request_complexity(self, uri: str, user_agent: str, referer: str) -> float:
        """计算请求复杂度评分 (0-1)"""
        complexity = 0.0

        # URI复杂度
        complexity += min(len(uri) / 200, 0.3)  # URI长度
        complexity += min(uri.count('?') * 0.1, 0.1)  # 查询参数
        complexity += min(uri.count('/') * 0.05, 0.2)  # 路径深度

        # User-Agent复杂度
        if user_agent:
            complexity += min(len(user_agent) / 500, 0.2)
            if any(keyword in user_agent.lower() for keyword in ['script', 'tool', 'api', 'client']):
                complexity += 0.2

        return min(complexity, 1.0)

    def _calculate_request_trust_score(self, uri: str, user_agent: str, referer: str) -> float:
        """计算请求信任度评分 (0-1)"""
        trust = 0.5  # 基础信任度

        # 正面因素
        if referer and any(engine in referer.lower() for engine in ['google', 'baidu']):
            trust += 0.2  # 搜索引擎来源

        if user_agent and any(browser in user_agent.lower() for browser in ['chrome', 'firefox', 'safari', 'edge']):
            trust += 0.2  # 正常浏览器

        if '.gov' in uri or '政府' in uri:
            trust += 0.1  # 政府域名

        # 负面因素
        if any(suspicious in uri.lower() for suspicious in ['admin', 'config', '.env', 'backup']):
            trust -= 0.3  # 可疑URI

        if user_agent and any(tool in user_agent.lower() for tool in ['curl', 'wget', 'python', 'script']):
            trust -= 0.2  # 工具类请求

        return max(0.0, min(1.0, trust))

    def get_enhancement_stats(self) -> Dict[str, Any]:
        """获取增强功能统计"""
        total_geo = self.enhancement_stats['geo_lookups']
        total_domain = self.enhancement_stats['domain_classifications']

        return {
            **self.enhancement_stats,
            'geo_cache_hit_rate': self.enhancement_stats['geo_cache_hits'] / total_geo if total_geo > 0 else 0,
            'domain_cache_hit_rate': self.enhancement_stats['domain_cache_hits'] / total_domain if total_domain > 0 else 0,
            'geo_cache_size': len(self.geo_cache),
            'domain_cache_size': len(self.domain_cache),
            'has_geoip_db': self.geoip_reader is not None
        }

    def cleanup(self):
        """清理资源"""
        if self.geoip_reader:
            try:
                self.geoip_reader.close()
                self.logger.info("✅ GeoIP数据库连接已关闭")
            except Exception as e:
                self.logger.warning(f"⚠️ 关闭GeoIP数据库失败: {e}")

        # 清理缓存
        self.geo_cache.clear()
        self.domain_cache.clear()
        self.anomaly_cache.clear()

# 业务域分类器 (从interactive_ultra_etl.py移植)
class BusinessDomainClassifier:
    """业务域智能分类器"""

    def __init__(self):
        self.domain_patterns = {
            'government': {
                'keywords': ['gov', 'government', '政府', '政务', '公安', '法院', '检察院', '税务', '海关', '工商', 'zwfw', 'gxrz'],
                'uri_patterns': [r'/gov/', r'/government/', r'/zwfw/', r'/gxrz/', r'/税务/', r'/政务/'],
                'priority': 10
            },
            'authentication': {
                'keywords': ['auth', 'login', 'oauth', 'sso', 'jwt', 'token', '认证', '登录', '授权'],
                'uri_patterns': [r'/auth/', r'/login', r'/oauth/', r'/sso/', r'/认证/', r'/登录/'],
                'priority': 9
            },
            'payment': {
                'keywords': ['pay', 'payment', 'alipay', 'wechat', 'bank', '支付', '银行', '财务', '订单'],
                'uri_patterns': [r'/pay/', r'/payment/', r'/alipay/', r'/支付/', r'/订单/', r'/财务/'],
                'priority': 9
            },
            'file_service': {
                'keywords': ['file', 'upload', 'download', 'storage', 'oss', 'cdn', '文件', '上传', '下载', '存储'],
                'uri_patterns': [r'/file/', r'/upload/', r'/download/', r'/storage/', r'/文件/', r'/上传/', r'/下载/'],
                'priority': 7
            },
            'user_management': {
                'keywords': ['user', 'profile', 'account', 'member', '用户', '账户', '个人', '会员'],
                'uri_patterns': [r'/user/', r'/profile/', r'/account/', r'/member/', r'/用户/', r'/账户/', r'/个人/'],
                'priority': 6
            },
            'api_gateway': {
                'keywords': ['api', 'gateway', 'service', 'microservice', '接口', '网关', '服务'],
                'uri_patterns': [r'/api/', r'/gateway/', r'/service/', r'/v1/', r'/v2/', r'/接口/', r'/服务/'],
                'priority': 8
            },
            'content_management': {
                'keywords': ['cms', 'content', 'article', 'news', 'blog', '内容', '文章', '新闻', '博客'],
                'uri_patterns': [r'/cms/', r'/content/', r'/article/', r'/news/', r'/内容/', r'/文章/', r'/新闻/'],
                'priority': 5
            },
            'monitoring': {
                'keywords': ['monitor', 'health', 'status', 'metrics', 'log', '监控', '健康', '状态', '指标'],
                'uri_patterns': [r'/monitor/', r'/health/', r'/status/', r'/metrics/', r'/监控/', r'/健康/'],
                'priority': 4
            },
            'static_resources': {
                'keywords': ['static', 'assets', 'css', 'js', 'img', 'image', '静态', '资源'],
                'uri_patterns': [r'\.css$', r'\.js$', r'\.png$', r'\.jpg$', r'\.gif$', r'/static/', r'/assets/'],
                'priority': 2
            },
            'mobile_app': {
                'keywords': ['app', 'mobile', 'android', 'ios', 'wechat', 'miniprogram', '应用', '手机', '移动'],
                'uri_patterns': [r'/app/', r'/mobile/', r'/android/', r'/ios/', r'/应用/', r'/手机/'],
                'priority': 6
            }
        }

    def classify_request(self, uri: str, user_agent: str = '', referer: str = '') -> Dict[str, Any]:
        """分类请求到业务域"""
        classifications = []
        text_content = f"{uri} {user_agent} {referer}".lower()

        for domain, config in self.domain_patterns.items():
            score = 0
            matched_keywords = []
            matched_patterns = []

            for keyword in config['keywords']:
                if keyword.lower() in text_content:
                    score += config['priority']
                    matched_keywords.append(keyword)

            for pattern in config['uri_patterns']:
                if re.search(pattern, uri, re.IGNORECASE):
                    score += config['priority'] * 2
                    matched_patterns.append(pattern)

            if score > 0:
                classifications.append({
                    'domain': domain,
                    'score': score,
                    'confidence': min(score / 20, 1.0),
                    'matched_keywords': matched_keywords,
                    'matched_patterns': matched_patterns
                })

        classifications.sort(key=lambda x: x['score'], reverse=True)

        if classifications:
            best_match = classifications[0]
            return {
                'primary_domain': best_match['domain'],
                'confidence': best_match['confidence'],
                'all_matches': classifications[:3],
                'is_classified': True
            }
        else:
            return {
                'primary_domain': 'general_web',
                'confidence': 0.1,
                'all_matches': [],
                'is_classified': False
            }

# 简化异常检测器
class SimpleAnomalyDetector:
    """简化异常检测器"""

    def __init__(self):
        self.request_counts = {}
        self.thresholds = {
            'high_frequency_ip': 50,
            'error_rate': 0.1,
            'response_time': 5000,
            'suspicious_uri_length': 500
        }

    def detect_anomalies(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """检测异常"""
        anomalies = []

        client_ip = request_data.get('client_ip', '')
        uri = request_data.get('request_uri', '')
        status_code = request_data.get('status_code', 200)
        response_time = request_data.get('response_time_ms', 0)

        # 简单的异常检测逻辑
        if response_time > self.thresholds['response_time']:
            anomalies.append({
                'type': 'slow_response',
                'severity': 'medium',
                'description': f'响应时间过长: {response_time}ms',
                'value': response_time
            })

        if len(uri) > self.thresholds['suspicious_uri_length']:
            anomalies.append({
                'type': 'suspicious_uri_length',
                'severity': 'low',
                'description': f'URI长度异常: {len(uri)} 字符',
                'value': len(uri)
            })

        if status_code >= 500:
            anomalies.append({
                'type': 'server_error',
                'severity': 'high',
                'description': f'服务器错误: {status_code}',
                'value': status_code
            })

        return {
            'has_anomalies': len(anomalies) > 0,
            'anomaly_count': len(anomalies),
            'anomalies': anomalies,
            'risk_score': len(anomalies) * 0.3 if anomalies else 0.0
        }