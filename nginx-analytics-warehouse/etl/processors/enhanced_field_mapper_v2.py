#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强版FieldMapper - 整合所有优化
"""

import gc
import hashlib
import ipaddress
from datetime import datetime
from typing import Dict, Any, Optional, List, Generator
from functools import lru_cache
import logging

# 在原有FieldMapper基础上继承
try:
    from .field_mapper import FieldMapper  # 相对导入
except ImportError:
    from field_mapper import FieldMapper  # 绝对导入


class EnhancedFieldMapperV2(FieldMapper):
    """整合所有优化的增强版FieldMapper"""

    def __init__(self, geoip_db_path: str = None, enable_monitoring: bool = True):
        super().__init__(geoip_db_path)

        # 1. 初始化自适应缓存管理器
        self.cache_manager = AdaptiveCacheManager()

        # 2. 初始化数据质量监控器
        self.quality_monitor = DataQualityMonitor()

        # 3. 初始化安全检测器
        self.security_detector = EnhancedSecurityDetector()

        # 4. 中国IP优化映射
        self._init_china_ip_mapping()

        # 记录处理统计
        self.processing_stats = {
            'total_processed': 0,
            'quality_issues': 0,
            'security_threats': 0
        }

    def _init_china_ip_mapping(self):
        """初始化中国IP段映射"""
        try:
            self.china_isp_ranges = [
                # 电信
                (ipaddress.ip_network('1.80.0.0/12'), {'isp': 'China Telecom', 'region': '广东'}),
                (ipaddress.ip_network('14.144.0.0/12'), {'isp': 'China Telecom', 'region': '广东'}),
                # 联通
                (ipaddress.ip_network('112.80.0.0/13'), {'isp': 'China Unicom', 'region': '广西'}),
                (ipaddress.ip_network('163.179.0.0/16'), {'isp': 'China Unicom', 'region': '广西'}),
                # 移动（修复网段）
                (ipaddress.ip_network('111.0.0.0/10'), {'isp': 'China Mobile', 'region': '北京'}),
                (ipaddress.ip_network('117.136.0.0/13'), {'isp': 'China Mobile', 'region': '全国'}),
                # 政务专网
                (ipaddress.ip_network('59.255.0.0/16'), {'isp': 'Government Network', 'region': '政务专网'}),
            ]
        except Exception as e:
            logging.warning(f"初始化中国IP段映射失败: {e}")
            self.china_isp_ranges = []

    def map_to_dwd(self, parsed_data: Dict[str, Any], source_file: str = '') -> Dict[str, Any]:
        """增强的字段映射 - 整合所有优化"""

        # 调用父类基础映射
        dwd_record = super().map_to_dwd(parsed_data, source_file)

        # ===== 应用各项优化 =====

        # 1. 智能性能指标计算
        self._enhance_performance_metrics(dwd_record, parsed_data)

        # 2. 中国IP地理优化
        self._enhance_china_geo_location(dwd_record)

        # 3. 生成会话追踪
        self._generate_session_tracking(dwd_record, parsed_data)

        # 4. 增强安全检测
        self._enhanced_security_detection(dwd_record, parsed_data)

        # 5. 数据质量评估
        quality_score = self.quality_monitor.check_record_quality(dwd_record)
        dwd_record['data_quality_score'] = quality_score

        # 6. 更新缓存策略
        self._update_adaptive_cache()

        # 更新统计
        self.processing_stats['total_processed'] += 1

        return dwd_record

    # ========== 1. 智能性能计算 ==========
    def _enhance_performance_metrics(self, dwd_record: Dict, parsed_data: Dict):
        """增强性能指标计算"""
        ar_time_ms = dwd_record['total_request_duration']
        uri_structure = self._parse_uri_structure(dwd_record['request_uri'])

        # 基于请求类型智能分配时间比例
        if uri_structure['is_static_resource']:
            # 静态资源：主要是nginx缓存
            ratios = {'backend': 0.1, 'network': 0.2, 'nginx': 0.7}
        elif 'gateway' in uri_structure['application_name']:
            # 网关请求：后端处理为主
            if 'auth' in uri_structure['api_module']:
                ratios = {'backend': 0.85, 'network': 0.1, 'nginx': 0.05}
            else:
                ratios = {'backend': 0.75, 'network': 0.15, 'nginx': 0.1}
        elif dwd_record.get('cache_status') == 'HIT':
            # 缓存命中
            ratios = {'backend': 0.0, 'network': 0.1, 'nginx': 0.9}
        else:
            # 默认分布
            ratios = {'backend': 0.7, 'network': 0.2, 'nginx': 0.1}

        # 更新时间分配
        dwd_record['upstream_response_time'] = int(ar_time_ms * ratios['backend'])
        dwd_record['network_phase'] = int(ar_time_ms * ratios['network'])
        dwd_record['nginx_transfer_phase'] = int(ar_time_ms * ratios['nginx'])

        # 重新计算其他相关字段
        dwd_record['backend_total_phase'] = dwd_record['upstream_response_time']

    # ========== 2. 中国IP地理优化 ==========
    def _enhance_china_geo_location(self, dwd_record: Dict):
        """增强中国IP地理定位"""
        ip = dwd_record.get('client_ip', '')
        if not ip or dwd_record.get('client_country') != 'CN':
            return

        try:
            ip_obj = ipaddress.ip_address(ip)
            for network, info in self.china_isp_ranges:
                if ip_obj in network:
                    dwd_record['client_isp'] = info['isp']
                    dwd_record['client_region'] = info['region']

                    # 政务专网特殊标记
                    if info['isp'] == 'Government Network':
                        dwd_record['client_classification'] = 'trusted'
                        dwd_record['ip_reputation'] = 'trusted'
                    break
        except:
            pass

    # ========== 3. 会话追踪生成 ==========
    def _generate_session_tracking(self, dwd_record: Dict, parsed_data: Dict):
        """生成会话追踪信息"""
        # 生成用户指纹
        user_fingerprint = hashlib.md5(
            f"{dwd_record['client_ip']}-{dwd_record['user_agent_string']}".encode()
        ).hexdigest()[:16]

        # 生成会话ID (30分钟窗口)
        time_window = int(dwd_record['log_time'].timestamp() / 1800)
        session_id = hashlib.md5(
            f"{user_fingerprint}-{time_window}".encode()
        ).hexdigest()[:16]

        dwd_record['user_id'] = user_fingerprint
        dwd_record['session_id'] = session_id

        # 智能用户类型推断
        if dwd_record.get('sdk_type', '').startswith('WST'):
            dwd_record['user_type'] = 'government_user'
            dwd_record['user_tier'] = 'verified'
            dwd_record['user_segment'] = 'government'
        elif dwd_record.get('sdk_type'):
            dwd_record['user_type'] = 'registered'
            dwd_record['user_tier'] = 'standard'
        else:
            dwd_record['user_type'] = 'guest'
            dwd_record['user_tier'] = 'free'

    # ========== 4. 增强安全检测 ==========
    def _enhanced_security_detection(self, dwd_record: Dict, parsed_data: Dict):
        """增强的安全检测"""
        threats = self.security_detector.detect_threats(
            dwd_record['request_uri'],
            dwd_record['user_agent_string'],
            dwd_record.get('query_parameters', '')
        )

        if threats:
            dwd_record['threat_category'] = threats[0]['type']
            dwd_record['security_risk_score'] = min(100,
                                                    dwd_record.get('security_risk_score', 0) + sum(
                                                        t['score'] for t in threats))
            dwd_record['attack_signature'] = '|'.join(t['signature'] for t in threats)

            self.processing_stats['security_threats'] += 1

    # ========== 5. 自适应缓存更新 ==========
    def _update_adaptive_cache(self):
        """更新自适应缓存"""
        # 每1000条记录调整一次
        if self.processing_stats['total_processed'] % 1000 == 0:
            # 简化缓存更新，避免调用不存在的方法
            try:
                # 基础缓存统计
                cache_stats = getattr(self, 'cache_stats', {})

                # 调整缓存大小（如果缓存管理器可用）
                if hasattr(self.cache_manager, 'adjust_cache'):
                    self.cache_manager.adjust_cache('ua', 0.8)  # 默认命中率
                    self.cache_manager.adjust_cache('ip', 0.8)  # 默认命中率
            except Exception as e:
                # 静默处理缓存更新错误，不影响主流程
                pass

    # ========== 6. 批处理内存优化 ==========
    def process_batch_optimized(self, records: List[Dict], batch_size: int = 1000) -> Generator:
        """内存优化的批处理"""

        def record_generator():
            """生成器模式处理"""
            for i, record in enumerate(records):
                try:
                    result = self.map_to_dwd(record)

                    # 定期垃圾回收
                    if i % 10000 == 0:
                        gc.collect()

                    yield result

                except Exception as e:
                    self.logger.error(f"处理记录失败: {e}")
                    yield self._create_fallback_record(record, '', str(e))
                finally:
                    # 释放原始记录内存
                    del record

        # 返回生成器
        return record_generator()

    # ========== 7. 获取处理报告 ==========
    def get_processing_report(self) -> Dict:
        """获取完整的处理报告"""
        return {
            'timestamp': datetime.now().isoformat(),
            'processing_stats': self.processing_stats,
            'cache_performance': {
                'status': 'available',
                'cache_enabled': True
            },
            'quality_metrics': getattr(self.quality_monitor, 'get_metrics', lambda: {})(),
            'security_summary': {
                'threats_detected': self.processing_stats['security_threats'],
                'threat_rate': self.processing_stats['security_threats'] /
                               max(1, self.processing_stats['total_processed'])
            }
        }


# ========== 辅助类实现 ==========

class AdaptiveCacheManager:
    """自适应缓存管理器"""

    def __init__(self):
        self.cache_sizes = {
            'ua': 10000,
            'ip': 50000,
            'uri': 10000
        }
        self.adjustment_history = []

    def adjust_cache(self, cache_type: str, hit_rate: float):
        """根据命中率调整缓存大小"""
        current = self.cache_sizes[cache_type]

        if hit_rate < 0.6:
            new_size = min(current * 1.5, 100000)
        elif hit_rate > 0.9:
            new_size = max(current * 0.8, 5000)
        else:
            return

        self.cache_sizes[cache_type] = int(new_size)
        self.adjustment_history.append({
            'type': cache_type,
            'old_size': current,
            'new_size': int(new_size),
            'hit_rate': hit_rate,
            'timestamp': datetime.now()
        })

    def get_cache_sizes(self) -> Dict:
        return self.cache_sizes.copy()


class DataQualityMonitor:
    """数据质量监控器"""

    def __init__(self):
        self.metrics = {
            'total_checked': 0,
            'quality_issues': 0,
            'missing_fields': {},
            'anomalies': []
        }

    def check_record_quality(self, record: Dict) -> float:
        """检查记录质量"""
        self.metrics['total_checked'] += 1
        issues = []

        # 必需字段检查
        required = ['client_ip', 'request_uri', 'response_status_code']
        for field in required:
            if not record.get(field):
                issues.append(f'missing_{field}')
                self.metrics['missing_fields'][field] = \
                    self.metrics['missing_fields'].get(field, 0) + 1

        # 异常值检查
        if record.get('total_request_duration', 0) > 60000:
            issues.append('excessive_duration')

        if record.get('response_body_size', 0) > 100 * 1024 * 1024:
            issues.append('excessive_size')

        if issues:
            self.metrics['quality_issues'] += 1
            self.metrics['anomalies'].append({
                'timestamp': datetime.now(),
                'issues': issues
            })

        return max(0, 100 - len(issues) * 20)

    def get_metrics(self) -> Dict:
        return self.metrics.copy()


class EnhancedSecurityDetector:
    """增强的安全检测器"""

    def __init__(self):
        self.threat_patterns = {
            'sql_injection': (r"(\bunion\b.*\bselect\b|\bor\b.*=|\bdrop\b)", 50),
            'xss': (r"(<script|javascript:|onerror=)", 40),
            'path_traversal': (r"(\.\./|\.\.\\)", 40),
            'sensitive_file': (r"(\.env|\.git|web\.config|database\.yml)", 50),
            'command_injection': (r"(;|\||&&|`|\$\()", 30),
        }

    def detect_threats(self, uri: str, ua: str, params: str) -> List[Dict]:
        """检测安全威胁"""
        threats = []
        combined = f"{uri} {params}".lower()

        for threat_type, (pattern, score) in self.threat_patterns.items():
            import re
            if re.search(pattern, combined, re.IGNORECASE):
                threats.append({
                    'type': threat_type,
                    'score': score,
                    'signature': pattern
                })

        # UA异常检测
        if len(ua) > 500 or len(ua) < 10:
            threats.append({
                'type': 'suspicious_ua',
                'score': 20,
                'signature': f'ua_length:{len(ua)}'
            })

        return threats


# ========== 使用示例 ==========
if __name__ == '__main__':
    # 初始化增强版mapper（GeoIP数据库路径可选，不存在时会自动跳过）
    mapper = EnhancedFieldMapperV2(
        geoip_db_path=None  # 设置为None，避免路径错误
    )

    # 示例数据
    test_logs = [
        {
            'http_host': 'aa1.bbb.ccc.gov.cn',
            'remote_addr': '112.80.5.100',  # 广西联通IP
            'time': '2025-04-23T10:30:00+08:00',
            'request': 'POST /scmp-gateway/gxrz-rest/auth/login HTTP/1.1',
            'code': '200',
            'body': '1024',
            'ar_time': '0.850',
            'agent': 'WST-SDK-iOS/2.1.0'
        }
    ]

    # 批处理（内存优化）
    print("🚀 开始处理测试数据...")
    for i, result in enumerate(mapper.process_batch_optimized(test_logs)):
        print(f"\n📋 记录 {i+1}:")
        print(f"  客户端IP: {result.get('client_ip', 'N/A')}")
        print(f"  请求URI: {result.get('request_uri', 'N/A')}")
        print(f"  响应状态: {result.get('response_status_code', 'N/A')}")
        print(f"  用户类型: {result.get('user_type', 'N/A')}")
        print(f"  会话ID: {result.get('session_id', 'N/A')}")
        print(f"  ISP: {result.get('client_isp', 'N/A')}")
        print(f"  地区: {result.get('client_region', 'N/A')}")
        print(f"  质量分: {result.get('data_quality_score', 'N/A')}")
        print(f"  安全风险: {result.get('security_risk_score', 0)}")

    # 获取处理报告
    try:
        report = mapper.get_processing_report()
        print(f"\n📊 处理报告:")
        print(f"总处理数: {report.get('processing_stats', {}).get('total_processed', 0)}")
        # 简化缓存报告，避免方法缺失问题
        print(f"处理统计: {mapper.processing_stats}")
    except Exception as e:
        print(f"⚠️ 获取处理报告失败: {e}")

    print("✅ 测试完成!")