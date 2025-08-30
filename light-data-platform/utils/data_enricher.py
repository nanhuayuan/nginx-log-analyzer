# -*- coding: utf-8 -*-
"""
数据标签化和富化工具
"""

import re
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd

class DataEnricher:
    """数据富化器 - 为原始数据添加维度标签"""
    
    def __init__(self, dimensions_config: Dict):
        self.dimensions_config = dimensions_config
        self._compile_patterns()
    
    def _compile_patterns(self):
        """预编译正则表达式模式，提高性能"""
        self.compiled_patterns = {}
        
        # 编译平台识别模式
        self.compiled_patterns['platform'] = {}
        for platform, config in self.dimensions_config['platform'].items():
            if 'pattern' in config:
                self.compiled_patterns['platform'][platform] = re.compile(
                    config['pattern'], re.IGNORECASE
                )
        
        # 编译API分类模式
        self.compiled_patterns['api_category'] = {}
        for category, config in self.dimensions_config['api_category'].items():
            self.compiled_patterns['api_category'][category] = []
            for pattern in config.get('patterns', []):
                self.compiled_patterns['api_category'][category].append(
                    re.compile(pattern, re.IGNORECASE)
                )
    
    def extract_platform(self, user_agent: str) -> str:
        """从User-Agent提取平台信息"""
        if not user_agent:
            return 'Unknown'
        
        user_agent_lower = user_agent.lower()
        
        # 按优先级检查平台模式
        for platform, pattern in self.compiled_patterns['platform'].items():
            if pattern.search(user_agent_lower):
                return platform
        
        return 'Other'
    
    def extract_platform_version(self, user_agent: str, platform: str) -> Optional[str]:
        """提取平台版本信息"""
        if not user_agent:
            return None
        
        # iOS版本提取
        if platform in ['iOS_SDK']:
            # zgt-ios/1.4.2 (iPhone; iOS 18.6; Scale/3.00)
            match = re.search(r'zgt-ios/([0-9.]+)', user_agent)
            if match:
                return match.group(1)
            
            # WST-SDK-iOS可能没有版本信息
            return None
        
        # Android版本提取
        elif platform in ['Android_SDK']:
            return None  # 目前Android SDK没有版本信息
        
        # 真正的iOS系统版本
        elif platform == 'iOS':
            match = re.search(r'iOS ([0-9_.]+)', user_agent)
            if match:
                return match.group(1).replace('_', '.')
        
        # Android系统版本
        elif platform == 'Android':
            match = re.search(r'Android ([0-9.]+)', user_agent)
            if match:
                return match.group(1)
        
        return None
    
    def classify_entry_source(self, referer: str) -> str:
        """分类入口来源"""
        if not referer or referer == '-':
            return 'Direct'
        
        referer_lower = referer.lower()
        
        # 按优先级检查来源
        for source, config in self.dimensions_config['entry_source'].items():
            for keyword in config.get('keywords', []):
                if keyword in referer_lower:
                    return source
        
        # 检查是否为HTTP链接
        if referer_lower.startswith('http'):
            return 'External'
        
        return 'Unknown'
    
    def classify_api(self, request_uri: str) -> str:
        """API分类"""
        if not request_uri:
            return 'Other'
        
        # 按优先级检查API类别
        for category, patterns in self.compiled_patterns['api_category'].items():
            for pattern in patterns:
                if pattern.search(request_uri):
                    return category
        
        return 'Other'
    
    def clean_request_uri(self, request_uri: str) -> str:
        """清洗请求URI"""
        if not request_uri:
            return ''
        
        # 移除查询参数中的敏感信息
        uri_parts = request_uri.split('?')
        base_uri = uri_parts[0]
        
        # 如果有查询参数，进行脱敏处理
        if len(uri_parts) > 1:
            # 保留结构，但移除具体参数值
            query_params = uri_parts[1]
            # 简单脱敏：只保留参数名
            sanitized_params = '&'.join([
                param.split('=')[0] + '=***' 
                for param in query_params.split('&') 
                if '=' in param
            ])
            return f"{base_uri}?{sanitized_params}"
        
        return base_uri
    
    def calculate_data_quality_score(self, record: Dict[str, Any]) -> float:
        """计算数据质量评分"""
        score = 1.0
        
        # 检查必要字段是否存在
        required_fields = ['timestamp', 'request_full_uri', 'response_status_code']
        missing_fields = sum(1 for field in required_fields if not record.get(field))
        score -= missing_fields * 0.2
        
        # 检查时间戳格式
        try:
            if isinstance(record.get('timestamp'), str):
                datetime.strptime(record['timestamp'], '%Y-%m-%d %H:%M:%S')
        except:
            score -= 0.1
        
        # 检查响应时间合理性
        response_time = record.get('total_request_duration')
        if response_time is not None:
            if response_time < 0 or response_time > 300:  # 异常响应时间
                score -= 0.1
        
        # 检查状态码格式
        status_code = record.get('response_status_code')
        if status_code and not re.match(r'^[1-5]\d{2}$', str(status_code)):
            score -= 0.1
        
        return max(0.0, score)
    
    def detect_anomaly(self, record: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """检测单条记录的异常"""
        
        # 响应时间异常
        response_time = record.get('total_request_duration', 0)
        if response_time > 60:  # 超过60秒认为异常
            return True, 'extreme_slow_response'
        
        # 状态码异常
        status_code = record.get('response_status_code', '200')
        if status_code in ['500', '502', '503', '504']:
            return True, 'server_error'
        
        # User-Agent异常
        user_agent = record.get('user_agent', '')
        if len(user_agent) > 1000:  # 异常长的User-Agent
            return True, 'suspicious_user_agent'
        
        # IP异常（简单检查）
        client_ip = record.get('client_ip', '')
        if client_ip and client_ip.count('.') != 3:  # IPv4格式检查
            return True, 'invalid_ip_format'
        
        return False, None
    
    def enrich_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """富化单条记录"""
        enriched = record.copy()
        
        # 提取维度信息
        user_agent = record.get('user_agent', '')
        platform = self.extract_platform(user_agent)
        
        enriched.update({
            # 时间分区
            'date_partition': record.get('timestamp', '')[:10] if record.get('timestamp') else '',
            'hour_partition': int(record.get('timestamp', '')[11:13]) if len(record.get('timestamp', '')) >= 13 else 0,
            
            # 维度标签
            'platform': platform,
            'platform_version': self.extract_platform_version(user_agent, platform),
            'entry_source': self.classify_entry_source(record.get('referer', '')),
            'api_category': self.classify_api(record.get('request_full_uri', '')),
            
            # 清洗后字段
            'request_uri': self.clean_request_uri(record.get('request_full_uri', '')),
            'response_time': record.get('total_request_duration', 0.0),
            'response_size_kb': record.get('response_body_size_kb', 0.0),
            
            # 业务标签
            'is_success': str(record.get('response_status_code', '')) in ['200'],
            'is_slow': (record.get('total_request_duration', 0.0) or 0.0) > 3.0,
            
            # 数据质量
            'data_quality_score': self.calculate_data_quality_score(record)
        })
        
        # 异常检测
        has_anomaly, anomaly_type = self.detect_anomaly(record)
        enriched.update({
            'has_anomaly': has_anomaly,
            'anomaly_type': anomaly_type
        })
        
        return enriched
    
    def enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """批量富化DataFrame"""
        enriched_records = []
        
        for _, record in df.iterrows():
            enriched_record = self.enrich_record(record.to_dict())
            enriched_records.append(enriched_record)
        
        return pd.DataFrame(enriched_records)

def test_data_enricher():
    """测试数据富化器"""
    from config.settings import DIMENSIONS
    
    # 创建富化器
    enricher = DataEnricher(DIMENSIONS)
    
    # 测试数据
    test_record = {
        'timestamp': '2025-08-29 15:30:45',
        'client_ip': '192.168.1.100',
        'request_full_uri': '/api/user/login?username=test&password=123',
        'response_status_code': '200',
        'total_request_duration': 1.234,
        'response_body_size_kb': 2.5,
        'user_agent': 'zgt-ios/1.4.2 (iPhone; iOS 18.6; Scale/3.00)',
        'referer': 'https://weixin.qq.com/some-page',
        'application_name': 'zgt-app',
        'service_name': 'user-service'
    }
    
    # 富化数据
    enriched = enricher.enrich_record(test_record)
    
    print("原始记录:")
    for k, v in test_record.items():
        print(f"  {k}: {v}")
    
    print("\n富化后记录:")
    for k, v in enriched.items():
        if k not in test_record:
            print(f"  [新增] {k}: {v}")

if __name__ == "__main__":
    test_data_enricher()