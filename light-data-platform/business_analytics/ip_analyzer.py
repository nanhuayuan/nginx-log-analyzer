# -*- coding: utf-8 -*-
"""
IP来源分析器
分析客户端IP分布、地域特征、异常访问
"""

import clickhouse_connect
from typing import Dict, List, Any
from datetime import datetime, timedelta


class IpAnalyzer:
    """IP来源分析器 - 客户端行为分析"""
    
    def __init__(self, clickhouse_config: Dict[str, Any]):
        self.clickhouse_config = clickhouse_config
        self.client = None
        self.connect()
    
    def connect(self):
        """连接ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(**self.clickhouse_config)
            self.client.command("SET session_timezone = 'Asia/Shanghai'")
            return True
        except Exception as e:
            print(f"ClickHouse连接失败: {e}")
            return False
    
    def get_top_clients(self, hours: int = 24, limit: int = 20) -> List[Dict[str, Any]]:
        """TOP客户端IP分析"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            top_clients = self.client.query(f"""
                SELECT 
                    client_ip,
                    COUNT(*) as total_requests,
                    COUNT(DISTINCT request_uri) as unique_apis,
                    COUNT(DISTINCT platform) as platform_count,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    COUNT(CASE WHEN is_success THEN 1 END) * 100.0 / COUNT(*) as success_rate,
                    COUNT(CASE WHEN is_error THEN 1 END) as error_count,
                    MAX(log_time) as last_seen,
                    MIN(log_time) as first_seen
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END
                GROUP BY client_ip
                ORDER BY total_requests DESC
                LIMIT {limit}
            """).result_rows
            
            results = []
            for row in top_clients:
                results.append({
                    'client_ip': row[0],
                    'total_requests': row[1],
                    'unique_apis': row[2],
                    'platform_count': row[3],
                    'avg_response_ms': round(row[4] or 0, 2),
                    'success_rate': round(row[5] or 0, 2),
                    'error_count': row[6],
                    'last_seen': row[7],
                    'first_seen': row[8],
                    'ip_type': self._classify_ip(row[0]),
                    'behavior_pattern': self._analyze_behavior(row[1], row[2], row[3]),
                    'risk_score': self._calculate_risk_score(row[1], row[6], row[5])
                })
            return results
        except Exception as e:
            raise Exception(f"获取TOP客户端失败: {e}")
    
    def get_geographical_distribution(self, hours: int = 24) -> List[Dict[str, Any]]:
        """地理分布分析 (基于IP分类)"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            geo_stats = self.client.query(f"""
                SELECT 
                    CASE 
                        WHEN client_ip LIKE '10.%' OR client_ip LIKE '172.16.%' OR client_ip LIKE '192.168.%' THEN 'Internal'
                        WHEN client_ip LIKE '127.%' THEN 'Localhost'  
                        WHEN client_ip LIKE '169.254.%' THEN 'Link-Local'
                        WHEN client_ip != '' AND client_ip IS NOT NULL THEN 'External'
                        ELSE 'Unknown'
                    END as ip_region,
                    COUNT(*) as requests,
                    COUNT(DISTINCT client_ip) as unique_ips,
                    COUNT(DISTINCT platform) as platforms,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    COUNT(CASE WHEN is_success THEN 1 END) * 100.0 / COUNT(*) as success_rate
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END
                GROUP BY ip_region
                ORDER BY requests DESC
            """).result_rows
            
            total_requests = sum(row[1] for row in geo_stats)
            results = []
            
            for row in geo_stats:
                results.append({
                    'region': row[0],
                    'requests': row[1],
                    'percentage': round((row[1] / total_requests * 100) if total_requests > 0 else 0, 2),
                    'unique_ips': row[2],
                    'platforms': row[3],
                    'avg_response_ms': round(row[4] or 0, 2),
                    'success_rate': round(row[5] or 0, 2),
                    'security_level': self._get_security_level(row[0])
                })
            return results
        except Exception as e:
            raise Exception(f"获取地理分布失败: {e}")
    
    def get_suspicious_activities(self, hours: int = 24) -> List[Dict[str, Any]]:
        """可疑活动检测"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            suspicious = self.client.query(f"""
                SELECT 
                    client_ip,
                    COUNT(*) as request_count,
                    COUNT(DISTINCT request_uri) as unique_paths,
                    COUNT(CASE WHEN is_error THEN 1 END) as error_count,
                    COUNT(CASE WHEN response_status_code = '404' THEN 1 END) as not_found_count,
                    COUNT(CASE WHEN is_slow THEN 1 END) as slow_count,
                    MAX(log_time) as last_activity,
                    AVG(total_request_duration) * 1000 as avg_response_ms
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END
                GROUP BY client_ip
                HAVING request_count > 50 OR error_count > 10 OR not_found_count > 20
                ORDER BY request_count DESC
                LIMIT 20
            """).result_rows
            
            results = []
            for row in suspicious:
                results.append({
                    'client_ip': row[0],
                    'request_count': row[1],
                    'unique_paths': row[2],
                    'error_count': row[3],
                    'not_found_count': row[4],
                    'slow_count': row[5],
                    'last_activity': row[6],
                    'avg_response_ms': round(row[7] or 0, 2),
                    'threat_level': self._assess_threat_level(row[1], row[3], row[4]),
                    'activity_type': self._identify_activity_type(row[1], row[2], row[3], row[4])
                })
            return results
        except Exception as e:
            raise Exception(f"获取可疑活动失败: {e}")
    
    def get_client_diversity(self, hours: int = 24) -> Dict[str, Any]:
        """客户端多样性分析"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            diversity_stats = self.client.query(f"""
                SELECT 
                    COUNT(DISTINCT client_ip) as total_unique_ips,
                    COUNT(*) as total_requests,
                    COUNT(DISTINCT platform) as unique_platforms,
                    COUNT(DISTINCT api_category) as api_categories_used,
                    AVG(total_request_duration) * 1000 as overall_avg_response_ms
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END
            """).first_row
            
            if diversity_stats:
                return {
                    'total_unique_ips': diversity_stats[0],
                    'total_requests': diversity_stats[1],
                    'unique_platforms': diversity_stats[2],
                    'api_categories_used': diversity_stats[3],
                    'overall_avg_response_ms': round(diversity_stats[4] or 0, 2),
                    'requests_per_ip': round(diversity_stats[1] / diversity_stats[0] if diversity_stats[0] > 0 else 0, 2),
                    'diversity_score': self._calculate_diversity_score(diversity_stats[0], diversity_stats[1]),
                    'user_concentration': self._analyze_user_concentration(diversity_stats[0], diversity_stats[1])
                }
            else:
                return {}
        except Exception as e:
            raise Exception(f"获取客户端多样性失败: {e}")
    
    def _classify_ip(self, ip: str) -> str:
        """IP分类"""
        if not ip:
            return 'Unknown'
        
        if ip.startswith(('10.', '172.16.', '192.168.')):
            return 'Internal'
        elif ip.startswith('127.'):
            return 'Localhost'
        elif ip.startswith('169.254.'):
            return 'Link-Local'
        else:
            return 'External'
    
    def _analyze_behavior(self, total_requests: int, unique_apis: int, platform_count: int) -> str:
        """分析行为模式"""
        if total_requests > 1000:
            return 'High Volume'
        elif unique_apis > 20:
            return 'API Explorer'
        elif platform_count > 3:
            return 'Multi-Platform'
        elif total_requests > 100:
            return 'Active User'
        else:
            return 'Normal User'
    
    def _calculate_risk_score(self, requests: int, errors: int, success_rate: float) -> int:
        """计算风险评分 (0-100)"""
        risk_score = 0
        
        # 请求量风险
        if requests > 10000:
            risk_score += 40
        elif requests > 1000:
            risk_score += 20
        elif requests > 100:
            risk_score += 5
        
        # 错误率风险
        if success_rate < 50:
            risk_score += 30
        elif success_rate < 80:
            risk_score += 15
        elif success_rate < 95:
            risk_score += 5
        
        # 错误数量风险
        if errors > 100:
            risk_score += 30
        elif errors > 50:
            risk_score += 15
        elif errors > 10:
            risk_score += 5
        
        return min(risk_score, 100)
    
    def _get_security_level(self, region: str) -> str:
        """获取安全等级"""
        levels = {
            'Internal': 'Trusted',
            'Localhost': 'Trusted',
            'External': 'Monitor',
            'Unknown': 'Caution'
        }
        return levels.get(region, 'Caution')
    
    def _assess_threat_level(self, requests: int, errors: int, not_found: int) -> str:
        """评估威胁级别"""
        if requests > 10000 or errors > 1000:
            return 'High'
        elif requests > 1000 or errors > 100 or not_found > 500:
            return 'Medium'
        elif requests > 100 or errors > 10:
            return 'Low'
        else:
            return 'Minimal'
    
    def _identify_activity_type(self, requests: int, paths: int, errors: int, not_found: int) -> str:
        """识别活动类型"""
        if not_found > requests * 0.5:
            return 'Scanner/Crawler'
        elif errors > requests * 0.3:
            return 'Error Generator'  
        elif paths > requests * 0.8:
            return 'Path Explorer'
        elif requests > 1000:
            return 'Heavy User'
        else:
            return 'Normal Activity'
    
    def _calculate_diversity_score(self, unique_ips: int, total_requests: int) -> float:
        """计算多样性评分"""
        if total_requests == 0:
            return 0.0
        
        # 多样性 = 独立IP数 / 总请求数 * 100
        diversity = (unique_ips / total_requests) * 100
        return round(min(diversity, 100.0), 2)
    
    def _analyze_user_concentration(self, unique_ips: int, total_requests: int) -> str:
        """分析用户集中度"""
        if unique_ips == 0:
            return 'No Data'
        
        avg_requests_per_ip = total_requests / unique_ips
        
        if avg_requests_per_ip > 1000:
            return 'Highly Concentrated'
        elif avg_requests_per_ip > 100:
            return 'Concentrated'
        elif avg_requests_per_ip > 10:
            return 'Moderate'
        else:
            return 'Distributed'