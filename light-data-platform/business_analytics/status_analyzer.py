# -*- coding: utf-8 -*-
"""
状态码分析器
分析HTTP状态码分布、错误模式、成功率趋势
"""

import clickhouse_connect
from typing import Dict, List, Any
from datetime import datetime, timedelta


class StatusAnalyzer:
    """状态码分析器 - 响应状态监控"""
    
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
    
    def get_status_distribution(self, hours: int = 24) -> List[Dict[str, Any]]:
        """状态码分布统计"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            status_stats = self.client.query(f"""
                SELECT 
                    response_status_code as status_code,
                    COUNT(*) as request_count,
                    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE CASE 
                        WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                        THEN log_time >= now() - INTERVAL {hours} HOUR
                        ELSE 1=1
                    END) as percentage,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    COUNT(DISTINCT client_ip) as unique_clients
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END
                GROUP BY response_status_code
                ORDER BY request_count DESC
            """).result_rows
            
            results = []
            for row in status_stats:
                results.append({
                    'status_code': row[0],
                    'request_count': row[1],
                    'percentage': round(row[2] or 0, 2),
                    'avg_response_ms': round(row[3] or 0, 2),
                    'unique_clients': row[4],
                    'status_type': self._get_status_type(row[0]),
                    'severity': self._get_severity_level(row[0], row[2])
                })
            return results
        except Exception as e:
            raise Exception(f"获取状态码分布失败: {e}")
    
    def get_error_trend(self, hours: int = 24) -> List[Dict[str, Any]]:
        """错误趋势分析"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            error_trend = self.client.query(f"""
                SELECT 
                    toHour(log_time) as hour,
                    COUNT(*) as total_requests,
                    COUNT(CASE WHEN response_status_code LIKE '4%' THEN 1 END) as client_errors,
                    COUNT(CASE WHEN response_status_code LIKE '5%' THEN 1 END) as server_errors,
                    COUNT(CASE WHEN is_error THEN 1 END) as total_errors,
                    COUNT(CASE WHEN is_error THEN 1 END) * 100.0 / COUNT(*) as error_rate
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END
                GROUP BY toHour(log_time)
                ORDER BY hour
            """).result_rows
            
            results = []
            for row in error_trend:
                results.append({
                    'hour': row[0],
                    'total_requests': row[1],
                    'client_errors': row[2], 
                    'server_errors': row[3],
                    'total_errors': row[4],
                    'error_rate': round(row[5] or 0, 2),
                    'health_level': self._get_health_level(row[5])
                })
            return results
        except Exception as e:
            raise Exception(f"获取错误趋势失败: {e}")
    
    def get_critical_errors(self, hours: int = 24) -> List[Dict[str, Any]]:
        """关键错误监控"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            critical_errors = self.client.query(f"""
                SELECT 
                    response_status_code,
                    request_uri,
                    COUNT(*) as error_count,
                    COUNT(DISTINCT client_ip) as affected_users,
                    MAX(log_time) as last_occurrence,
                    platform as main_platform
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END
                  AND response_status_code IN ('500', '502', '503', '504', '404')
                GROUP BY response_status_code, request_uri, platform
                ORDER BY error_count DESC
                LIMIT 20
            """).result_rows
            
            results = []
            for row in critical_errors:
                results.append({
                    'status_code': row[0],
                    'api': row[1],
                    'error_count': row[2],
                    'affected_users': row[3],
                    'last_occurrence': row[4],
                    'main_platform': row[5] or 'Unknown',
                    'priority': self._get_error_priority(row[0], row[2]),
                    'impact_level': self._get_impact_level(row[3])
                })
            return results
        except Exception as e:
            raise Exception(f"获取关键错误失败: {e}")
    
    def _get_status_type(self, status_code: str) -> str:
        """获取状态码类型"""
        if not status_code:
            return 'Unknown'
        
        code = status_code[0] if len(status_code) > 0 else '0'
        types = {
            '2': 'Success',
            '3': 'Redirect', 
            '4': 'Client Error',
            '5': 'Server Error',
            '1': 'Info'
        }
        return types.get(code, 'Unknown')
    
    def _get_severity_level(self, status_code: str, percentage: float) -> str:
        """获取严重程度"""
        if not status_code or not percentage:
            return 'Low'
            
        if status_code.startswith('5') and percentage > 1:
            return 'Critical'
        elif status_code.startswith('4') and percentage > 10:
            return 'High'
        elif percentage > 50:
            return 'Medium'
        else:
            return 'Low'
    
    def _get_health_level(self, error_rate: float) -> str:
        """获取健康水平"""
        if not error_rate:
            return 'Excellent'
        
        if error_rate < 1:
            return 'Excellent'
        elif error_rate < 5:
            return 'Good'
        elif error_rate < 10:
            return 'Warning'
        else:
            return 'Critical'
    
    def _get_error_priority(self, status_code: str, error_count: int) -> str:
        """获取错误优先级"""
        if status_code in ['500', '502', '503', '504']:
            return 'P0' if error_count > 10 else 'P1'
        elif status_code == '404':
            return 'P2' if error_count > 50 else 'P3'
        else:
            return 'P3'
    
    def _get_impact_level(self, affected_users: int) -> str:
        """获取影响程度"""
        if affected_users > 100:
            return 'High'
        elif affected_users > 10:
            return 'Medium'
        else:
            return 'Low'