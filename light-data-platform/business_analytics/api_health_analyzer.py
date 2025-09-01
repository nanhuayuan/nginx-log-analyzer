# -*- coding: utf-8 -*-
"""
接口健康度分析器
核心功能：分析接口性能、发现问题、支撑业务决策
"""

import clickhouse_connect
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional


class ApiHealthAnalyzer:
    """接口健康度分析 - 业务分析核心模块"""
    
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
    
    def get_business_overview(self, hours: int = 24, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """业务总览 - 核心指标仪表板"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            # 构建时间条件
            if start_time and end_time:
                time_condition = f"log_time >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND log_time <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'"
            else:
                time_condition = f"""CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END"""
            
            # 核心业务指标
            overview_stats = self.client.query(f"""
                SELECT 
                    COUNT(*) as total_pv,
                    COUNT(DISTINCT client_ip) as unique_users,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    SUM(CASE WHEN is_success THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate,
                    SUM(CASE WHEN is_error THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as error_rate,
                    SUM(CASE WHEN is_slow THEN 1 ELSE 0 END) as slow_requests,
                    COUNT(CASE WHEN response_status_code = '404' THEN 1 END) as error_404,
                    COUNT(CASE WHEN response_status_code = '500' THEN 1 END) as error_500
                FROM dwd_nginx_enriched 
                WHERE {time_condition}
            """).first_row
            
            return {
                'total_pv': overview_stats[0],
                'unique_users': overview_stats[1], 
                'avg_response_ms': round(overview_stats[2] or 0, 2),
                'success_rate': round(overview_stats[3] or 0, 2),
                'error_rate': round(overview_stats[4] or 0, 2),
                'slow_requests': overview_stats[5],
                'error_404': overview_stats[6],
                'error_500': overview_stats[7]
            }
        except Exception as e:
            raise Exception(f"获取业务总览失败: {e}")
    
    def get_hottest_apis(self, limit: int = 10, hours: int = 24, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """最热门10个接口 - 业务关注重点"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            # 构建时间条件
            if start_time and end_time:
                time_condition = f"log_time >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND log_time <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'"
            else:
                time_condition = f"""CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END"""
            
            hot_apis = self.client.query(f"""
                SELECT 
                    request_uri as api,
                    COUNT(*) as total_requests,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    quantile(0.95)(total_request_duration) * 1000 as p95_ms,
                    SUM(CASE WHEN is_success THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate,
                    SUM(CASE WHEN is_error THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as error_rate,
                    api_category,
                    platform as main_platform
                FROM dwd_nginx_enriched 
                WHERE {time_condition}
                GROUP BY request_uri, api_category, platform
                ORDER BY total_requests DESC
                LIMIT {limit}
            """).result_rows
            
            results = []
            for row in hot_apis:
                results.append({
                    'api': row[0],
                    'total_requests': row[1],
                    'avg_response_ms': round(row[2] or 0, 2),
                    'p95_ms': round(row[3] or 0, 2),
                    'success_rate': round(row[4] or 0, 2),
                    'error_rate': round(row[5] or 0, 2),
                    'api_category': row[6] or 'Other',
                    'main_platform': row[7] or 'Unknown',
                    'health_status': self._get_health_status(row[2], row[5])
                })
            return results
        except Exception as e:
            raise Exception(f"获取热门接口失败: {e}")
    
    def get_slowest_apis(self, limit: int = 10, hours: int = 24, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """最慢10个接口 - 问题识别"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            # 构建时间条件
            if start_time and end_time:
                time_condition = f"log_time >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND log_time <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'"
            else:
                time_condition = f"""CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END"""
            
            slow_apis = self.client.query(f"""
                SELECT 
                    request_uri as api,
                    COUNT(*) as total_requests,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    quantile(0.95)(total_request_duration) * 1000 as p95_ms,
                    SUM(CASE WHEN is_success THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate,
                    SUM(CASE WHEN is_slow THEN 1 ELSE 0 END) as slow_count,
                    AVG(backend_process_phase) * 1000 as avg_process_ms,
                    AVG(nginx_transfer_phase) * 1000 as avg_transfer_ms,
                    platform as main_platform
                FROM dwd_nginx_enriched 
                WHERE {time_condition}
                  AND total_request_duration > 0
                GROUP BY request_uri, platform
                ORDER BY avg_response_ms DESC
                LIMIT {limit}
            """).result_rows
            
            results = []
            for row in slow_apis:
                results.append({
                    'api': row[0],
                    'total_requests': row[1],
                    'avg_response_ms': round(row[2] or 0, 2),
                    'p95_ms': round(row[3] or 0, 2),
                    'success_rate': round(row[4] or 0, 2),
                    'slow_count': row[5],
                    'avg_process_ms': round(row[6] or 0, 2),
                    'avg_transfer_ms': round(row[7] or 0, 2),
                    'main_platform': row[8] or 'Unknown',
                    'problem_type': self._identify_problem_type(row[6], row[7], row[2])
                })
            return results
        except Exception as e:
            raise Exception(f"获取慢接口失败: {e}")
    
    def get_platform_distribution(self, hours: int = 24, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """平台流量分布"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            # 构建时间条件
            if start_time and end_time:
                time_condition = f"log_time >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND log_time <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'"
            else:
                time_condition = f"""CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END"""
            
            platform_stats = self.client.query(f"""
                SELECT 
                    platform,
                    COUNT(*) as requests,
                    COUNT(DISTINCT client_ip) as unique_users,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    SUM(CASE WHEN is_success THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate
                FROM dwd_nginx_enriched 
                WHERE {time_condition}
                GROUP BY platform
                ORDER BY requests DESC
            """).result_rows
            
            total_requests = sum(row[1] for row in platform_stats)
            results = []
            
            for row in platform_stats:
                results.append({
                    'platform': row[0] or 'Unknown',
                    'requests': row[1],
                    'percentage': round((row[1] / total_requests * 100) if total_requests > 0 else 0, 2),
                    'unique_users': row[2],
                    'avg_response_ms': round(row[3] or 0, 2),
                    'success_rate': round(row[4] or 0, 2)
                })
            return results
        except Exception as e:
            raise Exception(f"获取平台分布失败: {e}")
    
    def get_error_alerts(self, error_threshold: float = 5.0, hours: int = 24, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """错误告警 - 高错误率接口"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            # 构建时间条件
            if start_time and end_time:
                time_condition = f"log_time >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND log_time <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'"
            else:
                time_condition = f"""CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END"""
            
            error_apis = self.client.query(f"""
                SELECT 
                    request_uri as api,
                    COUNT(*) as total_requests,
                    SUM(CASE WHEN is_error THEN 1 ELSE 0 END) as error_count,
                    SUM(CASE WHEN is_error THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as error_rate,
                    response_status_code as main_error_code,
                    platform as main_platform
                FROM dwd_nginx_enriched 
                WHERE {time_condition}
                  AND total_request_duration > 0
                GROUP BY request_uri, response_status_code, platform
                HAVING error_rate >= {error_threshold}
                ORDER BY error_rate DESC, total_requests DESC
            """).result_rows
            
            results = []
            for row in error_apis:
                results.append({
                    'api': row[0],
                    'total_requests': row[1],
                    'error_count': row[2],
                    'error_rate': round(row[3] or 0, 2),
                    'main_error_code': row[4],
                    'main_platform': row[5] or 'Unknown',
                    'alert_level': 'HIGH' if row[3] > 20 else 'MEDIUM'
                })
            return results
        except Exception as e:
            raise Exception(f"获取错误告警失败: {e}")
    
    def _get_health_status(self, avg_response_ms: float, error_rate: float) -> str:
        """判断接口健康状态"""
        if not avg_response_ms:
            return 'Unknown'
        
        if avg_response_ms > 3000:  # 超过3秒
            return 'Critical'
        elif avg_response_ms > 1000 or (error_rate and error_rate > 5):  # 超过1秒或错误率>5%
            return 'Warning'
        else:
            return 'Healthy'
    
    def _identify_problem_type(self, process_ms: float, transfer_ms: float, total_ms: float) -> str:
        """识别性能问题类型"""
        if not process_ms and not transfer_ms:
            return 'Unknown'
        
        if process_ms and process_ms > 1000:
            return '后端处理慢'
        elif transfer_ms and transfer_ms > 500:
            return '数据传输慢'
        elif total_ms and total_ms > 3000:
            return '整体响应慢'
        else:
            return '轻微延迟'