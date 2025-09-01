# -*- coding: utf-8 -*-
"""
流量分析器
分析用户行为、入口来源、设备分布
"""

import clickhouse_connect
from typing import Dict, List, Any
from datetime import datetime, timedelta


class TrafficAnalyzer:
    """流量分析 - 用户行为和来源分析"""
    
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
    
    def get_traffic_overview(self, hours: int = 24) -> Dict[str, Any]:
        """流量总览"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            traffic_stats = self.client.query(f"""
                SELECT 
                    COUNT(*) as total_requests,
                    COUNT(DISTINCT client_ip) as unique_visitors,
                    AVG(connection_requests) as avg_session_requests,
                    COUNT(DISTINCT platform) as platform_types,
                    COUNT(DISTINCT api_category) as api_categories
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END
            """).first_row
            
            return {
                'total_requests': traffic_stats[0],
                'unique_visitors': traffic_stats[1],
                'avg_session_requests': round(traffic_stats[2] or 0, 2),
                'platform_types': traffic_stats[3],
                'api_categories': traffic_stats[4]
            }
        except Exception as e:
            raise Exception(f"获取流量总览失败: {e}")
    
    def get_platform_analysis(self, hours: int = 24) -> List[Dict[str, Any]]:
        """平台用户行为分析"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            platform_analysis = self.client.query(f"""
                SELECT 
                    platform,
                    device_type,
                    entry_source,
                    COUNT(*) as requests,
                    COUNT(DISTINCT client_ip) as unique_users,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    AVG(connection_requests) as avg_session_depth,
                    SUM(CASE WHEN is_success THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate
                FROM dwd_nginx_enriched 
                WHERE log_time >= now() - INTERVAL {hours} HOUR
                GROUP BY platform, device_type, entry_source
                ORDER BY requests DESC
            """).result_rows
            
            results = []
            for row in platform_analysis:
                results.append({
                    'platform': row[0] or 'Unknown',
                    'device_type': row[1] or 'Unknown', 
                    'entry_source': row[2] or 'Direct',
                    'requests': row[3],
                    'unique_users': row[4],
                    'avg_response_ms': round(row[5] or 0, 2),
                    'avg_session_depth': round(row[6] or 0, 2),
                    'success_rate': round(row[7] or 0, 2),
                    'engagement_level': self._get_engagement_level(row[6])
                })
            return results
        except Exception as e:
            raise Exception(f"获取平台分析失败: {e}")
    
    def get_api_category_distribution(self, hours: int = 24) -> List[Dict[str, Any]]:
        """API分类使用分布"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            category_stats = self.client.query(f"""
                SELECT 
                    api_category,
                    COUNT(*) as requests,
                    COUNT(DISTINCT client_ip) as unique_users,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    SUM(CASE WHEN is_slow THEN 1 ELSE 0 END) as slow_requests
                FROM dwd_nginx_enriched 
                WHERE log_time >= now() - INTERVAL {hours} HOUR
                GROUP BY api_category
                ORDER BY requests DESC
            """).result_rows
            
            total_requests = sum(row[1] for row in category_stats)
            results = []
            
            for row in category_stats:
                results.append({
                    'category': row[0] or 'Other',
                    'requests': row[1],
                    'percentage': round((row[1] / total_requests * 100) if total_requests > 0 else 0, 2),
                    'unique_users': row[2],
                    'avg_response_ms': round(row[3] or 0, 2),
                    'slow_requests': row[4],
                    'slow_rate': round((row[4] / row[1] * 100) if row[1] > 0 else 0, 2)
                })
            return results
        except Exception as e:
            raise Exception(f"获取API分类分布失败: {e}")
    
    def get_hourly_traffic_pattern(self, hours: int = 24) -> List[Dict[str, Any]]:
        """24小时流量模式分析"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            hourly_pattern = self.client.query(f"""
                SELECT 
                    hour_partition as hour,
                    COUNT(*) as requests,
                    COUNT(DISTINCT client_ip) as unique_users,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    SUM(CASE WHEN is_error THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as error_rate
                FROM dwd_nginx_enriched 
                WHERE log_time >= now() - INTERVAL {hours} HOUR
                GROUP BY hour_partition
                ORDER BY hour_partition
            """).result_rows
            
            results = []
            for row in hourly_pattern:
                results.append({
                    'hour': row[0],
                    'requests': row[1],
                    'unique_users': row[2],
                    'avg_response_ms': round(row[3] or 0, 2),
                    'error_rate': round(row[4] or 0, 2),
                    'traffic_level': self._get_traffic_level(row[1])
                })
            return results
        except Exception as e:
            raise Exception(f"获取小时流量模式失败: {e}")
    
    def _get_engagement_level(self, avg_session_requests: float) -> str:
        """用户参与度评估"""
        if not avg_session_requests:
            return 'Low'
        
        if avg_session_requests >= 10:
            return 'High'
        elif avg_session_requests >= 5:
            return 'Medium'
        else:
            return 'Low'
    
    def _get_traffic_level(self, requests: int) -> str:
        """流量水平评估"""
        if requests >= 1000:
            return 'Peak'
        elif requests >= 500:
            return 'High'
        elif requests >= 100:
            return 'Medium'
        else:
            return 'Low'