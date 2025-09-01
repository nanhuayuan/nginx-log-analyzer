# -*- coding: utf-8 -*-
"""
时间维度分析器
分析时间模式、峰谷规律、业务节奏
"""

import clickhouse_connect
from typing import Dict, List, Any
from datetime import datetime, timedelta


class TimeAnalyzer:
    """时间维度分析器 - 业务时间模式"""
    
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
    
    def get_hourly_pattern(self, hours: int = 24) -> List[Dict[str, Any]]:
        """24小时访问模式"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            hourly_stats = self.client.query(f"""
                SELECT 
                    toHour(log_time) as hour,
                    COUNT(*) as requests,
                    COUNT(DISTINCT client_ip) as unique_users,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    COUNT(CASE WHEN is_success THEN 1 END) * 100.0 / COUNT(*) as success_rate,
                    COUNT(CASE WHEN is_slow THEN 1 END) as slow_requests,
                    COUNT(DISTINCT request_uri) as unique_apis
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
            max_requests = max(row[1] for row in hourly_stats) if hourly_stats else 1
            
            for row in hourly_stats:
                results.append({
                    'hour': f"{row[0]:02d}:00",
                    'requests': row[1],
                    'unique_users': row[2],
                    'avg_response_ms': round(row[3] or 0, 2),
                    'success_rate': round(row[4] or 0, 2),
                    'slow_requests': row[5],
                    'unique_apis': row[6],
                    'traffic_intensity': round((row[1] / max_requests * 100), 2),
                    'business_period': self._get_business_period(row[0])
                })
            return results
        except Exception as e:
            raise Exception(f"获取小时模式失败: {e}")
    
    def get_peak_analysis(self, hours: int = 24) -> Dict[str, Any]:
        """峰谷分析"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            peak_stats = self.client.query(f"""
                WITH hourly_data AS (
                    SELECT 
                        toHour(log_time) as hour,
                        COUNT(*) as requests,
                        AVG(total_request_duration) * 1000 as avg_response_ms
                    FROM dwd_nginx_enriched 
                    WHERE CASE 
                        WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                        THEN log_time >= now() - INTERVAL {hours} HOUR
                        ELSE 1=1
                    END
                    GROUP BY toHour(log_time)
                )
                SELECT 
                    MAX(requests) as peak_requests,
                    MIN(requests) as valley_requests,
                    AVG(requests) as avg_requests,
                    argMax(hour, requests) as peak_hour,
                    argMin(hour, requests) as valley_hour
                FROM hourly_data
            """).first_row
            
            if peak_stats:
                return {
                    'peak_requests': peak_stats[0],
                    'valley_requests': peak_stats[1],
                    'avg_requests': round(peak_stats[2] or 0, 2),
                    'peak_hour': f"{peak_stats[3]:02d}:00" if peak_stats[3] else '00:00',
                    'valley_hour': f"{peak_stats[4]:02d}:00" if peak_stats[4] else '00:00',
                    'peak_valley_ratio': round((peak_stats[0] / peak_stats[1]) if peak_stats[1] > 0 else 0, 2),
                    'traffic_volatility': self._get_volatility_level(peak_stats[0], peak_stats[1])
                }
            else:
                return {}
        except Exception as e:
            raise Exception(f"获取峰谷分析失败: {e}")
    
    def get_response_time_trends(self, hours: int = 24) -> List[Dict[str, Any]]:
        """响应时间趋势"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            trends = self.client.query(f"""
                SELECT 
                    toHour(log_time) as hour,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    quantile(0.50)(total_request_duration) * 1000 as p50_ms,
                    quantile(0.95)(total_request_duration) * 1000 as p95_ms,
                    quantile(0.99)(total_request_duration) * 1000 as p99_ms,
                    MAX(total_request_duration) * 1000 as max_ms,
                    COUNT(*) as sample_count
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END
                  AND total_request_duration > 0
                GROUP BY toHour(log_time)
                ORDER BY hour
            """).result_rows
            
            results = []
            for row in trends:
                results.append({
                    'hour': f"{row[0]:02d}:00",
                    'avg_response_ms': round(row[1] or 0, 2),
                    'p50_ms': round(row[2] or 0, 2),
                    'p95_ms': round(row[3] or 0, 2),
                    'p99_ms': round(row[4] or 0, 2),
                    'max_ms': round(row[5] or 0, 2),
                    'sample_count': row[6],
                    'performance_grade': self._get_performance_grade(row[1], row[3])
                })
            return results
        except Exception as e:
            raise Exception(f"获取响应时间趋势失败: {e}")
    
    def get_business_insights(self, hours: int = 24) -> Dict[str, Any]:
        """业务洞察分析"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            # 工作时间 vs 非工作时间对比
            business_comparison = self.client.query(f"""
                SELECT 
                    CASE 
                        WHEN toHour(log_time) BETWEEN 9 AND 18 THEN 'business_hours'
                        ELSE 'off_hours'
                    END as period_type,
                    COUNT(*) as requests,
                    COUNT(DISTINCT client_ip) as unique_users,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    COUNT(CASE WHEN is_success THEN 1 END) * 100.0 / COUNT(*) as success_rate
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END
                GROUP BY period_type
            """).result_rows
            
            business_data = {}
            off_hours_data = {}
            
            for row in business_comparison:
                data = {
                    'requests': row[1],
                    'unique_users': row[2],
                    'avg_response_ms': round(row[3] or 0, 2),
                    'success_rate': round(row[4] or 0, 2)
                }
                
                if row[0] == 'business_hours':
                    business_data = data
                else:
                    off_hours_data = data
            
            # 计算对比指标
            insights = {
                'business_hours': business_data,
                'off_hours': off_hours_data,
                'business_load_ratio': round(
                    (business_data.get('requests', 0) / off_hours_data.get('requests', 1)) if off_hours_data.get('requests', 0) > 0 else 0, 2
                ),
                'performance_comparison': 'business_better' if business_data.get('avg_response_ms', 0) < off_hours_data.get('avg_response_ms', 0) else 'off_hours_better',
                'user_activity_pattern': self._analyze_user_pattern(business_data, off_hours_data)
            }
            
            return insights
        except Exception as e:
            raise Exception(f"获取业务洞察失败: {e}")
    
    def _get_business_period(self, hour: int) -> str:
        """获取业务时段"""
        if 6 <= hour < 9:
            return '早高峰'
        elif 9 <= hour < 12:
            return '上午业务'
        elif 12 <= hour < 14:
            return '午休时段'
        elif 14 <= hour < 18:
            return '下午业务'
        elif 18 <= hour < 21:
            return '晚高峰'
        elif 21 <= hour < 24:
            return '夜间活跃'
        else:
            return '深夜/凌晨'
    
    def _get_volatility_level(self, peak: int, valley: int) -> str:
        """获取波动水平"""
        if valley == 0:
            return 'Extreme'
        
        ratio = peak / valley
        if ratio > 10:
            return 'High'
        elif ratio > 5:
            return 'Medium'
        elif ratio > 2:
            return 'Low' 
        else:
            return 'Stable'
    
    def _get_performance_grade(self, avg_ms: float, p95_ms: float) -> str:
        """获取性能等级"""
        if not avg_ms or not p95_ms:
            return 'Unknown'
        
        if avg_ms < 100 and p95_ms < 500:
            return 'A'
        elif avg_ms < 500 and p95_ms < 1000:
            return 'B'
        elif avg_ms < 1000 and p95_ms < 2000:
            return 'C'
        else:
            return 'D'
    
    def _analyze_user_pattern(self, business_data: Dict, off_hours_data: Dict) -> str:
        """分析用户活动模式"""
        business_users = business_data.get('unique_users', 0)
        off_hours_users = off_hours_data.get('unique_users', 0)
        
        if business_users > off_hours_users * 3:
            return '典型工作日模式'
        elif business_users > off_hours_users:
            return '轻度工作日模式'
        elif business_users == off_hours_users:
            return '全天均衡模式'
        else:
            return '夜间活跃模式'