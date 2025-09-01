# -*- coding: utf-8 -*-
"""
请求头分析器
分析User-Agent、Referer、Accept等HTTP请求头信息
"""

import clickhouse_connect
from typing import Dict, List, Any
from datetime import datetime, timedelta
import re


class HeaderAnalyzer:
    """请求头分析器 - Self功能10.请求头分析对标"""
    
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
    
    def get_user_agent_analysis(self, hours: int = 24) -> List[Dict[str, Any]]:
        """User-Agent分析 - 浏览器、操作系统、设备分析"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            # 从platform字段分析User-Agent信息
            user_agent_stats = self.client.query(f"""
                SELECT 
                    platform,
                    device_type,
                    COUNT(*) as request_count,
                    COUNT(DISTINCT client_ip) as unique_users,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    COUNT(CASE WHEN is_success THEN 1 END) * 100.0 / COUNT(*) as success_rate,
                    COUNT(CASE WHEN is_slow THEN 1 END) as slow_requests,
                    COUNT(CASE WHEN is_error THEN 1 END) as error_requests
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END
                GROUP BY platform, device_type
                ORDER BY request_count DESC
            """).result_rows
            
            total_requests = sum(row[2] for row in user_agent_stats)
            results = []
            
            for row in user_agent_stats:
                platform = row[0] or 'Unknown'
                results.append({
                    'user_agent': platform,  # 模板期望的字段名
                    'platform': platform,
                    'category': 'Bot' if ('bot' in platform.lower() or 'crawler' in platform.lower()) else 'Normal',
                    'browser': self._classify_browser(platform),
                    'os': self._classify_os(platform),
                    'device_type': row[1] or 'Unknown',
                    'request_count': row[2],
                    'percentage': round((row[2] / total_requests * 100) if total_requests > 0 else 0, 2),
                    'unique_ips': row[3],
                    'unique_users': row[3],
                    'avg_response_ms': round(row[4] or 0, 2),
                    'success_rate': round(row[5] or 0, 2),
                    'slow_requests': row[6],
                    'error_requests': row[7],
                    'slow_rate': round((row[6] / row[2] * 100) if row[2] > 0 else 0, 2),
                    'error_rate': round((row[7] / row[2] * 100) if row[2] > 0 else 0, 2),
                    'browser_category': self._classify_browser(platform),
                    'performance_grade': self._get_performance_grade(row[4], row[6], row[2]),
                    'risk_level': 'High' if (row[7] / row[2] > 0.1) else 'Medium' if (row[6] / row[2] > 0.2) else 'Low'
                })
            
            return results
        except Exception as e:
            raise Exception(f"获取User-Agent分析失败: {e}")
    
    def get_user_agent_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """获取User-Agent统计概览"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            stats = self.client.query(f"""
                SELECT 
                    COUNT(DISTINCT platform) as total_platforms,
                    COUNT(DISTINCT 
                        CASE WHEN platform LIKE '%Chrome%' OR platform LIKE '%Firefox%' OR platform LIKE '%Safari%' OR platform LIKE '%Edge%'
                        THEN 'Browser' END
                    ) as total_browsers,
                    COUNT(DISTINCT 
                        CASE WHEN platform LIKE '%Windows%' OR platform LIKE '%Mac%' OR platform LIKE '%Linux%' OR platform LIKE '%Android%' OR platform LIKE '%iOS%'
                        THEN 'OS' END  
                    ) as total_os,
                    COUNT(DISTINCT device_type) as total_devices
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1 
                END
                AND platform IS NOT NULL 
                AND platform != ''
            """).result_rows
            
            if stats:
                return {
                    'total_platforms': stats[0][0] or 0,
                    'total_browsers': stats[0][1] or 0, 
                    'total_os': stats[0][2] or 0,
                    'total_devices': stats[0][3] or 0
                }
            else:
                return {'total_platforms': 0, 'total_browsers': 0, 'total_os': 0, 'total_devices': 0}
                
        except Exception as e:
            print(f"User-Agent统计查询失败: {e}")
            return {'total_platforms': 0, 'total_browsers': 0, 'total_os': 0, 'total_devices': 0}

    def get_referer_analysis(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Referer来源分析"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            # 从entry_source字段分析Referer信息
            referer_stats = self.client.query(f"""
                SELECT 
                    entry_source,
                    COUNT(*) as request_count,
                    COUNT(DISTINCT client_ip) as unique_users,
                    COUNT(DISTINCT request_uri) as unique_apis,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    COUNT(CASE WHEN is_success THEN 1 END) * 100.0 / COUNT(*) as success_rate,
                    AVG(connection_requests) as avg_session_depth
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END
                GROUP BY entry_source
                ORDER BY request_count DESC
            """).result_rows
            
            total_requests = sum(row[1] for row in referer_stats)
            results = []
            
            for row in referer_stats:
                results.append({
                    'entry_source': row[0] or 'Direct',
                    'request_count': row[1],
                    'percentage': round((row[1] / total_requests * 100) if total_requests > 0 else 0, 2),
                    'unique_users': row[2],
                    'unique_apis': row[3],
                    'avg_response_ms': round(row[4] or 0, 2),
                    'success_rate': round(row[5] or 0, 2),
                    'avg_session_depth': round(row[6] or 0, 2),
                    'source_type': self._classify_source_type(row[0]),
                    'user_engagement': self._get_engagement_level(row[6]),
                    'traffic_quality': self._assess_traffic_quality(row[5], row[6], row[3])
                })
            
            return results
        except Exception as e:
            raise Exception(f"获取Referer分析失败: {e}")
    
    def get_platform_performance_correlation(self, hours: int = 24) -> List[Dict[str, Any]]:
        """平台与性能关联分析 - Self功能11对标"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            correlation_stats = self.client.query(f"""
                SELECT 
                    platform,
                    device_type,
                    COUNT(*) as total_requests,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    quantile(0.50)(total_request_duration) * 1000 as p50_ms,
                    quantile(0.95)(total_request_duration) * 1000 as p95_ms,
                    quantile(0.99)(total_request_duration) * 1000 as p99_ms,
                    COUNT(CASE WHEN is_slow THEN 1 END) as slow_requests,
                    COUNT(CASE WHEN is_success THEN 1 END) * 100.0 / COUNT(*) as success_rate,
                    AVG(backend_process_phase) * 1000 as avg_backend_ms,
                    AVG(nginx_transfer_phase) * 1000 as avg_transfer_ms,
                    COUNT(DISTINCT client_ip) as unique_users
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END
                  AND total_request_duration > 0
                GROUP BY platform, device_type
                ORDER BY avg_response_ms DESC
            """).result_rows
            
            results = []
            for row in correlation_stats:
                results.append({
                    'platform': row[0] or 'Unknown',
                    'device_type': row[1] or 'Unknown',
                    'total_requests': row[2],
                    'avg_response_ms': round(row[3] or 0, 2),
                    'p50_ms': round(row[4] or 0, 2),
                    'p95_ms': round(row[5] or 0, 2),
                    'p99_ms': round(row[6] or 0, 2),
                    'slow_requests': row[7],
                    'slow_rate': round((row[7] / row[2] * 100) if row[2] > 0 else 0, 2),
                    'success_rate': round(row[8] or 0, 2),
                    'avg_backend_ms': round(row[9] or 0, 2),
                    'avg_transfer_ms': round(row[10] or 0, 2),
                    'unique_users': row[11],
                    'performance_score': self._calculate_performance_score(row[3], row[5], row[8]),
                    'bottleneck_type': self._identify_bottleneck(row[9], row[10], row[3]),
                    'optimization_priority': self._get_optimization_priority(row[2], row[7], row[3])
                })
            
            return results
        except Exception as e:
            raise Exception(f"获取平台性能关联分析失败: {e}")
    
    def get_performance_overview(self, hours: int = 24) -> Dict[str, Any]:
        """获取性能概览数据"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            overview = self.client.query(f"""
                SELECT 
                    COUNT(DISTINCT platform) as total_platforms,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    COUNT(CASE WHEN total_request_duration > 3 THEN 1 END) as slow_platforms,
                    stddevPop(total_request_duration) / AVG(total_request_duration) * 100 as performance_variance
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1 
                END
            """).result_rows
            
            if overview:
                return {
                    'total_platforms': overview[0][0] or 0,
                    'avg_response_ms': overview[0][1] or 0.0,
                    'slow_platforms': overview[0][2] or 0,
                    'performance_variance': overview[0][3] or 0.0
                }
            else:
                return {'total_platforms': 0, 'avg_response_ms': 0.0, 'slow_platforms': 0, 'performance_variance': 0.0}
                
        except Exception as e:
            print(f"性能概览查询失败: {e}")
            return {'total_platforms': 0, 'avg_response_ms': 0.0, 'slow_platforms': 0, 'performance_variance': 0.0}

    def get_bot_performance_analysis(self, hours: int = 24) -> List[Dict[str, Any]]:
        """获取Bot/爬虫性能分析"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            bot_data = self.client.query(f"""
                SELECT 
                    platform,
                    COUNT(*) as request_count,
                    AVG(total_request_duration) * 1000 as avg_response_ms,
                    COUNT(*) / (GREATEST({hours}, 1) / 24.0) as request_frequency,
                    COUNT(CASE WHEN is_success THEN 1 END) * 100.0 / COUNT(*) as success_rate
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1 
                END
                AND (platform LIKE '%bot%' OR platform LIKE '%crawler%' OR platform LIKE '%spider%')
                GROUP BY platform
                ORDER BY request_count DESC
                LIMIT 10
            """).result_rows
            
            results = []
            for row in bot_data:
                platform, request_count, avg_response_ms, request_frequency, success_rate = row
                
                results.append({
                    'bot_type': self._classify_bot_type(platform),
                    'bot_identifier': platform,
                    'request_count': request_count,
                    'avg_response_ms': avg_response_ms or 0.0,
                    'request_frequency': request_frequency or 0.0,
                    'success_rate': success_rate or 0.0,
                    'resource_impact': self._assess_resource_impact(request_count, avg_response_ms or 0),
                    'impact_assessment': self._assess_bot_impact(success_rate or 0, request_frequency or 0),
                    'strategy_recommendation': self._get_bot_strategy(platform, request_count)
                })
            
            return results
            
        except Exception as e:
            print(f"Bot性能分析查询失败: {e}")
            return []

    def get_optimization_suggestions(self, hours: int = 24) -> Dict[str, List[str]]:
        """获取优化建议"""
        return {
            'client_optimizations': [
                '针对移动端优化响应式设计',
                '优化前端资源加载和缓存策略',
                '提升页面渲染性能',
                '优化JavaScript执行效率'
            ],
            'server_optimizations': [
                '优化数据库查询和索引',
                '提升API响应速度',
                '启用HTTP/2和gzip压缩',
                '优化服务器资源配置'
            ],
            'architecture_optimizations': [
                '实施CDN加速',
                '负载均衡优化',
                '缓存策略调整',
                '微服务架构优化'
            ]
        }

    def get_header_security_insights(self, hours: int = 24) -> Dict[str, Any]:
        """请求头安全洞察"""
        if not self.client:
            raise Exception("无法连接到ClickHouse")
        
        try:
            # 分析潜在的安全风险模式
            security_stats = self.client.query(f"""
                SELECT 
                    platform,
                    entry_source,
                    device_type,
                    COUNT(*) as request_count,
                    COUNT(DISTINCT client_ip) as unique_ips,
                    COUNT(DISTINCT request_uri) as unique_paths,
                    COUNT(CASE WHEN is_error THEN 1 END) as error_count,
                    AVG(total_request_duration) * 1000 as avg_response_ms
                FROM dwd_nginx_enriched 
                WHERE CASE 
                    WHEN (SELECT COUNT(*) FROM dwd_nginx_enriched WHERE log_time >= now() - INTERVAL {hours} HOUR) > 0 
                    THEN log_time >= now() - INTERVAL {hours} HOUR
                    ELSE 1=1
                END
                GROUP BY platform, entry_source, device_type
                HAVING request_count > 10
                ORDER BY request_count DESC
            """).result_rows
            
            # 分析结果
            suspicious_patterns = []
            normal_patterns = []
            
            for row in security_stats:
                pattern_analysis = {
                    'platform': row[0] or 'Unknown',
                    'entry_source': row[1] or 'Direct',
                    'device_type': row[2] or 'Unknown',
                    'request_count': row[3],
                    'unique_ips': row[4],
                    'unique_paths': row[5],
                    'error_count': row[6],
                    'avg_response_ms': round(row[7] or 0, 2),
                    'requests_per_ip': round(row[3] / row[4] if row[4] > 0 else 0, 2),
                    'error_rate': round((row[6] / row[3] * 100) if row[3] > 0 else 0, 2)
                }
                
                # 判断是否为可疑模式
                risk_score = self._calculate_risk_score(pattern_analysis)
                pattern_analysis['risk_score'] = risk_score
                pattern_analysis['risk_level'] = self._get_risk_level(risk_score)
                
                if risk_score > 50:
                    suspicious_patterns.append(pattern_analysis)
                else:
                    normal_patterns.append(pattern_analysis)
            
            return {
                'suspicious_patterns': suspicious_patterns[:10],  # Top 10 suspicious
                'normal_patterns': normal_patterns[:20],          # Top 20 normal
                'total_patterns': len(security_stats),
                'suspicious_count': len(suspicious_patterns),
                'risk_distribution': {
                    'high_risk': len([p for p in suspicious_patterns if p['risk_score'] > 80]),
                    'medium_risk': len([p for p in suspicious_patterns if 50 < p['risk_score'] <= 80]),
                    'low_risk': len([p for p in normal_patterns if p['risk_score'] > 20])
                }
            }
        except Exception as e:
            raise Exception(f"获取请求头安全洞察失败: {e}")
    
    def _classify_os(self, platform: str) -> str:
        """分类操作系统"""
        if not platform:
            return 'Unknown'
        platform_lower = platform.lower()
        if 'windows' in platform_lower:
            return 'Windows'
        elif 'mac' in platform_lower or 'osx' in platform_lower:
            return 'macOS'
        elif 'linux' in platform_lower:
            return 'Linux'
        elif 'android' in platform_lower:
            return 'Android'
        elif 'ios' in platform_lower or 'iphone' in platform_lower:
            return 'iOS'
        else:
            return 'Unknown'

    def _classify_browser(self, platform: str) -> str:
        """浏览器分类"""
        if not platform:
            return 'Unknown'
        
        platform_lower = platform.lower()
        if 'chrome' in platform_lower or 'android' in platform_lower:
            return 'Webkit-based'
        elif 'firefox' in platform_lower:
            return 'Gecko-based'
        elif 'safari' in platform_lower or 'ios' in platform_lower:
            return 'WebKit-Safari'
        elif 'sdk' in platform_lower:
            return 'Native-SDK'
        else:
            return 'Other'
    
    def _get_performance_grade(self, avg_ms: float, slow_count: int, total_count: int) -> str:
        """性能等级评估"""
        if not avg_ms or total_count == 0:
            return 'Unknown'
        
        slow_rate = (slow_count / total_count) * 100 if total_count > 0 else 0
        
        if avg_ms < 500 and slow_rate < 5:
            return 'Excellent'
        elif avg_ms < 1000 and slow_rate < 10:
            return 'Good'
        elif avg_ms < 2000 and slow_rate < 20:
            return 'Fair'
        else:
            return 'Poor'
    
    def _classify_source_type(self, entry_source: str) -> str:
        """来源类型分类"""
        if not entry_source or entry_source == 'Direct':
            return 'Direct'
        
        source_lower = entry_source.lower()
        if 'google' in source_lower or 'baidu' in source_lower:
            return 'Search Engine'
        elif 'social' in source_lower or 'wechat' in source_lower:
            return 'Social Media'
        elif 'api' in source_lower or 'app' in source_lower:
            return 'API/App'
        else:
            return 'External Link'
    
    def _get_engagement_level(self, session_depth: float) -> str:
        """用户参与度评估"""
        if not session_depth:
            return 'Low'
        
        if session_depth >= 10:
            return 'Very High'
        elif session_depth >= 5:
            return 'High'
        elif session_depth >= 2:
            return 'Medium'
        else:
            return 'Low'
    
    def _assess_traffic_quality(self, success_rate: float, session_depth: float, unique_apis: int) -> str:
        """流量质量评估"""
        score = 0
        
        if success_rate > 95:
            score += 30
        elif success_rate > 90:
            score += 20
        elif success_rate > 80:
            score += 10
        
        if session_depth > 5:
            score += 30
        elif session_depth > 2:
            score += 20
        elif session_depth > 1:
            score += 10
        
        if unique_apis > 10:
            score += 40
        elif unique_apis > 5:
            score += 30
        elif unique_apis > 2:
            score += 20
        
        if score >= 80:
            return 'Excellent'
        elif score >= 60:
            return 'Good'
        elif score >= 40:
            return 'Fair'
        else:
            return 'Poor'
    
    def _calculate_performance_score(self, avg_ms: float, p95_ms: float, success_rate: float) -> int:
        """性能评分计算"""
        score = 100
        
        # 响应时间扣分
        if avg_ms > 3000:
            score -= 40
        elif avg_ms > 1000:
            score -= 20
        elif avg_ms > 500:
            score -= 10
        
        # P95响应时间扣分
        if p95_ms > 5000:
            score -= 30
        elif p95_ms > 2000:
            score -= 15
        elif p95_ms > 1000:
            score -= 5
        
        # 成功率扣分
        if success_rate < 90:
            score -= 20
        elif success_rate < 95:
            score -= 10
        elif success_rate < 99:
            score -= 5
        
        return max(score, 0)
    
    def _identify_bottleneck(self, backend_ms: float, transfer_ms: float, total_ms: float) -> str:
        """识别性能瓶颈"""
        if not backend_ms or not transfer_ms or not total_ms:
            return 'Unknown'
        
        backend_ratio = backend_ms / total_ms if total_ms > 0 else 0
        transfer_ratio = transfer_ms / total_ms if total_ms > 0 else 0
        
        if backend_ratio > 0.7:
            return 'Backend Processing'
        elif transfer_ratio > 0.3:
            return 'Network Transfer'
        elif total_ms > 3000:
            return 'Overall Slow'
        else:
            return 'Balanced'
    
    def _get_optimization_priority(self, request_count: int, slow_count: int, avg_ms: float) -> str:
        """优化优先级评估"""
        impact_score = request_count * 0.3 + slow_count * 0.5 + (avg_ms / 1000) * 0.2
        
        if impact_score > 1000:
            return 'Critical'
        elif impact_score > 500:
            return 'High'
        elif impact_score > 100:
            return 'Medium'
        else:
            return 'Low'
    
    def _calculate_risk_score(self, pattern: Dict[str, Any]) -> int:
        """计算风险评分"""
        risk_score = 0
        
        # 高频率访问风险
        if pattern['requests_per_ip'] > 1000:
            risk_score += 40
        elif pattern['requests_per_ip'] > 100:
            risk_score += 20
        elif pattern['requests_per_ip'] > 50:
            risk_score += 10
        
        # 高错误率风险
        if pattern['error_rate'] > 50:
            risk_score += 30
        elif pattern['error_rate'] > 20:
            risk_score += 15
        elif pattern['error_rate'] > 10:
            risk_score += 5
        
        # 路径多样性风险（可能是扫描）
        path_diversity = pattern['unique_paths'] / pattern['request_count'] if pattern['request_count'] > 0 else 0
        if path_diversity > 0.5:
            risk_score += 20
        
        # 平台异常风险
        if pattern['platform'] == 'Unknown' or 'bot' in pattern['platform'].lower():
            risk_score += 15
        
        return min(risk_score, 100)
    
    def _classify_bot_type(self, platform: str) -> str:
        """分类Bot类型"""
        platform_lower = platform.lower()
        if 'google' in platform_lower or 'bingbot' in platform_lower:
            return 'Search'
        elif 'monitor' in platform_lower or 'check' in platform_lower:
            return 'Monitor'  
        elif 'facebook' in platform_lower or 'twitter' in platform_lower:
            return 'Social'
        else:
            return 'Other'

    def _assess_resource_impact(self, request_count: int, avg_response_ms: float) -> str:
        """评估资源影响"""
        impact_score = request_count * (avg_response_ms / 1000.0)
        if impact_score > 10000:
            return 'High'
        elif impact_score > 1000:
            return 'Medium'
        else:
            return 'Low'

    def _assess_bot_impact(self, success_rate: float, request_frequency: float) -> str:
        """评估Bot影响"""
        if success_rate > 90 and request_frequency < 100:
            return 'Positive'
        elif success_rate < 70 or request_frequency > 1000:
            return 'Negative'
        else:
            return 'Neutral'

    def _get_bot_strategy(self, platform: str, request_count: int) -> str:
        """获取Bot策略建议"""
        if 'google' in platform.lower():
            return '允许访问，优化SEO'
        elif request_count > 10000:
            return '限制访问频率'
        else:
            return '正常监控'

    def _get_risk_level(self, risk_score: int) -> str:
        """获取风险等级"""
        if risk_score > 80:
            return 'Critical'
        elif risk_score > 60:
            return 'High'
        elif risk_score > 40:
            return 'Medium'
        elif risk_score > 20:
            return 'Low'
        else:
            return 'Minimal'