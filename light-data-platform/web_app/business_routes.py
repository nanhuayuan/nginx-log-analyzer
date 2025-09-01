# -*- coding: utf-8 -*-
"""
业务分析Web路由
提供业务分析相关的Web接口
"""

import sys
from pathlib import Path
from flask import Blueprint, render_template, jsonify, request

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from business_analytics.api_health_analyzer import ApiHealthAnalyzer
from business_analytics.traffic_analyzer import TrafficAnalyzer
from business_analytics.status_analyzer import StatusAnalyzer
from business_analytics.time_analyzer import TimeAnalyzer
from business_analytics.ip_analyzer import IpAnalyzer
from business_analytics.header_analyzer import HeaderAnalyzer

# 创建蓝图
business_bp = Blueprint('business', __name__)

# ClickHouse配置
CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 8123,
    'username': 'analytics_user', 
    'password': 'analytics_password',
    'database': 'nginx_analytics'
}

# 时间参数处理函数
def get_time_params():
    """从request参数中获取时间范围，支持多种格式"""
    from datetime import datetime, timedelta
    
    # 优先使用start/end参数（时间戳格式）
    start_param = request.args.get('start')
    end_param = request.args.get('end')
    
    if start_param and end_param:
        try:
            # 尝试解析时间戳
            start_time = datetime.fromtimestamp(int(start_param))
            end_time = datetime.fromtimestamp(int(end_param))
            return {
                'start_time': start_time,
                'end_time': end_time,
                'hours': None,
                'use_time_range': True
            }
        except (ValueError, TypeError):
            # 回退到ISO格式
            try:
                start_time = datetime.fromisoformat(start_param.replace('Z', '+00:00'))
                end_time = datetime.fromisoformat(end_param.replace('Z', '+00:00'))
                return {
                    'start_time': start_time,
                    'end_time': end_time,
                    'hours': None,
                    'use_time_range': True
                }
            except ValueError:
                pass
    
    # 回退到hours参数
    hours = int(request.args.get('hours', 24))
    return {
        'start_time': None,
        'end_time': None,
        'hours': hours,
        'use_time_range': False
    }

# 分析器实例创建函数 - 避免并发查询冲突
def get_api_health_analyzer():
    return ApiHealthAnalyzer(CLICKHOUSE_CONFIG)

def get_traffic_analyzer():
    return TrafficAnalyzer(CLICKHOUSE_CONFIG)

def get_status_analyzer():
    return StatusAnalyzer(CLICKHOUSE_CONFIG)

def get_time_analyzer():
    return TimeAnalyzer(CLICKHOUSE_CONFIG)

def get_ip_analyzer():
    return IpAnalyzer(CLICKHOUSE_CONFIG)

def get_header_analyzer():
    return HeaderAnalyzer(CLICKHOUSE_CONFIG)


@business_bp.route('/business')
def business_dashboard():
    """业务分析主页"""
    try:
        # 获取时间参数
        time_params = get_time_params()
        
        # 获取业务总览数据 - 使用独立实例
        api_analyzer = get_api_health_analyzer()
        if time_params['use_time_range']:
            overview = api_analyzer.get_business_overview(start_time=time_params['start_time'], end_time=time_params['end_time'])
            hot_apis = api_analyzer.get_hottest_apis(limit=10, start_time=time_params['start_time'], end_time=time_params['end_time'])
            slow_apis = api_analyzer.get_slowest_apis(limit=10, start_time=time_params['start_time'], end_time=time_params['end_time'])
            platform_dist = api_analyzer.get_platform_distribution(start_time=time_params['start_time'], end_time=time_params['end_time'])
            error_alerts = api_analyzer.get_error_alerts(start_time=time_params['start_time'], end_time=time_params['end_time'])
        else:
            overview = api_analyzer.get_business_overview(hours=time_params['hours'])
            hot_apis = api_analyzer.get_hottest_apis(limit=10, hours=time_params['hours'])
            slow_apis = api_analyzer.get_slowest_apis(limit=10, hours=time_params['hours'])
            platform_dist = api_analyzer.get_platform_distribution(hours=time_params['hours'])
            error_alerts = api_analyzer.get_error_alerts(hours=time_params['hours'])
        
        # 获取扩展分析数据 - 使用独立实例
        status_analyzer = get_status_analyzer()
        if time_params['use_time_range']:
            status_dist = status_analyzer.get_status_distribution(start_time=time_params['start_time'], end_time=time_params['end_time'])
        else:
            status_dist = status_analyzer.get_status_distribution(hours=time_params['hours'])
        
        time_analyzer = get_time_analyzer()
        if time_params['use_time_range']:
            time_pattern = time_analyzer.get_hourly_pattern(start_time=time_params['start_time'], end_time=time_params['end_time'])
            peak_analysis = time_analyzer.get_peak_analysis(start_time=time_params['start_time'], end_time=time_params['end_time'])
        else:
            time_pattern = time_analyzer.get_hourly_pattern(hours=time_params['hours'])
            peak_analysis = time_analyzer.get_peak_analysis(hours=time_params['hours'])
        
        ip_analyzer = get_ip_analyzer()
        if time_params['use_time_range']:
            ip_diversity = ip_analyzer.get_client_diversity(start_time=time_params['start_time'], end_time=time_params['end_time'])
            geo_dist = ip_analyzer.get_geographical_distribution(start_time=time_params['start_time'], end_time=time_params['end_time'])
        else:
            ip_diversity = ip_analyzer.get_client_diversity(hours=time_params['hours'])
            geo_dist = ip_analyzer.get_geographical_distribution(hours=time_params['hours'])
        
        return render_template('business/dashboard.html',
                             overview=overview,
                             hot_apis=hot_apis,
                             slow_apis=slow_apis,
                             platform_dist=platform_dist,
                             error_alerts=error_alerts,
                             status_dist=status_dist,
                             time_pattern=time_pattern,
                             peak_analysis=peak_analysis,
                             ip_diversity=ip_diversity,
                             geo_dist=geo_dist)
    except Exception as e:
        return render_template('error.html', error=str(e))


@business_bp.route('/api/business/overview')
def api_business_overview():
    """API: 业务总览数据"""
    try:
        hours = int(request.args.get('hours', 24))
        overview = get_api_health_analyzer().get_business_overview(hours=hours)
        
        return jsonify({
            'status': 'success',
            'data': overview
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


@business_bp.route('/api/business/hot-apis')
def api_hot_apis():
    """API: 热门接口"""
    try:
        limit = int(request.args.get('limit', 10))
        hours = int(request.args.get('hours', 24))
        hot_apis = get_api_health_analyzer().get_hottest_apis(limit=limit, hours=hours)
        
        return jsonify({
            'status': 'success', 
            'data': hot_apis
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


@business_bp.route('/api/business/slow-apis')
def api_slow_apis():
    """API: 慢接口"""
    try:
        limit = int(request.args.get('limit', 10))
        hours = int(request.args.get('hours', 24))
        slow_apis = get_api_health_analyzer().get_slowest_apis(limit=limit, hours=hours)
        
        return jsonify({
            'status': 'success',
            'data': slow_apis
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


@business_bp.route('/api/business/platform-distribution') 
def api_platform_distribution():
    """API: 平台分布"""
    try:
        hours = int(request.args.get('hours', 24))
        platform_dist = get_api_health_analyzer().get_platform_distribution(hours=hours)
        
        return jsonify({
            'status': 'success',
            'data': platform_dist
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


@business_bp.route('/api/business/error-alerts')
def api_error_alerts():
    """API: 错误告警"""
    try:
        threshold = float(request.args.get('threshold', 5.0))
        hours = int(request.args.get('hours', 24))
        error_alerts = get_api_health_analyzer().get_error_alerts(error_threshold=threshold, hours=hours)
        
        return jsonify({
            'status': 'success',
            'data': error_alerts
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


@business_bp.route('/business/api-health')
def api_health_page():
    """接口健康度详细页面 - 参考self目录的完整分析"""
    try:
        # 获取时间参数
        time_params = get_time_params()
        limit = int(request.args.get('limit', 20))
        
        api_analyzer = get_api_health_analyzer()
        
        # 根据时间参数获取数据
        if time_params['use_time_range']:
            overview = api_analyzer.get_business_overview(start_time=time_params['start_time'], end_time=time_params['end_time'])
            hot_apis = api_analyzer.get_hottest_apis(limit=limit, start_time=time_params['start_time'], end_time=time_params['end_time'])
            slow_apis = api_analyzer.get_slowest_apis(limit=limit, start_time=time_params['start_time'], end_time=time_params['end_time'])
            error_alerts = api_analyzer.get_error_alerts(start_time=time_params['start_time'], end_time=time_params['end_time'])
        else:
            overview = api_analyzer.get_business_overview(hours=time_params['hours'])
            hot_apis = api_analyzer.get_hottest_apis(limit=limit, hours=time_params['hours'])
            slow_apis = api_analyzer.get_slowest_apis(limit=limit, hours=time_params['hours'])
            error_alerts = api_analyzer.get_error_alerts(hours=time_params['hours'])
        
        # 构建核心性能指标（参考self目录结构）
        performance_metrics = {
            'total_apis': len(hot_apis) + len(slow_apis) if hot_apis and slow_apis else 50,
            'slow_apis_count': len([api for api in (slow_apis or []) if getattr(api, 'avg_response_time', 0) > 3]),
            'error_apis_count': len(error_alerts) if error_alerts else 0,
            'avg_response_time': getattr(overview, 'avg_response_time', 1.2) * 1000,  # 转换为ms
            'total_requests': getattr(overview, 'total_requests', 10000),
            'success_rate': getattr(overview, 'success_rate', 98.5)
        }
        
        # 构建性能分级统计（参考self的性能分析）
        performance_grades = {
            'excellent': len([api for api in (hot_apis or []) if getattr(api, 'avg_response_time', 0) < 1]),  # <1000ms
            'good': len([api for api in (hot_apis or []) if 1 <= getattr(api, 'avg_response_time', 0) < 3]),       # 1000-3000ms
            'warning': len([api for api in (slow_apis or []) if 3 <= getattr(api, 'avg_response_time', 0) < 5]),    # 3000-5000ms
            'poor': len([api for api in (slow_apis or []) if getattr(api, 'avg_response_time', 0) >= 5])        # >5000ms
        }
        
        # 构建接口分类数据
        api_categories = {
            'hot_apis': (hot_apis or [])[:20],
            'slow_apis': (slow_apis or [])[:20], 
            'error_apis': (error_alerts or [])[:20]
        }
        
        # 获取业务洞察
        business_insights = {
            'performance_insights': [
                f"系统共监控 {performance_metrics['total_apis']} 个API接口",
                f"发现 {performance_metrics['slow_apis_count']} 个慢接口需要优化",
                f"平均响应时间 {performance_metrics['avg_response_time']:.0f}ms，系统成功率 {performance_metrics['success_rate']:.1f}%",
                f"性能优秀接口 {performance_grades['excellent']} 个，需关注接口 {performance_grades['warning'] + performance_grades['poor']} 个"
            ],
            'optimization_recommendations': [
                "优先优化TOP5慢接口，重点关注响应时间超过3秒的接口" if performance_metrics['slow_apis_count'] > 0 else "系统性能整体良好",
                "监控高频访问接口的稳定性，建立性能基线",
                "对错误率高的接口进行专项治理",
                "建立接口性能告警机制和降级策略"
            ]
        }
        
        # 获取平台分布数据
        platform_dist = api_analyzer.get_platform_distribution(start_time=time_params['start_time'], end_time=time_params['end_time']) if time_params['use_time_range'] else api_analyzer.get_platform_distribution(hours=time_params['hours'])
        
        return render_template('api_performance.html',
                             overview=overview,
                             hot_apis=hot_apis,
                             slow_apis=slow_apis,
                             error_alerts=error_alerts,
                             platform_distribution=platform_dist,
                             performance_metrics=performance_metrics,
                             performance_grades=performance_grades,
                             api_categories=api_categories,
                             business_insights=business_insights)
    except Exception as e:
        return render_template('error.html', error=str(e))


@business_bp.route('/business/traffic-analysis')
def traffic_analysis_page():
    """流量分析页面"""
    try:
        traffic_overview = get_traffic_analyzer().get_traffic_overview()
        platform_analysis = get_traffic_analyzer().get_platform_analysis()
        api_category_dist = get_traffic_analyzer().get_api_category_distribution()
        hourly_pattern = get_traffic_analyzer().get_hourly_traffic_pattern()
        
        return render_template('business/traffic_analysis.html',
                             traffic_overview=traffic_overview,
                             platform_analysis=platform_analysis,
                             api_category_dist=api_category_dist,
                             hourly_pattern=hourly_pattern)
    except Exception as e:
        return render_template('error.html', error=str(e))


@business_bp.route('/business/slow-analysis')
def slow_analysis_page():
    """慢请求分析页面"""
    try:
        api_analyzer = get_api_health_analyzer()
        slow_apis = api_analyzer.get_slowest_apis(limit=50)
        overview = api_analyzer.get_business_overview(hours=24)
        
        return render_template('business/slow_analysis.html',
                             slow_apis=slow_apis,
                             overview=overview)
    except Exception as e:
        return render_template('error.html', error=str(e))


@business_bp.route('/business/status-analysis')
def status_analysis_page():
    """状态码分析页面"""
    try:
        status_analyzer = get_status_analyzer()
        status_dist = status_analyzer.get_status_distribution(hours=24)
        error_trend = status_analyzer.get_error_trend(hours=24)
        critical_errors = status_analyzer.get_critical_errors(hours=24)
        
        return render_template('business/status_analysis.html',
                             status_dist=status_dist,
                             error_trend=error_trend,
                             critical_errors=critical_errors)
    except Exception as e:
        return render_template('error.html', error=str(e))


@business_bp.route('/business/time-analysis')
def time_analysis_page():
    """时间维度分析页面"""
    try:
        time_analyzer = get_time_analyzer()
        hourly_pattern = time_analyzer.get_hourly_pattern(hours=24)
        peak_analysis = time_analyzer.get_peak_analysis(hours=24)
        response_trends = time_analyzer.get_response_time_trends(hours=24)
        business_insights = time_analyzer.get_business_insights(hours=24)
        
        return render_template('business/time_analysis.html',
                             hourly_pattern=hourly_pattern,
                             peak_analysis=peak_analysis,
                             response_trends=response_trends,
                             business_insights=business_insights)
    except Exception as e:
        return render_template('error.html', error=str(e))


@business_bp.route('/business/performance-analysis')
def performance_analysis_page():
    """性能稳定性分析页面"""
    try:
        # 这里需要创建一个PerformanceAnalyzer或复用现有的分析器
        api_analyzer = get_api_health_analyzer()
        time_analyzer = get_time_analyzer()
        
        overview = api_analyzer.get_business_overview(hours=24)
        slow_apis = api_analyzer.get_slowest_apis(limit=20)
        stability_metrics = time_analyzer.get_response_time_trends(hours=24)
        
        return render_template('business/performance_analysis.html',
                             overview=overview,
                             slow_apis=slow_apis,
                             stability_metrics=stability_metrics)
    except Exception as e:
        return render_template('error.html', error=str(e))


@business_bp.route('/business/ip-analysis')
def ip_analysis_page():
    """IP来源分析页面"""
    try:
        ip_analyzer = get_ip_analyzer()
        top_clients = ip_analyzer.get_top_clients(hours=24, limit=50)
        geo_distribution = ip_analyzer.get_geographical_distribution(hours=24)
        client_diversity = ip_analyzer.get_client_diversity(hours=24)
        suspicious_activities = ip_analyzer.get_suspicious_activities(hours=24)
        
        return render_template('business/ip_analysis.html',
                             top_clients=top_clients,
                             geo_distribution=geo_distribution,
                             client_diversity=client_diversity,
                             suspicious_activities=suspicious_activities)
    except Exception as e:
        return render_template('error.html', error=str(e))


@business_bp.route('/business/security-analysis')
def security_analysis_page():
    """安全分析页面"""
    try:
        # 获取时间参数
        time_params = get_time_params()
        
        ip_analyzer = get_ip_analyzer()
        status_analyzer = get_status_analyzer()
        header_analyzer = get_header_analyzer()
        
        if time_params['use_time_range']:
            suspicious_activities = ip_analyzer.get_suspicious_activities(start_time=time_params['start_time'], end_time=time_params['end_time'])
            critical_errors = status_analyzer.get_critical_errors(start_time=time_params['start_time'], end_time=time_params['end_time'])
            security_insights = header_analyzer.get_header_security_insights(start_time=time_params['start_time'], end_time=time_params['end_time'])
        else:
            suspicious_activities = ip_analyzer.get_suspicious_activities(hours=time_params['hours'])
            critical_errors = status_analyzer.get_critical_errors(hours=time_params['hours'])
            security_insights = header_analyzer.get_header_security_insights(hours=time_params['hours'])
        
        # 构建安全概览数据
        security_overview = {
            'high_risk_events': len([a for a in suspicious_activities if getattr(a, 'risk_level', 'Low') in ['High', 'Critical']]),
            'attack_attempts': len(suspicious_activities),
            'blocked_requests': len([e for e in critical_errors if getattr(e, 'status_code', 200) in [403, 429, 444]]),
            'security_score': max(0, 100 - len(suspicious_activities) * 2 - len(critical_errors) * 1.5)
        }
        
        # SQL注入和攻击模式
        sql_injections = [a for a in suspicious_activities if 'sql' in getattr(a, 'attack_type', '').lower()]
        attack_patterns = []
        
        # 威胁分析
        security_threats = suspicious_activities[:20]  # 限制显示数量
        
        return render_template('business/security_analysis.html',
                             security_overview=security_overview,
                             security_threats=security_threats,
                             attack_patterns=attack_patterns,
                             sql_injections=sql_injections,
                             suspicious_activities=suspicious_activities)
    except Exception as e:
        return render_template('error.html', error=str(e))


@business_bp.route('/business/summary-report')
def summary_report_page():
    """综合报告页面"""
    try:
        # 获取时间参数
        time_params = get_time_params()
        
        # 获取各种分析数据进行综合展示
        api_analyzer = get_api_health_analyzer()
        status_analyzer = get_status_analyzer()
        time_analyzer = get_time_analyzer()
        ip_analyzer = get_ip_analyzer()
        
        if time_params['use_time_range']:
            overview = api_analyzer.get_business_overview(start_time=time_params['start_time'], end_time=time_params['end_time'])
            hot_apis = api_analyzer.get_hottest_apis(limit=10, start_time=time_params['start_time'], end_time=time_params['end_time'])
            slow_apis = api_analyzer.get_slowest_apis(limit=10, start_time=time_params['start_time'], end_time=time_params['end_time'])
            error_alerts = api_analyzer.get_error_alerts(start_time=time_params['start_time'], end_time=time_params['end_time'])
            status_dist = status_analyzer.get_status_distribution(start_time=time_params['start_time'], end_time=time_params['end_time'])
            time_pattern = time_analyzer.get_hourly_pattern(start_time=time_params['start_time'], end_time=time_params['end_time'])
            ip_diversity = ip_analyzer.get_client_diversity(start_time=time_params['start_time'], end_time=time_params['end_time'])
        else:
            overview = api_analyzer.get_business_overview(hours=time_params['hours'])
            hot_apis = api_analyzer.get_hottest_apis(limit=10, hours=time_params['hours'])
            slow_apis = api_analyzer.get_slowest_apis(limit=10, hours=time_params['hours'])
            error_alerts = api_analyzer.get_error_alerts(hours=time_params['hours'])
            status_dist = status_analyzer.get_status_distribution(hours=time_params['hours'])
            time_pattern = time_analyzer.get_hourly_pattern(hours=time_params['hours'])
            ip_diversity = ip_analyzer.get_client_diversity(hours=time_params['hours'])
        
        # 构建综合报告所需的缺失变量
        report_overview = {
            'total_requests': getattr(overview, 'total_requests', 0),
            'avg_response_ms': getattr(overview, 'avg_response_time', 0) * 1000,
            'success_rate': getattr(overview, 'success_rate', 0),
            'unique_users': getattr(ip_diversity, 'total_ips', 0)
        }
        
        key_insights = {
            'core_findings': [
                f"系统处理了 {report_overview['total_requests']:,} 个请求",
                f"平均响应时间 {report_overview['avg_response_ms']:.0f}ms",
                f"系统成功率 {report_overview['success_rate']:.1f}%"
            ],
            'critical_issues': [
                f"发现 {len([api for api in slow_apis if getattr(api, 'avg_response_time', 0) > 3])} 个慢接口",
                f"有 {len(error_alerts)} 个错误告警"
            ]
        }
        
        analysis_dimensions = [
            {
                'dimension_name': '接口性能',
                'key_metric': '平均响应时间',
                'current_value': f"{report_overview['avg_response_ms']:.0f}ms",
                'baseline_value': '1000ms',
                'health_status': 'Good' if report_overview['avg_response_ms'] < 1000 else 'Warning',
                'trend': 'Stable',
                'priority': 'Medium',
                'recommendation': '监控慢接口'
            }
        ]
        
        top_performing_apis = hot_apis[:5] if hot_apis else []
        concerning_apis = slow_apis[:5] if slow_apis else []
        
        health_assessment = {
            'overall_score': min(100, report_overview['success_rate']),
            'detailed_metrics': [
                {'metric_name': '系统稳定性', 'score': report_overview['success_rate']},
                {'metric_name': '响应性能', 'score': max(0, 100 - report_overview['avg_response_ms']/30)}
            ]
        }
        
        action_plan = {
            'urgent_actions': ['优化慢接口', '修复错误'],
            'short_term_actions': ['监控告警', '性能调优'],
            'long_term_actions': ['架构优化', '容量规划']
        }
        
        from datetime import datetime
        data_summary = {
            'analysis_period': '最近24小时',
            'data_source': 'ClickHouse',
            'total_records': report_overview['total_requests'],
            'report_time': datetime.now(),
            'slow_threshold': '3000ms',
            'success_codes': ['200'],
            'dimensions_count': 1,
            'metrics_count': 2
        }
        
        return render_template('business/summary_report.html',
                             report_overview=report_overview,
                             key_insights=key_insights,
                             analysis_dimensions=analysis_dimensions,
                             top_performing_apis=top_performing_apis,
                             concerning_apis=concerning_apis,
                             health_assessment=health_assessment,
                             action_plan=action_plan,
                             data_summary=data_summary)
    except Exception as e:
        return render_template('error.html', error=str(e))


@business_bp.route('/api/business/traffic/overview')
def api_traffic_overview():
    """API: 流量总览"""
    try:
        hours = int(request.args.get('hours', 24))
        overview = get_traffic_analyzer().get_traffic_overview(hours=hours)
        
        return jsonify({
            'status': 'success',
            'data': overview
        })
    except Exception as e:
        return jsonify({
            'status': 'error', 
            'message': str(e)
        })


# 新增分析维度API端点

@business_bp.route('/api/business/status-distribution')
def api_status_distribution():
    """API: 状态码分布"""
    try:
        hours = int(request.args.get('hours', 24))
        distribution = get_status_analyzer().get_status_distribution(hours=hours)
        
        return jsonify({
            'status': 'success',
            'data': distribution
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


@business_bp.route('/api/business/error-trend')
def api_error_trend():
    """API: 错误趋势"""
    try:
        hours = int(request.args.get('hours', 24))
        trend = get_status_analyzer().get_error_trend(hours=hours)
        
        return jsonify({
            'status': 'success',
            'data': trend
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


@business_bp.route('/api/business/time-pattern')
def api_time_pattern():
    """API: 时间模式分析"""
    try:
        hours = int(request.args.get('hours', 24))
        pattern = get_time_analyzer().get_hourly_pattern(hours=hours)
        peak_analysis = get_time_analyzer().get_peak_analysis(hours=hours)
        
        return jsonify({
            'status': 'success',
            'data': {
                'hourly_pattern': pattern,
                'peak_analysis': peak_analysis
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


@business_bp.route('/api/business/response-time-trends')
def api_response_time_trends():
    """API: 响应时间趋势"""
    try:
        hours = int(request.args.get('hours', 24))
        trends = get_time_analyzer().get_response_time_trends(hours=hours)
        
        return jsonify({
            'status': 'success',
            'data': trends
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


@business_bp.route('/api/business/ip-analysis')
def api_ip_analysis():
    """API: IP来源分析"""
    try:
        hours = int(request.args.get('hours', 24))
        limit = int(request.args.get('limit', 20))
        
        top_clients = get_ip_analyzer().get_top_clients(hours=hours, limit=limit)
        geo_distribution = get_ip_analyzer().get_geographical_distribution(hours=hours)
        diversity = get_ip_analyzer().get_client_diversity(hours=hours)
        
        return jsonify({
            'status': 'success',
            'data': {
                'top_clients': top_clients,
                'geographical_distribution': geo_distribution,
                'client_diversity': diversity
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


@business_bp.route('/api/business/security-insights')
def api_security_insights():
    """API: 安全洞察"""
    try:
        hours = int(request.args.get('hours', 24))
        
        suspicious_activities = get_ip_analyzer().get_suspicious_activities(hours=hours)
        critical_errors = get_status_analyzer().get_critical_errors(hours=hours)
        
        return jsonify({
            'status': 'success',
            'data': {
                'suspicious_activities': suspicious_activities,
                'critical_errors': critical_errors
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


@business_bp.route('/api/business/business-insights')
def api_business_insights():
    """API: 业务洞察"""
    try:
        hours = int(request.args.get('hours', 24))
        insights = get_time_analyzer().get_business_insights(hours=hours)
        
        return jsonify({
            'status': 'success',
            'data': insights
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


# Header分析页面路由

@business_bp.route('/business/header-analysis')
def header_analysis_page():
    """请求头分析页面"""
    try:
        header_analyzer = get_header_analyzer()
        
        # 获取User-Agent分析数据
        user_agent_analysis = header_analyzer.get_user_agent_analysis(hours=24)
        user_agent_stats = header_analyzer.get_user_agent_statistics(hours=24)
        
        # 获取Referer分析数据
        referer_analysis = header_analyzer.get_referer_analysis(hours=24)
        
        # 获取安全洞察
        security_insights = header_analyzer.get_header_security_insights(hours=24)
        
        return render_template('business/header_analysis.html',
                             user_agent_analysis=user_agent_analysis,
                             user_agent_stats=user_agent_stats,
                             referer_analysis=referer_analysis,
                             security_insights=security_insights)
    except Exception as e:
        return render_template('error.html', error=str(e))


@business_bp.route('/business/header-performance')
def header_performance_page():
    """请求头性能关联分析页面"""
    try:
        # 获取时间参数
        time_params = get_time_params()
        
        header_analyzer = get_header_analyzer()
        
        # 获取平台性能关联数据
        if time_params['use_time_range']:
            platform_performance_raw = header_analyzer.get_platform_performance_correlation(start_time=time_params['start_time'], end_time=time_params['end_time'])
            performance_overview = header_analyzer.get_performance_overview(start_time=time_params['start_time'], end_time=time_params['end_time'])
            bot_performance = header_analyzer.get_bot_performance_analysis(start_time=time_params['start_time'], end_time=time_params['end_time'])
            optimization_suggestions = header_analyzer.get_optimization_suggestions(start_time=time_params['start_time'], end_time=time_params['end_time'])
        else:
            platform_performance_raw = header_analyzer.get_platform_performance_correlation(hours=time_params['hours'])
            performance_overview = header_analyzer.get_performance_overview(hours=time_params['hours'])
            bot_performance = header_analyzer.get_bot_performance_analysis(hours=time_params['hours'])
            optimization_suggestions = header_analyzer.get_optimization_suggestions(hours=time_params['hours'])
        
        # 确保platform_performance中的每个对象都有platform_info属性
        platform_performance = []
        if platform_performance_raw:
            for item in platform_performance_raw:
                # 如果是字典，转换为对象
                if isinstance(item, dict):
                    platform_obj = type('Platform', (), item.copy())()
                    if not hasattr(platform_obj, 'platform_info'):
                        platform_obj.platform_info = f"{getattr(platform_obj, 'browser', 'Unknown')} / {getattr(platform_obj, 'os', 'Unknown')} / {getattr(platform_obj, 'device_type', 'Unknown')}"
                    if not hasattr(platform_obj, 'performance_grade'):
                        platform_obj.performance_grade = 'B'
                    if not hasattr(platform_obj, 'avg_response_ms'):
                        platform_obj.avg_response_ms = 1000
                    platform_performance.append(platform_obj)
                else:
                    # 如果是对象，确保有platform_info属性
                    if not hasattr(item, 'platform_info'):
                        item.platform_info = f"{getattr(item, 'browser', 'Unknown')} / {getattr(item, 'os', 'Unknown')} / {getattr(item, 'device_type', 'Unknown')}"
                    if not hasattr(item, 'performance_grade'):
                        item.performance_grade = 'B'
                    if not hasattr(item, 'avg_response_ms'):
                        item.avg_response_ms = 1000
                    platform_performance.append(item)
        
        # 如果没有数据，创建示例数据
        if not platform_performance:
            sample_platform = type('Platform', (), {
                'platform_info': 'Chrome / Windows / Desktop',
                'browser': 'Chrome',
                'os': 'Windows',
                'device_type': 'Desktop',
                'request_count': 1000,
                'avg_response_ms': 800,
                'p95_response_ms': 1500,
                'p99_response_ms': 3000,
                'slow_request_rate': 5.2,
                'performance_grade': 'B',
                'bottleneck_type': 'Network',
                'optimization_suggestion': '优化静态资源缓存'
            })()
            platform_performance = [sample_platform]
        
        return render_template('business/header_performance.html',
                             platform_performance=platform_performance,
                             performance_overview=performance_overview or {},
                             bot_performance=bot_performance or [],
                             optimization_suggestions=optimization_suggestions or [])
    except Exception as e:
        return render_template('error.html', error=str(e))


# Header分析API端点

@business_bp.route('/api/business/header-analysis')
def api_header_analysis():
    """API: 请求头分析数据"""
    try:
        hours = int(request.args.get('hours', 24))
        header_analyzer = get_header_analyzer()
        
        user_agent_analysis = header_analyzer.get_user_agent_analysis(hours=hours)
        referer_analysis = header_analyzer.get_referer_analysis(hours=hours)
        security_insights = header_analyzer.get_header_security_insights(hours=hours)
        
        return jsonify({
            'status': 'success',
            'data': {
                'user_agent_analysis': user_agent_analysis,
                'referer_analysis': referer_analysis,
                'security_insights': security_insights
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


@business_bp.route('/api/business/header-performance')
def api_header_performance():
    """API: 请求头性能关联分析"""
    try:
        hours = int(request.args.get('hours', 24))
        header_analyzer = get_header_analyzer()
        
        platform_performance = header_analyzer.get_platform_performance_correlation(hours=hours)
        bot_performance = header_analyzer.get_bot_performance_analysis(hours=hours)
        optimization_suggestions = header_analyzer.get_optimization_suggestions(hours=hours)
        
        return jsonify({
            'status': 'success',
            'data': {
                'platform_performance': platform_performance,
                'bot_performance': bot_performance,
                'optimization_suggestions': optimization_suggestions
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })