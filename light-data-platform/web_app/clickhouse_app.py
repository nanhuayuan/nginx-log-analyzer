# -*- coding: utf-8 -*-
"""
Web应用主入口 - ClickHouse版本
"""

import os
import sys
from pathlib import Path
from flask import Flask, render_template, jsonify, request
import clickhouse_connect

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))

from data_pipeline.clickhouse_processor import ClickHouseProcessor
from config.settings import WEB_SERVER
from business_routes import business_bp

app = Flask(__name__)

# 添加自定义Jinja2过滤器
def number_format(value):
    """数字格式化过滤器"""
    if value is None:
        return '0'
    return f"{value:,}"

app.jinja_env.filters['number_format'] = number_format

# 注册业务分析蓝图
app.register_blueprint(business_bp)

# 全局ClickHouse处理器实例
ck_processor = ClickHouseProcessor()

@app.route('/')
def index():
    """主页 - 数据概览Dashboard"""
    # 返回静态模板，数据通过AJAX API获取
    return render_template('index.html')

# 通用时间参数处理函数
def parse_time_params():
    """解析时间参数，返回datetime对象或None"""
    start_timestamp = request.args.get('start')
    end_timestamp = request.args.get('end')
    
    if start_timestamp and end_timestamp:
        try:
            from datetime import datetime, timedelta
            # 用户时间戳通常是本地时间(Beijing)，需要转换为UTC时间用于数据库查询
            # 因为ClickHouse数据以UTC字符串形式存储
            local_start = datetime.fromtimestamp(int(start_timestamp))
            local_end = datetime.fromtimestamp(int(end_timestamp))
            
            # 转换为UTC时间(Beijing时间 - 8小时)
            utc_start = local_start - timedelta(hours=8)
            utc_end = local_end - timedelta(hours=8)
            
            return utc_start, utc_end
        except (ValueError, TypeError) as e:
            print(f"时间戳转换错误: {e}")
            return None, None
    return None, None

@app.route('/api/metrics/basic')
def api_basic_metrics():
    """API: 基础指标 - 总请求数、成功率等"""
    try:
        start_time, end_time = parse_time_params()
        stats = ck_processor.get_statistics(start_time, end_time)
        
        return jsonify({
            'status': 'success', 
            'data': {
                'total_records': stats.get('total_records', 0),
                'success_rate': stats.get('success_rate', 0),
                'slow_rate': stats.get('slow_rate', 0),
                'anomaly_rate': stats.get('anomaly_rate', 0),
                'avg_response_time': stats.get('avg_response_time', 0)
            }
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/metrics/distributions')
def api_distributions():
    """API: 分布统计 - 平台、来源、API分类分布"""
    try:
        start_time, end_time = parse_time_params()
        stats = ck_processor.get_statistics(start_time, end_time)
        
        return jsonify({
            'status': 'success',
            'data': {
                'platform_distribution': stats.get('platform_distribution', {}),
                'entry_source_distribution': stats.get('entry_source_distribution', {}),
                'api_category_distribution': stats.get('api_category_distribution', {})
            }
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

# 保留原有overview接口用于向后兼容
@app.route('/api/overview')
def api_overview():
    """API: 数据概览 (兼容接口)"""
    try:
        start_time, end_time = parse_time_params()
        stats = ck_processor.get_statistics(start_time, end_time)
        
        return jsonify({
            'status': 'success',
            'data': {
                'dwd': stats
            }
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/dimensions')
def api_dimensions():
    """API: 多维度分析"""
    try:
        analysis = ck_processor.analyze_dimensions()
        return jsonify({
            'status': 'success',
            'data': analysis
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

@app.route('/analysis')
def analysis_page():
    """多维度分析页面"""
    try:
        analysis = ck_processor.analyze_dimensions()
        return render_template('analysis.html', analysis=analysis)
    except Exception as e:
        return render_template('error.html', error=str(e))

@app.route('/api/platform/<platform>')
def api_platform_detail(platform):
    """API: 特定平台详细分析"""
    try:
        if not ck_processor.client:
            if not ck_processor.connect():
                raise Exception("无法连接到ClickHouse")
        
        # 基础统计
        total = ck_processor.client.command(f"SELECT count(*) FROM dwd_nginx_enriched WHERE platform = '{platform}'")
        
        # 成功率统计
        success_count = ck_processor.client.command(f"SELECT count(*) FROM dwd_nginx_enriched WHERE platform = '{platform}' AND is_success = true")
        
        # 慢请求统计
        slow_count = ck_processor.client.command(f"SELECT count(*) FROM dwd_nginx_enriched WHERE platform = '{platform}' AND is_slow = true")
        
        # 响应时间统计
        response_time_stats = ck_processor.client.query(f"""
            SELECT 
                avg(total_request_duration) as avg,
                min(total_request_duration) as min,
                max(total_request_duration) as max
            FROM dwd_nginx_enriched 
            WHERE platform = '{platform}'
        """).first_row
        
        # API分布
        api_distribution = ck_processor.client.query(f"""
            SELECT api_category, count(*) as cnt 
            FROM dwd_nginx_enriched 
            WHERE platform = '{platform}'
            GROUP BY api_category
        """).result_rows
        
        result = {
            'platform': platform,
            'total_requests': total,
            'success_rate': round((success_count / total * 100) if total > 0 else 0, 2),
            'slow_rate': round((slow_count / total * 100) if total > 0 else 0, 2),
            'response_time': {
                'avg': round(response_time_stats[0] or 0, 3),
                'min': round(response_time_stats[1] or 0, 3),
                'max': round(response_time_stats[2] or 0, 3)
            },
            'api_distribution': dict(api_distribution)
        }
        
        return jsonify({
            'status': 'success',
            'data': result
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

@app.route('/platform/<platform>')
def platform_detail_page(platform):
    """平台详细分析页面"""
    return render_template('platform_detail.html', platform=platform)

@app.route('/api/search')
def api_search():
    """API: 搜索查询"""
    try:
        # 获取查询参数
        platform = request.args.get('platform', '')
        entry_source = request.args.get('entry_source', '')
        api_category = request.args.get('api_category', '')
        
        # 支持新的时间参数格式（start/end）和旧的格式（start_time/end_time）
        start_time = request.args.get('start', request.args.get('start_time', ''))
        end_time = request.args.get('end', request.args.get('end_time', ''))
        limit = int(request.args.get('limit', 50))
        
        if not ck_processor.client:
            if not ck_processor.connect():
                raise Exception("无法连接到ClickHouse")
        
        # 格式化时间参数，支持多种格式
        if start_time:
            # 支持ISO格式：'2025-08-29T16:00:00.000Z' -> '2025-08-29 16:00:00'
            if 'T' in start_time:
                start_time = start_time.replace('T', ' ')
                # 移除毫秒和时区信息
                if '.000Z' in start_time:
                    start_time = start_time.replace('.000Z', '')
                elif 'Z' in start_time:
                    start_time = start_time.replace('Z', '')
            if len(start_time) == 16:  # 格式: YYYY-MM-DD HH:MM
                start_time += ':00'  # 添加秒数
        
        if end_time:
            # 支持ISO格式：'2025-08-31T15:59:00.000Z' -> '2025-08-31 15:59:00'
            if 'T' in end_time:
                end_time = end_time.replace('T', ' ')
                # 移除毫秒和时区信息
                if '.000Z' in end_time:
                    end_time = end_time.replace('.000Z', '')
                elif 'Z' in end_time:
                    end_time = end_time.replace('Z', '')
            if len(end_time) == 16:  # 格式: YYYY-MM-DD HH:MM
                end_time += ':00'  # 添加秒数
        
        # 构建查询条件
        where_conditions = []
        if platform:
            where_conditions.append(f"platform = '{platform}'")
        if entry_source:
            where_conditions.append(f"entry_source = '{entry_source}'")
        if api_category:
            where_conditions.append(f"api_category = '{api_category}'")
        if start_time:
            where_conditions.append(f"log_time >= '{start_time}'")
        if end_time:
            where_conditions.append(f"log_time <= '{end_time}'")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        # 执行查询
        query = f"""
            SELECT 
                id, log_time as timestamp, platform, entry_source, api_category,
                request_uri, response_status_code, total_request_duration as response_time,
                is_success, is_slow, has_anomaly
            FROM dwd_nginx_enriched 
            WHERE {where_clause}
            ORDER BY log_time DESC
            LIMIT {limit}
        """
        
        results = ck_processor.client.query(query).result_rows
        
        # 转换为JSON格式
        data = []
        for row in results:
            data.append({
                'id': row[0],
                'timestamp': row[1].strftime('%Y-%m-%d %H:%M:%S') if row[1] else '',
                'platform': row[2],
                'entry_source': row[3],
                'api_category': row[4],
                'request_uri': row[5],
                'response_status_code': row[6],
                'response_time': row[7],
                'is_success': row[8],
                'is_slow': row[9],
                'has_anomaly': row[10]
            })
        
        return jsonify({
            'status': 'success',
            'data': data,
            'count': len(data)
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

@app.route('/search')
def search_page():
    """搜索查询页面"""
    try:
        if not ck_processor.client:
            if not ck_processor.connect():
                raise Exception("无法连接到ClickHouse")
        
        # 获取维度选项
        platforms = [row[0] for row in ck_processor.client.query("SELECT DISTINCT platform FROM dwd_nginx_enriched ORDER BY platform").result_rows]
        entry_sources = [row[0] for row in ck_processor.client.query("SELECT DISTINCT entry_source FROM dwd_nginx_enriched ORDER BY entry_source").result_rows]
        api_categories = [row[0] for row in ck_processor.client.query("SELECT DISTINCT api_category FROM dwd_nginx_enriched ORDER BY api_category").result_rows]
        
        return render_template('search.html',
                             platforms=platforms,
                             entry_sources=entry_sources,
                             api_categories=api_categories)
    except Exception as e:
        return render_template('error.html', error=str(e))

@app.route('/help')
def help_page():
    """帮助页面"""
    return render_template('help.html')

@app.errorhandler(404)
def page_not_found(e):
    return render_template('error.html', error="页面未找到"), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('error.html', error="内部服务器错误"), 500

def main():
    """启动Web服务"""
    print("启动ClickHouse数据平台Web服务...")
    print(f"访问地址: http://{WEB_SERVER['host']}:{WEB_SERVER['port'] + 1}")  # 使用不同端口避免冲突
    
    app.run(
        host=WEB_SERVER['host'],
        port=WEB_SERVER['port'] + 1,  # 5001端口
        debug=WEB_SERVER['debug']
    )

if __name__ == "__main__":
    main()