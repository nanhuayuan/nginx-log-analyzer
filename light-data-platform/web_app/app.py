# -*- coding: utf-8 -*-
"""
Web应用主入口 - 轻量级数据平台查询界面
"""

import os
import sys
from pathlib import Path
from flask import Flask, render_template, jsonify, request
import json

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))

from data_pipeline.ods_processor import OdsProcessor
from data_pipeline.dwd_processor import DwdProcessor
from config.settings import WEB_SERVER

app = Flask(__name__)

# 全局处理器实例
ods_processor = OdsProcessor()
dwd_processor = DwdProcessor()

@app.route('/')
def index():
    """主页 - 数据概览Dashboard"""
    try:
        # 获取基础统计
        ods_stats = ods_processor.get_ods_statistics()
        dwd_stats = dwd_processor.get_dwd_statistics()
        
        return render_template('index.html', 
                             ods_stats=ods_stats,
                             dwd_stats=dwd_stats)
    except Exception as e:
        return render_template('error.html', error=str(e))

@app.route('/api/overview')
def api_overview():
    """API: 数据概览"""
    try:
        ods_stats = ods_processor.get_ods_statistics()
        dwd_stats = dwd_processor.get_dwd_statistics()
        
        return jsonify({
            'status': 'success',
            'data': {
                'ods': ods_stats,
                'dwd': dwd_stats
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

@app.route('/api/dimensions')
def api_dimensions():
    """API: 多维度分析"""
    try:
        analysis = dwd_processor.analyze_dimensions()
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
        analysis = dwd_processor.analyze_dimensions()
        return render_template('analysis.html', analysis=analysis)
    except Exception as e:
        return render_template('error.html', error=str(e))

@app.route('/api/platform/<platform>')
def api_platform_detail(platform):
    """API: 特定平台详细分析"""
    try:
        from database.models import DwdNginxEnriched, get_session
        from sqlalchemy import func
        
        session = get_session()
        
        # 基础统计
        total = session.query(DwdNginxEnriched).filter(
            DwdNginxEnriched.platform == platform
        ).count()
        
        # 成功率统计
        success_count = session.query(DwdNginxEnriched).filter(
            DwdNginxEnriched.platform == platform,
            DwdNginxEnriched.is_success == True
        ).count()
        
        # 慢请求统计
        slow_count = session.query(DwdNginxEnriched).filter(
            DwdNginxEnriched.platform == platform,
            DwdNginxEnriched.is_slow == True
        ).count()
        
        # 响应时间统计
        response_time_stats = session.query(
            func.avg(DwdNginxEnriched.response_time).label('avg'),
            func.min(DwdNginxEnriched.response_time).label('min'),
            func.max(DwdNginxEnriched.response_time).label('max')
        ).filter(DwdNginxEnriched.platform == platform).first()
        
        # API分布
        api_distribution = session.query(
            DwdNginxEnriched.api_category,
            func.count(DwdNginxEnriched.id).label('count')
        ).filter(
            DwdNginxEnriched.platform == platform
        ).group_by(DwdNginxEnriched.api_category).all()
        
        session.close()
        
        result = {
            'platform': platform,
            'total_requests': total,
            'success_rate': round((success_count / total * 100) if total > 0 else 0, 2),
            'slow_rate': round((slow_count / total * 100) if total > 0 else 0, 2),
            'response_time': {
                'avg': round(response_time_stats.avg or 0, 3),
                'min': round(response_time_stats.min or 0, 3),
                'max': round(response_time_stats.max or 0, 3)
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
        start_time = request.args.get('start_time', '')
        end_time = request.args.get('end_time', '')
        limit = int(request.args.get('limit', 50))
        
        from database.models import DwdNginxEnriched, get_session
        
        session = get_session()
        query = session.query(DwdNginxEnriched)
        
        # 应用过滤条件
        if platform:
            query = query.filter(DwdNginxEnriched.platform == platform)
        if entry_source:
            query = query.filter(DwdNginxEnriched.entry_source == entry_source)
        if api_category:
            query = query.filter(DwdNginxEnriched.api_category == api_category)
        if start_time:
            query = query.filter(DwdNginxEnriched.timestamp >= start_time)
        if end_time:
            query = query.filter(DwdNginxEnriched.timestamp <= end_time)
        
        # 执行查询
        results = query.order_by(DwdNginxEnriched.timestamp.desc()).limit(limit).all()
        session.close()
        
        # 转换为JSON格式
        data = []
        for record in results:
            data.append({
                'id': record.id,
                'timestamp': record.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'platform': record.platform,
                'entry_source': record.entry_source,
                'api_category': record.api_category,
                'request_uri': record.request_uri,
                'response_status_code': record.response_status_code,
                'response_time': record.response_time,
                'is_success': record.is_success,
                'is_slow': record.is_slow,
                'has_anomaly': record.has_anomaly
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
        # 获取维度选项
        from database.models import DwdNginxEnriched, get_session
        
        session = get_session()
        
        platforms = [row[0] for row in session.query(DwdNginxEnriched.platform).distinct().all()]
        entry_sources = [row[0] for row in session.query(DwdNginxEnriched.entry_source).distinct().all()]
        api_categories = [row[0] for row in session.query(DwdNginxEnriched.api_category).distinct().all()]
        
        session.close()
        
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
    print("启动轻量级数据平台Web服务...")
    print(f"访问地址: http://{WEB_SERVER['host']}:{WEB_SERVER['port']}")
    
    app.run(
        host=WEB_SERVER['host'],
        port=WEB_SERVER['port'],
        debug=WEB_SERVER['debug']
    )

if __name__ == "__main__":
    main()