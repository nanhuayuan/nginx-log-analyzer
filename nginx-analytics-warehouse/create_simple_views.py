#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建简化版的物化视图，避免复杂子查询
"""

import clickhouse_connect

def main():
    client = clickhouse_connect.get_client(
        host='localhost', 
        port=8123,
        username='analytics_user',
        password='analytics_password'
    )

    print('🔧 创建简化版物化视图...')

    # 1. 创建简化的 mv_status_code_hourly
    sql_status_code = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_status_code_hourly
    TO nginx_analytics.ads_status_code_analysis
    AS SELECT
        toStartOfHour(log_time) as stat_time,
        'hour' as time_granularity,
        platform,
        access_type,
        request_uri as api_path,
        api_module,
        api_category,
        business_domain,
        toString(response_status_code) as status_code,
        
        multiIf(
            response_status_code >= 200 AND response_status_code < 300, '2xx_success',
            response_status_code >= 300 AND response_status_code < 400, '3xx_redirect',
            response_status_code >= 400 AND response_status_code < 500, '4xx_client_error',
            response_status_code >= 500, '5xx_server_error',
            'unknown'
        ) as status_class,
        
        count() as request_count,
        0.0 as percentage,  -- 简化处理
        
        multiIf(
            response_status_code >= 400 AND response_status_code < 500, 'client_error',
            response_status_code >= 500, 'server_error',
            'success'
        ) as error_type,
        
        [] as common_error_apis,
        0.0 as vs_previous_period,
        false as is_anomaly,
        
        now() as created_at
    FROM nginx_analytics.dwd_nginx_enriched_v2
    WHERE log_time >= now() - INTERVAL 1 DAY
    GROUP BY 
        stat_time, platform, access_type, api_path, api_module, api_category, 
        business_domain, status_code, status_class, error_type
    """

    try:
        client.command(sql_status_code)
        print('✅ mv_status_code_hourly 创建成功')
    except Exception as e:
        print(f'❌ mv_status_code_hourly 创建失败: {str(e)[:200]}')

    # 2. 创建简化的 mv_error_analysis_hourly  
    sql_error_analysis = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_error_analysis_hourly
    TO nginx_analytics.ads_error_analysis_detailed
    AS SELECT
        toStartOfHour(log_time) as stat_time,
        'hour' as time_granularity,
        platform,
        access_type,
        request_uri as api_path,
        toString(response_status_code) as response_status_code,
        
        multiIf(
            response_status_code >= 400 AND response_status_code < 500, '4xx_client',
            response_status_code >= 500 AND response_status_code < 600, '5xx_server',
            response_status_code IN (502, 503, 504), 'gateway',
            'upstream'
        ) as error_code_group,
        
        multiIf(
            response_status_code >= 400 AND response_status_code < 500, 'client_error',
            response_status_code >= 500 AND response_status_code < 600, 'server_error',
            'unknown'
        ) as http_error_class,
        
        multiIf(
            response_status_code IN (500, 502, 503, 504), 'critical',
            response_status_code IN (401, 403, 429), 'high',
            'medium'
        ) as error_severity_level,
        
        multiIf(upstream_server != '', upstream_server, 'direct') as upstream_server,
        toString(response_status_code) as upstream_status_code,
        'application' as error_location,
        'client->gateway->direct' as error_propagation_path,
        'other' as business_operation_type,
        'active_session' as user_session_stage,
        'normal' as api_importance_level,
        'external' as client_ip_type,
        'desktop' as user_agent_category,
        'desktop_user' as user_type,
        'business_hours' as time_pattern,
        'single' as error_burst_indicator,
        
        toUInt64(count()) as error_count,
        toUInt64(0) as total_requests,
        toFloat64(100.0) as error_rate,
        toUInt64(uniq(client_ip)) as unique_error_users,
        toUInt64(uniq(trace_id)) as error_sessions,
        toFloat64(count() * 0.1) as business_loss_estimate,
        toFloat64(90.0) as user_experience_score,
        toFloat64(95.0) as sla_impact,
        toFloat64(5.0) as mean_time_to_recovery,
        toUInt32(30) as error_duration,
        toFloat64(95.0) as resolution_success_rate,
        toFloat64(50.0) as error_trend_score,
        toFloat64(30.0) as anomaly_score,
        'medium' as escalation_risk,
        
        now() as created_at
    FROM nginx_analytics.dwd_nginx_enriched_v2
    WHERE log_time >= now() - INTERVAL 1 DAY
      AND response_status_code >= 400
    GROUP BY 
        stat_time, platform, access_type, api_path, response_status_code,
        error_code_group, http_error_class, error_severity_level, upstream_server,
        upstream_status_code
    """

    try:
        client.command(sql_error_analysis)
        print('✅ mv_error_analysis_hourly 创建成功')
    except Exception as e:
        print(f'❌ mv_error_analysis_hourly 创建失败: {str(e)[:200]}')

    # 3. 创建简化的 mv_request_header_hourly
    sql_request_header = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_request_header_hourly
    TO nginx_analytics.ads_request_header_analysis
    AS SELECT
        toStartOfHour(log_time) as stat_time,
        'hour' as time_granularity,
        platform,
        browser_type as browser_name,
        platform_version as browser_version,
        os_type as os_name,
        os_version,
        device_type,
        '' as device_model,
        sdk_type,
        sdk_version,
        app_version,
        'direct' as referer_domain,
        'direct' as referer_type,
        '' as search_engine,
        
        count() as request_count,
        uniq(client_ip) as unique_users,
        0.0 as market_share,  -- 简化处理
        avg(total_request_duration) as avg_response_time,
        countIf(is_error) * 100.0 / count() as error_rate,
        countIf(is_slow) * 100.0 / count() as slow_request_rate,
        
        [] as compatibility_issues,
        [] as performance_issues,
        
        now() as created_at
    FROM nginx_analytics.dwd_nginx_enriched_v2
    WHERE log_time >= now() - INTERVAL 1 DAY
    GROUP BY 
        stat_time, platform, browser_name, browser_version, os_name, os_version,
        device_type, device_model, sdk_type, sdk_version, app_version,
        referer_domain, referer_type, search_engine
    """

    try:
        client.command(sql_request_header)
        print('✅ mv_request_header_hourly 创建成功')
    except Exception as e:
        print(f'❌ mv_request_header_hourly 创建失败: {str(e)[:200]}')

    print('\n🔍 最终验证所有物化视图:')
    views = client.query("""
        SELECT name, engine 
        FROM system.tables 
        WHERE database = 'nginx_analytics' 
        AND engine = 'MaterializedView'
        ORDER BY name
    """)

    total_views = len(views.result_rows)
    for row in views.result_rows:
        print(f'   ✅ {row[0]}: {row[1]}')

    print(f'\n📊 物化视图创建总结: {total_views}/7 个')
    
    # 触发物化视图数据填充（如果有新数据）
    print('\n🔄 检查是否有数据流入...')
    dwd_count = client.query("SELECT count() FROM nginx_analytics.dwd_nginx_enriched_v2").result_rows[0][0]
    print(f'📊 DWD层数据量: {dwd_count:,} 条')

    client.close()

if __name__ == "__main__":
    main()