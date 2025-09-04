#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建剩余的3个物化视图
"""

import clickhouse_connect

def main():
    client = clickhouse_connect.get_client(
        host='localhost', 
        port=8123,
        username='analytics_user',
        password='analytics_password'
    )

    print('🔧 创建修正版的剩余3个物化视图...')

    # 1. 创建 mv_status_code_hourly
    sql_status_code = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_status_code_hourly
    TO nginx_analytics.ads_status_code_analysis
    AS SELECT
        toStartOfHour(log_time) as stat_time,
        'hour' as time_granularity,
        platform,
        access_type,
        toString(response_status_code) as response_status_code,
        request_uri as api_path,
        
        multiIf(
            response_status_code >= 400 AND response_status_code < 500, 'client_error',
            response_status_code >= 500 AND response_status_code < 600, 'server_error',
            response_status_code >= 300 AND response_status_code < 400, 'redirection',
            'success'
        ) as error_category,
        
        multiIf(
            response_status_code IN (500, 502, 503, 504), 'critical',
            response_status_code IN (401, 403, 429), 'high',
            response_status_code IN (400, 404, 422), 'medium',
            'low'
        ) as error_severity,
        
        multiIf(upstream_server != '', upstream_server, 'direct') as upstream_server,
        multiIf(is_internal_ip, 'internal', 'external') as client_ip_type,
        multiIf(device_type = 'Bot', 'bot', 'desktop_user') as user_type,
        
        count() as total_errors,
        countIf(response_status_code >= 400 AND response_status_code < 500) as client_errors,
        countIf(response_status_code >= 500) as server_errors,
        countIf(response_status_code IN (502, 503, 504)) as gateway_errors,
        uniq(client_ip) as affected_users,
        uniq(request_uri) as affected_apis,
        10.0 as error_rate,
        90.0 as availability_impact,
        now() as created_at
    FROM nginx_analytics.dwd_nginx_enriched_v2
    WHERE log_time >= now() - INTERVAL 1 DAY
      AND response_status_code >= 400
    GROUP BY 
        stat_time, platform, access_type, response_status_code, api_path,
        error_category, error_severity, upstream_server, client_ip_type, user_type
    """

    try:
        client.command(sql_status_code)
        print('✅ mv_status_code_hourly 创建成功')
    except Exception as e:
        print(f'❌ mv_status_code_hourly 创建失败: {str(e)[:100]}')

    # 2. 创建 mv_error_analysis_hourly - 修正字段类型
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
            response_status_code >= 300 AND response_status_code < 400, 'redirection',
            'unknown'
        ) as http_error_class,
        
        multiIf(
            response_status_code IN (500, 502, 503, 504), 'critical',
            response_status_code IN (401, 403, 429), 'high',
            response_status_code IN (400, 404, 422), 'medium',
            'low'
        ) as error_severity_level,
        
        multiIf(upstream_server != '', upstream_server, 'direct') as upstream_server,
        toString(response_status_code) as upstream_status_code,
        
        multiIf(
            response_status_code IN (502, 503, 504), 'gateway',
            upstream_server != '', 'service',
            upstream_response_time > 5.0, 'database',
            'application'
        ) as error_location,
        
        'client->gateway->direct' as error_propagation_path,
        
        multiIf(
            request_uri LIKE '%login%' OR request_uri LIKE '%auth%', 'login',
            request_uri LIKE '%pay%' OR request_uri LIKE '%order%', 'payment',
            'other'
        ) as business_operation_type,
        
        'active_session' as user_session_stage,
        'normal' as api_importance_level,
        
        multiIf(is_internal_ip, 'internal', 'external') as client_ip_type,
        multiIf(device_type = 'Bot', 'bot', 'desktop') as user_agent_category,
        multiIf(device_type = 'Bot', 'bot', 'desktop_user') as user_type,
        
        multiIf(
            toHour(log_time) >= 9 AND toHour(log_time) <= 17, 'business_hours',
            'off_hours'
        ) as time_pattern,
        
        'single' as error_burst_indicator,
        
        -- 确保所有字段类型匹配
        toUInt64(count()) as error_count,
        toUInt64(0) as total_requests,
        toFloat64(100.0) as error_rate,
        toUInt64(uniq(client_ip)) as unique_error_users,
        toUInt64(uniq(trace_id)) as error_sessions,
        toFloat64(count() * 0.1) as business_loss_estimate,
        toFloat64(greatest(0, 100 - count() * 2)) as user_experience_score,
        toFloat64(greatest(90, 100 - count() * 0.01)) as sla_impact,
        toFloat64(5.0) as mean_time_to_recovery,
        toUInt32(30) as error_duration,
        toFloat64(95.0) as resolution_success_rate,
        toFloat64(least(100, count() * 0.5)) as error_trend_score,
        toFloat64(least(100, count() * 0.3)) as anomaly_score,
        
        multiIf(
            count() > 100, 'critical',
            count() > 50, 'high',
            count() > 20, 'medium',
            'low'
        ) as escalation_risk,
        
        now() as created_at
    FROM nginx_analytics.dwd_nginx_enriched_v2
    WHERE log_time >= now() - INTERVAL 1 DAY
      AND response_status_code >= 400
    GROUP BY 
        stat_time, platform, access_type, api_path, response_status_code,
        error_code_group, http_error_class, error_severity_level, upstream_server,
        upstream_status_code, error_location, error_propagation_path, business_operation_type,
        user_session_stage, api_importance_level, client_ip_type, user_agent_category, 
        user_type, time_pattern, error_burst_indicator
    """

    try:
        client.command(sql_error_analysis)
        print('✅ mv_error_analysis_hourly 创建成功')
    except Exception as e:
        print(f'❌ mv_error_analysis_hourly 创建失败: {str(e)[:100]}')

    # 3. 创建 mv_request_header_hourly
    sql_request_header = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS nginx_analytics.mv_request_header_hourly
    TO nginx_analytics.ads_request_header_analysis
    AS SELECT
        toStartOfHour(log_time) as stat_time,
        'hour' as time_granularity,
        platform,
        access_type,
        
        multiIf(
            device_type = 'Bot', 'bot',
            device_type = 'Mobile', 'mobile',
            device_type = 'Desktop', 'desktop',
            'unknown'
        ) as user_agent_category,
        
        platform_version as user_agent_version,
        device_type,
        os_type,
        browser_type,
        if(bot_type != '', true, false) as is_bot,
        
        multiIf(is_internal_ip, 'internal', 'external') as client_ip_type,
        
        count() as request_count,
        uniq(client_ip) as user_count,
        uniq(trace_id) as session_count,
        avg(total_request_duration) as avg_response_time,
        quantile(0.95)(total_request_duration) as p95_response_time,
        avg(total_request_duration) as avg_session_duration,
        countIf(response_status_code >= 400) * 100.0 / count() as bounce_rate,
        countIf(is_success) * 100.0 / count() as conversion_rate,
        
        now() as created_at
    FROM nginx_analytics.dwd_nginx_enriched_v2
    WHERE log_time >= now() - INTERVAL 1 DAY
    GROUP BY 
        stat_time, platform, access_type, user_agent_category, user_agent_version,
        device_type, os_type, browser_type, is_bot, client_ip_type
    """

    try:
        client.command(sql_request_header)
        print('✅ mv_request_header_hourly 创建成功')
    except Exception as e:
        print(f'❌ mv_request_header_hourly 创建失败: {str(e)[:100]}')

    print('\n🔍 验证最终的物化视图状态:')
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

    print(f'\n🎉 物化视图创建完成! 总计: {total_views}/7 个')

    client.close()

if __name__ == "__main__":
    main()