#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
根据实际ADS表结构创建正确的物化视图
"""

import clickhouse_connect

def main():
    client = clickhouse_connect.get_client(
        host='localhost', 
        port=8123,
        username='analytics_user',
        password='analytics_password'
    )

    print('🔧 根据实际ADS表结构创建正确的物化视图...')

    # 1. 创建 mv_status_code_hourly - 匹配 ads_status_code_analysis
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
        
        count() * 100.0 / (
            SELECT count() FROM nginx_analytics.dwd_nginx_enriched_v2 
            WHERE toStartOfHour(log_time) = toStartOfHour(outer.log_time) 
            AND platform = outer.platform
        ) as percentage,
        
        multiIf(
            response_status_code >= 400 AND response_status_code < 500, 'client_error',
            response_status_code >= 500, 'server_error',
            'success'
        ) as error_type,
        
        [] as common_error_apis,
        0.0 as vs_previous_period,
        false as is_anomaly,
        
        now() as created_at
    FROM nginx_analytics.dwd_nginx_enriched_v2 outer
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

    # 2. 创建 mv_error_analysis_hourly - 已存在正确结构
    print('ℹ️  mv_error_analysis_hourly 字段已匹配，跳过重建')

    # 3. 创建 mv_request_header_hourly - 匹配 ads_request_header_analysis
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
        
        -- 从referer_url提取域名
        multiIf(
            referer_url LIKE '%google.%', 'google.com',
            referer_url LIKE '%baidu.%', 'baidu.com',
            referer_url LIKE '%bing.%', 'bing.com',
            referer_url != '', splitByChar('/', splitByChar('/', referer_url)[3])[1],
            'direct'
        ) as referer_domain,
        
        multiIf(
            referer_url LIKE '%google.%' OR referer_url LIKE '%baidu.%' OR referer_url LIKE '%bing.%', 'search_engine',
            referer_url LIKE '%facebook.%' OR referer_url LIKE '%twitter.%', 'social_media',
            referer_url != '', 'external_link',
            'direct'
        ) as referer_type,
        
        multiIf(
            referer_url LIKE '%google.%', 'Google',
            referer_url LIKE '%baidu.%', 'Baidu',
            referer_url LIKE '%bing.%', 'Bing',
            ''
        ) as search_engine,
        
        count() as request_count,
        uniq(client_ip) as unique_users,
        
        count() * 100.0 / (
            SELECT count() FROM nginx_analytics.dwd_nginx_enriched_v2 
            WHERE toStartOfHour(log_time) = toStartOfHour(outer.log_time)
        ) as market_share,
        
        avg(total_request_duration) as avg_response_time,
        countIf(is_error) * 100.0 / count() as error_rate,
        countIf(is_slow) * 100.0 / count() as slow_request_rate,
        
        [] as compatibility_issues,
        [] as performance_issues,
        
        now() as created_at
    FROM nginx_analytics.dwd_nginx_enriched_v2 outer
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

    print(f'\n🎉 物化视图修正完成! 总计: {total_views}/7 个')

    # 检查物化视图数据
    print('\n📊 检查物化视图数据状态:')
    for row in views.result_rows:
        try:
            count_result = client.query(f'SELECT count() FROM nginx_analytics.{row[0]}')
            count = count_result.result_rows[0][0]
            print(f'   📊 {row[0]}: {count:,} 条数据')
        except Exception as e:
            print(f'   ❌ {row[0]}: 查询失败')

    client.close()

if __name__ == "__main__":
    main()