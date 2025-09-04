#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Debug ClickHouse insertion - 直接测试INSERT语句
"""

import clickhouse_connect
from datetime import datetime, date

def main():
    # 连接数据库
    client = clickhouse_connect.get_client(
        host='localhost', 
        port=8123, 
        database='nginx_analytics', 
        username='analytics_user', 
        password='analytics_password'
    )
    
    print("✅ 连接成功")
    
    # 测试1: 查看表结构
    result = client.query("DESCRIBE TABLE dwd_nginx_enriched_v2")
    print(f"\n表字段数: {len(result.result_rows)}")
    
    # 找到 'date' 字段
    for i, row in enumerate(result.result_rows):
        name = row[0]
        type_def = row[1]
        if name == 'date':
            print(f"发现 'date' 字段: 位置={i+1}, 类型={type_def}")
    
    # 测试2: 尝试简单插入
    print("\n测试简单INSERT...")
    
    try:
        # 最简单的测试：只插入必需字段
        test_sql = """
        INSERT INTO dwd_nginx_enriched_v2 (
            log_time, date_partition, hour_partition, minute_partition, second_partition,
            client_ip, client_port, server_name, request_method, request_uri,
            http_protocol_version, response_status_code, response_body_size, 
            total_bytes_sent, total_request_duration, date, hour, minute, second,
            created_at, updated_at
        ) VALUES (
            '2025-04-23 00:00:02', '2025-04-23', 0, 0, 2,
            '100.100.8.44', 10305, 'test.com', 'GET', '/test',
            'HTTP/1.1', '200', 1000,
            1000, 0.1, '2025-04-23', 0, 0, 2,
            now(), now()
        )
        """
        
        client.command(test_sql)
        print("✅ 简单INSERT成功")
        
    except Exception as e:
        print(f"❌ 简单INSERT失败: {e}")
    
    # 测试3: 使用client.insert方法
    print("\n测试client.insert方法...")
    
    try:
        # 准备测试数据
        test_data = [
            [
                datetime(2025, 4, 23, 0, 0, 2), date(2025, 4, 23), 0, 0, 2,
                '100.100.8.44', 10305, '', 'test.com', 'GET',
                '/test', '', '', '',
                'HTTP/1.1', '200', 1000, 1.0,
                1000, 1.0, 0.1,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                '', '', '', '', '', '', '', '', '',
                '', '', '', '', '', '', '', '', '', '', '', '', 1, '', '', '',
                True, True, False, False, False, False, False, False, '', '', '', '', 50, 85.0, [],
                '', '', '', False,
                date(2025, 4, 23), 0, 0, 2, '202504230000', '20250423000000', 3, False, 'night',
                datetime.now(), datetime.now()
            ]
        ]
        
        # DWD字段列表（排除id, ods_id）
        dwd_fields = [
            'log_time', 'date_partition', 'hour_partition', 'minute_partition', 'second_partition',
            'client_ip', 'client_port', 'xff_ip', 'server_name', 'request_method',
            'request_uri', 'request_uri_normalized', 'request_full_uri', 'query_parameters',
            'http_protocol_version', 'response_status_code', 'response_body_size', 'response_body_size_kb',
            'total_bytes_sent', 'total_bytes_sent_kb', 'total_request_duration',
            'upstream_connect_time', 'upstream_header_time', 'upstream_response_time',
            'backend_connect_phase', 'backend_process_phase', 'backend_transfer_phase',
            'nginx_transfer_phase', 'backend_total_phase', 'network_phase', 'processing_phase',
            'transfer_phase', 'response_transfer_speed', 'total_transfer_speed', 'nginx_transfer_speed',
            'backend_efficiency', 'network_overhead', 'transfer_ratio', 'connection_cost_ratio',
            'processing_efficiency_index',
            'platform', 'platform_version', 'app_version', 'device_type', 'browser_type',
            'os_type', 'os_version', 'sdk_type', 'sdk_version', 'bot_type',
            'entry_source', 'referer_domain', 'search_engine', 'social_media',
            'api_category', 'api_module', 'api_version', 'business_domain', 'access_type',
            'client_category', 'application_name', 'service_name', 'trace_id', 'business_sign',
            'cluster_node', 'upstream_server', 'connection_requests', 'cache_status',
            'referer_url', 'user_agent_string', 'log_source_file',
            'is_success', 'is_business_success', 'is_slow', 'is_very_slow', 'is_error',
            'is_client_error', 'is_server_error', 'has_anomaly', 'anomaly_type',
            'user_experience_level', 'apdex_classification', 'api_importance', 'business_value_score',
            'data_quality_score', 'parsing_errors', 'client_region', 'client_isp', 'ip_risk_level', 'is_internal_ip',
            'date', 'hour', 'minute', 'second', 'date_hour', 'date_hour_minute',
            'weekday', 'is_weekend', 'time_period', 'created_at', 'updated_at'
        ]
        
        print(f"字段数量: {len(dwd_fields)}")
        print(f"数据数量: {len(test_data[0])}")
        
        client.insert(
            table='dwd_nginx_enriched_v2',
            data=test_data,
            column_names=dwd_fields
        )
        
        print("✅ client.insert成功")
        
    except Exception as e:
        print(f"❌ client.insert失败: {e}")
        import traceback
        traceback.print_exc()
    
    client.close()

if __name__ == "__main__":
    main()