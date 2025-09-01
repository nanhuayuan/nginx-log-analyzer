# -*- coding: utf-8 -*-
"""
简单的数据流程测试
"""

import clickhouse_connect

def test_pipeline():
    """测试数据管道"""
    
    print("=== Nginx日志分析数据管道测试 ===")
    
    try:
        # 连接ClickHouse
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='analytics_user',
            password='analytics_password',
            database='nginx_analytics'
        )
        
        # 测试连接
        version = client.command("SELECT version()")
        print(f"ClickHouse版本: {version}")
        
        # 检查表数量
        tables = client.query("SHOW TABLES").result_rows
        print(f"数据库包含 {len(tables)} 个表")
        
        # 检查ODS数据
        ods_count = client.command("SELECT count() FROM ods_nginx_raw")
        print(f"ODS表记录数: {ods_count}")
        
        if ods_count > 0:
            # 状态码统计
            status_stats = client.query(
                "SELECT response_status_code, count() as cnt FROM ods_nginx_raw GROUP BY response_status_code"
            ).result_rows
            
            print("状态码分布:")
            for status, count in status_stats:
                print(f"  {status}: {count}")
                
            # API性能统计
            api_stats = client.query("""
                SELECT 
                    request_uri,
                    count() as cnt,
                    round(avg(total_request_time), 3) as avg_time
                FROM ods_nginx_raw 
                WHERE total_request_time > 0
                GROUP BY request_uri
                ORDER BY avg_time DESC
                LIMIT 5
            """).result_rows
            
            print("\nTop 5 慢接口:")
            for uri, count, avg_time in api_stats:
                uri_short = uri[:40] + "..." if len(uri) > 40 else uri
                print(f"  {uri_short} ({count}次): {avg_time}s")
        
        print("\n管道测试完成!")
        return True
        
    except Exception as e:
        print(f"测试失败: {e}")
        return False

if __name__ == "__main__":
    test_pipeline()