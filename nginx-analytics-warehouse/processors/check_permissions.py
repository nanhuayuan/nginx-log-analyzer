#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查用户权限
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../ddl'))

from database_manager import DatabaseManager

def check_permissions():
    """检查权限"""
    manager = DatabaseManager()
    
    if not manager.connect():
        print("❌ 连接失败")
        return
    
    print("✅ 连接成功")
    
    try:
        # 检查当前用户
        result = manager.client.query("SELECT currentUser()")
        if result.result_rows:
            current_user = result.result_rows[0][0]
            print(f"👤 当前用户: {current_user}")
        
        # 检查当前数据库
        result = manager.client.query("SELECT currentDatabase()")
        if result.result_rows:
            current_db = result.result_rows[0][0]
            print(f"💾 当前数据库: {current_db}")
        
        # 检查用户权限
        try:
            result = manager.client.query("SHOW GRANTS FOR analytics_user")
            print("🔐 用户权限:")
            for row in result.result_rows:
                print(f"   {row[0]}")
        except Exception as e:
            print(f"⚠️  无法查看权限: {e}")
        
        # 尝试在默认数据库创建临时表测试
        print("\n🔄 测试创建临时表...")
        try:
            manager.client.command("CREATE TABLE temp_test (id UInt64, name String) ENGINE = Memory")
            print("✅ 创建表成功")
            
            # 测试插入
            manager.client.command("INSERT INTO temp_test VALUES (1, 'test')")
            print("✅ 插入成功")
            
            # 测试查询
            result = manager.client.query("SELECT * FROM temp_test")
            print(f"✅ 查询成功: {result.result_rows}")
            
            # 清理
            manager.client.command("DROP TABLE temp_test")
            print("✅ 清理成功")
            
        except Exception as e:
            print(f"❌ 临时表测试失败: {e}")
        
        # 测试在nginx_analytics数据库的权限
        print("\n🔄 测试nginx_analytics数据库权限...")
        try:
            import clickhouse_connect
            # 切换数据库
            temp_client = clickhouse_connect.get_client(
                host=manager.config['host'],
                port=manager.config['port'],
                username=manager.config['username'],
                password=manager.config['password'],
                database='nginx_analytics'
            )
            
            result = temp_client.query("SELECT COUNT(*) FROM ods_nginx_raw")
            print(f"✅ 查询nginx_analytics.ods_nginx_raw成功: {result.result_rows[0][0]} 条")
            
            # 尝试简单插入 - 只插入必需的字段
            temp_client.command("INSERT INTO ods_nginx_raw (id, log_time, server_name, client_ip, response_status_code, client_port, response_body_size, total_bytes_sent, connection_requests, total_request_time, upstream_connect_time, upstream_header_time, upstream_response_time) VALUES (555, now(), 'test.com', '1.1.1.1', '200', 80, 1024, 1024, 1, 0.1, 0.0, 0.0, 0.0)")
            print("✅ 插入nginx_analytics.ods_nginx_raw成功")
            
            # 验证插入
            result = temp_client.query("SELECT COUNT(*) FROM ods_nginx_raw WHERE id = 555")
            count = result.result_rows[0][0] if result.result_rows else 0
            print(f"✅ 验证插入: 找到 {count} 条记录")
            
            temp_client.close()
            
        except Exception as e:
            print(f"❌ nginx_analytics数据库测试失败: {e}")
            
    except Exception as e:
        print(f"❌ 权限检查失败: {e}")

if __name__ == "__main__":
    check_permissions()