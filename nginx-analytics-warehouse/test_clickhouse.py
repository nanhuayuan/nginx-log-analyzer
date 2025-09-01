# -*- coding: utf-8 -*-
"""
测试ClickHouse连接和表创建
"""

import os
import sys
from pathlib import Path
import clickhouse_connect

# 添加项目路径
sys.path.append(str(Path(__file__).parent))

def test_clickhouse_connection():
    """测试ClickHouse连接"""
    print("测试ClickHouse连接...")
    
    config = {
        'host': 'localhost',
        'port': 8123,
        'username': 'default',  # 使用默认用户
        'password': '',
        'database': 'nginx_analytics'
    }
    
    try:
        # 先连接到默认数据库
        temp_config = config.copy()
        temp_config['database'] = 'default'
        
        print(f"尝试连接到 {config['host']}:{config['port']}...")
        client = clickhouse_connect.get_client(**temp_config)
        
        # 测试连接
        result = client.command("SELECT version()")
        print(f"ClickHouse版本: {result}")
        
        # 创建数据库
        client.command(f"CREATE DATABASE IF NOT EXISTS {config['database']}")
        print(f"数据库 {config['database']} 创建成功")
        
        # 切换到目标数据库
        client.close()
        client = clickhouse_connect.get_client(**config)
        client.command("SET session_timezone = 'Asia/Shanghai'")
        
        # 测试简单表创建
        test_table_sql = """
        CREATE TABLE IF NOT EXISTS test_connection (
            id UInt64,
            message String,
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY id
        """
        
        client.command(test_table_sql)
        print("测试表创建成功")
        
        # 插入测试数据
        client.command("INSERT INTO test_connection (id, message) VALUES (1, 'Hello ClickHouse!')")
        
        # 查询测试数据
        result = client.query("SELECT * FROM test_connection").result_rows
        print(f"测试数据查询成功: {result}")
        
        # 清理测试表
        client.command("DROP TABLE test_connection")
        print("测试表清理完成")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"ClickHouse连接失败: {e}")
        return False

def main():
    """主函数"""
    print("ClickHouse连接测试")
    print("=" * 50)
    
    if test_clickhouse_connection():
        print("\nClickHouse连接测试成功!")
        print("接下来可以:")
        print("  1. 运行数据库初始化脚本")
        print("  2. 开始处理nginx日志数据")
    else:
        print("\nClickHouse连接测试失败!")
        print("请检查:")
        print("  1. ClickHouse服务是否启动")
        print("  2. 端口8123是否可访问")
        print("  3. 用户名密码是否正确")

if __name__ == "__main__":
    main()