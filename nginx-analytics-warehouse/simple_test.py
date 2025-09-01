# -*- coding: utf-8 -*-
"""
简单的ClickHouse测试
"""

import subprocess
import sys

def run_cmd(cmd):
    """运行命令"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8', errors='ignore')
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def main():
    print("ClickHouse简单测试")
    print("=" * 30)
    
    # 检查容器状态
    success, stdout, stderr = run_cmd('docker ps | findstr clickhouse')
    if success and 'clickhouse' in stdout:
        print("容器运行正常")
    else:
        print("容器未运行")
        return
    
    # 创建数据库
    print("\n创建数据库...")
    cmd = 'docker exec nginx-analytics-clickhouse-simple clickhouse-client -q "CREATE DATABASE IF NOT EXISTS nginx_analytics"'
    success, stdout, stderr = run_cmd(cmd)
    if success:
        print("数据库创建成功")
    else:
        print(f"数据库创建失败: {stderr}")
        return
    
    # 创建测试表
    print("\n创建测试表...")
    table_sql = """
    CREATE TABLE IF NOT EXISTS nginx_analytics.test_logs (
        id UInt64,
        log_time DateTime,
        api_path String,
        response_time Float64,
        status_code UInt16
    ) ENGINE = MergeTree()
    ORDER BY (log_time, id)
    """
    
    # 将SQL写入临时文件
    with open('temp_table.sql', 'w', encoding='utf-8') as f:
        f.write(table_sql)
    
    cmd = 'docker exec -i nginx-analytics-clickhouse-simple clickhouse-client -d nginx_analytics < temp_table.sql'
    success, stdout, stderr = run_cmd(cmd)
    if success:
        print("测试表创建成功")
    else:
        print(f"测试表创建失败: {stderr}")
    
    # 插入测试数据
    print("\n插入测试数据...")
    insert_sql = "INSERT INTO test_logs (id, log_time, api_path, response_time, status_code) VALUES (1, now(), '/api/test', 0.123, 200)"
    cmd = f'docker exec nginx-analytics-clickhouse-simple clickhouse-client -d nginx_analytics -q "{insert_sql}"'
    success, stdout, stderr = run_cmd(cmd)
    if success:
        print("测试数据插入成功")
    else:
        print(f"测试数据插入失败: {stderr}")
    
    # 查询测试数据
    print("\n查询测试数据...")
    cmd = 'docker exec nginx-analytics-clickhouse-simple clickhouse-client -d nginx_analytics -q "SELECT * FROM test_logs"'
    success, stdout, stderr = run_cmd(cmd)
    if success:
        print(f"查询结果: {stdout.strip()}")
    else:
        print(f"查询失败: {stderr}")
    
    # 清理
    import os
    if os.path.exists('temp_table.sql'):
        os.remove('temp_table.sql')
    
    print("\n测试完成!")

if __name__ == "__main__":
    main()