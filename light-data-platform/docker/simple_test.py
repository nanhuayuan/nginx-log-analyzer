# -*- coding: utf-8 -*-
"""
简化ClickHouse测试脚本
"""

import requests
import subprocess
import sys

def test_clickhouse():
    print("ClickHouse Docker部署测试")
    print("=" * 40)
    
    # 1. 测试Docker容器状态
    print("\n[1] 检查Docker容器...")
    try:
        result = subprocess.run(['docker', 'ps', '--filter', 'name=clickhouse'], 
                              capture_output=True, text=True)
        if 'clickhouse' in result.stdout:
            print("[OK] ClickHouse容器正在运行")
        else:
            print("[ERROR] ClickHouse容器未运行")
            return False
    except Exception as e:
        print(f"[ERROR] Docker检查失败: {e}")
        return False
    
    # 2. 测试HTTP接口
    print("\n[2] 测试HTTP接口...")
    try:
        response = requests.get('http://localhost:8123/ping', timeout=5)
        if response.status_code == 200:
            print("[OK] HTTP接口正常")
        else:
            print(f"[ERROR] HTTP接口异常: {response.status_code}")
            return False
    except Exception as e:
        print(f"[ERROR] HTTP连接失败: {e}")
        return False
    
    # 3. 测试数据库连接
    print("\n[3] 测试数据库连接...")
    try:
        result = subprocess.run(['docker', 'exec', 'nginx-analytics-clickhouse-simple',
                               'clickhouse-client', '--query', 'SELECT version()'],
                               capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            version = result.stdout.strip()
            print(f"[OK] 数据库连接成功，版本: {version}")
        else:
            print(f"[ERROR] 数据库连接失败: {result.stderr}")
            return False
    except Exception as e:
        print(f"[ERROR] 数据库测试失败: {e}")
        return False
    
    # 4. 测试数据库创建
    print("\n[4] 测试数据库...")
    try:
        result = subprocess.run(['docker', 'exec', 'nginx-analytics-clickhouse-simple',
                               'clickhouse-client', '--query', 'SHOW DATABASES'],
                               capture_output=True, text=True, timeout=10)
        if 'nginx_analytics' in result.stdout:
            print("[OK] nginx_analytics数据库存在")
        else:
            print("[INFO] 创建nginx_analytics数据库...")
            subprocess.run(['docker', 'exec', 'nginx-analytics-clickhouse-simple',
                           'clickhouse-client', '--query', 'CREATE DATABASE IF NOT EXISTS nginx_analytics'],
                           timeout=10)
            print("[OK] nginx_analytics数据库已创建")
    except Exception as e:
        print(f"[ERROR] 数据库操作失败: {e}")
        return False
    
    print("\n" + "=" * 40)
    print("[SUCCESS] ClickHouse部署测试通过!")
    print("\n连接信息:")
    print("  HTTP接口: http://localhost:8123")
    print("  Web界面: http://localhost:8123/play")
    print("  Native TCP: localhost:9000")
    print("  数据库: nginx_analytics")
    
    print("\n客户端连接:")
    print("  docker exec -it nginx-analytics-clickhouse-simple clickhouse-client")
    
    print("\n下一步:")
    print("  1. 执行数据迁移: python ../migration/clickhouse_migration.py --migrate")
    print("  2. 验证迁移结果: python ../migration/clickhouse_migration.py --verify")
    
    return True

if __name__ == "__main__":
    try:
        success = test_clickhouse()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n测试被中断")
        sys.exit(1)