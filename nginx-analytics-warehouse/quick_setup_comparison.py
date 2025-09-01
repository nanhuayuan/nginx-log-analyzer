# -*- coding: utf-8 -*-
"""
快速设置Grafana vs Superset对比环境
"""

import time
import requests
import subprocess
import clickhouse_connect
import os
from pathlib import Path

def run_cmd(cmd):
    """运行命令"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, 
                                encoding='utf-8', errors='ignore')
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def wait_for_service(url, timeout=60, service_name="服务"):
    """等待服务就绪"""
    print(f"等待{service_name}启动...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f"{service_name}已就绪")
                return True
        except:
            pass
        time.sleep(2)
        print(f"继续等待{service_name}...")
    
    print(f"{service_name}启动超时")
    return False

def setup_clickhouse_data():
    """设置ClickHouse数据"""
    print("设置ClickHouse数据...")
    
    # 等待ClickHouse就绪
    max_retries = 30
    for i in range(max_retries):
        try:
            client = clickhouse_connect.get_client(
                host='localhost',
                port=8123,
                username='analytics_user',
                password='analytics_password',
                database='nginx_analytics'
            )
            client.command("SELECT 1")
            print("ClickHouse连接成功")
            break
        except Exception as e:
            if i < max_retries - 1:
                print(f"ClickHouse连接尝试 {i+1}/{max_retries}...")
                time.sleep(3)
            else:
                print(f"ClickHouse连接失败: {e}")
                return False
    
    # 导入样例数据
    log_file = r"D:\project\nginx-log-analyzer\data\demo\自研Ng2025.05.09日志-样例\enterprise_space_app_access - 副本.log"
    if os.path.exists(log_file):
        print("导入nginx日志样例数据...")
        success, stdout, stderr = run_cmd(f'cd nginx-analytics-warehouse && "D:\\soft\\Anaconda3\\python.exe" processors/nginx_log_processor.py')
        if success:
            print("数据导入成功")
            return True
        else:
            print(f"数据导入失败: {stderr}")
    
    return False

def main():
    """主函数"""
    print("=== Grafana vs Superset 对比环境快速设置 ===")
    print()
    
    # 1. 检查服务状态
    print("1. 检查当前服务状态...")
    success, stdout, stderr = run_cmd("cd nginx-analytics-warehouse && docker-compose -f docker-compose-full.yml ps")
    print(stdout)
    
    # 2. 等待ClickHouse健康
    print("2. 等待ClickHouse服务健康...")
    if wait_for_service("http://localhost:8123/ping", timeout=120, service_name="ClickHouse"):
        # 3. 设置数据
        if setup_clickhouse_data():
            print("数据设置完成")
        else:
            print("数据设置失败")
    
    # 4. 检查Grafana
    print("4. 检查Grafana状态...")
    if wait_for_service("http://localhost:3000/api/health", timeout=60, service_name="Grafana"):
        print("Grafana可访问: http://localhost:3000 (admin/admin123)")
    
    # 5. 检查Superset（可能需要很长时间）
    print("5. 检查Superset状态...")
    if wait_for_service("http://localhost:8088/health", timeout=180, service_name="Superset"):
        print("Superset可访问: http://localhost:8088 (admin/admin123)")
    else:
        print("Superset可能仍在启动中，请稍后检查 http://localhost:8088")
    
    print("\n=== 对比环境设置完成 ===")
    print()
    print("可访问地址:")
    print("• ClickHouse: http://localhost:8123/play")
    print("• Grafana: http://localhost:3000 (admin/admin123)")
    print("• Superset: http://localhost:8088 (admin/admin123)")
    print()
    print("ClickHouse连接信息:")
    print("• 主机: localhost:8123")
    print("• 数据库: nginx_analytics") 
    print("• 用户: analytics_user")
    print("• 密码: analytics_password")
    print()
    print("现在可以:")
    print("1. 在Grafana中配置ClickHouse数据源")
    print("2. 在Superset中配置ClickHouse数据源")
    print("3. 创建仪表板对比两个工具的效果")

if __name__ == "__main__":
    main()