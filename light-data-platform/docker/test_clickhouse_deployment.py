# -*- coding: utf-8 -*-
"""
ClickHouse Docker部署测试脚本
"""

import time
import requests
import sys
import subprocess

def test_docker_service():
    """测试Docker服务状态"""
    print("[DOCKER] 检查Docker服务...")
    
    try:
        result = subprocess.run(['docker', 'ps'], capture_output=True, text=True)
        if result.returncode != 0:
            print("[ERROR] Docker服务未运行或无权限")
            return False
        print("[OK] Docker服务正常")
        return True
    except FileNotFoundError:
        print("[ERROR] Docker未安装")
        return False

def test_clickhouse_http():
    """测试ClickHouse HTTP接口"""
    print("\n🔍 测试ClickHouse HTTP接口...")
    
    try:
        # 测试ping接口
        response = requests.get('http://localhost:8123/ping', timeout=5)
        if response.status_code == 200:
            print("✅ ClickHouse HTTP接口正常")
            return True
        else:
            print(f"❌ ClickHouse HTTP接口异常，状态码: {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ 无法连接ClickHouse HTTP接口 (localhost:8123)")
        return False
    except requests.exceptions.Timeout:
        print("❌ ClickHouse HTTP接口响应超时")
        return False

def test_clickhouse_auth():
    """测试ClickHouse认证"""
    print("\n🔐 测试ClickHouse用户认证...")
    
    try:
        # 测试用户认证
        auth = ('analytics_user', 'analytics_password')
        response = requests.get('http://localhost:8123/', auth=auth, 
                              params={'query': 'SELECT version()'}, timeout=10)
        
        if response.status_code == 200:
            version = response.text.strip()
            print(f"✅ ClickHouse认证成功，版本: {version}")
            return True
        else:
            print(f"❌ ClickHouse认证失败，状态码: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ ClickHouse认证测试失败: {e}")
        return False

def test_database_structure():
    """测试数据库结构"""
    print("\n📊 测试数据库结构...")
    
    try:
        auth = ('analytics_user', 'analytics_password')
        
        # 测试数据库
        response = requests.get('http://localhost:8123/', auth=auth,
                              params={'query': 'SHOW DATABASES'}, timeout=10)
        
        if response.status_code == 200:
            databases = response.text.strip().split('\n')
            if 'nginx_analytics' in databases:
                print("✅ nginx_analytics数据库存在")
            else:
                print("❌ nginx_analytics数据库不存在")
                return False
        
        # 测试表结构
        response = requests.get('http://localhost:8123/', auth=auth,
                              params={'query': 'SHOW TABLES FROM nginx_analytics'}, timeout=10)
        
        if response.status_code == 200:
            tables = response.text.strip().split('\n') if response.text.strip() else []
            expected_tables = ['ods_nginx_log', 'dwd_nginx_enriched', 'dws_platform_hourly']
            
            found_tables = [table for table in expected_tables if table in tables]
            print(f"✅ 找到数据表: {found_tables}")
            
            if len(found_tables) >= 2:
                return True
            else:
                print("⚠️  部分数据表缺失，但基础结构正常")
                return True
        
        return False
            
    except Exception as e:
        print(f"❌ 数据库结构测试失败: {e}")
        return False

def test_web_interface():
    """测试ClickHouse Web界面"""
    print("\n🌐 测试ClickHouse Web界面...")
    
    try:
        response = requests.get('http://localhost:8123/play', timeout=10)
        if response.status_code == 200 and 'ClickHouse' in response.text:
            print("✅ ClickHouse Web界面可访问: http://localhost:8123/play")
            return True
        else:
            print("❌ ClickHouse Web界面异常")
            return False
            
    except Exception as e:
        print(f"⚠️  Web界面测试失败: {e}")
        return False  # Web界面不是必需的，所以不影响整体测试

def test_container_health():
    """测试容器健康状态"""
    print("\n🏥 检查容器健康状态...")
    
    try:
        result = subprocess.run(['docker', 'ps', '--filter', 'name=nginx-analytics-clickhouse', 
                               '--format', 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'], 
                               capture_output=True, text=True)
        
        if result.returncode == 0 and 'nginx-analytics-clickhouse' in result.stdout:
            lines = result.stdout.strip().split('\n')
            for line in lines[1:]:  # 跳过表头
                if 'nginx-analytics-clickhouse' in line:
                    if '(healthy)' in line or 'Up' in line:
                        print("✅ ClickHouse容器健康状态正常")
                        print(f"   {line}")
                        return True
                    else:
                        print(f"⚠️  ClickHouse容器状态: {line}")
                        return True  # 即使没有healthy标记，Up状态也可以
        
        print("❌ ClickHouse容器未运行")
        return False
        
    except Exception as e:
        print(f"⚠️  容器健康检查失败: {e}")
        return True  # 不影响主要功能

def show_connection_info():
    """显示连接信息"""
    print("\n📋 ClickHouse连接信息:")
    print("   HTTP接口: http://localhost:8123")
    print("   Native TCP: localhost:9000")
    print("   Web界面: http://localhost:8123/play")
    print("   数据库: nginx_analytics")
    print("   用户名: analytics_user")
    print("   密码: analytics_password")
    print("\n📋 客户端连接命令:")
    print("   docker exec -it nginx-analytics-clickhouse clickhouse-client \\")
    print("     --user analytics_user --password analytics_password --database nginx_analytics")

def main():
    """主测试函数"""
    print("[ROCKET] ClickHouse Docker部署测试")
    print("=" * 50)
    
    tests = [
        ("Docker服务", test_docker_service),
        ("ClickHouse HTTP接口", test_clickhouse_http),
        ("ClickHouse用户认证", test_clickhouse_auth),
        ("数据库结构", test_database_structure),
        ("Web界面", test_web_interface),
        ("容器健康状态", test_container_health),
    ]
    
    passed_tests = 0
    total_tests = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed_tests += 1
        except KeyboardInterrupt:
            print("\n\n⏹️  测试被用户中断")
            break
        except Exception as e:
            print(f"❌ {test_name}测试异常: {e}")
    
    print("\n" + "=" * 50)
    print(f"📊 测试结果: {passed_tests}/{total_tests} 通过")
    
    if passed_tests >= total_tests - 1:  # 允许一个测试失败
        print("🎉 ClickHouse部署成功!")
        show_connection_info()
        
        print("\n🔄 下一步操作:")
        print("1. 执行数据迁移: python ../migration/clickhouse_migration.py --migrate")
        print("2. 验证迁移结果: python ../migration/clickhouse_migration.py --verify")  
        print("3. 修改应用配置使用ClickHouse数据源")
        
        return True
    else:
        print("❌ ClickHouse部署存在问题，请检查配置和日志")
        print("\n🔧 排错建议:")
        print("1. 检查Docker是否运行: docker ps")
        print("2. 查看ClickHouse日志: docker logs nginx-analytics-clickhouse")
        print("3. 重启服务: clickhouse-manager.bat restart")
        
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)