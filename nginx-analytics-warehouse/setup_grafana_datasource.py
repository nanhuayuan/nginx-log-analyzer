# -*- coding: utf-8 -*-
"""
自动配置Grafana ClickHouse数据源
"""

import requests
import json
import time

def setup_grafana_datasource():
    """设置Grafana ClickHouse数据源"""
    
    # Grafana配置
    grafana_url = "http://localhost:3000"
    grafana_user = "admin"
    grafana_password = "admin123"
    
    # ClickHouse配置
    datasource_config = {
        "name": "ClickHouse-nginx-analytics",
        "type": "grafana-clickhouse-datasource",
        "url": "http://localhost:8123",
        "database": "nginx_analytics",
        "user": "analytics_user",
        "basicAuth": False,
        "isDefault": True,
        "jsonData": {
            "defaultDatabase": "nginx_analytics",
            "username": "analytics_user",
            "maxOpenConns": 10,
            "maxIdleConns": 10,
            "connMaxLifetime": 14400,
            "queryTimeout": 60,
            "dialTimeout": 10
        },
        "secureJsonData": {
            "password": "analytics_password"
        }
    }
    
    print("开始配置Grafana ClickHouse数据源...")
    
    # 等待Grafana就绪
    print("等待Grafana服务就绪...")
    for i in range(30):
        try:
            response = requests.get(f"{grafana_url}/api/health", timeout=5)
            if response.status_code == 200:
                print("Grafana服务已就绪")
                break
        except:
            pass
        time.sleep(2)
    else:
        print("Grafana服务未就绪，请检查服务状态")
        return False
    
    # 创建数据源
    print("创建ClickHouse数据源...")
    try:
        response = requests.post(
            f"{grafana_url}/api/datasources",
            auth=(grafana_user, grafana_password),
            headers={"Content-Type": "application/json"},
            data=json.dumps(datasource_config),
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"数据源创建成功，ID: {result.get('id', 'unknown')}")
            return True
        elif response.status_code == 409:
            print("数据源已存在，尝试更新...")
            # 获取现有数据源
            list_response = requests.get(
                f"{grafana_url}/api/datasources",
                auth=(grafana_user, grafana_password)
            )
            if list_response.status_code == 200:
                datasources = list_response.json()
                for ds in datasources:
                    if ds['name'] == datasource_config['name']:
                        # 更新数据源
                        update_response = requests.put(
                            f"{grafana_url}/api/datasources/{ds['id']}",
                            auth=(grafana_user, grafana_password),
                            headers={"Content-Type": "application/json"},
                            data=json.dumps({**datasource_config, "id": ds['id']})
                        )
                        if update_response.status_code == 200:
                            print("数据源更新成功")
                            return True
        else:
            print(f"创建数据源失败: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"配置过程中出错: {e}")
        return False

def test_datasource():
    """测试数据源连接"""
    print("\n测试数据源连接...")
    
    grafana_url = "http://localhost:3000"
    grafana_user = "admin"
    grafana_password = "admin123"
    
    try:
        # 获取数据源列表
        response = requests.get(
            f"{grafana_url}/api/datasources",
            auth=(grafana_user, grafana_password)
        )
        
        if response.status_code == 200:
            datasources = response.json()
            clickhouse_ds = None
            for ds in datasources:
                if 'clickhouse' in ds.get('type', '').lower():
                    clickhouse_ds = ds
                    break
            
            if clickhouse_ds:
                print(f"找到ClickHouse数据源: {clickhouse_ds['name']}")
                
                # 测试数据源连接
                test_response = requests.post(
                    f"{grafana_url}/api/datasources/{clickhouse_ds['id']}/resources/test",
                    auth=(grafana_user, grafana_password),
                    headers={"Content-Type": "application/json"}
                )
                
                if test_response.status_code == 200:
                    print("数据源连接测试成功")
                    return True
                else:
                    print(f"数据源连接测试失败: {test_response.status_code}")
                    print(test_response.text)
            else:
                print("未找到ClickHouse数据源")
        else:
            print(f"获取数据源列表失败: {response.status_code}")
            
    except Exception as e:
        print(f"测试过程中出错: {e}")
        
    return False

def main():
    """主函数"""
    print("=== Grafana ClickHouse 数据源自动配置 ===")
    
    if setup_grafana_datasource():
        print("数据源配置完成")
        test_datasource()
        
        print("\n配置完成！")
        print("访问 http://localhost:3000 查看Grafana")
        print("用户名: admin")
        print("密码: admin123")
        print("\n建议下一步:")
        print("1. 创建nginx性能监控仪表板")
        print("2. 设置数据刷新间隔")
        print("3. 配置告警规则")
    else:
        print("数据源配置失败")
        print("\n手动配置步骤:")
        print("1. 访问 http://localhost:3000")
        print("2. 登录 (admin/admin123)")
        print("3. Configuration -> Data Sources -> Add data source")
        print("4. 选择 ClickHouse 并使用以下配置:")
        print("   URL: http://localhost:8123")
        print("   Database: nginx_analytics")
        print("   Username: analytics_user")
        print("   Password: analytics_password")

if __name__ == "__main__":
    main()