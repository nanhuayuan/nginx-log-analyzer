#!/usr/bin/env python3
"""
Superset自动化设置脚本
用于自动配置ClickHouse数据源和导入dashboard模板
"""

import json
import os
import requests
import datetime
from typing import Dict, List, Optional

class SupersetSetup:
    def __init__(self, base_url: str = "http://localhost:8088", username: str = "admin", password: str = "admin123"):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.csrf_token = None
        self.access_token = None
        
    def login(self) -> bool:
        """登录Superset获取访问token"""
        try:
            # 方法1: 直接使用API登录获取JWT token
            api_login_url = f"{self.base_url}/api/v1/security/login"
            api_login_data = {
                'username': self.username,
                'password': self.password,
                'provider': 'db',
                'refresh': True
            }
            
            api_response = self.session.post(api_login_url, json=api_login_data)
            if api_response.status_code == 200:
                token_data = api_response.json()
                self.access_token = token_data.get('access_token')
                self.session.headers.update({
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/json'
                })
                
                # 获取CSRF token用于需要的API调用
                csrf_url = f"{self.base_url}/api/v1/security/csrf_token/"
                csrf_response = self.session.get(csrf_url)
                if csrf_response.status_code == 200:
                    csrf_data = csrf_response.json()
                    self.csrf_token = csrf_data.get('result')
                    if self.csrf_token:
                        self.session.headers.update({
                            'X-CSRFToken': self.csrf_token
                        })
                
                print("✅ Superset登录成功")
                return True
            else:
                print(f"❌ API登录失败: {api_response.text}")
                
        except Exception as e:
            print(f"❌ 登录异常: {e}")
            return False
            
        return False
    
    def create_database(self) -> Optional[str]:
        """创建ClickHouse数据库连接"""
        try:
            # 检查数据库是否已存在
            db_list_url = f"{self.base_url}/api/v1/database/"
            response = self.session.get(db_list_url)
            
            if response.status_code == 200:
                databases = response.json().get('result', [])
                for db in databases:
                    if db.get('database_name') == 'nginx_analytics_clickhouse':
                        print(f"✅ 数据库连接已存在，UUID: {db['uuid']}")
                        return db['uuid']
            
            # 创建新的数据库连接
            db_data = {
                'database_name': 'nginx_analytics_clickhouse',
                'sqlalchemy_uri': 'clickhouse+native://analytics_user:analytics_password_change_in_prod@clickhouse:9000/nginx_analytics',
                'cache_timeout': 0,
                'expose_in_sqllab': True,
                'allow_run_async': True,
                'allow_csv_upload': False,
                'allow_ctas': True,
                'allow_cvas': True,
                'allow_dml': False,
                'extra': json.dumps({
                    'metadata_params': {},
                    'engine_params': {
                        'pool_size': 20,
                        'pool_recycle': 3600,
                        'pool_pre_ping': True,
                        'connect_args': {
                            'compression': 'lz4',
                            'connect_timeout': 10,
                            'send_receive_timeout': 30
                        }
                    }
                })
            }
            
            create_url = f"{self.base_url}/api/v1/database/"
            response = self.session.post(create_url, json=db_data)
            
            if response.status_code == 201:
                db_info = response.json()
                db_uuid = db_info['result']['uuid']
                print(f"✅ 数据库连接创建成功，UUID: {db_uuid}")
                return db_uuid
            else:
                print(f"❌ 数据库创建失败: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ 数据库连接创建异常: {e}")
            return None
    
    def test_database_connection(self, db_uuid: str) -> bool:
        """测试数据库连接"""
        try:
            test_url = f"{self.base_url}/api/v1/database/{db_uuid}/test_connection/"
            response = self.session.post(test_url)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('message') == 'OK':
                    print("✅ 数据库连接测试成功")
                    return True
                else:
                    print(f"⚠️ 数据库连接测试警告: {result}")
                    return True  # 有些情况下虽然有警告但连接可用
            else:
                print(f"❌ 数据库连接测试失败: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ 数据库连接测试异常: {e}")
            return False
    
    def replace_template_variables(self, dashboard_json: Dict, db_uuid: str) -> Dict:
        """替换dashboard模板中的变量"""
        template_str = json.dumps(dashboard_json)
        
        # 替换变量
        today = datetime.date.today()
        seven_days_ago = today - datetime.timedelta(days=7)
        thirty_days_ago = today - datetime.timedelta(days=30)
        
        replacements = {
            '{{ CLICKHOUSE_DB_UUID }}': db_uuid,
            '{{ TODAY }}': today.strftime('%Y-%m-%d'),
            '{{ SEVEN_DAYS_AGO }}': seven_days_ago.strftime('%Y-%m-%d'),
            '{{ THIRTY_DAYS_AGO }}': thirty_days_ago.strftime('%Y-%m-%d'),
            '{{ CLICKHOUSE_DATASOURCE }}': f'nginx_analytics_clickhouse_{db_uuid[:8]}'
        }
        
        for placeholder, value in replacements.items():
            template_str = template_str.replace(placeholder, value)
        
        return json.loads(template_str)
    
    def import_dashboard(self, dashboard_file: str, db_uuid: str) -> bool:
        """导入dashboard配置"""
        try:
            if not os.path.exists(dashboard_file):
                print(f"❌ Dashboard文件不存在: {dashboard_file}")
                return False
            
            with open(dashboard_file, 'r', encoding='utf-8') as f:
                dashboard_json = json.load(f)
            
            # 替换模板变量
            dashboard_json = self.replace_template_variables(dashboard_json, db_uuid)
            
            # 方法1: 使用新的导入API端点
            import_url = f"{self.base_url}/api/v1/assets/import/"
            
            # 确保请求头包含CSRF token
            headers = self.session.headers.copy()
            if self.csrf_token:
                headers['X-CSRFToken'] = self.csrf_token
            
            # 使用multipart/form-data格式
            files = {
                'formData': ('dashboard.json', json.dumps(dashboard_json, ensure_ascii=False), 'application/json')
            }
            
            # 临时移除Content-Type让requests自动设置multipart
            temp_headers = {k: v for k, v in headers.items() if k.lower() != 'content-type'}
            
            response = self.session.post(import_url, files=files, headers=temp_headers)
            
            if response.status_code in [200, 201]:
                dashboard_name = os.path.basename(dashboard_file)
                print(f"✅ Dashboard导入成功: {dashboard_name}")
                return True
            elif response.status_code == 422:
                # 尝试方法2: 直接创建dashboard
                return self.create_dashboard_directly(dashboard_json)
            else:
                print(f"❌ Dashboard导入失败 {dashboard_file}: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Dashboard导入异常 {dashboard_file}: {e}")
            return False
    
    def create_dashboard_directly(self, dashboard_json: Dict) -> bool:
        """直接创建dashboard（备用方法）"""
        try:
            # 提取dashboard信息
            if 'dashboards' not in dashboard_json or not dashboard_json['dashboards']:
                print("❌ Dashboard配置格式不正确")
                return False
            
            dashboard_info = dashboard_json['dashboards'][0]
            
            # 创建dashboard
            create_url = f"{self.base_url}/api/v1/dashboard/"
            dashboard_data = {
                'dashboard_title': dashboard_info.get('dashboard_title', '未命名Dashboard'),
                'slug': dashboard_info.get('slug', ''),
                'published': dashboard_info.get('published', True),
                'json_metadata': dashboard_info.get('json_metadata', '{}'),
                'position_json': dashboard_info.get('position_json', '{}')
            }
            
            response = self.session.post(create_url, json=dashboard_data)
            
            if response.status_code in [200, 201]:
                print(f"✅ Dashboard创建成功: {dashboard_data['dashboard_title']}")
                return True
            else:
                print(f"❌ Dashboard创建失败: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Dashboard创建异常: {e}")
            return False
    
    def setup_complete_environment(self):
        """完整环境设置"""
        print("🚀 开始Superset环境设置...")
        
        # 1. 登录
        if not self.login():
            print("❌ 设置失败：无法登录Superset")
            return False
        
        # 2. 创建数据库连接
        db_uuid = self.create_database()
        if not db_uuid:
            print("❌ 设置失败：无法创建数据库连接")
            return False
        
        # 3. 测试连接
        if not self.test_database_connection(db_uuid):
            print("⚠️ 数据库连接测试失败，但继续处理...")
        
        # 4. 导入所有dashboard
        dashboard_dir = "nginx-analytics-warehouse/superset/dashboards"
        success_count = 0
        total_count = 0
        
        if os.path.exists(dashboard_dir):
            for filename in os.listdir(dashboard_dir):
                if filename.endswith('.json'):
                    total_count += 1
                    dashboard_path = os.path.join(dashboard_dir, filename)
                    if self.import_dashboard(dashboard_path, db_uuid):
                        success_count += 1
        
        print(f"\n📊 设置完成统计:")
        print(f"   - 数据库连接: ✅ 已创建 (UUID: {db_uuid})")
        print(f"   - Dashboard导入: {success_count}/{total_count} 成功")
        
        if success_count == total_count:
            print("🎉 Superset环境设置完全成功！")
            print(f"🌐 访问地址: {self.base_url}")
            print(f"👤 用户名: {self.username}")
        else:
            print("⚠️ 部分设置有问题，请检查日志")
        
        return success_count == total_count


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Superset自动化设置脚本')
    parser.add_argument('--url', default='http://localhost:8088', help='Superset URL')
    parser.add_argument('--username', default='admin', help='用户名')
    parser.add_argument('--password', default='admin123', help='密码')
    parser.add_argument('--test-only', action='store_true', help='仅测试连接')
    
    args = parser.parse_args()
    
    setup = SupersetSetup(args.url, args.username, args.password)
    
    if args.test_only:
        print("🧪 测试模式：仅验证连接...")
        if setup.login():
            print("✅ 连接测试成功")
        else:
            print("❌ 连接测试失败")
    else:
        setup.setup_complete_environment()


if __name__ == "__main__":
    main()