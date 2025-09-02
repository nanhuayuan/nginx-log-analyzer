#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
目录结构初始化脚本
Initialize directory structure for nginx-analytics-warehouse
"""

import os
import shutil
from pathlib import Path

def create_directory_structure():
    """创建标准目录结构"""
    
    base_dir = Path(__file__).parent
    
    # 定义目录结构
    directories = [
        # Docker相关
        'docker',
        
        # 数据目录
        'data/clickhouse/data',
        'data/clickhouse/metadata', 
        'data/clickhouse/tmp',
        'data/grafana/dashboards',
        'data/grafana/provisioning',
        'data/postgres/pgdata',
        'data/redis',
        'data/superset/static',
        
        # 日志目录
        'logs/clickhouse',
        'logs/grafana',
        'logs/postgres',
        'logs/redis', 
        'logs/superset',
        'logs/nginx-processor',
        
        # 配置目录
        'config/clickhouse',
        'config/grafana/datasources',
        'config/grafana/dashboards',
        'config/nginx',
        
        # 处理器代码目录
        'processors',
        
        # Nginx日志目录
        'nginx_logs/20250422',
        'nginx_logs/archive/2025-04',
        
        # 备份目录
        'backup/daily',
        'backup/weekly', 
        'backup/monthly',
        
        # 脚本目录
        'scripts',
        
        # 文档目录
        'docs'
    ]
    
    print("=== 创建nginx-analytics-warehouse目录结构 ===")
    
    created_count = 0
    for directory in directories:
        dir_path = base_dir / directory
        
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"[OK] 创建目录: {directory}")
            created_count += 1
        else:
            print(f"[SKIP] 目录已存在: {directory}")
    
    print(f"\n目录创建完成: 新建{created_count}个目录")
    
    return created_count > 0

def create_config_files():
    """创建基础配置文件"""
    
    base_dir = Path(__file__).parent
    
    print("\n=== 创建基础配置文件 ===")
    
    # ClickHouse用户配置
    clickhouse_users_config = """<?xml version="1.0"?>
<clickhouse>
    <users>
        <analytics_user>
            <password>analytics_password</password>
            <profile>default</profile>
            <quota>default</quota>
            <networks>
                <ip>::/0</ip>
            </networks>
        </analytics_user>
    </users>
</clickhouse>
"""
    
    clickhouse_users_path = base_dir / 'config/clickhouse/users.xml'
    if not clickhouse_users_path.exists():
        clickhouse_users_path.write_text(clickhouse_users_config, encoding='utf-8')
        print("[OK] 创建ClickHouse用户配置文件")
    
    # Grafana ClickHouse数据源配置
    grafana_datasource_config = """apiVersion: 1

datasources:
  - name: ClickHouse
    type: vertamedia-clickhouse-datasource
    access: proxy
    url: http://clickhouse:8123
    database: nginx_analytics
    basicAuth: true
    basicAuthUser: analytics_user
    secureJsonData:
      basicAuthPassword: analytics_password
    isDefault: true
    editable: true
"""
    
    grafana_datasource_path = base_dir / 'config/grafana/datasources/clickhouse.yml'
    if not grafana_datasource_path.exists():
        grafana_datasource_path.write_text(grafana_datasource_config, encoding='utf-8')
        print("[OK] 创建Grafana数据源配置文件")
    
    # Nginx日志格式建议配置
    nginx_log_config = """# 建议的Nginx日志格式配置
# 添加到nginx.conf的http块中

log_format json_analytics escape=json '{'
    '"time": "$time_iso8601",'
    '"remote_addr": "$remote_addr",'
    '"http_host": "$http_host",'
    '"request_method": "$request_method",'
    '"request_uri": "$uri",'
    '"request_args": "$args",'
    '"request_protocol": "$server_protocol",'
    '"status": "$status",'
    '"response_time": "$request_time",'
    '"upstream_time": "$upstream_response_time",'
    '"body_bytes_sent": "$body_bytes_sent",'
    '"http_referer": "$http_referer",'
    '"http_user_agent": "$http_user_agent",'
    '"http_x_forwarded_for": "$http_x_forwarded_for",'
    '"trace_id": "$http_x_trace_id",'
    '"request_id": "$request_id"'
'}';

# 在server块中使用
access_log /var/log/nginx/access.log json_analytics;

# 或者使用简化的底座格式（当前系统支持的格式）
log_format base_format '$http_host:$http_host '
                       'remote_addr:\"$remote_addr\" '
                       'time:\"$time_iso8601\" '
                       'request:\"$request\" '
                       'status:\"$status\" '
                       'response_time:\"$request_time\" '
                       'upstream_response_time:\"$upstream_response_time\" '
                       'body_bytes_sent:\"$body_bytes_sent\" '
                       'http_referer:\"$http_referer\" '
                       'http_user_agent:\"$http_user_agent\"';

access_log /var/log/nginx/access.log base_format;
"""
    
    nginx_config_path = base_dir / 'config/nginx/log-format.conf'
    if not nginx_config_path.exists():
        nginx_config_path.write_text(nginx_log_config, encoding='utf-8')
        print("[OK] 创建Nginx日志格式配置文件")
    
    # .gitignore文件
    gitignore_content = """# 数据文件
data/*/
!data/.gitkeep

# 日志文件
logs/*/
!logs/.gitkeep

# 备份文件
backup/*/
!backup/.gitkeep

# 环境配置
docker/.env

# Python
__pycache__/
*.py[cod]
*$py.class
*.egg-info/
.venv/
venv/

# 处理记录
processors/processed_logs*.json

# IDE
.idea/
.vscode/
*.swp
*.swo

# 系统文件
.DS_Store
Thumbs.db

# 临时文件
*.tmp
*.temp
*.bak
*.orig
"""
    
    gitignore_path = base_dir / '.gitignore'
    if not gitignore_path.exists():
        gitignore_path.write_text(gitignore_content, encoding='utf-8')
        print("[OK] 创建.gitignore文件")
    
    print("配置文件创建完成")

def create_placeholder_files():
    """创建占位文件以保持目录结构"""
    
    base_dir = Path(__file__).parent
    
    placeholder_dirs = [
        'data/clickhouse',
        'data/grafana', 
        'data/postgres',
        'data/redis',
        'data/superset',
        'logs/clickhouse',
        'logs/grafana',
        'logs/postgres', 
        'logs/redis',
        'logs/superset',
        'logs/nginx-processor',
        'backup/daily',
        'backup/weekly',
        'backup/monthly'
    ]
    
    print("\n=== 创建占位文件 ===")
    
    for directory in placeholder_dirs:
        gitkeep_path = base_dir / directory / '.gitkeep'
        if not gitkeep_path.exists():
            gitkeep_path.write_text('# 保持目录结构的占位文件\n', encoding='utf-8')
            print(f"[OK] 创建占位文件: {directory}/.gitkeep")

def set_permissions():
    """设置目录权限（仅在Unix系统上）"""
    
    if os.name == 'nt':  # Windows
        print("\n=== Windows系统，跳过权限设置 ===")
        return
    
    base_dir = Path(__file__).parent
    
    print("\n=== 设置目录权限 ===")
    
    # 设置基本权限
    try:
        os.chmod(base_dir / 'data', 0o755)
        os.chmod(base_dir / 'logs', 0o755) 
        os.chmod(base_dir / 'config', 0o755)
        print("[OK] 基本权限设置完成")
    except Exception as e:
        print(f"[WARNING] 权限设置失败: {e}")
        print("   请手动运行: chmod -R 755 data/ logs/ config/")

def move_existing_files():
    """移动现有文件到新的目录结构"""
    
    base_dir = Path(__file__).parent
    processors_dir = base_dir / 'processors'
    
    print("\n=== 移动现有文件 ===")
    
    # 如果processors目录不是空的，不需要移动
    if processors_dir.exists() and any(processors_dir.iterdir()):
        print("processors目录已存在文件，跳过移动")
        return
    
    # 检查是否有现有的处理器文件需要移动
    potential_files = [
        'main_simple.py',
        'nginx_processor_complete.py', 
        'init_database.py',
        'manage_volumes.py',
        'validate_processing.py',
        'show_data_flow.py',
        'processed_logs_complete.json'
    ]
    
    moved_count = 0
    for filename in potential_files:
        source_path = base_dir / filename
        if source_path.exists():
            target_path = processors_dir / filename
            shutil.move(str(source_path), str(target_path))
            print(f"[OK] 移动文件: {filename} -> processors/")
            moved_count += 1
    
    if moved_count == 0:
        print("无需移动现有文件")
    else:
        print(f"移动了{moved_count}个文件到processors目录")

def main():
    """主函数"""
    
    print("nginx-analytics-warehouse 目录结构初始化")
    print("=" * 50)
    
    # 1. 创建目录结构
    create_directory_structure()
    
    # 2. 移动现有文件
    move_existing_files()
    
    # 3. 创建配置文件
    create_config_files()
    
    # 4. 创建占位文件
    create_placeholder_files()
    
    # 5. 设置权限
    set_permissions()
    
    print("\n" + "=" * 50)
    print("[SUCCESS] 目录结构初始化完成!")
    print("\n下一步操作:")
    print("1. 复制 docker/.env.example 为 docker/.env 并修改配置")
    print("2. 进入docker目录: cd docker")
    print("3. 启动服务: docker-compose up -d")
    print("4. 初始化数据库: cd ../processors && python init_database.py")
    print("5. 开始处理日志: python main_simple.py process-all")
    
if __name__ == "__main__":
    main()