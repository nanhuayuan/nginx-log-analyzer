#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ClickHouse数据库初始化脚本
Database Initialization Script for ClickHouse
使用DDL目录中的SQL文件进行初始化
"""

import clickhouse_connect
import sys
from pathlib import Path

def create_database_and_tables():
    """使用DDL文件创建数据库和所有必需的表"""
    
    # 连接配置
    config = {
        'host': 'localhost',
        'port': 8123,
        'username': 'analytics_user',
        'password': 'analytics_password'
    }
    
    print("=" * 60)
    print("   ClickHouse数据库初始化 (使用DDL文件)")
    print("   Database Initialization (Using DDL Files)")
    print("=" * 60)
    
    try:
        # 执行DDL脚本
        print("步骤1: 执行DDL脚本...")
        ddl_dir = Path(__file__).parent.parent / "ddl"
        ddl_script = ddl_dir / "execute_ddl.py"
        
        if ddl_script.exists():
            print(f"调用DDL执行器: {ddl_script}")
            # 导入并执行DDL脚本
            sys.path.append(str(ddl_dir))
            from execute_ddl import main as execute_ddl_main
            
            success = execute_ddl_main()
            if not success:
                print("❌ DDL执行失败")
                return False
        else:
            print(f"❌ DDL脚本不存在: {ddl_script}")
            print("正在回退到内置DDL...")
            
            # 回退到直接执行DDL
            client = clickhouse_connect.get_client(
                host=config['host'],
                port=config['port'],
                username=config['username'],
                password=config['password']
            )
            
            # 创建数据库
            client.command('CREATE DATABASE IF NOT EXISTS nginx_analytics')
            print("[OK] 数据库创建成功")
            
            # 使用简化的表创建（如果DDL文件不可用）
            print("⚠️ 使用简化表结构，建议使用DDL文件")
            return False
        
        print("\n" + "=" * 60)
        print("✅ 数据库初始化完成!")
        print("✅ Database initialization completed!")
        print("=" * 60)
        
        print("\n数据库连接信息:")
        print(f"  主机: {config['host']}:{config['port']}")
        print(f"  数据库: nginx_analytics")
        print(f"  用户名: {config['username']}")
        print(f"  Web界面: http://localhost:8123")
        
        return True
        
    except Exception as e:
        print(f"❌ 初始化失败: {e}")
        print("\n请检查:")
        print("1. ClickHouse服务是否正在运行")
        print("2. Docker容器是否启动")
        print("3. 网络连接是否正常")
        print("4. 用户名密码是否正确")
        print("5. DDL文件是否存在")
        return False

def verify_database_setup():
    """验证数据库设置"""
    try:
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            database='nginx_analytics',
            username='analytics_user',
            password='analytics_password'
        )
        
        print("\n验证数据库连接...")
        version = client.command('SELECT version()')
        print(f"✓ ClickHouse版本: {version}")
        
        print("\n表结构信息:")
        tables = ['ods_nginx_raw', 'dwd_nginx_enriched', 'ads_top_hot_apis']
        for table in tables:
            count = client.command(f'SELECT count() FROM {table}')
            print(f"  {table}: {count} 条记录")
        
        return True
        
    except Exception as e:
        print(f"❌ 验证失败: {e}")
        return False

def main():
    """主函数"""
    print("开始初始化ClickHouse数据库...")
    
    # 检查是否需要帮助
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help']:
        print("""
使用方法:
  python init_database.py        # 初始化数据库和表
  python init_database.py -h     # 显示帮助

功能:
  1. 创建nginx_analytics数据库
  2. 创建ODS层表 (ods_nginx_raw)
  3. 创建DWD层表 (dwd_nginx_enriched)  
  4. 创建ADS层表 (ads_top_hot_apis, ads_platform_stats, ads_status_stats)
  5. 验证表结构

前置条件:
  - ClickHouse服务运行在localhost:8123
  - 用户analytics_user/analytics_password已配置
  - Docker容器nginx-analytics-clickhouse-simple正在运行
        """)
        return
    
    # 执行初始化
    success = create_database_and_tables()
    
    if success:
        # 验证设置
        verify_database_setup()
        print("\n🎉 可以开始使用nginx日志分析系统了!")
        print("   运行: python main_simple.py process-all")
    else:
        print("\n💡 建议:")
        print("1. 先启动服务: python main_simple.py start-services")
        print("2. 等待服务完全启动后重新运行此脚本")
        sys.exit(1)

if __name__ == "__main__":
    main()