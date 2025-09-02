#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DDL执行脚本 - 独立的数据库初始化工具
Execute DDL Scripts - Standalone Database Initialization Tool
"""

import clickhouse_connect
import sys
import os
from pathlib import Path

def execute_ddl_file(client, sql_file):
    """执行DDL文件"""
    print(f"正在执行: {sql_file.name}")
    
    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # 分割SQL语句（按分号分割，但忽略注释中的分号）
        statements = []
        current_statement = ""
        in_comment = False
        
        for line in sql_content.split('\n'):
            stripped_line = line.strip()
            
            # 跳过注释行
            if stripped_line.startswith('--') or not stripped_line:
                continue
            
            current_statement += line + '\n'
            
            # 如果行以分号结尾，认为是一个完整的语句
            if stripped_line.endswith(';'):
                statements.append(current_statement.strip())
                current_statement = ""
        
        # 执行每个语句
        success_count = 0
        for i, statement in enumerate(statements):
            if statement.strip():
                try:
                    client.command(statement)
                    success_count += 1
                    print(f"  [OK] 语句 {i+1}/{len(statements)} 执行成功")
                except Exception as e:
                    print(f"  [ERROR] 语句 {i+1}/{len(statements)} 执行失败: {e}")
                    print(f"  SQL: {statement[:100]}...")
        
        print(f"[SUCCESS] {sql_file.name}: {success_count}/{len(statements)} 语句执行成功")
        return success_count == len(statements)
        
    except Exception as e:
        print(f"[ERROR] 读取文件失败: {e}")
        return False

def main():
    """主函数"""
    print("=" * 60)
    print("   DDL执行工具 - Nginx Analytics Data Warehouse")
    print("   DDL Execution Tool")
    print("=" * 60)
    
    # 连接配置
    config = {
        'host': 'localhost',
        'port': 8123,
        'username': 'analytics_user',
        'password': 'analytics_password'
    }
    
    ddl_dir = Path(__file__).parent
    
    try:
        # 连接ClickHouse
        print("步骤1: 连接ClickHouse服务器...")
        client = clickhouse_connect.get_client(
            host=config['host'],
            port=config['port'],
            username=config['username'],
            password=config['password']
        )
        
        print("[OK] ClickHouse连接成功")
        
        # 检查参数
        if len(sys.argv) > 1:
            # 执行指定的SQL文件
            sql_file = ddl_dir / sys.argv[1]
            if sql_file.exists():
                execute_ddl_file(client, sql_file)
            else:
                print(f"[ERROR] 文件不存在: {sql_file}")
                return False
        else:
            # 执行所有DDL文件
            print("步骤2: 执行DDL脚本...")
            
            # 按顺序执行DDL文件
            ddl_files = [
                'init_tables.sql'
            ]
            
            success_count = 0
            for filename in ddl_files:
                sql_file = ddl_dir / filename
                if sql_file.exists():
                    if execute_ddl_file(client, sql_file):
                        success_count += 1
                else:
                    print(f"[WARNING] 跳过不存在的文件: {filename}")
            
            print(f"\n步骤3: 验证表结构...")
            
            # 验证表创建
            try:
                # 切换到nginx_analytics数据库
                tables = client.query("SHOW TABLES FROM nginx_analytics").result_rows
                table_names = [table[0] for table in tables]
                
                expected_tables = [
                    'ods_nginx_raw',
                    'dwd_nginx_enriched',
                    'ads_top_hot_apis',
                    'ads_platform_stats',
                    'ads_status_stats'
                ]
                
                print("已创建的表:")
                for i, table_name in enumerate(table_names, 1):
                    print(f"  {i}. {table_name}")
                
                # 检查缺失的表
                missing_tables = set(expected_tables) - set(table_names)
                if missing_tables:
                    print(f"[WARNING] 缺少表: {missing_tables}")
                else:
                    print("[OK] 所有必需的表都已创建")
                
                # 显示每个表的记录数
                print("\n表记录统计:")
                for table_name in expected_tables:
                    if table_name in table_names:
                        try:
                            count = client.command(f'SELECT count() FROM nginx_analytics.{table_name}')
                            print(f"  {table_name}: {count:,} 条记录")
                        except Exception as e:
                            print(f"  {table_name}: 查询失败 - {e}")
                
            except Exception as e:
                print(f"[ERROR] 验证失败: {e}")
        
        print("\n" + "=" * 60)
        print("[SUCCESS] DDL执行完成!")
        print("=" * 60)
        
        print("\n数据库信息:")
        print(f"  主机: {config['host']}:{config['port']}")
        print(f"  数据库: nginx_analytics") 
        print(f"  用户: {config['username']}")
        print(f"  Web界面: http://localhost:8123")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] DDL执行失败: {e}")
        print("\n请检查:")
        print("1. ClickHouse服务是否正在运行")
        print("2. 连接配置是否正确")
        print("3. 用户权限是否足够")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)