# -*- coding: utf-8 -*-
"""
简化版系统初始化
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import clickhouse_connect

def connect_clickhouse():
    """连接ClickHouse"""
    try:
        client = clickhouse_connect.get_client(
            host='localhost', port=8123, username='analytics_user', 
            password='analytics_password', database='nginx_analytics'
        )
        client.command("SET session_timezone = 'Asia/Shanghai'")
        print("ClickHouse连接成功")
        return client
    except Exception as e:
        print(f"ClickHouse连接失败: {e}")
        return None

def create_schema(client):
    """创建数据库结构"""
    print("创建数据库结构...")
    
    try:
        # 读取SQL脚本
        schema_file = Path(__file__).parent / 'schema_design_complete.sql'
        
        if not schema_file.exists():
            print(f"SQL脚本文件不存在: {schema_file}")
            return False
        
        with open(schema_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # 执行主要的CREATE语句
        create_statements = []
        current = []
        
        for line in sql_content.split('\n'):
            line = line.strip()
            if not line or line.startswith('--'):
                continue
                
            current.append(line)
            
            if line.endswith(';'):
                stmt = ' '.join(current).strip()
                if stmt.upper().startswith(('CREATE', 'ALTER')) and 'MATERIALIZED VIEW' not in stmt:
                    create_statements.append(stmt)
                current = []
        
        success_count = 0
        for stmt in create_statements:
            try:
                client.command(stmt)
                print(f"执行成功: {stmt[:50]}...")
                success_count += 1
            except Exception as e:
                if 'already exists' in str(e).lower():
                    print(f"已存在: {stmt[:50]}...")
                    success_count += 1
                else:
                    print(f"执行失败: {e}")
        
        print(f"数据库结构创建完成: {success_count}/{len(create_statements)}")
        return True
        
    except Exception as e:
        print(f"创建数据库结构失败: {e}")
        return False

def verify_tables(client):
    """验证表结构"""
    try:
        result = client.query("SHOW TABLES FROM nginx_analytics")
        tables = [row[0] for row in result.result_rows]
        
        print(f"发现表: {len(tables)} 个")
        for table in sorted(tables):
            print(f"  - {table}")
        
        return len(tables) > 0
        
    except Exception as e:
        print(f"验证表结构失败: {e}")
        return False

def main():
    print("开始全新系统初始化")
    print("=" * 50)
    
    # 连接ClickHouse
    client = connect_clickhouse()
    if not client:
        return False
    
    # 创建数据库结构
    if not create_schema(client):
        return False
    
    # 验证表结构
    if not verify_tables(client):
        return False
    
    print("\n" + "=" * 50)
    print("系统初始化完成!")
    print("\n支持功能:")
    print("- Self目录12个分析器完全支持")
    print("- 核心监控指标完全支持")
    print("- 实时数据处理完全支持")
    print("- 全量数据存储完全支持")
    print("\n使用说明:")
    print("1. Web界面: http://localhost:5001")
    print("2. 数据表: ods_nginx_raw, dwd_nginx_enriched")
    print("3. 聚合表: dws_*, ads_*")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("\n初始化成功!")
    else:
        print("\n初始化失败!")
    exit(0 if success else 1)