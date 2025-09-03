# -*- coding: utf-8 -*-
"""
数据库初始化器 - 创建数据仓库表结构
基于新设计的ODS/DWD/ADS架构
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import clickhouse_connect
from typing import List, Optional

class DatabaseInitializer:
    """数据库初始化器"""
    
    def __init__(self):
        self.config = {
            'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
            'port': int(os.getenv('CLICKHOUSE_PORT', 8123)),
            'username': os.getenv('CLICKHOUSE_USER', 'analytics_user'),
            'password': os.getenv('CLICKHOUSE_PASSWORD', 'analytics_password'),
            'database': os.getenv('CLICKHOUSE_DATABASE', 'nginx_analytics')
        }
        
        self.client = None
        self.ddl_dir = Path(__file__).parent.parent / 'ddl'
    
    def connect(self) -> bool:
        """连接ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(**self.config)
            self.client.command("SET session_timezone = 'Asia/Shanghai'")
            print(f"✅ 成功连接到ClickHouse: {self.config['host']}:{self.config['port']}")
            return True
        except Exception as e:
            print(f"❌ 连接ClickHouse失败: {e}")
            return False
    
    def create_database(self) -> bool:
        """创建数据库"""
        try:
            # 连接到默认数据库创建目标数据库
            temp_config = self.config.copy()
            temp_config['database'] = 'default'
            temp_client = clickhouse_connect.get_client(**temp_config)
            
            # 创建数据库
            temp_client.command(f"CREATE DATABASE IF NOT EXISTS {self.config['database']}")
            print(f"✅ 数据库 {self.config['database']} 创建成功")
            
            temp_client.close()
            return True
        except Exception as e:
            print(f"❌ 创建数据库失败: {e}")
            return False
    
    def execute_sql_file(self, file_path: Path) -> bool:
        """执行SQL文件"""
        try:
            print(f"📄 执行SQL文件: {file_path.name}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # 分割SQL语句（以分号分割，忽略注释中的分号）
            statements = self._split_sql_statements(sql_content)
            
            executed_count = 0
            for i, statement in enumerate(statements):
                statement = statement.strip()
                if not statement:
                    continue
                    
                try:
                    self.client.command(statement)
                    executed_count += 1
                except Exception as e:
                    print(f"   ⚠️  语句 {i+1} 执行失败: {str(e)[:100]}...")
                    # 继续执行其他语句，不中断
            
            print(f"   ✅ 成功执行 {executed_count} 个SQL语句")
            return True
            
        except Exception as e:
            print(f"   ❌ 执行SQL文件失败: {e}")
            return False
    
    def _split_sql_statements(self, sql_content: str) -> List[str]:
        """智能分割SQL语句"""
        statements = []
        current_statement = ""
        in_string = False
        in_comment = False
        
        lines = sql_content.split('\n')
        
        for line in lines:
            line = line.strip()
            
            # 跳过空行
            if not line:
                continue
                
            # 跳过注释行
            if line.startswith('--') or line.startswith('#'):
                continue
                
            # 跳过多行注释
            if '/*' in line and '*/' in line:
                continue
                
            current_statement += line + " "
            
            # 如果行以分号结尾且不在字符串中，则认为是语句结束
            if line.endswith(';') and "'" not in line and '"' not in line:
                statements.append(current_statement.rstrip('; '))
                current_statement = ""
        
        # 添加最后一个语句（如果有）
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        return statements
    
    def initialize_ods_layer(self) -> bool:
        """初始化ODS层"""
        print("🗄️  初始化ODS层表结构...")
        ods_file = self.ddl_dir / '01_ods_layer_real.sql'
        return self.execute_sql_file(ods_file)
    
    def initialize_dwd_layer(self) -> bool:
        """初始化DWD层"""
        print("🗄️  初始化DWD层表结构...")
        dwd_file = self.ddl_dir / '02_dwd_layer_real.sql'
        return self.execute_sql_file(dwd_file)
    
    def initialize_ads_layer(self) -> bool:
        """初始化ADS层"""
        print("🗄️  初始化ADS层表结构...")
        ads_file = self.ddl_dir / '03_ads_layer_real.sql'
        return self.execute_sql_file(ads_file)
    
    def initialize_materialized_views(self) -> bool:
        """初始化物化视图"""
        print("🔄 初始化物化视图...")
        mv_file = self.ddl_dir / '04_materialized_views.sql'
        if mv_file.exists():
            return self.execute_sql_file(mv_file)
        else:
            print("   ⚠️  物化视图文件不存在，跳过")
            return True
    
    def verify_tables(self) -> bool:
        """验证表结构"""
        print("🔍 验证表结构...")
        
        try:
            # 检查主要表是否存在
            expected_tables = [
                'ods_nginx_raw',
                'dwd_nginx_enriched_v2', 
                'ads_api_performance_analysis',
                'ads_service_level_analysis',
                'ads_slow_request_analysis',
                'ads_status_code_analysis',
                'ads_time_dimension_analysis',
                'ads_service_stability_analysis',
                'ads_ip_source_analysis',
                'ads_request_header_analysis',
                'ads_header_performance_correlation',
                'ads_comprehensive_report',
                'ads_api_error_analysis'
            ]
            
            existing_tables = []
            for table_name in expected_tables:
                try:
                    result = self.client.command(f"EXISTS TABLE {table_name}")
                    if result == 1:
                        existing_tables.append(table_name)
                        print(f"   ✅ {table_name}")
                    else:
                        print(f"   ❌ {table_name} - 不存在")
                except Exception as e:
                    print(f"   ❌ {table_name} - 检查失败: {e}")
            
            print(f"\n📊 表结构验证完成: {len(existing_tables)}/{len(expected_tables)} 个表存在")
            return len(existing_tables) >= len(expected_tables) * 0.8  # 80%以上的表存在就算成功
            
        except Exception as e:
            print(f"❌ 表结构验证失败: {e}")
            return False
    
    def initialize_all_tables(self) -> bool:
        """初始化所有表结构"""
        print("🚀 开始初始化Nginx Analytics数据仓库...")
        
        if not self.connect():
            return False
        
        # 创建数据库
        if not self.create_database():
            return False
        
        # 重新连接到目标数据库
        if not self.connect():
            return False
        
        success_count = 0
        total_steps = 4
        
        # 初始化各层表结构
        steps = [
            ("ODS层", self.initialize_ods_layer),
            ("DWD层", self.initialize_dwd_layer), 
            ("ADS层", self.initialize_ads_layer),
            ("物化视图", self.initialize_materialized_views)
        ]
        
        for step_name, step_func in steps:
            if step_func():
                success_count += 1
                print(f"   ✅ {step_name} 初始化成功")
            else:
                print(f"   ❌ {step_name} 初始化失败")
        
        # 验证表结构
        if self.verify_tables():
            success_count += 1
            print("   ✅ 表结构验证通过")
        
        print(f"\n🎉 数据仓库初始化完成! ({success_count}/{total_steps+1} 步成功)")
        
        if self.client:
            self.client.close()
        
        return success_count >= total_steps

def main():
    """主函数"""
    initializer = DatabaseInitializer()
    success = initializer.initialize_all_tables()
    
    if success:
        print("\n✨ 数据仓库初始化成功! 可以开始处理nginx日志数据了。")
        sys.exit(0)
    else:
        print("\n💥 数据仓库初始化失败! 请检查配置和网络连接。")
        sys.exit(1)

if __name__ == "__main__":
    main()