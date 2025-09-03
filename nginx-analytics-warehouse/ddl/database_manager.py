#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ClickHouse数据库管理工具 - 增强版
Enhanced ClickHouse Database Management Tool
"""

import os
import sys
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import clickhouse_connect
from dataclasses import dataclass


@dataclass
class TableInfo:
    """表信息"""
    name: str
    exists: bool
    record_count: int = 0
    create_time: Optional[str] = None


class DatabaseManager:
    """增强的数据库管理器"""
    
    def __init__(self, config_file: Optional[str] = None):
        """初始化管理器"""
        self.config = self._load_config(config_file)
        self.client = None
        self.ddl_dir = Path(__file__).parent
        self.database = self.config['database']
        
    def _load_config(self, config_file: Optional[str]) -> Dict:
        """加载配置"""
        default_config = {
            'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
            'port': int(os.getenv('CLICKHOUSE_PORT', 8123)),
            'username': os.getenv('CLICKHOUSE_USER', 'analytics_user'), 
            'password': os.getenv('CLICKHOUSE_PASSWORD', 'analytics_password'),
            'database': os.getenv('CLICKHOUSE_DATABASE', 'nginx_analytics')
        }
        
        # 如果有配置文件，可以在这里加载
        if config_file and Path(config_file).exists():
            # 实现配置文件加载逻辑
            pass
            
        return default_config
    
    def connect(self) -> bool:
        """连接ClickHouse"""
        try:
            # 先连接默认数据库测试连接
            temp_config = self.config.copy()
            temp_config['database'] = 'default'
            
            self.client = clickhouse_connect.get_client(**temp_config)
            self.client.command("SET session_timezone = 'Asia/Shanghai'")
            print(f"✅ 成功连接到ClickHouse: {self.config['host']}:{self.config['port']}")
            return True
        except Exception as e:
            print(f"❌ 连接ClickHouse失败: {e}")
            return False
    
    def discover_ddl_files(self) -> List[Path]:
        """自动发现DDL文件"""
        ddl_files = []
        patterns = ['*.sql']
        
        # 排除的文件列表
        exclude_files = {
            'database_manager.py', 
            'test_syntax.py', 
            'test_single_sql.py',
            'fix_comments.py',
            'remove_comments.py',
            'simple_test.sql',
            '04_materialized_views.sql',  # 暂时禁用物化视图
            'init_tables.sql'  # 禁用旧的初始化文件
        }
        
        for pattern in patterns:
            files = sorted(self.ddl_dir.glob(pattern))
            ddl_files.extend([f for f in files if f.name not in exclude_files])
            
        # 按文件名排序，确保执行顺序
        return sorted(ddl_files)
    
    def extract_table_names(self, sql_content: str) -> List[str]:
        """从SQL内容中提取表名"""
        table_names = []
        
        # 匹配CREATE TABLE语句
        create_patterns = [
            r'CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(?:nginx_analytics\.)?(\w+)',
            r'CREATE\s+TABLE\s+(?:nginx_analytics\.)?(\w+)',
            r'CREATE\s+MATERIALIZED\s+VIEW\s+IF\s+NOT\s+EXISTS\s+(?:nginx_analytics\.)?(\w+)',
            r'CREATE\s+VIEW\s+IF\s+NOT\s+EXISTS\s+(?:nginx_analytics\.)?(\w+)'
        ]
        
        for pattern in create_patterns:
            matches = re.findall(pattern, sql_content, re.IGNORECASE)
            table_names.extend(matches)
        
        return list(set(table_names))  # 去重
    
    def get_database_info(self) -> Dict:
        """获取数据库信息"""
        try:
            # 检查数据库是否存在
            databases = self.client.query(f"SHOW DATABASES").result_rows
            db_exists = any(db[0] == self.database for db in databases)
            
            if not db_exists:
                return {
                    'exists': False,
                    'tables': [],
                    'total_tables': 0
                }
            
            # 获取表列表
            tables = self.client.query(f"SHOW TABLES FROM {self.database}").result_rows
            table_names = [table[0] for table in tables]
            
            # 获取表详细信息
            table_infos = []
            for table_name in table_names:
                try:
                    count = self.client.command(f'SELECT count() FROM {self.database}.{table_name}')
                    table_infos.append(TableInfo(
                        name=table_name,
                        exists=True,
                        record_count=count
                    ))
                except Exception:
                    table_infos.append(TableInfo(
                        name=table_name,
                        exists=True,
                        record_count=0
                    ))
            
            return {
                'exists': True,
                'tables': table_infos,
                'total_tables': len(table_infos)
            }
            
        except Exception as e:
            print(f"❌ 获取数据库信息失败: {e}")
            return {'exists': False, 'tables': [], 'total_tables': 0}
    
    def create_database(self) -> bool:
        """创建数据库"""
        try:
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            print(f"✅ 数据库 {self.database} 创建成功")
            return True
        except Exception as e:
            print(f"❌ 创建数据库失败: {e}")
            return False
    
    def drop_database(self) -> bool:
        """删除数据库（强制重建时使用）"""
        try:
            # 确认操作
            print(f"⚠️  即将删除数据库 {self.database} 及其所有数据!")
            confirm = input("输入 'YES' 确认删除: ")
            if confirm != 'YES':
                print("❌ 操作已取消")
                return False
                
            self.client.command(f"DROP DATABASE IF EXISTS {self.database}")
            print(f"✅ 数据库 {self.database} 已删除")
            return True
        except Exception as e:
            print(f"❌ 删除数据库失败: {e}")
            return False
    
    def execute_sql_file(self, file_path: Path) -> Tuple[bool, int, int]:
        """执行SQL文件，返回(成功标志, 成功数, 总数)"""
        try:
            print(f"📄 执行SQL文件: {file_path.name}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # 智能分割SQL语句
            statements = self._split_sql_statements(sql_content)
            
            success_count = 0
            total_count = len([s for s in statements if s.strip()])
            
            for i, statement in enumerate(statements):
                statement = statement.strip()
                if not statement:
                    continue
                    
                try:
                    self.client.command(statement)
                    success_count += 1
                    print(f"   ✅ 语句 {success_count}/{total_count} 执行成功")
                except Exception as e:
                    print(f"   ❌ 语句 {i+1} 执行失败: {str(e)[:100]}...")
                    
            print(f"   📊 文件执行完成: {success_count}/{total_count} 成功")
            return success_count == total_count, success_count, total_count
            
        except Exception as e:
            print(f"   ❌ 执行SQL文件失败: {e}")
            return False, 0, 0
    
    def _split_sql_statements(self, sql_content: str) -> List[str]:
        """智能分割SQL语句"""
        statements = []
        current_statement = ""
        in_string = False
        in_comment = False
        quote_char = None
        
        # 预处理：移除注释行
        lines = []
        for line in sql_content.split('\n'):
            stripped = line.strip()
            # 跳过注释行和空行
            if not stripped or stripped.startswith('--') or stripped.startswith('#'):
                continue
            # 移除行内注释（简单处理）
            if '--' in line and "'" not in line.split('--')[0] and '"' not in line.split('--')[0]:
                line = line.split('--')[0].strip()
                if not line:
                    continue
            lines.append(line)
        
        # 重新组装成单个字符串
        cleaned_content = '\n'.join(lines)
        
        # 按分号分割，但要考虑字符串中的分号
        i = 0
        while i < len(cleaned_content):
            char = cleaned_content[i]
            
            if not in_string:
                if char in ("'", '"'):
                    in_string = True
                    quote_char = char
                elif char == ';':
                    # 语句结束
                    stmt = current_statement.strip()
                    if stmt:
                        statements.append(stmt)
                    current_statement = ""
                    i += 1
                    continue
                    
            else:  # in_string
                if char == quote_char:
                    # 检查是否是转义的引号
                    if i > 0 and cleaned_content[i-1] != '\\':
                        in_string = False
                        quote_char = None
            
            current_statement += char
            i += 1
        
        # 处理最后一个语句
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        # 过滤空语句
        return [stmt for stmt in statements if stmt and stmt.strip()]
    
    def verify_tables(self, expected_tables: Optional[List[str]] = None) -> bool:
        """验证表结构"""
        print("🔍 验证表结构...")
        
        db_info = self.get_database_info()
        if not db_info['exists']:
            print("❌ 数据库不存在")
            return False
        
        existing_tables = [table.name for table in db_info['tables']]
        
        if expected_tables:
            missing_tables = set(expected_tables) - set(existing_tables)
            extra_tables = set(existing_tables) - set(expected_tables)
            
            print(f"📋 预期表数: {len(expected_tables)}")
            print(f"📋 实际表数: {len(existing_tables)}")
            
            if missing_tables:
                print(f"❌ 缺失表: {missing_tables}")
            if extra_tables:
                print(f"⚠️  额外表: {extra_tables}")
            
            success_rate = len(existing_tables) / len(expected_tables) if expected_tables else 0
            print(f"📊 表创建成功率: {success_rate:.1%}")
            
            return success_rate >= 0.8
        else:
            # 显示所有表
            print("📋 数据库中的表:")
            for i, table in enumerate(db_info['tables'], 1):
                print(f"   {i:2d}. {table.name:30s} ({table.record_count:,} 条记录)")
            
            return len(existing_tables) > 0
    
    def quick_setup(self) -> bool:
        """快速建表"""
        print("🚀 开始快速建表...")
        
        if not self.connect():
            return False
        
        # 获取DDL文件
        ddl_files = self.discover_ddl_files()
        if not ddl_files:
            print("❌ 未找到DDL文件")
            return False
        
        print(f"📄 发现 {len(ddl_files)} 个DDL文件:")
        for file in ddl_files:
            print(f"   - {file.name}")
        
        # 创建数据库
        if not self.create_database():
            return False
        
        # 执行DDL文件
        total_success = 0
        total_statements = 0
        
        for ddl_file in ddl_files:
            success, success_count, total_count = self.execute_sql_file(ddl_file)
            total_success += success_count
            total_statements += total_count
        
        # 验证结果
        success_rate = total_success / total_statements if total_statements > 0 else 0
        print(f"\n📊 总体执行结果: {total_success}/{total_statements} ({success_rate:.1%})")
        
        # 验证表
        self.verify_tables()
        
        return success_rate >= 0.8
    
    def force_rebuild(self) -> bool:
        """强制重建（删除数据库后重新创建）"""
        print("🔄 开始强制重建数据库...")
        
        if not self.connect():
            return False
        
        # 删除数据库
        if not self.drop_database():
            return False
        
        # 快速建表
        return self.quick_setup()
    
    def show_status(self) -> None:
        """显示数据库状态"""
        print("📊 数据库状态检查...")
        
        if not self.connect():
            return
        
        db_info = self.get_database_info()
        
        if db_info['exists']:
            print(f"✅ 数据库 {self.database} 存在")
            print(f"📋 表总数: {db_info['total_tables']}")
            
            if db_info['tables']:
                print("\n📋 表详情:")
                for table in db_info['tables']:
                    status = "✅" if table.exists else "❌"
                    print(f"   {status} {table.name:30s} {table.record_count:>10,} 条记录")
            
            # 检查DDL文件中定义的表
            ddl_files = self.discover_ddl_files()
            expected_tables = []
            
            for ddl_file in ddl_files:
                with open(ddl_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                expected_tables.extend(self.extract_table_names(content))
            
            expected_tables = list(set(expected_tables))  # 去重
            existing_tables = [table.name for table in db_info['tables']]
            
            missing_tables = set(expected_tables) - set(existing_tables)
            if missing_tables:
                print(f"\n⚠️  DDL中定义但未创建的表: {missing_tables}")
        else:
            print(f"❌ 数据库 {self.database} 不存在")
    
    def interactive_menu(self) -> None:
        """交互式菜单"""
        while True:
            print("\n" + "="*60)
            print("🏛️   ClickHouse 数据库管理工具")
            print("="*60)
            print("1. 快速建表（读取所有DDL文件）")
            print("2. 强制重建（删除数据库后重新创建）")  
            print("3. 检查表状态（显示已有表）")
            print("4. 验证表结构（对比DDL与实际结构）")
            print("5. 单独执行指定DDL文件")
            print("0. 退出")
            print("-"*60)
            
            try:
                choice = input("请选择操作 [0-5]: ").strip()
                
                if choice == '0':
                    print("👋 再见！")
                    break
                elif choice == '1':
                    self.quick_setup()
                elif choice == '2':
                    self.force_rebuild()
                elif choice == '3':
                    self.show_status()
                elif choice == '4':
                    self.verify_tables()
                elif choice == '5':
                    self._execute_single_file()
                else:
                    print("❌ 无效选择，请重新输入")
                    
                input("\n按回车键继续...")
                
            except KeyboardInterrupt:
                print("\n👋 再见！")
                break
            except Exception as e:
                print(f"❌ 操作失败: {e}")
                input("按回车键继续...")
    
    def _execute_single_file(self) -> None:
        """执行单个DDL文件"""
        ddl_files = self.discover_ddl_files()
        
        if not ddl_files:
            print("❌ 未找到DDL文件")
            return
        
        print("\n📄 可用的DDL文件:")
        for i, file in enumerate(ddl_files, 1):
            print(f"   {i}. {file.name}")
        
        try:
            choice = int(input(f"\n选择文件 [1-{len(ddl_files)}]: ").strip())
            if 1 <= choice <= len(ddl_files):
                selected_file = ddl_files[choice - 1]
                
                if not self.connect():
                    return
                
                # 确保数据库存在
                self.create_database()
                
                success, success_count, total_count = self.execute_sql_file(selected_file)
                if success:
                    print(f"✅ 文件 {selected_file.name} 执行成功")
                else:
                    print(f"❌ 文件 {selected_file.name} 执行部分成功 ({success_count}/{total_count})")
            else:
                print("❌ 无效选择")
        except ValueError:
            print("❌ 请输入数字")
    
    def close(self) -> None:
        """关闭连接"""
        if self.client:
            self.client.close()


def main():
    """主函数"""
    # 支持命令行参数
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        manager = DatabaseManager()
        
        try:
            if arg == 'quick' or arg == '1':
                success = manager.quick_setup()
                sys.exit(0 if success else 1)
            elif arg == 'rebuild' or arg == '2':
                success = manager.force_rebuild()
                sys.exit(0 if success else 1)
            elif arg == 'status' or arg == '3':
                manager.show_status()
                sys.exit(0)
            elif arg == 'verify' or arg == '4':
                success = manager.verify_tables()
                sys.exit(0 if success else 1)
            else:
                print(f"❌ 未知参数: {arg}")
                print("可用参数: quick, rebuild, status, verify")
                sys.exit(1)
        finally:
            manager.close()
    else:
        # 交互式模式
        manager = DatabaseManager()
        try:
            manager.interactive_menu()
        finally:
            manager.close()


if __name__ == "__main__":
    main()