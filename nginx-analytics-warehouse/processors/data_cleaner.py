#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据清理器模块 - 专门负责数据仓库的清理操作
Data Cleaner Module - Specialized for Data Warehouse Cleaning Operations

提供灵活、安全的数据清理功能：
1. 按层级清理（ODS、DWD、ADS）
2. 按日期范围清理
3. 自定义表清理
4. 安全确认机制
"""

import time
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple
import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError

class DataCleaner:
    """数据清理器 - 提供灵活的数据清理功能"""
    
    def __init__(self, host: str = 'localhost', port: int = 8123, 
                 database: str = 'nginx_analytics', user: str = 'analytics_user', 
                 password: str = 'analytics_password'):
        """
        初始化数据清理器
        
        Args:
            host: ClickHouse服务器地址
            port: ClickHouse HTTP端口
            database: 数据库名称
            user: 用户名
            password: 密码
        """
        self.config = {
            'host': host,
            'port': port,
            'username': user,
            'password': password,
            'database': database
        }
        self.database = database
        self.client = None
        self.logger = logging.getLogger(__name__)
        
        # 数据仓库分层定义 - 动态获取，不写死表名
        self.layer_definitions = {
            'ODS': {
                'description': 'ODS原始数据层',
                'pattern': 'ods_*',
                'tables': []  # 动态获取
            },
            'DWD': {
                'description': 'DWD明细数据层', 
                'pattern': 'dwd_*',
                'tables': []  # 动态获取
            },
            'ADS': {
                'description': 'ADS聚合分析层',
                'pattern': 'ads_*', 
                'tables': []  # 动态获取
            }
        }
        
    def connect(self) -> bool:
        """
        连接ClickHouse数据库
        
        Returns:
            bool: 连接是否成功
        """
        try:
            self.client = clickhouse_connect.get_client(**self.config)
            # 测试连接
            result = self.client.query("SELECT 1")
            self.logger.info(f"✅ 数据清理器连接成功: {self.config['host']}:{self.config['port']}")
            
            # 动态获取表列表
            self._refresh_table_list()
            return True
        except Exception as e:
            self.logger.error(f"❌ 数据清理器连接失败: {str(e)}")
            return False
    
    def _refresh_table_list(self):
        """刷新表列表 - 动态获取当前数据库的表"""
        try:
            # 获取所有表
            query = f"""
            SELECT name, engine 
            FROM system.tables 
            WHERE database = '{self.database}'
            ORDER BY name
            """
            result = self.client.query(query)
            
            # 清空现有列表
            for layer in self.layer_definitions.values():
                layer['tables'] = []
            
            # 按模式分类表
            for row in result.result_rows:
                table_name, engine = row
                
                # 跳过物化视图
                if engine == 'MaterializedView':
                    continue
                    
                # 按前缀分类
                if table_name.startswith('ods_'):
                    self.layer_definitions['ODS']['tables'].append(table_name)
                elif table_name.startswith('dwd_'):
                    self.layer_definitions['DWD']['tables'].append(table_name)
                elif table_name.startswith('ads_'):
                    self.layer_definitions['ADS']['tables'].append(table_name)
                    
            self.logger.info("📋 表列表刷新完成")
            
        except Exception as e:
            self.logger.error(f"❌ 刷新表列表失败: {str(e)}")
    
    def get_table_info(self) -> Dict[str, Any]:
        """
        获取表信息统计
        
        Returns:
            Dict: 表信息字典
        """
        if not self.client:
            return {}
        
        self._refresh_table_list()
        
        info = {
            'layers': {},
            'total_tables': 0,
            'total_records': 0
        }
        
        for layer_name, layer_info in self.layer_definitions.items():
            layer_stats = {
                'description': layer_info['description'],
                'table_count': len(layer_info['tables']),
                'tables': [],
                'total_records': 0
            }
            
            for table_name in layer_info['tables']:
                try:
                    # 获取表记录数
                    count_query = f"SELECT count() FROM {self.database}.{table_name}"
                    result = self.client.query(count_query)
                    record_count = result.result_rows[0][0]
                    
                    # 获取表大小（可选，可能较慢）
                    size_query = f"""
                    SELECT formatReadableSize(sum(bytes)) as size
                    FROM system.parts 
                    WHERE database = '{self.database}' AND table = '{table_name}'
                    """
                    try:
                        size_result = self.client.query(size_query)
                        table_size = size_result.result_rows[0][0] if size_result.result_rows else "Unknown"
                    except:
                        table_size = "Unknown"
                    
                    table_info = {
                        'name': table_name,
                        'records': record_count,
                        'size': table_size
                    }
                    
                    layer_stats['tables'].append(table_info)
                    layer_stats['total_records'] += record_count
                    
                except Exception as e:
                    self.logger.error(f"获取表 {table_name} 信息失败: {str(e)}")
                    layer_stats['tables'].append({
                        'name': table_name,
                        'records': -1,
                        'size': "Error"
                    })
            
            info['layers'][layer_name] = layer_stats
            info['total_tables'] += layer_stats['table_count']
            info['total_records'] += layer_stats['total_records']
        
        return info
    
    def clear_by_layers(self, layers: List[str], confirm_token: str = None) -> Dict[str, Any]:
        """
        按层级清理数据
        
        Args:
            layers: 要清理的层级列表 ['ODS', 'DWD', 'ADS']
            confirm_token: 确认令牌（安全机制）
            
        Returns:
            Dict: 清理结果
        """
        if not self.client:
            return {'success': False, 'error': '数据库未连接'}
        
        # 安全确认
        if confirm_token != 'CONFIRMED':
            return {'success': False, 'error': '需要确认令牌'}
        
        results = {
            'success': True,
            'layers_processed': [],
            'tables_cleared': [],
            'total_records_deleted': 0,
            'errors': []
        }
        
        self._refresh_table_list()
        
        for layer_name in layers:
            if layer_name not in self.layer_definitions:
                results['errors'].append(f'未知层级: {layer_name}')
                continue
            
            layer_info = self.layer_definitions[layer_name]
            layer_result = {
                'layer': layer_name,
                'description': layer_info['description'],
                'tables': [],
                'records_deleted': 0
            }
            
            self.logger.info(f"🔄 开始清理 {layer_name} 层 ({layer_info['description']})")
            
            for table_name in layer_info['tables']:
                try:
                    # 获取删除前记录数
                    count_before = self._get_table_count(table_name)
                    
                    # 执行清理
                    start_time = time.time()
                    self.client.command(f"TRUNCATE TABLE {self.database}.{table_name}")
                    duration = time.time() - start_time
                    
                    # 验证清理结果
                    count_after = self._get_table_count(table_name)
                    
                    table_result = {
                        'table': table_name,
                        'records_before': count_before,
                        'records_after': count_after,
                        'records_deleted': count_before - count_after,
                        'duration': duration,
                        'success': True
                    }
                    
                    layer_result['tables'].append(table_result)
                    layer_result['records_deleted'] += table_result['records_deleted']
                    results['tables_cleared'].append(table_name)
                    
                    self.logger.info(f"✅ {table_name}: 删除 {table_result['records_deleted']:,} 条记录")
                    
                except Exception as e:
                    error_msg = f"清理表 {table_name} 失败: {str(e)}"
                    self.logger.error(f"❌ {error_msg}")
                    results['errors'].append(error_msg)
                    
                    layer_result['tables'].append({
                        'table': table_name,
                        'success': False,
                        'error': str(e)
                    })
            
            results['layers_processed'].append(layer_result)
            results['total_records_deleted'] += layer_result['records_deleted']
        
        # 如果有错误，标记为部分成功
        if results['errors']:
            results['success'] = 'partial'
        
        self.logger.info(f"🎉 清理完成: 删除 {results['total_records_deleted']:,} 条记录")
        return results
    
    def clear_by_date_range(self, start_date: str, end_date: str, 
                          tables: List[str] = None, confirm_token: str = None) -> Dict[str, Any]:
        """
        按日期范围清理数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD) 
            tables: 指定表列表，None表示所有表
            confirm_token: 确认令牌
            
        Returns:
            Dict: 清理结果
        """
        if not self.client:
            return {'success': False, 'error': '数据库未连接'}
            
        if confirm_token != 'CONFIRMED':
            return {'success': False, 'error': '需要确认令牌'}
        
        # 验证日期格式
        try:
            datetime.strptime(start_date, '%Y-%m-%d')
            datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            return {'success': False, 'error': '日期格式错误，请使用YYYY-MM-DD'}
        
        self._refresh_table_list()
        
        # 确定要处理的表
        if tables is None:
            target_tables = []
            for layer_info in self.layer_definitions.values():
                target_tables.extend(layer_info['tables'])
        else:
            target_tables = tables
        
        results = {
            'success': True,
            'date_range': f'{start_date} ~ {end_date}',
            'tables_processed': [],
            'total_records_deleted': 0,
            'errors': []
        }
        
        for table_name in target_tables:
            try:
                # 检查表是否有日期字段
                date_columns = ['date', 'log_date', 'stat_date', 'created_date']
                date_field = None
                
                for col in date_columns:
                    if self._column_exists(table_name, col):
                        date_field = col
                        break
                
                if not date_field:
                    results['errors'].append(f'表 {table_name} 没有找到日期字段')
                    continue
                
                # 获取删除前记录数
                count_before = self._get_table_count(table_name)
                
                # 构建删除SQL
                delete_sql = f"""
                DELETE FROM {self.database}.{table_name} 
                WHERE {date_field} >= '{start_date}' 
                AND {date_field} <= '{end_date}'
                """
                
                start_time = time.time()
                self.client.command(delete_sql)
                duration = time.time() - start_time
                
                # 获取删除后记录数
                count_after = self._get_table_count(table_name)
                
                table_result = {
                    'table': table_name,
                    'date_field': date_field,
                    'records_before': count_before,
                    'records_after': count_after,
                    'records_deleted': count_before - count_after,
                    'duration': duration,
                    'success': True
                }
                
                results['tables_processed'].append(table_result)
                results['total_records_deleted'] += table_result['records_deleted']
                
                self.logger.info(f"✅ {table_name}: 删除 {table_result['records_deleted']:,} 条记录")
                
            except Exception as e:
                error_msg = f"按日期清理表 {table_name} 失败: {str(e)}"
                self.logger.error(f"❌ {error_msg}")
                results['errors'].append(error_msg)
        
        if results['errors']:
            results['success'] = 'partial'
        
        return results
    
    def clear_specific_tables(self, table_names: List[str], confirm_token: str = None) -> Dict[str, Any]:
        """
        清理指定表
        
        Args:
            table_names: 表名列表
            confirm_token: 确认令牌
            
        Returns:
            Dict: 清理结果
        """
        if not self.client:
            return {'success': False, 'error': '数据库未连接'}
            
        if confirm_token != 'CONFIRMED':
            return {'success': False, 'error': '需要确认令牌'}
        
        results = {
            'success': True,
            'tables_processed': [],
            'total_records_deleted': 0,
            'errors': []
        }
        
        for table_name in table_names:
            try:
                # 验证表存在
                if not self._table_exists(table_name):
                    results['errors'].append(f'表不存在: {table_name}')
                    continue
                
                # 获取删除前记录数
                count_before = self._get_table_count(table_name)
                
                # 执行清理
                start_time = time.time()
                self.client.command(f"TRUNCATE TABLE {self.database}.{table_name}")
                duration = time.time() - start_time
                
                # 验证清理结果
                count_after = self._get_table_count(table_name)
                
                table_result = {
                    'table': table_name,
                    'records_before': count_before,
                    'records_after': count_after,
                    'records_deleted': count_before - count_after,
                    'duration': duration,
                    'success': True
                }
                
                results['tables_processed'].append(table_result)
                results['total_records_deleted'] += table_result['records_deleted']
                
                self.logger.info(f"✅ {table_name}: 删除 {table_result['records_deleted']:,} 条记录")
                
            except Exception as e:
                error_msg = f"清理表 {table_name} 失败: {str(e)}"
                self.logger.error(f"❌ {error_msg}")
                results['errors'].append(error_msg)
        
        if results['errors']:
            results['success'] = 'partial'
            
        return results
    
    def _get_table_count(self, table_name: str) -> int:
        """获取表记录数"""
        try:
            result = self.client.query(f"SELECT count() FROM {self.database}.{table_name}")
            return result.result_rows[0][0]
        except:
            return 0
    
    def _table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            query = f"""
            SELECT count() FROM system.tables 
            WHERE database = '{self.database}' AND name = '{table_name}'
            """
            result = self.client.query(query)
            return result.result_rows[0][0] > 0
        except:
            return False
    
    def _column_exists(self, table_name: str, column_name: str) -> bool:
        """检查列是否存在"""
        try:
            query = f"""
            SELECT count() FROM system.columns 
            WHERE database = '{self.database}' 
            AND table = '{table_name}' 
            AND name = '{column_name}'
            """
            result = self.client.query(query)
            return result.result_rows[0][0] > 0
        except:
            return False
    
    def print_table_info(self):
        """打印表信息报告"""
        info = self.get_table_info()
        
        print("\n" + "="*80)
        print("📊 数据仓库表信息报告")
        print("="*80)
        
        for layer_name, layer_stats in info['layers'].items():
            print(f"\n📋 {layer_name} 层 - {layer_stats['description']}")
            print(f"   表数量: {layer_stats['table_count']}")
            print(f"   总记录数: {layer_stats['total_records']:,}")
            
            if layer_stats['tables']:
                print("   详细信息:")
                for table_info in layer_stats['tables']:
                    records = table_info['records']
                    if records >= 0:
                        print(f"     ✅ {table_info['name']:<35} {records:>8,} 条  ({table_info['size']})")
                    else:
                        print(f"     ❌ {table_info['name']:<35} {'错误':>8}  ({table_info['size']})")
        
        print(f"\n📈 总计: {info['total_tables']} 个表, {info['total_records']:,} 条记录")
        print("="*80)


def main():
    """主函数 - 用于测试和管理数据清理"""
    import sys
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建清理器
    cleaner = DataCleaner()
    
    if not cleaner.connect():
        print("❌ 无法连接到ClickHouse，请检查服务状态")
        sys.exit(1)
    
    # 根据参数执行不同操作
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'info':
            cleaner.print_table_info()
            
        elif command == 'clear-source':
            # 清理源数据（ODS + DWD）
            confirm = input("⚠️  确认清理源数据（ODS + DWD）？这将删除原始和明细数据 (输入 'CONFIRMED' 确认): ")
            if confirm == 'CONFIRMED':
                results = cleaner.clear_by_layers(['ODS', 'DWD'], confirm_token='CONFIRMED')
                print(f"✅ 清理完成: 删除 {results['total_records_deleted']:,} 条记录")
            else:
                print("❌ 操作已取消")
                
        elif command == 'clear-all':
            # 清理所有数据
            confirm = input("⚠️  确认清理所有数据（ODS + DWD + ADS）？这将删除全部数据 (输入 'CONFIRMED' 确认): ")
            if confirm == 'CONFIRMED':
                results = cleaner.clear_by_layers(['ODS', 'DWD', 'ADS'], confirm_token='CONFIRMED')
                print(f"✅ 清理完成: 删除 {results['total_records_deleted']:,} 条记录")
            else:
                print("❌ 操作已取消")
                
        elif command == 'clear-ads':
            # 仅清理ADS聚合数据
            confirm = input("⚠️  确认清理ADS聚合数据？这将删除所有分析报表数据 (输入 'CONFIRMED' 确认): ")
            if confirm == 'CONFIRMED':
                results = cleaner.clear_by_layers(['ADS'], confirm_token='CONFIRMED')
                print(f"✅ 清理完成: 删除 {results['total_records_deleted']:,} 条记录")
            else:
                print("❌ 操作已取消")
                
        else:
            print("❌ 未知命令，支持的命令: info, clear-source, clear-all, clear-ads")
    else:
        # 默认显示信息
        cleaner.print_table_info()


if __name__ == "__main__":
    main()