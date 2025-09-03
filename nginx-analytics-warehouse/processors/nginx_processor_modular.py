#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nginx日志处理器 - 模块化架构主控制器
Nginx Log Processor - Modular Architecture Main Controller

统一调度和管理日志解析、数据处理、数据库写入等模块
支持日常日志处理、状态跟踪、数据清理等功能
"""

import os
import json
import time
import argparse
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging

# 导入模块化组件
from log_parser import NginxLogParser
from data_processor import DataProcessor  
from database_writer import DatabaseWriter

class NginxProcessorModular:
    """模块化Nginx日志处理器主控制器"""
    
    def __init__(self, base_log_dir: str = None, state_file: str = None):
        # 基础配置
        self.base_log_dir = Path(base_log_dir) if base_log_dir else Path("D:/project/nginx-log-analyzer/nginx-analytics-warehouse/nginx_logs")
        self.state_file = Path(state_file) if state_file else Path("processed_logs_state.json")
        
        # 初始化各个组件
        self.log_parser = NginxLogParser()
        self.data_processor = DataProcessor()
        self.database_writer = DatabaseWriter(host='localhost', port=8123,
                 database='nginx_analytics', user='analytics_user', password='analytics_password')
        
        # 日志配置
        self.logger = logging.getLogger(__name__)
        
        # 处理状态
        self.processed_state = self.load_state()
        
        # 性能配置
        self.chunk_size = 500  # 每批处理的记录数
        self.max_memory_mb = 512  # 最大内存使用MB
        
        print("✅ 模块化Nginx日志处理器初始化完成")
        print(f"📁 日志目录: {self.base_log_dir}")
        print(f"📄 状态文件: {self.state_file}")
    
    def load_state(self) -> Dict[str, Any]:
        """加载处理状态"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    self.logger.info(f"加载状态文件: {len(state.get('processed_files', {}))} 个已处理文件")
                    return state
            except Exception as e:
                self.logger.error(f"加载状态文件失败: {e}")
        
        return {
            'processed_files': {},
            'last_update': None,
            'total_processed_records': 0,
            'processing_history': []
        }
    
    def save_state(self):
        """保存处理状态"""
        try:
            self.processed_state['last_update'] = datetime.now().isoformat()
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.processed_state, f, indent=2, ensure_ascii=False, default=str)
            self.logger.info("状态文件保存成功")
        except Exception as e:
            self.logger.error(f"保存状态文件失败: {e}")
    
    def scan_log_directories(self) -> Dict[str, List[Path]]:
        """扫描日志目录，返回按日期分组的日志文件"""
        if not self.base_log_dir.exists():
            self.logger.error(f"日志目录不存在: {self.base_log_dir}")
            return {}
        
        log_files_by_date = {}
        
        # 扫描YYYYMMDD格式的目录
        for date_dir in self.base_log_dir.iterdir():
            if date_dir.is_dir() and date_dir.name.isdigit() and len(date_dir.name) == 8:
                try:
                    # 验证日期格式
                    datetime.strptime(date_dir.name, '%Y%m%d')
                    
                    # 查找.log文件
                    log_files = list(date_dir.glob("*.log"))
                    if log_files:
                        log_files_by_date[date_dir.name] = sorted(log_files)
                        
                except ValueError:
                    self.logger.warning(f"忽略无效日期目录: {date_dir.name}")
        
        self.logger.info(f"扫描到 {len(log_files_by_date)} 个日期目录")
        return log_files_by_date
    
    def is_file_processed(self, file_path: Path) -> bool:
        """检查文件是否已处理"""
        file_key = str(file_path)
        if file_key not in self.processed_state['processed_files']:
            return False
        
        # 检查文件修改时间是否变化
        try:
            current_mtime = file_path.stat().st_mtime
            recorded_mtime = self.processed_state['processed_files'][file_key].get('mtime', 0)
            return abs(current_mtime - recorded_mtime) < 1.0  # 1秒误差容忍
        except:
            return False
    
    def mark_file_processed(self, file_path: Path, record_count: int, processing_time: float):
        """标记文件为已处理"""
        file_key = str(file_path)
        try:
            mtime = file_path.stat().st_mtime
            self.processed_state['processed_files'][file_key] = {
                'processed_at': datetime.now().isoformat(),
                'record_count': record_count,
                'processing_time': processing_time,
                'mtime': mtime,
                'file_size': file_path.stat().st_size
            }
        except Exception as e:
            self.logger.error(f"标记文件状态失败 {file_path}: {e}")
    
    def process_specific_date(self, date_str: str, force_reprocess: bool = False) -> Dict[str, Any]:
        """处理指定日期的日志"""
        self.logger.info(f"开始处理 {date_str} 的日志 (强制重新处理: {force_reprocess})")
        
        start_time = time.time()
        
        # 检查日期目录
        date_dir = self.base_log_dir / date_str
        if not date_dir.exists():
            return {
                'success': False,
                'error': f'日期目录不存在: {date_dir}',
                'date': date_str,
                'processed_files': 0,
                'total_records': 0
            }
        
        log_files = list(date_dir.glob("*.log"))
        if not log_files:
            return {
                'success': False,
                'error': f'目录中没有找到.log文件: {date_dir}',
                'date': date_str,
                'processed_files': 0,
                'total_records': 0
            }
        
        self.logger.info(f"找到 {len(log_files)} 个日志文件")
        
        # 连接数据库
        if not self.database_writer.connect():
            return {
                'success': False,
                'error': 'ClickHouse连接失败',
                'date': date_str,
                'processed_files': 0,
                'total_records': 0
            }
        
        try:
            total_records = 0
            processed_files = 0
            errors = []
            
            for log_file in sorted(log_files):
                self.logger.info(f"处理文件: {log_file.name}")
                
                # 检查是否需要处理
                if not force_reprocess and self.is_file_processed(log_file):
                    self.logger.info(f"跳过已处理文件: {log_file.name}")
                    continue
                
                # 处理单个文件
                file_result = self.process_single_file(log_file)
                
                if file_result['success']:
                    total_records += file_result['record_count']
                    processed_files += 1
                    
                    # 标记为已处理
                    self.mark_file_processed(log_file, file_result['record_count'], file_result['duration'])
                else:
                    errors.append(f"{log_file.name}: {file_result['error']}")
                    self.logger.error(f"文件处理失败 {log_file.name}: {file_result['error']}")
            
            # 保存状态
            self.save_state()
            
            duration = time.time() - start_time
            success = len(errors) == 0
            
            result = {
                'success': success,
                'date': date_str,
                'processed_files': processed_files,
                'total_records': total_records,
                'duration': duration,
                'errors': errors
            }
            
            if success:
                self.logger.info(f"日期 {date_str} 处理完成: {processed_files} 文件, {total_records} 记录, 耗时 {duration:.2f}s")
                
                # 记录处理历史
                self.processed_state['processing_history'].append({
                    'date': date_str,
                    'processed_at': datetime.now().isoformat(),
                    'files': processed_files,
                    'records': total_records,
                    'duration': duration
                })
            
            return result
            
        finally:
            self.database_writer.close()
    
    def process_single_file(self, file_path: Path) -> Dict[str, Any]:
        """处理单个日志文件"""
        start_time = time.time()
        
        try:
            # 解析阶段
            self.logger.info(f"第一阶段: 解析日志文件 {file_path.name}")
            parsed_records = []
            
            for parsed_record in self.log_parser.parse_log_file(file_path):
                parsed_records.append(parsed_record)
                
                # 分批处理避免内存问题
                if len(parsed_records) >= self.chunk_size:
                    # 数据处理阶段
                    processed_records = []
                    for record in parsed_records:
                        processed_record = self.data_processor.process_single_record(record)
                        processed_records.append(processed_record)
                    
                    # 数据库写入阶段
                    write_result = self.database_writer.write_processed_records(processed_records)
                    if not write_result['success']:
                        return {
                            'success': False,
                            'error': f"数据写入失败: {write_result['message']}",
                            'record_count': 0,
                            'duration': time.time() - start_time
                        }
                    
                    parsed_records = []  # 清空缓存
            
            # 处理剩余记录
            if parsed_records:
                # 数据处理阶段
                processed_records = []
                for record in parsed_records:
                    processed_record = self.data_processor.process_single_record(record)
                    processed_records.append(processed_record)
                
                # 数据库写入阶段
                write_result = self.database_writer.write_processed_records(processed_records)
                if not write_result['success']:
                    return {
                        'success': False,
                        'error': f"数据写入失败: {write_result['message']}",
                        'record_count': 0,
                        'duration': time.time() - start_time
                    }
            
            duration = time.time() - start_time
            total_count = len(parsed_records) if parsed_records else 0
            
            return {
                'success': True,
                'record_count': total_count,
                'duration': duration
            }
            
        except Exception as e:
            self.logger.error(f"文件处理异常 {file_path}: {e}")
            return {
                'success': False,
                'error': str(e),
                'record_count': 0,
                'duration': time.time() - start_time
            }
    
    def process_all_unprocessed_logs(self) -> Dict[str, Any]:
        """处理所有未处理的日志"""
        self.logger.info("开始处理所有未处理的日志")
        start_time = time.time()
        
        log_files_by_date = self.scan_log_directories()
        if not log_files_by_date:
            return {
                'success': False,
                'error': '没有找到日志文件',
                'processed_dates': 0,
                'total_records': 0
            }
        
        processed_dates = 0
        total_records = 0
        errors = []
        
        for date_str in sorted(log_files_by_date.keys()):
            self.logger.info(f"处理日期: {date_str}")
            
            result = self.process_specific_date(date_str, force_reprocess=False)
            
            if result['success']:
                processed_dates += 1
                total_records += result['total_records']
            else:
                errors.extend(result.get('errors', [result.get('error', '未知错误')]))
        
        duration = time.time() - start_time
        success = len(errors) == 0
        
        return {
            'success': success,
            'processed_dates': processed_dates,
            'total_records': total_records,
            'duration': duration,
            'errors': errors
        }
    
    def clear_all_data(self):
        """清空所有数据"""
        self.logger.info("开始清空所有数据")
        
        if not self.database_writer.connect():
            print("❌ 数据库连接失败")
            return
        
        try:
            # 清空数据库表
            result = self.database_writer.clear_data()
            
            success_count = sum(1 for r in result.values() if r['success'])
            total_count = len(result)
            
            if success_count == total_count:
                print(f"✅ 成功清空 {total_count} 个表")
                
                # 清空状态文件
                self.processed_state = {
                    'processed_files': {},
                    'last_update': None,
                    'total_processed_records': 0,
                    'processing_history': []
                }
                self.save_state()
                print("✅ 状态文件已重置")
            else:
                print(f"⚠️  部分清空成功: {success_count}/{total_count}")
                for table, info in result.items():
                    if not info['success']:
                        print(f"   ❌ {table}: {info['message']}")
                        
        finally:
            self.database_writer.close()
    
    def show_status(self):
        """显示系统状态"""
        print("=" * 60)
        print("📊 系统状态概览")
        print("=" * 60)
        
        # 1. 日志目录状态
        log_files_by_date = self.scan_log_directories()
        print(f"📁 日志目录: {self.base_log_dir}")
        print(f"   找到 {len(log_files_by_date)} 个日期目录")
        
        if log_files_by_date:
            total_files = sum(len(files) for files in log_files_by_date.values())
            print(f"   总计 {total_files} 个日志文件")
            
            # 显示最近的几个日期
            recent_dates = sorted(log_files_by_date.keys())[-5:]
            print(f"   最近的日期: {', '.join(recent_dates)}")
        
        # 2. 数据库状态
        if self.database_writer.connect():
            try:
                status = self.database_writer.check_table_status()
                counts = self.database_writer.get_data_counts()
                
                print(f"\n💾 数据库状态:")
                for table, info in status.items():
                    count = counts.get(table, 0)
                    if info['exists']:
                        print(f"   ✅ {table}: {count:,} 条记录")
                        if info['last_update']:
                            print(f"      最后更新: {info['last_update']}")
                    else:
                        print(f"   ❌ {table}: 表不存在")
                        
            finally:
                self.database_writer.close()
        else:
            print(f"\n💾 数据库状态: ❌ 连接失败")
        
        # 3. 处理状态
        processed_files_count = len(self.processed_state.get('processed_files', {}))
        print(f"\n📈 处理状态:")
        print(f"   已处理文件: {processed_files_count} 个")
        
        if self.processed_state.get('last_update'):
            print(f"   最后更新: {self.processed_state['last_update']}")
        
        # 最近处理历史
        history = self.processed_state.get('processing_history', [])
        if history:
            print(f"   最近处理记录:")
            for record in history[-3:]:  # 显示最近3条
                date_str = record.get('date', 'Unknown')
                files = record.get('files', 0)
                records = record.get('records', 0)
                duration = record.get('duration', 0)
                print(f"     {date_str}: {files} 文件, {records:,} 记录, {duration:.1f}s")
    
    def interactive_menu(self):
        """交互式菜单"""
        while True:
            print("\n" + "=" * 60)
            print("🚀  Nginx日志处理器 - 模块化架构")
            print("=" * 60)
            print("1. 处理所有未处理的日志 (推荐)")
            print("2. 处理指定日期的日志")
            print("3. 查看系统状态")
            print("4. 清空所有数据 (仅开发环境)")
            print("5. 强制重新处理所有日志")
            print("0. 退出")
            print("-" * 60)
            
            try:
                choice = input("请选择操作 [0-5]: ").strip()
                
                if choice == '0':
                    print("👋 再见！")
                    break
                
                elif choice == '1':
                    print("\n🔄 开始处理所有未处理的日志...")
                    result = self.process_all_unprocessed_logs()
                    self._print_process_result(result, "批量处理")
                    input("\n按回车键继续...")
                
                elif choice == '2':
                    date_str = input("\n请输入日期 (YYYYMMDD格式，如: 20250422): ").strip()
                    if not date_str or len(date_str) != 8 or not date_str.isdigit():
                        print("❌ 日期格式错误，请使用YYYYMMDD格式")
                        input("按回车键继续...")
                        continue
                    
                    try:
                        datetime.strptime(date_str, '%Y%m%d')
                    except ValueError:
                        print("❌ 无效的日期")
                        input("按回车键继续...")
                        continue
                    
                    force = input("是否强制重新处理？(y/N): ").strip().lower() == 'y'
                    
                    print(f"\n🔄 开始处理 {date_str} 的日志...")
                    result = self.process_specific_date(date_str, force)
                    self._print_single_date_result(result)
                    input("\n按回车键继续...")
                
                elif choice == '3':
                    print()
                    self.show_status()
                    input("\n按回车键继续...")
                
                elif choice == '4':
                    print("\n⚠️  清空所有数据 (仅开发环境使用)")
                    confirm = input("确认清空所有数据？这将删除数据库中的所有日志数据 (y/N): ").strip().lower()
                    if confirm == 'y':
                        second_confirm = input("再次确认！这将不可恢复地删除所有数据 (输入 'DELETE' 确认): ").strip()
                        if second_confirm == 'DELETE':
                            print("\n🔄 开始清空数据...")
                            self.clear_all_data()
                        else:
                            print("❌ 确认失败，操作已取消")
                    else:
                        print("❌ 操作已取消")
                    input("\n按回车键继续...")
                
                elif choice == '5':
                    print("\n⚠️  强制重新处理所有日志")
                    confirm = input("确认强制重新处理所有日志？这将忽略处理状态重新处理 (y/N): ").strip().lower()
                    if confirm == 'y':
                        print("\n🔄 开始强制重新处理所有日志...")
                        # 临时清空状态以强制重处理
                        backup_state = self.processed_state.copy()
                        self.processed_state = {'processed_files': {}, 'last_update': None, 'total_processed_records': 0, 'processing_history': []}
                        
                        result = self.process_all_unprocessed_logs()
                        
                        # 如果失败，恢复状态
                        if not result['success']:
                            self.processed_state = backup_state
                        
                        self._print_process_result(result, "强制重新处理")
                    else:
                        print("❌ 操作已取消")
                    input("\n按回车键继续...")
                
                else:
                    print("❌ 无效选择，请输入 0-5")
                    input("按回车键继续...")
                    
            except KeyboardInterrupt:
                print("\n\n👋 用户中断，再见！")
                break
            except Exception as e:
                print(f"\n❌ 操作过程中发生错误: {e}")
                input("按回车键继续...")
    
    def _print_process_result(self, result: Dict[str, Any], operation_name: str):
        """打印处理结果"""
        if result['success']:
            print(f"✅ {operation_name}成功!")
            print(f"   处理日期: {result.get('processed_dates', 0)} 个")
            print(f"   总记录数: {result.get('total_records', 0):,} 条")
            print(f"   耗时: {result.get('duration', 0):.2f} 秒")
        else:
            print(f"❌ {operation_name}失败: {result.get('error', '未知错误')}")
            errors = result.get('errors', [])
            if errors:
                print("   详细错误:")
                for i, error in enumerate(errors[:5], 1):  # 只显示前5个错误
                    print(f"     {i}. {error}")
                if len(errors) > 5:
                    print(f"     ... 还有 {len(errors) - 5} 个错误")
    
    def _print_single_date_result(self, result: Dict[str, Any]):
        """打印单个日期的处理结果"""
        if result['success']:
            print(f"✅ 日志处理成功!")
            print(f"   日期: {result['date']}")
            print(f"   处理文件: {result['processed_files']} 个")
            print(f"   总记录数: {result['total_records']:,} 条")
            print(f"   耗时: {result['duration']:.2f} 秒")
        else:
            print(f"❌ 日志处理失败: {result.get('error', '未知错误')}")
            errors = result.get('errors', [])
            if errors:
                print("   详细错误:")
                for i, error in enumerate(errors, 1):
                    print(f"     {i}. {error}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='模块化Nginx日志处理器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python nginx_processor_modular.py                             # 交互式菜单 (推荐)
  python nginx_processor_modular.py process --date 20250422     # 处理指定日期
  python nginx_processor_modular.py process --date 20250422 --force  # 强制重新处理
  python nginx_processor_modular.py process-all                 # 处理所有未处理日志
  python nginx_processor_modular.py status                      # 查看系统状态
  python nginx_processor_modular.py clear-all                   # 清空所有数据
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # process命令
    process_parser = subparsers.add_parser('process', help='处理指定日期的日志')
    process_parser.add_argument('--date', required=True, help='日期 (YYYYMMDD格式)')
    process_parser.add_argument('--force', action='store_true', help='强制重新处理')
    
    # 其他命令
    subparsers.add_parser('process-all', help='处理所有未处理的日志')
    subparsers.add_parser('status', help='查看系统状态')
    subparsers.add_parser('clear-all', help='清空所有数据 (开发环境使用)')
    
    args = parser.parse_args()
    
    # 初始化处理器
    processor = NginxProcessorModular()
    
    # 如果没有参数，显示交互式菜单
    if not args.command:
        processor.interactive_menu()
        return
    
    # 执行对应命令
    if args.command == 'process':
        # 验证日期格式
        try:
            datetime.strptime(args.date, '%Y%m%d')
        except ValueError:
            print("❌ 日期格式错误，请使用YYYYMMDD格式，例如: 20250422")
            return
        
        result = processor.process_specific_date(args.date, args.force)
        processor._print_single_date_result(result)
    
    elif args.command == 'process-all':
        result = processor.process_all_unprocessed_logs()
        processor._print_process_result(result, "批量处理")
    
    elif args.command == 'status':
        processor.show_status()
    
    elif args.command == 'clear-all':
        confirm = input("⚠️  确认清空所有数据？这将删除数据库中的所有日志数据 (y/N): ")
        if confirm.lower() == 'y':
            second_confirm = input("再次确认！这将不可恢复地删除所有数据 (输入 'DELETE' 确认): ").strip()
            if second_confirm == 'DELETE':
                processor.clear_all_data()
            else:
                print("❌ 确认失败，操作已取消")
        else:
            print("❌ 操作已取消")

if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    main()