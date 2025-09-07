#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能化ETL控制器 - 支持多文件、状态跟踪、按日期处理
Intelligent ETL Controller - Multi-file processing, state tracking, date-based processing

基于simple_etl_controller升级，参考nginx_processor_modular设计思路
"""

import sys
import os
import json
import time
import argparse
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

# 添加路径以导入其他模块
current_dir = Path(__file__).parent
etl_root = current_dir.parent
sys.path.append(str(etl_root))

from parsers.base_log_parser import BaseLogParser
from processors.field_mapper import FieldMapper
from writers.dwd_writer import DWDWriter

class IntelligentETLController:
    """智能化ETL控制器"""
    
    def __init__(self, 
                 base_log_dir: str = None, 
                 state_file: str = None,
                 batch_size: int = 50):
        """
        初始化智能化ETL控制器
        
        Args:
            base_log_dir: 日志根目录
            state_file: 状态文件路径
            batch_size: 批处理大小
        """
        # 基础配置
        self.base_log_dir = Path(base_log_dir) if base_log_dir else \
            Path("D:/project/nginx-log-analyzer/nginx-analytics-warehouse/nginx_logs")
        self.state_file = Path(state_file) if state_file else \
            Path(etl_root / "processed_logs_state.json")
        self.batch_size = batch_size
        
        # 初始化组件
        self.parser = BaseLogParser()
        self.mapper = FieldMapper()
        self.writer = DWDWriter()
        
        # 日志配置
        import logging
        self.logger = logging.getLogger(__name__)
        
        # 处理状态
        self.processed_state = self.load_state()
        
        # 全局统计信息
        self.session_stats = {
            'start_time': None,
            'end_time': None,
            'total_files_processed': 0,
            'total_lines_processed': 0,
            'total_records_written': 0,
            'total_errors': 0,
            'processing_errors': []
        }
        
        self.logger.info("智能化ETL控制器初始化完成")
        self.logger.info(f"日志目录: {self.base_log_dir}")
        self.logger.info(f"状态文件: {self.state_file}")
        self.logger.info(f"批处理大小: {self.batch_size}")
    
    def load_state(self) -> Dict[str, Any]:
        """加载处理状态"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    processed_count = len(state.get('processed_files', {}))
                    self.logger.info(f"加载状态文件成功: {processed_count} 个已处理文件")
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
        
        self.logger.info(f"扫描完成: 找到 {len(log_files_by_date)} 个日期目录")
        total_files = sum(len(files) for files in log_files_by_date.values())
        self.logger.info(f"总计 {total_files} 个日志文件")
        
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
    
    def process_single_file(self, file_path: Path, test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """
        处理单个日志文件 - 基于simple_etl_controller的process_file方法
        
        Args:
            file_path: 日志文件路径
            test_mode: 测试模式（不实际写入数据库）
            limit: 限制处理的行数
            
        Returns:
            处理结果字典
        """
        if not file_path.exists():
            error_msg = f"文件不存在: {file_path}"
            self.logger.error(error_msg)
            return self._create_error_result(error_msg)
        
        start_time = time.time()
        
        # 文件级别的统计信息
        file_stats = {
            'total_lines': 0,
            'parsed_lines': 0,
            'mapped_lines': 0,
            'written_lines': 0,
            'failed_lines': 0,
            'batches_processed': 0,
            'processing_errors': []
        }
        
        self.logger.info(f"开始处理文件: {file_path.name}")
        if test_mode:
            self.logger.info("🧪 测试模式：将不会实际写入数据库")
        if limit:
            self.logger.info(f"🔢 限制处理行数: {limit}")
        
        try:
            # 连接数据库（非测试模式）
            if not test_mode:
                if not self.writer.connect():
                    return self._create_error_result("数据库连接失败")
            
            # 批量处理
            batch = []
            processed_count = 0
            
            for parsed_data in self.parser.parse_file(file_path):
                file_stats['total_lines'] += 1
                
                if parsed_data:
                    file_stats['parsed_lines'] += 1
                    
                    try:
                        # 字段映射
                        mapped_data = self.mapper.map_to_dwd(parsed_data, file_path.name)
                        file_stats['mapped_lines'] += 1
                        
                        batch.append(mapped_data)
                        
                        # 批量写入
                        if len(batch) >= self.batch_size:
                            write_result = self._write_batch(batch, test_mode)
                            if write_result['success']:
                                file_stats['written_lines'] += write_result['count']
                            else:
                                file_stats['failed_lines'] += len(batch)
                                file_stats['processing_errors'].append(write_result['error'])
                            
                            file_stats['batches_processed'] += 1
                            batch = []
                        
                        processed_count += 1
                        
                        # 检查限制
                        if limit and processed_count >= limit:
                            self.logger.info(f"达到处理限制 ({limit} 行)，停止处理")
                            break
                            
                    except Exception as e:
                        self.logger.error(f"字段映射失败: {e}")
                        file_stats['failed_lines'] += 1
                        file_stats['processing_errors'].append(str(e))
                else:
                    file_stats['failed_lines'] += 1
            
            # 处理最后一批
            if batch:
                write_result = self._write_batch(batch, test_mode)
                if write_result['success']:
                    file_stats['written_lines'] += write_result['count']
                else:
                    file_stats['failed_lines'] += len(batch)
                    file_stats['processing_errors'].append(write_result['error'])
                
                file_stats['batches_processed'] += 1
            
            duration = time.time() - start_time
            
            # 关闭连接
            if not test_mode:
                self.writer.close()
            
            # 更新全局统计
            self.session_stats['total_files_processed'] += 1
            self.session_stats['total_lines_processed'] += file_stats['total_lines']
            self.session_stats['total_records_written'] += file_stats['written_lines']
            self.session_stats['total_errors'] += file_stats['failed_lines']
            
            # 生成结果报告
            return {
                'success': True,
                'file_path': file_path,
                'duration': duration,
                'stats': file_stats,
                'message': f'文件 {file_path.name} 处理完成',
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"处理文件失败: {e}")
            if not test_mode:
                self.writer.close()
            return {
                'success': False,
                'file_path': file_path,
                'duration': duration,
                'error': str(e),
                'stats': file_stats,
                'timestamp': datetime.now()
            }
    
    def process_specific_date(self, date_str: str, force_reprocess: bool = False, 
                            test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """
        处理指定日期的所有日志
        
        Args:
            date_str: 日期字符串 (YYYYMMDD格式)
            force_reprocess: 强制重新处理
            test_mode: 测试模式
            limit: 限制处理的行数（每个文件）
            
        Returns:
            处理结果字典
        """
        self.logger.info(f"开始处理 {date_str} 的日志 (强制重新处理: {force_reprocess})")
        
        self.session_stats['start_time'] = datetime.now()
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
        
        total_records = 0
        processed_files = 0
        errors = []
        file_results = []
        
        for log_file in sorted(log_files):
            self.logger.info(f"处理文件: {log_file.name}")
            
            # 检查是否需要处理
            if not force_reprocess and self.is_file_processed(log_file):
                self.logger.info(f"跳过已处理文件: {log_file.name}")
                continue
            
            # 处理单个文件
            file_result = self.process_single_file(log_file, test_mode, limit)
            file_results.append(file_result)
            
            if file_result['success']:
                record_count = file_result['stats']['written_lines']
                total_records += record_count
                processed_files += 1
                
                # 标记为已处理（非测试模式）
                if not test_mode:
                    self.mark_file_processed(log_file, record_count, file_result['duration'])
            else:
                errors.append(f"{log_file.name}: {file_result['error']}")
                self.logger.error(f"文件处理失败 {log_file.name}: {file_result['error']}")
        
        # 保存状态
        if not test_mode:
            self.save_state()
        
        self.session_stats['end_time'] = datetime.now()
        duration = time.time() - start_time
        success = len(errors) == 0
        
        result = {
            'success': success,
            'date': date_str,
            'processed_files': processed_files,
            'total_records': total_records,
            'duration': duration,
            'errors': errors,
            'file_results': file_results
        }
        
        if success:
            self.logger.info(f"日期 {date_str} 处理完成: {processed_files} 文件, {total_records} 记录, 耗时 {duration:.2f}s")
            
            # 记录处理历史
            if not test_mode:
                self.processed_state['processing_history'].append({
                    'date': date_str,
                    'processed_at': datetime.now().isoformat(),
                    'files': processed_files,
                    'records': total_records,
                    'duration': duration
                })
        
        return result
    
    def process_all_unprocessed_logs(self, test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """
        处理所有未处理的日志
        
        Args:
            test_mode: 测试模式
            limit: 限制处理的行数（每个文件）
            
        Returns:
            处理结果字典
        """
        self.logger.info("开始处理所有未处理的日志")
        self.session_stats['start_time'] = datetime.now()
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
        date_results = []
        
        for date_str in sorted(log_files_by_date.keys()):
            self.logger.info(f"处理日期: {date_str}")
            
            result = self.process_specific_date(date_str, force_reprocess=False, 
                                              test_mode=test_mode, limit=limit)
            date_results.append(result)
            
            if result['success'] and result['processed_files'] > 0:
                processed_dates += 1
                total_records += result['total_records']
            else:
                if result.get('errors'):
                    errors.extend(result['errors'])
                elif result.get('error'):
                    errors.append(f"{date_str}: {result['error']}")
        
        self.session_stats['end_time'] = datetime.now()
        duration = time.time() - start_time
        success = len(errors) == 0
        
        return {
            'success': success,
            'processed_dates': processed_dates,
            'total_records': total_records,
            'duration': duration,
            'errors': errors,
            'date_results': date_results
        }
    
    def show_status(self):
        """显示系统状态"""
        print("=" * 80)
        print("📊 智能化ETL控制器状态报告")
        print("=" * 80)
        
        # 1. 日志目录状态
        log_files_by_date = self.scan_log_directories()
        print(f"📁 日志根目录: {self.base_log_dir}")
        print(f"   找到 {len(log_files_by_date)} 个日期目录")
        
        if log_files_by_date:
            total_files = sum(len(files) for files in log_files_by_date.values())
            print(f"   总计 {total_files} 个日志文件")
            
            # 显示最近的几个日期
            recent_dates = sorted(log_files_by_date.keys())[-5:]
            print(f"   最近日期: {', '.join(recent_dates)}")
            
            # 显示每个日期的文件数
            print("   各日期文件统计:")
            for date_str in sorted(log_files_by_date.keys())[-10:]:  # 显示最近10天
                file_count = len(log_files_by_date[date_str])
                # 检查已处理的文件数
                processed_count = 0
                for log_file in log_files_by_date[date_str]:
                    if self.is_file_processed(log_file):
                        processed_count += 1
                status = "✅" if processed_count == file_count else f"⚠️ {processed_count}/{file_count}"
                print(f"     {date_str}: {file_count} 个文件 {status}")
        
        # 2. 处理状态统计
        processed_files_count = len(self.processed_state.get('processed_files', {}))
        total_processed_records = sum(
            info.get('record_count', 0) 
            for info in self.processed_state.get('processed_files', {}).values()
        )
        
        print(f"\n📈 处理状态统计:")
        print(f"   已处理文件: {processed_files_count} 个")
        print(f"   已处理记录: {total_processed_records:,} 条")
        
        if self.processed_state.get('last_update'):
            print(f"   最后更新: {self.processed_state['last_update']}")
        
        # 3. 最近处理历史
        history = self.processed_state.get('processing_history', [])
        if history:
            print(f"\n🕒 最近处理记录:")
            for record in history[-5:]:  # 显示最近5条
                date_str = record.get('date', 'Unknown')
                files = record.get('files', 0)
                records = record.get('records', 0)
                duration = record.get('duration', 0)
                processed_at = record.get('processed_at', '')[:19].replace('T', ' ')
                print(f"     {date_str} - {processed_at}: {files} 文件, {records:,} 记录, {duration:.1f}s")
        
        # 4. 会话统计
        if self.session_stats['start_time']:
            print(f"\n📋 当前会话统计:")
            print(f"   开始时间: {self.session_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            if self.session_stats['end_time']:
                print(f"   结束时间: {self.session_stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   处理文件: {self.session_stats['total_files_processed']} 个")
            print(f"   处理记录: {self.session_stats['total_records_written']:,} 条")
            print(f"   处理行数: {self.session_stats['total_lines_processed']:,} 行")
            if self.session_stats['total_errors'] > 0:
                print(f"   错误数量: {self.session_stats['total_errors']:,} 条")
    
    def clear_all_data(self):
        """清空所有数据（开发环境使用）"""
        self.logger.info("开始清空所有数据")
        
        print("⚠️  警告：这将清空所有ETL数据和处理状态")
        print("1. 清空ClickHouse数据库表")
        print("2. 重置处理状态文件")
        
        # 连接数据库进行清理
        if not self.writer.connect():
            print("❌ 数据库连接失败")
            return
        
        try:
            # 执行表清空 - 使用DWD Writer的简单清空方法
            tables_to_clear = ['dwd_nginx_enriched_v2']  # 主要数据表
            cleared_count = 0
            
            for table_name in tables_to_clear:
                try:
                    result = self.writer.client.command(f"TRUNCATE TABLE {table_name}")
                    print(f"✅ 已清空表: {table_name}")
                    cleared_count += 1
                except Exception as e:
                    print(f"❌ 清空表失败 {table_name}: {e}")
            
            print(f"✅ 成功清空 {cleared_count}/{len(tables_to_clear)} 个表")
            
            # 重置状态文件
            self.processed_state = {
                'processed_files': {},
                'last_update': None,
                'total_processed_records': 0,
                'processing_history': []
            }
            self.save_state()
            print("✅ 处理状态已重置")
            
            # 重置会话统计
            self.session_stats = {
                'start_time': None,
                'end_time': None,
                'total_files_processed': 0,
                'total_lines_processed': 0,
                'total_records_written': 0,
                'total_errors': 0,
                'processing_errors': []
            }
            
        finally:
            self.writer.close()
    
    def interactive_menu(self):
        """交互式菜单"""
        while True:
            print("\n" + "=" * 80)
            print("🚀 智能化ETL控制器 - 基于Phase 1成功验证")
            print("=" * 80)
            print("1. 🔄 处理所有未处理的日志 (推荐)")
            print("2. 📅 处理指定日期的日志")
            print("3. 📊 查看系统状态")
            print("4. 🧪 测试模式处理 (不写入数据库)")
            print("5. 🧹 清空所有数据 (开发环境)")
            print("6. 🔁 强制重新处理所有日志")
            print("0. 👋 退出")
            print("-" * 80)
            
            try:
                choice = input("请选择操作 [0-6]: ").strip()
                
                if choice == '0':
                    print("👋 再见！")
                    break
                
                elif choice == '1':
                    print("\\n🔄 开始处理所有未处理的日志...")
                    
                    # 询问是否限制处理行数
                    limit_input = input("是否限制每个文件的处理行数？(留空表示不限制): ").strip()
                    limit = int(limit_input) if limit_input.isdigit() else None
                    
                    result = self.process_all_unprocessed_logs(test_mode=False, limit=limit)
                    self._print_batch_process_result(result)
                    input("\\n按回车键继续...")
                
                elif choice == '2':
                    date_str = input("\\n请输入日期 (YYYYMMDD格式，如: 20250829): ").strip()
                    if not self._validate_date_format(date_str):
                        continue
                    
                    force = input("是否强制重新处理？(y/N): ").strip().lower() == 'y'
                    limit_input = input("是否限制每个文件的处理行数？(留空表示不限制): ").strip()
                    limit = int(limit_input) if limit_input.isdigit() else None
                    
                    print(f"\\n🔄 开始处理 {date_str} 的日志...")
                    result = self.process_specific_date(date_str, force, test_mode=False, limit=limit)
                    self._print_single_date_result(result)
                    input("\\n按回车键继续...")
                
                elif choice == '3':
                    print()
                    self.show_status()
                    input("\\n按回车键继续...")
                
                elif choice == '4':
                    print("\\n🧪 测试模式处理")
                    sub_choice = input("选择: 1)处理所有未处理日志 2)处理指定日期 [1-2]: ").strip()
                    
                    if sub_choice == '1':
                        limit_input = input("限制每个文件的处理行数 (建议10-100): ").strip()
                        limit = int(limit_input) if limit_input.isdigit() else 10
                        
                        print("\\n🧪 测试模式：处理所有未处理的日志...")
                        result = self.process_all_unprocessed_logs(test_mode=True, limit=limit)
                        self._print_batch_process_result(result)
                        
                    elif sub_choice == '2':
                        date_str = input("请输入日期 (YYYYMMDD格式): ").strip()
                        if not self._validate_date_format(date_str):
                            continue
                        
                        limit_input = input("限制每个文件的处理行数 (建议10-100): ").strip()
                        limit = int(limit_input) if limit_input.isdigit() else 10
                        
                        print(f"\\n🧪 测试模式：处理 {date_str} 的日志...")
                        result = self.process_specific_date(date_str, False, test_mode=True, limit=limit)
                        self._print_single_date_result(result)
                    
                    input("\\n按回车键继续...")
                
                elif choice == '5':
                    print("\\n⚠️  清空所有数据")
                    confirm = input("确认清空所有ETL数据？(y/N): ").strip().lower()
                    if confirm == 'y':
                        second_confirm = input("再次确认！输入 'CLEAR' 确认删除: ").strip()
                        if second_confirm == 'CLEAR':
                            self.clear_all_data()
                        else:
                            print("❌ 确认失败，操作已取消")
                    else:
                        print("❌ 操作已取消")
                    input("\\n按回车键继续...")
                
                elif choice == '6':
                    print("\\n⚠️  强制重新处理所有日志")
                    print("这将忽略处理状态，重新处理所有日志文件")
                    confirm = input("确认执行？(y/N): ").strip().lower()
                    if confirm == 'y':
                        limit_input = input("是否限制每个文件的处理行数？(留空表示不限制): ").strip()
                        limit = int(limit_input) if limit_input.isdigit() else None
                        
                        print("\\n🔄 开始强制重新处理所有日志...")
                        
                        # 临时备份状态
                        backup_state = self.processed_state.copy()
                        
                        # 清空状态以强制重处理
                        self.processed_state = {
                            'processed_files': {},
                            'last_update': None,
                            'total_processed_records': 0,
                            'processing_history': []
                        }
                        
                        result = self.process_all_unprocessed_logs(test_mode=False, limit=limit)
                        
                        # 如果失败，恢复状态
                        if not result['success']:
                            self.processed_state = backup_state
                            print("❌ 处理失败，已恢复原始状态")
                        
                        self._print_batch_process_result(result)
                    else:
                        print("❌ 操作已取消")
                    input("\\n按回车键继续...")
                
                else:
                    print("❌ 无效选择，请输入 0-6")
                    input("按回车键继续...")
                    
            except KeyboardInterrupt:
                print("\\n\\n👋 用户中断，再见！")
                break
            except Exception as e:
                print(f"\\n❌ 操作过程中发生错误: {e}")
                input("按回车键继续...")
    
    # === 私有辅助方法 ===
    
    def _write_batch(self, batch: List[Dict[str, Any]], test_mode: bool) -> Dict[str, Any]:
        """写入批量数据 - 基于simple_etl_controller"""
        if test_mode:
            return {
                'success': True,
                'count': len(batch),
                'message': f'测试模式：模拟写入 {len(batch)} 条记录'
            }
        else:
            return self.writer.write_batch(batch)
    
    def _create_error_result(self, error: str) -> Dict[str, Any]:
        """创建错误结果"""
        return {
            'success': False,
            'error': error,
            'stats': {},
            'timestamp': datetime.now()
        }
    
    def _validate_date_format(self, date_str: str) -> bool:
        """验证日期格式"""
        if not date_str or len(date_str) != 8 or not date_str.isdigit():
            print("❌ 日期格式错误，请使用YYYYMMDD格式")
            return False
        
        try:
            datetime.strptime(date_str, '%Y%m%d')
            return True
        except ValueError:
            print("❌ 无效的日期")
            return False
    
    def _print_batch_process_result(self, result: Dict[str, Any]):
        """打印批量处理结果"""
        print("\\n" + "=" * 60)
        if result['success']:
            print("✅ 批量处理成功!")
            print("=" * 60)
            print(f"📄 处理日期数: {result.get('processed_dates', 0)} 个")
            print(f"📊 总记录数: {result.get('total_records', 0):,} 条")
            print(f"⏱️  总耗时: {result.get('duration', 0):.2f} 秒")
            
            if result.get('total_records', 0) > 0 and result.get('duration', 0) > 0:
                speed = result['total_records'] / result['duration']
                print(f"🚀 处理速度: {speed:.1f} 记录/秒")
        else:
            print("❌ 批量处理失败!")
            print("=" * 60)
            print(f"❌ 错误: {result.get('error', '未知错误')}")
            errors = result.get('errors', [])
            if errors:
                print(f"📋 详细错误 ({len(errors)} 个):")
                for i, error in enumerate(errors[:5], 1):
                    print(f"   {i}. {error}")
                if len(errors) > 5:
                    print(f"   ... 还有 {len(errors) - 5} 个错误")
        print("=" * 60)
    
    def _print_single_date_result(self, result: Dict[str, Any]):
        """打印单个日期的处理结果"""
        print("\\n" + "=" * 60)
        if result['success']:
            print("✅ 日期处理成功!")
            print("=" * 60)
            print(f"📅 处理日期: {result['date']}")
            print(f"📄 处理文件: {result['processed_files']} 个")
            print(f"📊 总记录数: {result['total_records']:,} 条")
            print(f"⏱️  总耗时: {result['duration']:.2f} 秒")
            
            if result['total_records'] > 0 and result['duration'] > 0:
                speed = result['total_records'] / result['duration']
                print(f"🚀 处理速度: {speed:.1f} 记录/秒")
        else:
            print("❌ 日期处理失败!")
            print("=" * 60)
            print(f"📅 处理日期: {result['date']}")
            print(f"❌ 错误: {result.get('error', '未知错误')}")
            errors = result.get('errors', [])
            if errors:
                print(f"📋 详细错误:")
                for i, error in enumerate(errors, 1):
                    print(f"   {i}. {error}")
        print("=" * 60)


def main():
    """主函数"""
    import logging
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(
        description='智能化ETL控制器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python intelligent_etl_controller.py                          # 交互式菜单 (推荐)
  python intelligent_etl_controller.py process --date 20250829  # 处理指定日期
  python intelligent_etl_controller.py process --date 20250829 --force  # 强制重新处理
  python intelligent_etl_controller.py process-all              # 处理所有未处理日志
  python intelligent_etl_controller.py status                   # 查看系统状态
  python intelligent_etl_controller.py clear-all                # 清空所有数据 (开发环境)
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # process命令
    process_parser = subparsers.add_parser('process', help='处理指定日期的日志')
    process_parser.add_argument('--date', required=True, help='日期 (YYYYMMDD格式)')
    process_parser.add_argument('--force', action='store_true', help='强制重新处理')
    process_parser.add_argument('--limit', type=int, help='限制每个文件的处理行数')
    process_parser.add_argument('--test', action='store_true', help='测试模式，不写入数据库')
    
    # 其他命令
    process_all_parser = subparsers.add_parser('process-all', help='处理所有未处理的日志')
    process_all_parser.add_argument('--limit', type=int, help='限制每个文件的处理行数')
    process_all_parser.add_argument('--test', action='store_true', help='测试模式，不写入数据库')
    
    subparsers.add_parser('status', help='查看系统状态')
    subparsers.add_parser('clear-all', help='清空所有数据 (开发环境使用)')
    
    args = parser.parse_args()
    
    # 初始化控制器
    controller = IntelligentETLController()
    
    # 如果没有参数，显示交互式菜单
    if not args.command:
        controller.interactive_menu()
        return
    
    # 执行对应命令
    if args.command == 'process':
        if not controller._validate_date_format(args.date):
            return
        
        result = controller.process_specific_date(
            args.date, 
            args.force, 
            test_mode=args.test,
            limit=args.limit
        )
        controller._print_single_date_result(result)
    
    elif args.command == 'process-all':
        result = controller.process_all_unprocessed_logs(
            test_mode=args.test,
            limit=args.limit
        )
        controller._print_batch_process_result(result)
    
    elif args.command == 'status':
        controller.show_status()
    
    elif args.command == 'clear-all':
        confirm = input("⚠️  确认清空所有数据？这将删除数据库中的所有日志数据 (y/N): ")
        if confirm.lower() == 'y':
            second_confirm = input("再次确认！输入 'CLEAR' 确认删除: ").strip()
            if second_confirm == 'CLEAR':
                controller.clear_all_data()
            else:
                print("❌ 确认失败，操作已取消")
        else:
            print("❌ 操作已取消")


if __name__ == "__main__":
    main()