#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高性能ETL控制器 - 架构优化版本
基于backup-20250913-105701版本，保留有效优化，移除过度复杂化的部分

优化策略 (B. 架构优化):
1. 保留基础多线程和批处理
2. 简化缓存策略，移除过度优化
3. 增强错误信息输出和诊断能力
4. 移除复杂的预编译正则和过度优化的组件
5. 降低批处理大小和线程数到合理范围
"""

import sys
import os
import json
import time
import argparse
import threading
import queue
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import gc
import traceback

# 添加路径以导入其他模块
current_dir = Path(__file__).parent
etl_root = current_dir.parent
sys.path.append(str(etl_root))

from parsers.base_log_parser import BaseLogParser
from processors.field_mapper import FieldMapper
from writers.dwd_writer import DWDWriter

class HighPerformanceETLController:
    """高性能ETL控制器 - 架构优化版本"""

    def __init__(self,
                 base_log_dir: str = None,
                 state_file: str = None,
                 batch_size: int = 2000,        # 恢复到合理的批量大小
                 max_workers: int = 4,          # 恢复到合理的线程数
                 connection_pool_size: int = None,
                 memory_limit_mb: int = 512):
        """
        初始化高性能ETL控制器

        Args:
            batch_size: 批处理大小（恢复到2000，平衡性能和稳定性）
            max_workers: 最大工作线程数（恢复到4，避免资源竞争）
            connection_pool_size: 数据库连接池大小
            memory_limit_mb: 内存使用限制（MB）
        """
        # 基础配置
        self.base_log_dir = Path(base_log_dir) if base_log_dir else \
            Path("D:/project/nginx-log-analyzer/nginx-analytics-warehouse/nginx_logs")
        self.state_file = Path(state_file) if state_file else \
            Path(etl_root / "processed_logs_state.json")

        # 性能优化配置 - 回归保守但稳定的设置
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.connection_pool_size = connection_pool_size if connection_pool_size is not None else max_workers
        self.memory_limit_mb = memory_limit_mb

        # 日志配置
        import logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # 创建详细的console handler
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

        # === 增强的错误统计和诊断系统 ===
        # 必须先初始化这个，因为其他方法会用到
        self.session_stats = {
            'start_time': None,
            'end_time': None,
            'total_files_processed': 0,
            'total_lines_processed': 0,
            'total_records_written': 0,
            'total_errors': 0,
            'cache_hit_rate': 0.0,
            'avg_processing_speed': 0.0,
            'peak_memory_usage_mb': 0,
            'processing_errors': [],
            # === 详细错误统计 ===
            'error_stats': {
                'parsing_errors': 0,        # 解析错误
                'field_mapping_errors': 0,  # 字段映射错误
                'database_write_errors': 0, # 数据库写入错误
                'connection_errors': 0,     # 连接错误
                'timeout_errors': 0,        # 超时错误
                'critical_errors': 0,       # 致命错误
                'warning_errors': 0,        # 警告级错误
                'skipped_lines': 0,         # 跳过的行数
                'invalid_records': 0,       # 无效记录数
                'file_access_errors': 0,    # 文件访问错误
                'memory_errors': 0,         # 内存相关错误
                'thread_errors': 0          # 线程相关错误
            },
            'error_details': [],            # 详细错误记录
            'file_error_stats': {},         # 按文件统计错误
            'performance_warnings': [],      # 性能警告
            'diagnostic_info': {             # 诊断信息
                'thread_status': {},
                'connection_status': {},
                'memory_usage_history': [],
                'processing_speed_history': []
            }
        }

        # 线程安全的组件池
        self.parser_pool = [BaseLogParser() for _ in range(max_workers)]
        self.mapper_pool = [FieldMapper() for _ in range(max_workers)]
        self.writer_pool = []

        # 简化的缓存策略 - 移除过度优化
        self.ua_cache = {}  # User-Agent解析缓存
        self.uri_cache = {}  # URI解析缓存
        self.cache_hit_stats = {'ua_hits': 0, 'uri_hits': 0, 'total_requests': 0}

        # 线程同步
        self.result_queue = queue.Queue()
        self.error_queue = queue.Queue()
        self.stats_lock = threading.Lock()

        # 处理状态
        self.processed_state = self.load_state()

        # 初始化连接池 - 放在最后，因为需要使用session_stats
        self._init_connection_pool()

        self.logger.info("🚀 高性能ETL控制器初始化完成 - 架构优化版本")
        self.logger.info(f"📁 日志目录: {self.base_log_dir}")
        self.logger.info(f"⚙️ 批处理大小: {self.batch_size:,}")
        self.logger.info(f"🧵 最大工作线程: {self.max_workers}")
        self.logger.info(f"🔗 连接池大小: {self.connection_pool_size}")

        # 性能建议
        if self.batch_size > 5000:
            self.record_performance_warning(
                f"批处理大小 {self.batch_size} 可能过大，建议使用 2000-3000",
                {'batch_size': self.batch_size, 'recommendation': '2000-3000'}
            )

        if self.max_workers > 6:
            self.record_performance_warning(
                f"线程数 {self.max_workers} 可能过高，建议使用 4-6",
                {'max_workers': self.max_workers, 'recommendation': '4-6'}
            )

    def _init_connection_pool(self):
        """初始化数据库连接池"""
        self.logger.info("🔗 初始化数据库连接池...")
        success_count = 0
        failed_count = 0

        for i in range(self.connection_pool_size):
            try:
                writer = DWDWriter()
                if writer.connect():
                    self.writer_pool.append(writer)
                    success_count += 1
                    self.logger.info(f"✅ 连接 {i+1} 建立成功")
                else:
                    failed_count += 1
                    error_msg = f"连接 {i+1} 建立失败 - connect() 返回 False"
                    self.logger.error(f"❌ {error_msg}")
                    self.record_error('connection_errors', error_msg, {'connection_id': i+1})

            except Exception as e:
                failed_count += 1
                error_msg = f"连接 {i+1} 初始化异常: {str(e)}"
                self.logger.error(f"❌ {error_msg}")
                self.record_error('connection_errors', error_msg, {
                    'connection_id': i+1,
                    'exception_type': type(e).__name__,
                    'traceback': traceback.format_exc()
                })

        if not self.writer_pool:
            critical_error = "无法建立任何数据库连接"
            self.record_error('critical_errors', critical_error)
            raise RuntimeError(f"❌ {critical_error}")

        # 记录连接池状态
        self.session_stats['diagnostic_info']['connection_status'] = {
            'total_requested': self.connection_pool_size,
            'successful': success_count,
            'failed': failed_count,
            'success_rate': (success_count / self.connection_pool_size) * 100
        }

        self.logger.info(f"🔗 连接池初始化完成：{success_count}/{self.connection_pool_size} 个连接成功")

        if failed_count > 0:
            self.record_performance_warning(
                f"连接池初始化不完整：{failed_count} 个连接失败",
                {'failed_connections': failed_count, 'total_connections': self.connection_pool_size}
            )

    def get_writer(self) -> Optional[DWDWriter]:
        """从连接池获取Writer（线程安全）"""
        with self.stats_lock:
            if self.writer_pool:
                return self.writer_pool.pop()
        return None

    def return_writer(self, writer: DWDWriter):
        """归还Writer到连接池（线程安全）"""
        if writer is None:
            return

        with self.stats_lock:
            if len(self.writer_pool) < self.connection_pool_size:
                self.writer_pool.append(writer)

    def cached_ua_parse(self, user_agent: str, mapper: FieldMapper) -> Dict[str, str]:
        """简化的User-Agent解析缓存"""
        if not user_agent:
            return {}

        with self.stats_lock:
            self.cache_hit_stats['total_requests'] += 1

            if user_agent in self.ua_cache:
                self.cache_hit_stats['ua_hits'] += 1
                return self.ua_cache[user_agent]

        # 缓存未命中，执行解析
        try:
            result = mapper._parse_user_agent(user_agent)
        except Exception as e:
            error_msg = f"User-Agent解析失败: {str(e)}"
            self.record_error('field_mapping_errors', error_msg, {
                'user_agent': user_agent[:100],  # 只记录前100个字符
                'exception_type': type(e).__name__
            })
            result = {}

        with self.stats_lock:
            # 限制缓存大小，防止内存溢出
            if len(self.ua_cache) < 5000:  # 降低缓存大小
                self.ua_cache[user_agent] = result

        return result

    def cached_uri_parse(self, uri: str, mapper: FieldMapper) -> Dict[str, str]:
        """简化的URI结构解析缓存"""
        if not uri:
            return {}

        with self.stats_lock:
            if uri in self.uri_cache:
                self.cache_hit_stats['uri_hits'] += 1
                return self.uri_cache[uri]

        # 缓存未命中，执行解析
        try:
            result = mapper._parse_uri_structure(uri)
        except Exception as e:
            error_msg = f"URI解析失败: {str(e)}"
            self.record_error('field_mapping_errors', error_msg, {
                'uri': uri[:200],  # 只记录前200个字符
                'exception_type': type(e).__name__
            })
            result = {}

        with self.stats_lock:
            # 限制缓存大小
            if len(self.uri_cache) < 3000:  # 降低缓存大小
                self.uri_cache[uri] = result

        return result

    def process_file_batch(self, file_paths: List[Path], thread_id: int,
                          test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """
        批量处理文件（单线程执行）- 增强错误处理版本
        """
        start_time = time.time()
        thread_name = f"Thread-{thread_id}"

        self.logger.info(f"🧵{thread_id} 开始处理 {len(file_paths)} 个文件")

        # 更新线程状态
        with self.stats_lock:
            self.session_stats['diagnostic_info']['thread_status'][thread_name] = {
                'status': 'running',
                'start_time': start_time,
                'files_assigned': len(file_paths),
                'current_file': None
            }

        # 获取线程专用组件
        parser = self.parser_pool[thread_id % len(self.parser_pool)]
        mapper = self.mapper_pool[thread_id % len(self.mapper_pool)]
        writer = None

        if not test_mode:
            writer = self.get_writer()
            if not writer:
                error_msg = f"线程 {thread_id} 无法获取数据库连接"
                self.record_error('connection_errors', error_msg, {'thread_id': thread_id})
                return {
                    'success': False,
                    'error': error_msg,
                    'thread_id': thread_id,
                    'error_details': {'error_type': 'connection_error'}
                }

        try:
            batch_stats = {
                'thread_id': thread_id,
                'total_files': len(file_paths),
                'processed_files': 0,
                'total_records': 0,
                'total_lines': 0,
                'errors': [],
                'file_results': [],
                'detailed_errors': []
            }

            # 批量缓冲区
            mega_batch = []

            for file_index, file_path in enumerate(file_paths):
                # 更新当前处理文件状态
                with self.stats_lock:
                    self.session_stats['diagnostic_info']['thread_status'][thread_name]['current_file'] = file_path.name

                self.logger.info(f"🧵{thread_id} 处理文件 {file_index+1}/{len(file_paths)}: {file_path.name}")

                file_start = time.time()
                file_records = 0
                file_lines = 0
                file_errors = []

                try:
                    # 检查文件是否存在和可读
                    if not file_path.exists():
                        error_msg = f"文件不存在: {file_path}"
                        self.record_error('file_access_errors', error_msg, {
                            'file_path': str(file_path),
                            'thread_id': thread_id
                        })
                        file_errors.append(error_msg)
                        continue

                    if not file_path.is_file():
                        error_msg = f"路径不是文件: {file_path}"
                        self.record_error('file_access_errors', error_msg, {
                            'file_path': str(file_path),
                            'thread_id': thread_id
                        })
                        file_errors.append(error_msg)
                        continue

                    # 流式处理文件
                    line_number = 0
                    for parsed_data in parser.parse_file(file_path):
                        line_number += 1
                        file_lines += 1

                        if parsed_data:
                            try:
                                # 使用缓存优化的映射
                                user_agent = parsed_data.get('user_agent', '')
                                uri = parsed_data.get('request', '').split(' ')[1] if 'request' in parsed_data else ''

                                # 使用缓存
                                if user_agent:
                                    parsed_data['_cached_ua'] = self.cached_ua_parse(user_agent, mapper)
                                if uri:
                                    parsed_data['_cached_uri'] = self.cached_uri_parse(uri, mapper)

                                # 字段映射
                                mapped_data = mapper.map_to_dwd(parsed_data, file_path.name)
                                mega_batch.append(mapped_data)
                                file_records += 1

                                # 检查批量大小
                                if len(mega_batch) >= self.batch_size:
                                    if not test_mode:
                                        write_result = writer.write_batch(mega_batch)
                                        if not write_result['success']:
                                            error_msg = f"批量写入失败: {write_result['error']}"
                                            self.record_error('database_write_errors', error_msg, {
                                                'file_name': file_path.name,
                                                'thread_id': thread_id,
                                                'batch_size': len(mega_batch)
                                            })
                                            file_errors.append(error_msg)

                                    mega_batch.clear()
                                    gc.collect()  # 强制垃圾回收

                                # 检查限制
                                if limit and file_records >= limit:
                                    self.logger.info(f"🧵{thread_id} 达到行数限制 {limit}，停止处理 {file_path.name}")
                                    break

                            except Exception as e:
                                error_msg = f"记录处理错误 (行 {line_number}): {str(e)}"
                                self.record_error('field_mapping_errors', error_msg, {
                                    'file_name': file_path.name,
                                    'line_number': line_number,
                                    'thread_id': thread_id,
                                    'exception_type': type(e).__name__,
                                    'traceback': traceback.format_exc()
                                })
                                file_errors.append(error_msg)
                        else:
                            # 解析失败的行
                            self.record_error('parsing_errors', f"解析失败 (行 {line_number})", {
                                'file_name': file_path.name,
                                'line_number': line_number,
                                'thread_id': thread_id
                            })

                except Exception as e:
                    error_msg = f"文件处理异常 {file_path.name}: {str(e)}"
                    self.logger.error(f"🧵{thread_id} {error_msg}")
                    self.record_error('file_access_errors', error_msg, {
                        'file_name': file_path.name,
                        'thread_id': thread_id,
                        'exception_type': type(e).__name__,
                        'traceback': traceback.format_exc()
                    })
                    file_errors.append(error_msg)

                file_duration = time.time() - file_start
                file_result = {
                    'file': file_path.name,
                    'records': file_records,
                    'lines': file_lines,
                    'duration': file_duration,
                    'errors': file_errors,
                    'error_count': len(file_errors)
                }
                batch_stats['file_results'].append(file_result)

                batch_stats['processed_files'] += 1
                batch_stats['total_records'] += file_records
                batch_stats['total_lines'] += file_lines
                batch_stats['errors'].extend(file_errors)

                # 记录处理速度
                speed = file_records / file_duration if file_duration > 0 else 0
                self.logger.info(f"🧵{thread_id} 完成 {file_path.name}: {file_records} 记录, {file_duration:.2f}s, {speed:.1f} rec/s")

            # 处理剩余批次
            if mega_batch:
                if not test_mode:
                    try:
                        write_result = writer.write_batch(mega_batch)
                        if not write_result['success']:
                            error_msg = f"最终批次写入失败: {write_result['error']}"
                            self.record_error('database_write_errors', error_msg, {
                                'thread_id': thread_id,
                                'batch_size': len(mega_batch)
                            })
                            batch_stats['errors'].append(error_msg)
                    except Exception as e:
                        error_msg = f"最终批次写入异常: {str(e)}"
                        self.record_error('database_write_errors', error_msg, {
                            'thread_id': thread_id,
                            'exception_type': type(e).__name__,
                            'traceback': traceback.format_exc()
                        })
                        batch_stats['errors'].append(error_msg)
                mega_batch.clear()

            batch_stats['duration'] = time.time() - start_time
            batch_stats['success'] = len(batch_stats['errors']) == 0

            # 更新线程完成状态
            with self.stats_lock:
                self.session_stats['diagnostic_info']['thread_status'][thread_name].update({
                    'status': 'completed',
                    'end_time': time.time(),
                    'duration': batch_stats['duration'],
                    'records_processed': batch_stats['total_records'],
                    'files_processed': batch_stats['processed_files'],
                    'error_count': len(batch_stats['errors'])
                })

            return batch_stats

        except Exception as e:
            critical_error = f"线程 {thread_id} 发生致命错误: {str(e)}"
            self.logger.error(f"❌ {critical_error}")
            self.record_error('critical_errors', critical_error, {
                'thread_id': thread_id,
                'exception_type': type(e).__name__,
                'traceback': traceback.format_exc()
            })

            # 更新线程错误状态
            with self.stats_lock:
                self.session_stats['diagnostic_info']['thread_status'][thread_name].update({
                    'status': 'error',
                    'error': str(e),
                    'end_time': time.time()
                })

            return {
                'success': False,
                'error': critical_error,
                'thread_id': thread_id,
                'error_details': {
                    'error_type': 'critical_error',
                    'exception_type': type(e).__name__,
                    'traceback': traceback.format_exc()
                }
            }

        finally:
            # 归还连接到池
            if writer:
                self.return_writer(writer)

    def process_date_parallel(self, date_str: str, force_reprocess: bool = False,
                             test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """
        并行处理指定日期的所有日志 - 增强错误处理版本
        """
        self.logger.info(f"🚀 开始并行处理 {date_str} 的日志")
        self.session_stats['start_time'] = datetime.now()
        start_time = time.time()

        # 检查日期目录
        date_dir = self.base_log_dir / date_str
        if not date_dir.exists():
            error_msg = f'日期目录不存在: {date_dir}'
            self.record_error('file_access_errors', error_msg, {'date': date_str})
            return {
                'success': False,
                'error': error_msg,
                'date': date_str,
                'error_details': {'error_type': 'directory_not_found'}
            }

        # 获取所有日志文件
        try:
            all_log_files = list(date_dir.glob("*.log"))
        except Exception as e:
            error_msg = f'扫描日志文件失败: {str(e)}'
            self.record_error('file_access_errors', error_msg, {
                'date': date_str,
                'directory': str(date_dir),
                'exception_type': type(e).__name__
            })
            return {
                'success': False,
                'error': error_msg,
                'date': date_str,
                'error_details': {'error_type': 'file_scan_error'}
            }

        if not all_log_files:
            error_msg = f'目录中没有找到.log文件: {date_dir}'
            self.record_error('file_access_errors', error_msg, {'date': date_str})
            return {
                'success': False,
                'error': error_msg,
                'date': date_str,
                'error_details': {'error_type': 'no_log_files'}
            }

        # 过滤需要处理的文件
        if not force_reprocess:
            pending_files = [f for f in all_log_files if not self.is_file_processed(f)]
        else:
            pending_files = all_log_files

        if not pending_files:
            self.logger.info(f"📋 日期 {date_str} 的所有文件都已处理")
            return {
                'success': True,
                'processed_files': 0,
                'message': '所有文件都已处理',
                'date': date_str,
                'total_records': 0,
                'duration': 0,
                'processing_speed': 0,
                'cache_hit_rate': 0
            }

        self.logger.info(f"📁 找到 {len(pending_files)} 个待处理文件")

        # 将文件分批分配给线程
        files_per_thread = max(1, len(pending_files) // self.max_workers)
        file_batches = []

        for i in range(0, len(pending_files), files_per_thread):
            batch = pending_files[i:i + files_per_thread]
            if batch:
                file_batches.append(batch)

        actual_workers = min(self.max_workers, len(file_batches))
        self.logger.info(f"🧵 分配 {len(file_batches)} 个批次给 {actual_workers} 个线程")

        # 并行处理
        all_results = []
        total_records = 0
        total_errors = []

        try:
            with ThreadPoolExecutor(max_workers=actual_workers) as executor:
                # 提交任务
                future_to_batch = {}
                for i, batch in enumerate(file_batches):
                    future = executor.submit(self.process_file_batch, batch, i, test_mode, limit)
                    future_to_batch[future] = i

                # 收集结果
                for future in as_completed(future_to_batch):
                    batch_id = future_to_batch[future]
                    try:
                        result = future.result(timeout=300)  # 5分钟超时
                        all_results.append(result)

                        if result['success']:
                            total_records += result['total_records']
                            self.logger.info(f"✅ 批次 {batch_id} 完成: {result['processed_files']} 文件, "
                                           f"{result['total_records']} 记录")
                        else:
                            total_errors.extend(result.get('errors', []))
                            self.logger.error(f"❌ 批次 {batch_id} 失败: {result.get('error', '未知错误')}")

                    except Exception as e:
                        error_msg = f"批次 {batch_id} 执行异常: {str(e)}"
                        self.logger.error(f"❌ {error_msg}")
                        self.record_error('thread_errors', error_msg, {
                            'batch_id': batch_id,
                            'exception_type': type(e).__name__,
                            'traceback': traceback.format_exc()
                        })
                        total_errors.append(error_msg)

        except Exception as e:
            error_msg = f"线程池执行异常: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            self.record_error('critical_errors', error_msg, {
                'date': date_str,
                'exception_type': type(e).__name__,
                'traceback': traceback.format_exc()
            })
            return {
                'success': False,
                'error': error_msg,
                'date': date_str,
                'error_details': {'error_type': 'thread_pool_error'}
            }

        # 更新处理状态（非测试模式）
        if not test_mode:
            for file_path in pending_files:
                # 根据处理结果更新状态
                file_processed = any(
                    any(fr['file'] == file_path.name for fr in r.get('file_results', []))
                    for r in all_results if r.get('success', False)
                )
                if file_processed:
                    self.mark_file_processed(file_path, 0, 0)  # 简化状态更新
            self.save_state()

        # 计算缓存命中率
        cache_hit_rate = 0.0
        if self.cache_hit_stats['total_requests'] > 0:
            total_hits = self.cache_hit_stats['ua_hits'] + self.cache_hit_stats['uri_hits']
            cache_hit_rate = (total_hits / (self.cache_hit_stats['total_requests'] * 2)) * 100

        duration = time.time() - start_time
        self.session_stats['end_time'] = datetime.now()

        # 性能统计
        speed = total_records / duration if duration > 0 else 0
        self.session_stats['avg_processing_speed'] = speed
        self.session_stats['cache_hit_rate'] = cache_hit_rate
        self.session_stats['total_records_written'] = total_records

        success = len(total_errors) == 0

        result = {
            'success': success,
            'date': date_str,
            'processed_files': sum(r.get('processed_files', 0) for r in all_results),
            'total_records': total_records,
            'duration': duration,
            'processing_speed': speed,
            'cache_hit_rate': cache_hit_rate,
            'errors': total_errors,
            'thread_results': all_results,
            'error_details': {
                'total_errors': len(total_errors),
                'thread_count': actual_workers,
                'batch_count': len(file_batches)
            }
        }

        if success:
            self.logger.info(f"🎉 日期 {date_str} 并行处理完成!")
            self.logger.info(f"📊 {result['processed_files']} 文件, {total_records:,} 记录")
            self.logger.info(f"⏱️  耗时 {duration:.2f}s, 速度 {speed:.1f} 记录/秒")
            self.logger.info(f"🎯 缓存命中率: {cache_hit_rate:.1f}%")
        else:
            self.logger.error(f"❌ 日期 {date_str} 处理完成但有错误: {len(total_errors)} 个错误")

        return result

    # === 错误处理和统计方法 ===

    def record_error(self, error_type: str, error_msg: str, context: Dict[str, Any] = None):
        """记录错误信息和统计 - 增强版本"""
        timestamp = datetime.now()

        with self.stats_lock:
            # 更新错误统计
            if error_type in self.session_stats['error_stats']:
                self.session_stats['error_stats'][error_type] += 1
            else:
                self.session_stats['error_stats'][error_type] = 1

            self.session_stats['total_errors'] += 1

            # 记录详细错误信息
            error_detail = {
                'timestamp': timestamp.isoformat(),
                'error_type': error_type,
                'error_message': error_msg,
                'context': context or {}
            }

            self.session_stats['error_details'].append(error_detail)

            # 按文件统计错误
            if context and 'file_name' in context:
                file_name = context['file_name']
                if file_name not in self.session_stats['file_error_stats']:
                    self.session_stats['file_error_stats'][file_name] = {
                        'total_errors': 0,
                        'error_types': defaultdict(int),
                        'first_error_time': timestamp.isoformat()
                    }

                self.session_stats['file_error_stats'][file_name]['total_errors'] += 1
                self.session_stats['file_error_stats'][file_name]['error_types'][error_type] += 1
                self.session_stats['file_error_stats'][file_name]['last_error_time'] = timestamp.isoformat()

            # 限制错误详情数量，避免内存溢出
            if len(self.session_stats['error_details']) > 1000:
                self.session_stats['error_details'] = self.session_stats['error_details'][-500:]

        # 记录到日志
        if error_type in ['critical_errors', 'database_write_errors', 'connection_errors']:
            self.logger.error(f"❌ [{error_type}] {error_msg}")
        elif error_type in ['warning_errors', 'parsing_errors']:
            self.logger.warning(f"⚠️ [{error_type}] {error_msg}")
        else:
            self.logger.info(f"ℹ️ [{error_type}] {error_msg}")

    def record_performance_warning(self, warning_msg: str, context: Dict[str, Any] = None):
        """记录性能警告"""
        with self.stats_lock:
            warning = {
                'timestamp': datetime.now().isoformat(),
                'message': warning_msg,
                'context': context or {}
            }
            self.session_stats['performance_warnings'].append(warning)

            # 限制警告数量
            if len(self.session_stats['performance_warnings']) > 100:
                self.session_stats['performance_warnings'] = self.session_stats['performance_warnings'][-50:]

        self.logger.warning(f"⚠️ [性能警告] {warning_msg}")

    def get_error_summary(self) -> Dict[str, Any]:
        """获取错误摘要报告"""
        with self.stats_lock:
            total_errors = self.session_stats['total_errors']
            total_records = self.session_stats['total_records_written']

            if total_records == 0:
                error_rate = 0.0 if total_errors == 0 else 100.0
            else:
                error_rate = (total_errors / (total_records + total_errors)) * 100

            return {
                'total_errors': total_errors,
                'error_rate_percent': round(error_rate, 2),
                'error_breakdown': dict(self.session_stats['error_stats']),
                'files_with_errors': len(self.session_stats['file_error_stats']),
                'performance_warnings_count': len(self.session_stats['performance_warnings']),
                'most_common_errors': self._get_most_common_errors(),
                'error_trend': self._analyze_error_trend(),
                'thread_diagnostic': self.session_stats['diagnostic_info']['thread_status'],
                'connection_diagnostic': self.session_stats['diagnostic_info']['connection_status']
            }

    def _get_most_common_errors(self) -> List[Dict[str, Any]]:
        """获取最常见的错误类型"""
        error_counts = self.session_stats['error_stats']
        sorted_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)
        return [{'error_type': k, 'count': v} for k, v in sorted_errors[:5] if v > 0]

    def _analyze_error_trend(self) -> Dict[str, Any]:
        """分析错误趋势"""
        error_details = self.session_stats['error_details']
        if len(error_details) < 2:
            return {'trend': 'insufficient_data', 'recent_errors': len(error_details)}

        # 分析最近的错误
        recent_errors = error_details[-10:]
        error_types = defaultdict(int)
        for error in recent_errors:
            error_types[error['error_type']] += 1

        return {
            'trend': 'stable' if len(set(e['error_type'] for e in recent_errors)) <= 2 else 'varied',
            'recent_errors': len(recent_errors),
            'recent_error_types': dict(error_types)
        }

    def print_error_report(self):
        """打印详细的错误报告 - 增强版本"""
        summary = self.get_error_summary()

        print("\n" + "="*80)
        print("📊 详细错误统计报告 (架构优化版本)")
        print("="*80)

        if summary['total_errors'] == 0:
            print("✅ 没有发现错误 - 处理完全成功！")
            print("="*80)
            return

        print(f"📈 总错误数: {summary['total_errors']}")
        print(f"📈 错误率: {summary['error_rate_percent']}%")
        print(f"📁 有错误的文件数: {summary['files_with_errors']}")
        print(f"⚠️  性能警告数: {summary['performance_warnings_count']}")

        if summary['most_common_errors']:
            print(f"\n🔝 最常见错误:")
            for error in summary['most_common_errors']:
                print(f"   • {error['error_type']}: {error['count']} 次")

        print(f"\n📋 错误详细分类:")
        for error_type, count in summary['error_breakdown'].items():
            if count > 0:
                print(f"   • {error_type}: {count}")

        # 显示文件错误统计（只显示错误最多的前5个文件）
        if self.session_stats['file_error_stats']:
            print(f"\n📁 文件错误统计（前5个）:")
            file_errors = sorted(
                self.session_stats['file_error_stats'].items(),
                key=lambda x: x[1]['total_errors'],
                reverse=True
            )[:5]

            for file_name, stats in file_errors:
                print(f"   • {file_name}: {stats['total_errors']} 错误")
                # 显示主要错误类型
                top_error_types = sorted(
                    stats['error_types'].items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:3]
                for error_type, count in top_error_types:
                    print(f"     - {error_type}: {count}")

        # 显示线程诊断信息
        if summary['thread_diagnostic']:
            print(f"\n🧵 线程诊断信息:")
            for thread_name, status in summary['thread_diagnostic'].items():
                thread_status = status.get('status', 'unknown')
                error_count = status.get('error_count', 0)
                records = status.get('records_processed', 0)
                print(f"   • {thread_name}: {thread_status}, 错误: {error_count}, 记录: {records}")

        # 显示连接诊断信息
        if summary['connection_diagnostic']:
            conn_info = summary['connection_diagnostic']
            print(f"\n🔗 连接诊断信息:")
            print(f"   • 成功连接: {conn_info.get('successful', 0)}/{conn_info.get('total_requested', 0)}")
            print(f"   • 成功率: {conn_info.get('success_rate', 0):.1f}%")

        # 显示最近的性能警告
        if self.session_stats['performance_warnings']:
            print(f"\n⚠️  最近的性能警告（最近5个）:")
            recent_warnings = self.session_stats['performance_warnings'][-5:]
            for warning in recent_warnings:
                print(f"   • {warning['message']}")

        print("="*80)

    # === 继承的方法（保持兼容性）===

    def process_all_parallel(self, test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """并行处理所有未处理的日志"""
        self.logger.info("🚀 开始并行处理所有未处理的日志")
        self.session_stats['start_time'] = datetime.now()
        start_time = time.time()

        # 扫描所有日期目录
        log_files_by_date = self.scan_log_directories()
        if not log_files_by_date:
            return {'success': False, 'error': '没有找到日志文件'}

        total_records = 0
        processed_dates = 0
        all_errors = []
        date_results = []

        # 按日期顺序处理（但每个日期内部并行）
        for date_str in sorted(log_files_by_date.keys()):
            self.logger.info(f"📅 开始处理日期: {date_str}")

            result = self.process_date_parallel(date_str, force_reprocess=False,
                                              test_mode=test_mode, limit=limit)
            date_results.append(result)

            if result['success'] and result.get('processed_files', 0) > 0:
                processed_dates += 1
                total_records += result['total_records']
                self.logger.info(f"✅ {date_str} 完成: {result['total_records']:,} 记录")
            else:
                if result.get('errors'):
                    all_errors.extend(result['errors'])
                self.logger.warning(f"⚠️ {date_str} 跳过或失败")

        duration = time.time() - start_time
        success = len(all_errors) == 0
        overall_speed = total_records / duration if duration > 0 else 0

        self.session_stats['end_time'] = datetime.now()
        self.session_stats['avg_processing_speed'] = overall_speed

        return {
            'success': success,
            'processed_dates': processed_dates,
            'total_records': total_records,
            'duration': duration,
            'processing_speed': overall_speed,
            'errors': all_errors,
            'date_results': date_results
        }

    def load_state(self) -> Dict[str, Any]:
        """加载处理状态"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
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
        except Exception as e:
            self.logger.error(f"保存状态文件失败: {e}")

    def scan_log_directories(self) -> Dict[str, List[Path]]:
        """扫描日志目录"""
        if not self.base_log_dir.exists():
            return {}

        log_files_by_date = {}
        for date_dir in self.base_log_dir.iterdir():
            if date_dir.is_dir() and date_dir.name.isdigit() and len(date_dir.name) == 8:
                try:
                    datetime.strptime(date_dir.name, '%Y%m%d')
                    log_files = list(date_dir.glob("*.log"))
                    if log_files:
                        log_files_by_date[date_dir.name] = sorted(log_files)
                except ValueError:
                    continue

        return log_files_by_date

    def is_file_processed(self, file_path: Path) -> bool:
        """检查文件是否已处理"""
        file_key = str(file_path)
        return file_key in self.processed_state.get('processed_files', {})

    def mark_file_processed(self, file_path: Path, record_count: int, processing_time: float):
        """标记文件为已处理"""
        file_key = str(file_path)
        try:
            self.processed_state['processed_files'][file_key] = {
                'processed_at': datetime.now().isoformat(),
                'record_count': record_count,
                'processing_time': processing_time,
                'mtime': file_path.stat().st_mtime,
                'file_size': file_path.stat().st_size
            }
        except Exception as e:
            self.logger.error(f"标记文件状态失败 {file_path}: {e}")

    def show_performance_stats(self):
        """显示性能统计信息"""
        print("\n" + "=" * 80)
        print("🚀 高性能ETL控制器 - 性能统计报告 (架构优化版本)")
        print("=" * 80)

        print(f"⚙️  配置信息:")
        print(f"   批处理大小: {self.batch_size:,}")
        print(f"   最大工作线程: {self.max_workers}")
        print(f"   连接池大小: {self.connection_pool_size}")
        print(f"   当前可用连接: {len(self.writer_pool)}")

        print(f"\n📈 缓存统计:")
        print(f"   User-Agent缓存: {len(self.ua_cache)} 项")
        print(f"   URI缓存: {len(self.uri_cache)} 项")
        print(f"   缓存命中率: {self.session_stats.get('cache_hit_rate', 0):.1f}%")

        if self.session_stats.get('avg_processing_speed', 0) > 0:
            print(f"\n🏃 性能指标:")
            print(f"   平均处理速度: {self.session_stats['avg_processing_speed']:.1f} 记录/秒")
            print(f"   总处理记录数: {self.session_stats.get('total_records_written', 0):,}")

            if self.session_stats.get('start_time') and self.session_stats.get('end_time'):
                duration = (self.session_stats['end_time'] - self.session_stats['start_time']).total_seconds()
                print(f"   总处理时间: {duration:.1f} 秒")

        # 显示错误汇总
        if self.session_stats['total_errors'] > 0:
            print(f"\n❌ 错误汇总:")
            print(f"   总错误数: {self.session_stats['total_errors']}")
            top_errors = sorted(
                self.session_stats['error_stats'].items(),
                key=lambda x: x[1],
                reverse=True
            )[:3]
            for error_type, count in top_errors:
                if count > 0:
                    print(f"   {error_type}: {count}")

        print("=" * 80)

    def cleanup(self):
        """清理资源"""
        self.logger.info("🧹 清理资源...")

        # 关闭所有数据库连接
        for writer in self.writer_pool:
            try:
                writer.close()
            except:
                pass
        self.writer_pool.clear()

        # 清理缓存
        self.ua_cache.clear()
        self.uri_cache.clear()

        # 强制垃圾回收
        gc.collect()

        self.logger.info("✅ 资源清理完成")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    # === 简化的交互式菜单（保持兼容性）===

    def interactive_menu(self):
        """简化的交互式菜单"""
        while True:
            print("\n" + "=" * 80)
            print("🚀 高性能ETL控制器 - 架构优化版本")
            print("=" * 80)
            print("1. 🔥 处理所有未处理的日志")
            print("2. 📅 处理指定日期的日志")
            print("3. 📊 查看状态和性能统计")
            print("4. 🧪 测试模式处理")
            print("5. 📋 详细错误报告")
            print("0. 👋 退出")
            print("-" * 80)
            print(f"📊 当前配置: 批量{self.batch_size} | 线程{self.max_workers} | 连接池{self.connection_pool_size}")

            try:
                choice = input("请选择操作 [0-5]: ").strip()

                if choice == '0':
                    print("👋 再见！")
                    break

                elif choice == '1':
                    print("\n🔥 处理所有未处理的日志...")
                    limit_input = input("限制每个文件的处理行数 (留空表示不限制): ").strip()
                    limit = int(limit_input) if limit_input.isdigit() else None

                    start_time = time.time()
                    result = self.process_all_parallel(test_mode=False, limit=limit)
                    total_time = time.time() - start_time

                    if result['success']:
                        print(f"\n✅ 处理完成: {result['total_records']:,} 记录, {total_time:.2f}s")
                    else:
                        print(f"\n❌ 处理失败: {result.get('error', '未知错误')}")

                    input("\n按回车键继续...")

                elif choice == '2':
                    date_str = input("\n请输入日期 (YYYYMMDD格式): ").strip()
                    if len(date_str) != 8 or not date_str.isdigit():
                        print("❌ 日期格式错误")
                        continue

                    force = input("强制重新处理？(y/N): ").strip().lower() == 'y'
                    limit_input = input("限制每个文件的处理行数 (留空表示不限制): ").strip()
                    limit = int(limit_input) if limit_input.isdigit() else None

                    print(f"\n🔥 处理 {date_str} 的日志...")
                    result = self.process_date_parallel(date_str, force, test_mode=False, limit=limit)

                    if result['success']:
                        print(f"\n✅ 处理完成: {result['total_records']:,} 记录")
                    else:
                        print(f"\n❌ 处理失败: {result.get('error', '未知错误')}")

                    input("\n按回车键继续...")

                elif choice == '3':
                    self.show_performance_stats()
                    input("\n按回车键继续...")

                elif choice == '4':
                    print("\n🧪 测试模式处理")
                    date_str = input("请输入日期 (YYYYMMDD格式): ").strip()
                    if len(date_str) != 8 or not date_str.isdigit():
                        print("❌ 日期格式错误")
                        continue

                    limit_input = input("限制每个文件的处理行数 (建议100-1000): ").strip()
                    limit = int(limit_input) if limit_input.isdigit() else 100

                    print(f"\n🧪 测试模式：处理 {date_str} 的日志...")
                    result = self.process_date_parallel(date_str, False, test_mode=True, limit=limit)

                    if result['success']:
                        print(f"\n✅ 测试完成: {result['total_records']:,} 记录")
                    else:
                        print(f"\n❌ 测试失败: {result.get('error', '未知错误')}")

                    input("\n按回车键继续...")

                elif choice == '5':
                    self.print_error_report()
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

def main():
    """主函数"""
    import logging

    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(
        description='高性能ETL控制器 - 架构优化版本'
    )

    parser.add_argument('--date', help='处理指定日期 (YYYYMMDD格式)')
    parser.add_argument('--all', action='store_true', help='处理所有未处理日志')
    parser.add_argument('--force', action='store_true', help='强制重新处理')
    parser.add_argument('--test', action='store_true', help='测试模式')
    parser.add_argument('--limit', type=int, help='每个文件的行数限制')
    parser.add_argument('--batch-size', type=int, default=2000, help='批处理大小')
    parser.add_argument('--workers', type=int, default=4, help='工作线程数')

    args = parser.parse_args()

    try:
        with HighPerformanceETLController(
            batch_size=args.batch_size,
            max_workers=args.workers
        ) as controller:

            # 如果没有参数，显示交互式菜单
            if not any([args.date, args.all]):
                controller.interactive_menu()
                return

            if args.date:
                result = controller.process_date_parallel(
                    args.date,
                    force_reprocess=args.force,
                    test_mode=args.test,
                    limit=args.limit
                )

                print(f"\n🎯 处理结果:")
                print(f"日期: {result['date']}")
                print(f"文件: {result.get('processed_files', 0)}")
                print(f"记录: {result.get('total_records', 0):,}")
                print(f"耗时: {result.get('duration', 0):.2f}s")
                print(f"速度: {result.get('processing_speed', 0):.1f} 记录/秒")

            elif args.all:
                result = controller.process_all_parallel(
                    test_mode=args.test,
                    limit=args.limit
                )

                print(f"\n🎯 批量处理结果:")
                print(f"日期数: {result.get('processed_dates', 0)}")
                print(f"记录数: {result.get('total_records', 0):,}")
                print(f"总耗时: {result.get('duration', 0):.2f}s")
                print(f"平均速度: {result.get('processing_speed', 0):.1f} 记录/秒")

            controller.show_performance_stats()
            controller.print_error_report()

    except KeyboardInterrupt:
        print("\n👋 用户中断")
    except Exception as e:
        print(f"\n❌ 执行错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()