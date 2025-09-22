#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高性能ETL控制器 - 回归高性能版本 (方案A)
基于backup-20250913-014803，优化改进：
1. 保留详细错误日志输出 - 解决错误定位问题
2. 可配置的批处理、批大小和线程池大小 - 灵活调优
3. 移除过度监控开销 - 恢复高性能 (目标1200+ rps)
4. 简化而不失实用的错误处理
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

# 导入动态批大小优化器
from utils.dynamic_batch_optimizer import DynamicBatchOptimizer, BatchSizeRecommendation

from parsers.base_log_parser import BaseLogParser
from processors.field_mapper import FieldMapper
from writers.dwd_writer import DWDWriter

class HighPerformanceETLController:
    """高性能ETL控制器 - 回归高性能版本"""

    def __init__(self,
                 base_log_dir: str = None,
                 state_file: str = None,
                 batch_size: int = 25000,       # 可配置批处理大小
                 max_workers: int = 6,          # 可配置线程数
                 connection_pool_size: int = None,  # 可配置连接池大小
                 memory_limit_mb: int = 512,    # 内存限制
                 enable_detailed_logging: bool = True):  # 详细日志开关
        """
        初始化高性能ETL控制器

        Args:
            batch_size: 批处理大小（推荐1000-5000，可根据内存调整）
            max_workers: 最大工作线程数（推荐2-8，根据CPU核心数）
            connection_pool_size: 数据库连接池大小（默认与max_workers相同）
            memory_limit_mb: 内存使用限制（MB）
            enable_detailed_logging: 是否启用详细错误日志
        """
        # 基础配置
        self.base_log_dir = Path(base_log_dir) if base_log_dir else \
            Path("D:/project/nginx-log-analyzer/nginx-analytics-warehouse/nginx_logs")
        self.state_file = Path(state_file) if state_file else \
            Path(etl_root / "processed_logs_state.json")

        # 性能配置 - 可调优参数
        self.initial_batch_size = batch_size
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.connection_pool_size = connection_pool_size if connection_pool_size is not None else max_workers
        self.memory_limit_mb = memory_limit_mb
        self.enable_detailed_logging = enable_detailed_logging

        # 初始化动态批大小优化器
        self.batch_optimizer = DynamicBatchOptimizer(
            initial_batch_size=batch_size,
            min_batch_size=max(1000, batch_size // 10),
            max_batch_size=min(100000, batch_size * 4),
            memory_threshold=0.8,
            cpu_threshold=0.9
        )

        # 日志配置
        import logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # 创建详细的console handler（如果启用）
        if self.enable_detailed_logging and not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

        # 线程安全的组件池
        self.parser_pool = [BaseLogParser() for _ in range(max_workers)]
        self.mapper_pool = [FieldMapper() for _ in range(max_workers)]
        self.writer_pool = []

        # 数据库写入优化参数
        self.enable_async_writes = True  # 异步写入
        self.write_buffer_size = batch_size * 2  # 写入缓冲区大小
        self.delayed_commit_seconds = 0.5  # 延迟提交时间
        self.enable_batch_aggregation = True  # 批量聚合
        self.max_concurrent_writes = max(2, max_workers // 2)  # 最大并发写入数

        # 写入缓冲区和队列
        self.write_buffer = []
        self.write_buffer_lock = threading.Lock()
        self.async_write_queue = queue.Queue(maxsize=100)
        self.write_thread_pool = None

        # 性能统计
        self.write_stats = {
            'total_writes': 0,
            'total_records': 0,
            'total_write_time': 0,
            'buffer_flushes': 0,
            'async_writes': 0
        }

        # 初始化连接池
        self._init_connection_pool()

        # 启动异步写入线程
        self._start_async_write_threads()

        # 简化的缓存策略 - 高效但不过度
        self.ua_cache = {}  # User-Agent解析缓存
        self.uri_cache = {}  # URI解析缓存
        self.cache_hit_stats = {'ua_hits': 0, 'uri_hits': 0, 'total_requests': 0}

        # 线程同步
        self.result_queue = queue.Queue()
        self.error_queue = queue.Queue()

        # 写入优化控制
        self.last_buffer_flush = time.time()
        self.shutdown_event = threading.Event()
        self.stats_lock = threading.Lock()

        # 处理状态
        self.processed_state = self.load_state()

        # 简化的统计信息 - 保留核心指标，移除过度监控
        self.session_stats = {
            'start_time': None,
            'end_time': None,
            'total_files_processed': 0,
            'total_lines_processed': 0,
            'total_records_written': 0,
            'total_errors': 0,
            'cache_hit_rate': 0.0,
            'avg_processing_speed': 0.0,
            'processing_errors': [],
            # 简化的错误统计 - 只保留关键错误类型
            'error_stats': {
                'parsing_errors': 0,
                'mapping_errors': 0,
                'database_errors': 0,
                'critical_errors': 0
            },
            'detailed_error_log': []  # 详细错误日志，但不过度统计
        }

        self.logger.info("🚀 高性能ETL控制器初始化完成 - 回归高性能版本")
        self.logger.info(f"📁 日志目录: {self.base_log_dir}")
        self.logger.info(f"⚙️ 批处理大小: {self.batch_size:,} (可调优)")
        self.logger.info(f"🧵 最大工作线程: {self.max_workers} (可调优)")
        self.logger.info(f"🔗 连接池大小: {self.connection_pool_size} (可调优)")
        self.logger.info(f"📋 详细日志: {'启用' if self.enable_detailed_logging else '禁用'}")

        # 性能建议
        if self.max_workers > 8:
            self.logger.warning(f"⚠️  线程数 {self.max_workers} 较高，建议监控CPU使用率")
        if self.batch_size > 5000:
            self.logger.info(f"💡 大批量模式 ({self.batch_size:,})，确保内存充足")

    def _start_async_write_threads(self):
        """启动异步写入线程池"""
        if self.enable_async_writes:
            self.write_thread_pool = ThreadPoolExecutor(
                max_workers=self.max_concurrent_writes,
                thread_name_prefix="AsyncWriter"
            )

            # 为异步写入线程创建专用写入器池
            self.async_writer_pool = []
            for i in range(self.max_concurrent_writes):
                try:
                    async_writer = DWDWriter()
                    if async_writer.connect():
                        self.async_writer_pool.append(async_writer)
                        self.logger.info(f"✅ 异步写入器 {i+1} 连接成功")
                    else:
                        self.logger.error(f"❌ 异步写入器 {i+1} 连接失败")
                except Exception as e:
                    self.logger.error(f"❌ 创建异步写入器 {i+1} 失败: {e}")

            self.logger.info(f"🚀 启动异步写入线程池: {self.max_concurrent_writes}个线程, {len(self.async_writer_pool)}个专用写入器")

    def _init_connection_pool(self):
        """初始化数据库连接池 - 简化版本"""
        self.logger.info("🔗 初始化数据库连接池...")
        success_count = 0

        for i in range(self.connection_pool_size):
            try:
                writer = DWDWriter()
                if writer.connect():
                    self.writer_pool.append(writer)
                    success_count += 1
                    if self.enable_detailed_logging:
                        self.logger.info(f"✅ 连接 {i+1} 建立成功")
                else:
                    self._log_error('critical_errors', f"连接 {i+1} 建立失败", {
                        'connection_id': i+1,
                        'type': 'connection_failure'
                    })
            except Exception as e:
                self._log_error('critical_errors', f"连接 {i+1} 初始化异常: {str(e)}", {
                    'connection_id': i+1,
                    'exception_type': type(e).__name__,
                    'traceback': traceback.format_exc() if self.enable_detailed_logging else None
                })

        if not self.writer_pool:
            error_msg = "无法建立任何数据库连接"
            self._log_error('critical_errors', error_msg)
            raise RuntimeError(f"❌ {error_msg}")

        self.logger.info(f"🔗 连接池初始化完成：{success_count}/{self.connection_pool_size} 个连接成功")

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
        """高效的User-Agent解析缓存"""
        if not user_agent:
            return {}

        # 减少锁竞争 - 先检查缓存
        with self.stats_lock:
            self.cache_hit_stats['total_requests'] += 1
            if user_agent in self.ua_cache:
                self.cache_hit_stats['ua_hits'] += 1
                return self.ua_cache[user_agent]

        # 缓存未命中，执行解析
        try:
            result = mapper._parse_user_agent(user_agent)
        except Exception as e:
            if self.enable_detailed_logging:
                self._log_error('mapping_errors', f"User-Agent解析失败: {str(e)}", {
                    'user_agent': user_agent[:100],
                    'exception_type': type(e).__name__
                })
            result = {}

        # 更新缓存
        with self.stats_lock:
            if len(self.ua_cache) < 5000:  # 限制缓存大小
                self.ua_cache[user_agent] = result

        return result

    def cached_uri_parse(self, uri: str, mapper: FieldMapper) -> Dict[str, str]:
        """高效的URI结构解析缓存"""
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
            if self.enable_detailed_logging:
                self._log_error('mapping_errors', f"URI解析失败: {str(e)}", {
                    'uri': uri[:200],
                    'exception_type': type(e).__name__
                })
            result = {}

        with self.stats_lock:
            if len(self.uri_cache) < 3000:  # 限制缓存大小
                self.uri_cache[uri] = result

        return result

    def _log_error(self, error_type: str, error_msg: str, context: Dict[str, Any] = None):
        """统一的错误日志方法 - 详细但高效"""
        # 更新简化的错误统计
        with self.stats_lock:
            if error_type in self.session_stats['error_stats']:
                self.session_stats['error_stats'][error_type] += 1
            self.session_stats['total_errors'] += 1

            # 详细错误日志（如果启用）
            if self.enable_detailed_logging:
                error_detail = {
                    'timestamp': datetime.now().isoformat(),
                    'type': error_type,
                    'message': error_msg,
                    'context': context or {}
                }
                self.session_stats['detailed_error_log'].append(error_detail)

                # 限制详细日志大小
                if len(self.session_stats['detailed_error_log']) > 500:
                    self.session_stats['detailed_error_log'] = self.session_stats['detailed_error_log'][-250:]

        # 记录到标准日志
        if error_type == 'critical_errors':
            self.logger.error(f"❌ [{error_type}] {error_msg}")
        elif self.enable_detailed_logging:
            self.logger.warning(f"⚠️ [{error_type}] {error_msg}")

    def _optimized_batch_write(self, data_batch: List[Dict[str, Any]], writer: DWDWriter) -> bool:
        """优化的批量写入方法 - 使用所有可用的数据库写入优化技术"""
        if not data_batch:
            return True

        start_time = time.time()

        try:
            # 优化1: 预处理和验证数据
            validated_batch = []
            for record in data_batch:
                if self._validate_record(record):
                    validated_batch.append(record)

            if not validated_batch:
                return True

            # 优化2: 使用ClickHouse优化的批量插入
            success = writer.batch_write_optimized(validated_batch)

            if success:
                write_time = time.time() - start_time
                with self.write_buffer_lock:
                    self.write_stats['total_writes'] += 1
                    self.write_stats['total_records'] += len(validated_batch)
                    self.write_stats['total_write_time'] += write_time

                return True
            else:
                self._log_error('database_errors', f"批量写入失败: {len(validated_batch)} 记录")
                return False

        except Exception as e:
            self._log_error('database_errors', f"批量写入异常: {e}")
            return False

    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """快速验证记录的有效性"""
        return (
            record and
            isinstance(record, dict) and
            'log_time' in record and
            'client_ip' in record
        )

    def _async_write_worker(self, data_batch: List[Dict[str, Any]]):
        """异步写入工作线程"""
        max_retries = 3
        retry_delay = 0.1

        for attempt in range(max_retries):
            try:
                # 优先使用专用异步写入器 - 使用线程安全的分配
                if hasattr(self, 'async_writer_pool') and self.async_writer_pool:
                    import time
                    # 使用时间戳和线程ID确保更好的分布
                    thread_id = threading.current_thread().ident or 0
                    time_factor = int(time.time() * 1000) % 1000  # 毫秒级时间因子
                    writer_index = (thread_id + time_factor) % len(self.async_writer_pool)
                    async_writer = self.async_writer_pool[writer_index]

                    # 检查写入器连接状态
                    if not async_writer or not async_writer.client:
                        self.logger.warning(f"异步写入器 {writer_index} 连接不可用，回退到主写入器池")
                    else:
                        return self._optimized_batch_write(data_batch, async_writer)

                # 回退到主写入器池
                if not self._ensure_writer_pool_available():
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    self._log_error('database_errors', "无可用的写入连接")
                    return False

                # 线程安全地获取写入器
                with self.stats_lock:
                    if not self.writer_pool:
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            continue
                        self._log_error('database_errors', "无可用的写入连接")
                        return False

                    # 获取写入器（轮询策略）
                    thread_id = threading.current_thread().ident or 0
                    writer_index = thread_id % len(self.writer_pool)
                    writer = self.writer_pool[writer_index]

                # 在锁外执行写入操作
                return self._optimized_batch_write(data_batch, writer)

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                    continue
                self._log_error('database_errors', f"异步写入工作线程异常: {e}")
                return False

        return False

    def _flush_write_buffer(self, force: bool = False):
        """刷新写入缓冲区"""
        with self.write_buffer_lock:
            current_time = time.time()
            should_flush = (
                force or
                len(self.write_buffer) >= self.write_buffer_size or
                (self.write_buffer and
                 current_time - self.last_buffer_flush > self.delayed_commit_seconds)
            )

            if should_flush and self.write_buffer:
                buffer_to_flush = self.write_buffer.copy()
                self.write_buffer.clear()
                self.last_buffer_flush = current_time
                self.write_stats['buffer_flushes'] += 1

                # 检查写入器池状态 - 检查主池或异步池
                has_main_writers = bool(self.writer_pool)
                has_async_writers = hasattr(self, 'async_writer_pool') and bool(self.async_writer_pool)

                if not has_main_writers and not has_async_writers:
                    self._log_error('database_errors', "刷新缓冲区时无可用的写入连接")
                    return False

                # 异步提交写入
                if (self.enable_async_writes and
                    self.write_thread_pool and
                    not self.shutdown_event.is_set() and
                    has_async_writers):
                    try:
                        future = self.write_thread_pool.submit(self._async_write_worker, buffer_to_flush)
                        self.write_stats['async_writes'] += 1
                        return future
                    except Exception as e:
                        self._log_error('database_errors', f"异步写入提交失败: {e}")
                        # 降级到同步写入
                        if has_main_writers:
                            writer = self.writer_pool[0]
                            return self._optimized_batch_write(buffer_to_flush, writer)
                        else:
                            return False
                else:
                    # 同步写入
                    if has_main_writers:
                        writer = self.writer_pool[0]
                        return self._optimized_batch_write(buffer_to_flush, writer)
                    elif has_async_writers:
                        # 使用异步写入器进行同步写入
                        async_writer = self.async_writer_pool[0]
                        return self._optimized_batch_write(buffer_to_flush, async_writer)
                    else:
                        self._log_error('database_errors', "同步写入时无可用的写入连接")
                        return False

        return True

    def _add_to_write_buffer(self, data_batch: List[Dict[str, Any]]):
        """添加数据到写入缓冲区"""
        if not data_batch:
            return

        with self.write_buffer_lock:
            self.write_buffer.extend(data_batch)

        # 检查是否需要刷新缓冲区
        self._flush_write_buffer()

    def _ensure_writer_pool_available(self) -> bool:
        """确保写入器池可用，如果不可用则尝试重新初始化"""
        if self.writer_pool:
            return True

        self.logger.warning("写入器池为空，尝试重新初始化...")
        try:
            # 重新初始化连接池
            success_count = 0
            for i in range(min(2, self.connection_pool_size)):  # 至少创建2个连接
                try:
                    writer = DWDWriter()
                    if writer.connect():
                        self.writer_pool.append(writer)
                        success_count += 1
                except Exception as e:
                    self.logger.error(f"重新创建连接 {i+1} 失败: {e}")

            if success_count > 0:
                self.logger.info(f"成功重新创建 {success_count} 个数据库连接")
                return True
            else:
                self.logger.error("无法重新创建任何数据库连接")
                return False

        except Exception as e:
            self.logger.error(f"重新初始化写入器池失败: {e}")
            return False

    def process_file_batch(self, file_paths: List[Path], thread_id: int,
                          test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """
        高性能文件批量处理 - 移除过度监控，保留关键错误日志
        """
        start_time = time.time()

        # 获取线程专用组件
        parser = self.parser_pool[thread_id % len(self.parser_pool)]
        mapper = self.mapper_pool[thread_id % len(self.mapper_pool)]
        writer = None

        if not test_mode:
            writer = self.get_writer()
            if not writer:
                error_msg = f"线程 {thread_id} 无法获取数据库连接"
                self._log_error('critical_errors', error_msg, {'thread_id': thread_id})
                return {
                    'success': False,
                    'error': error_msg,
                    'thread_id': thread_id
                }

        try:
            batch_stats = {
                'thread_id': thread_id,
                'total_files': len(file_paths),
                'processed_files': 0,
                'total_records': 0,
                'total_lines': 0,
                'errors': []
            }

            # 高性能批量缓冲区
            mega_batch = []

            for file_path in file_paths:
                if self.enable_detailed_logging:
                    self.logger.info(f"🧵{thread_id} 处理文件: {file_path.name}")

                file_start = time.time()
                file_records = 0
                file_lines = 0

                try:
                    # 流式处理文件 - 核心高性能循环
                    for parsed_data in parser.parse_file(file_path):
                        file_lines += 1

                        if parsed_data:
                            try:
                                # 高效缓存映射
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

                                # 动态批量写入检查 - 使用动态优化的批大小
                                current_batch_size = self.batch_optimizer.get_current_batch_size()
                                if len(mega_batch) >= current_batch_size:
                                    if not test_mode:
                                        # 记录批处理性能开始时间
                                        batch_start_time = time.time()

                                        # 使用优化的缓冲写入系统
                                        self._add_to_write_buffer(mega_batch.copy())

                                        # 记录批处理性能
                                        batch_duration = time.time() - batch_start_time
                                        self.batch_optimizer.record_batch_performance(
                                            current_batch_size,
                                            len(mega_batch),
                                            batch_duration
                                        )

                                        # 尝试优化批大小
                                        new_batch_size, reason = self.batch_optimizer.optimize_batch_size()
                                        if new_batch_size != current_batch_size:
                                            self.logger.info(f"🔧 批大小优化: {current_batch_size} -> {new_batch_size}, 原因: {reason}")

                                    mega_batch.clear()
                                    gc.collect()  # 内存优化

                                # 检查限制
                                if limit and file_records >= limit:
                                    break

                            except Exception as e:
                                # 详细错误定位 - 关键改进
                                error_msg = f"记录处理错误 (行 {file_lines}): {str(e)}"
                                error_context = {
                                    'file_name': file_path.name,
                                    'line_number': file_lines,
                                    'thread_id': thread_id,
                                    'exception_type': type(e).__name__
                                }
                                if self.enable_detailed_logging:
                                    error_context['traceback'] = traceback.format_exc()
                                    error_context['parsed_data_keys'] = list(parsed_data.keys()) if hasattr(parsed_data, 'keys') else str(type(parsed_data))

                                self._log_error('mapping_errors', error_msg, error_context)

                except Exception as e:
                    error_msg = f"文件处理异常 {file_path.name}: {str(e)}"
                    self._log_error('parsing_errors', error_msg, {
                        'file_name': file_path.name,
                        'thread_id': thread_id,
                        'exception_type': type(e).__name__,
                        'traceback': traceback.format_exc() if self.enable_detailed_logging else None
                    })
                    batch_stats['errors'].append(error_msg)

                file_duration = time.time() - file_start
                batch_stats['processed_files'] += 1
                batch_stats['total_records'] += file_records
                batch_stats['total_lines'] += file_lines

                # 性能反馈
                if self.enable_detailed_logging and file_records > 0:
                    speed = file_records / file_duration if file_duration > 0 else 0
                    self.logger.info(f"🧵{thread_id} 完成 {file_path.name}: {file_records} 记录, {speed:.1f} rec/s")

            # 处理剩余批次 - 使用优化的缓冲写入
            if mega_batch and not test_mode:
                # 使用优化的缓冲写入系统处理剩余数据
                self._add_to_write_buffer(mega_batch)
                # 强制刷新缓冲区以确保所有数据都被写入
                self._flush_write_buffer(force=True)

            batch_stats['duration'] = time.time() - start_time
            batch_stats['success'] = len(batch_stats['errors']) == 0

            return batch_stats

        finally:
            # 归还连接到池
            if writer:
                self.return_writer(writer)

    def process_date_parallel(self, date_str: str, force_reprocess: bool = False,
                             test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """
        并行处理指定日期的所有日志 - 高性能版本
        """
        self.logger.info(f"🚀 开始并行处理 {date_str} 的日志")
        self.session_stats['start_time'] = datetime.now()
        start_time = time.time()

        # 检查日期目录
        date_dir = self.base_log_dir / date_str
        if not date_dir.exists():
            error_msg = f'日期目录不存在: {date_dir}'
            self._log_error('critical_errors', error_msg, {'date': date_str})
            return {'success': False, 'error': error_msg, 'date': date_str}

        # 获取所有日志文件
        try:
            all_log_files = list(date_dir.glob("*.log"))
        except Exception as e:
            error_msg = f'扫描日志文件失败: {str(e)}'
            self._log_error('critical_errors', error_msg, {'date': date_str, 'directory': str(date_dir)})
            return {'success': False, 'error': error_msg, 'date': date_str}

        if not all_log_files:
            return {'success': True, 'processed_files': 0, 'message': '没有找到日志文件', 'date': date_str}

        # 过滤需要处理的文件
        if not force_reprocess:
            pending_files = [f for f in all_log_files if not self.is_file_processed(f)]
        else:
            pending_files = all_log_files

        if not pending_files:
            self.logger.info(f"📋 日期 {date_str} 的所有文件都已处理")
            return {'success': True, 'processed_files': 0, 'message': '所有文件都已处理', 'date': date_str}

        self.logger.info(f"📁 找到 {len(pending_files)} 个待处理文件")

        # 高效的文件分批策略
        files_per_thread = max(1, len(pending_files) // self.max_workers)
        file_batches = []

        for i in range(0, len(pending_files), files_per_thread):
            batch = pending_files[i:i + files_per_thread]
            if batch:
                file_batches.append(batch)

        actual_workers = min(self.max_workers, len(file_batches))
        self.logger.info(f"🧵 分配 {len(file_batches)} 个批次给 {actual_workers} 个线程")

        # 高性能并行处理
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
                            if self.enable_detailed_logging:
                                self.logger.info(f"✅ 批次 {batch_id} 完成: {result['processed_files']} 文件, "
                                               f"{result['total_records']} 记录")
                        else:
                            total_errors.extend(result.get('errors', []))
                            self.logger.error(f"❌ 批次 {batch_id} 失败: {result.get('error', '未知错误')}")

                    except Exception as e:
                        error_msg = f"批次 {batch_id} 执行异常: {str(e)}"
                        self._log_error('critical_errors', error_msg, {
                            'batch_id': batch_id,
                            'exception_type': type(e).__name__
                        })
                        total_errors.append(error_msg)

        except Exception as e:
            error_msg = f"线程池执行异常: {str(e)}"
            self._log_error('critical_errors', error_msg, {'date': date_str})
            return {'success': False, 'error': error_msg, 'date': date_str}

        # 更新处理状态
        if not test_mode:
            for file_path in pending_files:
                self.mark_file_processed(file_path, 0, 0)
            self.save_state()

        # 计算性能指标
        cache_hit_rate = 0.0
        if self.cache_hit_stats['total_requests'] > 0:
            total_hits = self.cache_hit_stats['ua_hits'] + self.cache_hit_stats['uri_hits']
            cache_hit_rate = (total_hits / (self.cache_hit_stats['total_requests'] * 2)) * 100

        duration = time.time() - start_time
        speed = total_records / duration if duration > 0 else 0

        # 更新会话统计
        self.session_stats['end_time'] = datetime.now()
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
            'errors': total_errors
        }

        if success:
            self.logger.info(f"🎉 日期 {date_str} 高性能处理完成!")
            self.logger.info(f"📊 {result['processed_files']} 文件, {total_records:,} 记录")
            self.logger.info(f"⏱️  耗时 {duration:.2f}s, 速度 {speed:.1f} 记录/秒")
            self.logger.info(f"🎯 缓存命中率: {cache_hit_rate:.1f}%")

        return result

    # === 继承的核心方法 ===

    def process_all_parallel(self, test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """并行处理所有未处理的日志"""
        self.logger.info("🚀 开始高性能处理所有未处理的日志")
        start_time = time.time()

        log_files_by_date = self.scan_log_directories()
        if not log_files_by_date:
            return {'success': False, 'error': '没有找到日志文件'}

        total_records = 0
        processed_dates = 0
        all_errors = []

        for date_str in sorted(log_files_by_date.keys()):
            result = self.process_date_parallel(date_str, force_reprocess=False,
                                              test_mode=test_mode, limit=limit)
            if result['success'] and result.get('processed_files', 0) > 0:
                processed_dates += 1
                total_records += result['total_records']
            else:
                if result.get('errors'):
                    all_errors.extend(result['errors'])

        duration = time.time() - start_time
        overall_speed = total_records / duration if duration > 0 else 0

        return {
            'success': len(all_errors) == 0,
            'processed_dates': processed_dates,
            'total_records': total_records,
            'duration': duration,
            'processing_speed': overall_speed,
            'errors': all_errors
        }

    def load_state(self) -> Dict[str, Any]:
        """加载处理状态"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self._log_error('critical_errors', f"加载状态文件失败: {e}")

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
            self._log_error('critical_errors', f"保存状态文件失败: {e}")

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
            self._log_error('critical_errors', f"标记文件状态失败 {file_path}: {e}")

    def show_performance_stats(self):
        """显示性能统计信息"""
        print("\n" + "=" * 80)
        print("🚀 高性能ETL控制器 - 性能统计报告 (回归高性能版本)")
        print("=" * 80)

        print(f"⚙️  配置信息:")
        print(f"   初始批处理大小: {self.initial_batch_size:,}")
        print(f"   当前批处理大小: {self.batch_optimizer.get_current_batch_size():,} (动态优化)")
        print(f"   最大工作线程: {self.max_workers} (可调优)")
        print(f"   连接池大小: {self.connection_pool_size} (可调优)")
        print(f"   详细日志: {'启用' if self.enable_detailed_logging else '禁用'}")

        # 显示动态批大小优化器统计
        optimizer_stats = self.batch_optimizer.get_performance_stats()
        if optimizer_stats:
            print(f"\n🔧  动态批大小优化统计:")
            print(f"   测量次数: {optimizer_stats.get('total_measurements', 0)}")
            print(f"   平均吞吐量: {optimizer_stats.get('avg_throughput', 0):.1f} 记录/秒")
            print(f"   峰值吞吐量: {optimizer_stats.get('max_throughput', 0):.1f} 记录/秒")
            print(f"   连续良好性能: {optimizer_stats.get('consecutive_good_performance', 0)}")
            print(f"   连续不佳性能: {optimizer_stats.get('consecutive_bad_performance', 0)}")

            if 'avg_memory_usage' in optimizer_stats:
                print(f"   平均内存使用: {optimizer_stats['avg_memory_usage']*100:.1f}%")
            if 'avg_cpu_usage' in optimizer_stats:
                print(f"   平均CPU使用: {optimizer_stats['avg_cpu_usage']*100:.1f}%")

        print(f"\n📈 缓存统计:")
        print(f"   User-Agent缓存: {len(self.ua_cache)} 项")
        print(f"   URI缓存: {len(self.uri_cache)} 项")
        print(f"   缓存命中率: {self.session_stats.get('cache_hit_rate', 0):.1f}%")

        if self.session_stats.get('avg_processing_speed', 0) > 0:
            print(f"\n🏃 性能指标:")
            print(f"   平均处理速度: {self.session_stats['avg_processing_speed']:.1f} 记录/秒")
            print(f"   总处理记录数: {self.session_stats.get('total_records_written', 0):,}")

        # 简化的错误汇总
        if self.session_stats['total_errors'] > 0:
            print(f"\n❌ 错误汇总:")
            print(f"   总错误数: {self.session_stats['total_errors']}")
            for error_type, count in self.session_stats['error_stats'].items():
                if count > 0:
                    print(f"   {error_type}: {count}")

        print("=" * 80)

    def print_detailed_error_log(self):
        """打印详细错误日志 - 帮助定位问题"""
        if not self.enable_detailed_logging:
            print("详细错误日志未启用，请在初始化时设置 enable_detailed_logging=True")
            return

        error_log = self.session_stats.get('detailed_error_log', [])
        if not error_log:
            print("✅ 没有详细错误记录")
            return

        print("\n" + "="*80)
        print("🔍 详细错误日志 (最近10个)")
        print("="*80)

        for error in error_log[-10:]:
            print(f"\n📅 时间: {error['timestamp']}")
            print(f"🏷️  类型: {error['type']}")
            print(f"📝 消息: {error['message']}")
            if error.get('context'):
                print(f"🔍 上下文: {error['context']}")
            print("-" * 40)

        print("="*80)

    def cleanup(self):
        """清理资源"""
        self.logger.info("🧹 清理资源...")

        # 设置关闭事件
        self.shutdown_event.set()

        # 刷新任何剩余的写入缓冲区
        try:
            self._flush_write_buffer(force=True)
            self.logger.info("✅ 写入缓冲区已刷新")
        except Exception as e:
            self.logger.error(f"❌ 刷新写入缓冲区失败: {e}")

        # 关闭异步写入线程池
        if self.write_thread_pool:
            try:
                self.write_thread_pool.shutdown(wait=True)
                self.logger.info("✅ 异步写入线程池已关闭")
            except Exception as e:
                self.logger.error(f"❌ 关闭异步写入线程池失败: {e}")

        # 关闭专用异步写入器
        if hasattr(self, 'async_writer_pool'):
            for async_writer in self.async_writer_pool:
                try:
                    async_writer.close()
                except:
                    pass
            self.async_writer_pool.clear()

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
        self.write_buffer.clear()

        # 显示写入统计
        if self.write_stats['total_writes'] > 0:
            avg_write_time = self.write_stats['total_write_time'] / self.write_stats['total_writes']
            self.logger.info(f"📊 写入统计: {self.write_stats['total_writes']} 次写入, "
                           f"{self.write_stats['total_records']} 记录, "
                           f"平均写入时间: {avg_write_time:.3f}s")
            self.logger.info(f"📊 缓冲统计: {self.write_stats['buffer_flushes']} 次刷新, "
                           f"{self.write_stats['async_writes']} 次异步写入")

        gc.collect()
        self.logger.info("✅ 资源清理完成")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def _dynamic_batch_optimization_menu(self):
        """动态批大小优化管理菜单"""
        while True:
            print("\n" + "=" * 80)
            print("🔧 动态批大小优化管理")
            print("=" * 80)

            # 显示当前状态
            current_batch_size = self.batch_optimizer.get_current_batch_size()
            optimizer_stats = self.batch_optimizer.get_performance_stats()

            print(f"📊 当前状态:")
            print(f"   初始批大小: {self.initial_batch_size:,}")
            print(f"   当前批大小: {current_batch_size:,}")
            print(f"   测量次数: {optimizer_stats.get('total_measurements', 0)}")

            if optimizer_stats.get('avg_throughput', 0) > 0:
                print(f"   平均吞吐量: {optimizer_stats['avg_throughput']:.1f} 记录/秒")
                print(f"   峰值吞吐量: {optimizer_stats['max_throughput']:.1f} 记录/秒")

            print(f"\n📋 管理选项:")
            print("1. 📈 查看详细优化统计")
            print("2. 🔄 手动触发优化")
            print("3. 🎯 强制设置批大小")
            print("4. 🔄 重置优化器")
            print("5. 💡 获取系统推荐批大小")
            print("0. 🔙 返回主菜单")

            try:
                choice = input("请选择操作 [0-5]: ").strip()

                if choice == '0':
                    break

                elif choice == '1':
                    # 显示详细统计
                    print("\n📈 详细优化统计:")
                    for key, value in optimizer_stats.items():
                        if isinstance(value, float):
                            if 'usage' in key:
                                print(f"   {key}: {value*100:.1f}%")
                            else:
                                print(f"   {key}: {value:.2f}")
                        else:
                            print(f"   {key}: {value}")

                elif choice == '2':
                    # 手动触发优化
                    print("\n🔄 手动触发批大小优化...")
                    new_size, reason = self.batch_optimizer.optimize_batch_size()
                    print(f"优化结果: {current_batch_size} -> {new_size}")
                    print(f"优化原因: {reason}")

                elif choice == '3':
                    # 强制设置批大小
                    new_size_input = input(f"请输入新的批大小 (当前 {current_batch_size}): ").strip()
                    if new_size_input.isdigit():
                        new_size = int(new_size_input)
                        if 1000 <= new_size <= 100000:
                            self.batch_optimizer.force_batch_size(new_size)
                            print(f"✅ 已强制设置批大小为: {new_size:,}")
                        else:
                            print("❌ 批大小必须在 1,000 - 100,000 范围内")
                    else:
                        print("❌ 请输入有效数字")

                elif choice == '4':
                    # 重置优化器
                    confirm = input("确认重置优化器？这将清除所有性能历史 (y/N): ").strip().lower()
                    if confirm == 'y':
                        self.batch_optimizer.reset_optimizer()
                        print("✅ 优化器已重置")
                    else:
                        print("❌ 已取消重置")

                elif choice == '5':
                    # 系统推荐
                    print("\n💡 系统推荐批大小:")
                    system_info = BatchSizeRecommendation.get_system_info()

                    if 'error' not in system_info:
                        recommended_size = BatchSizeRecommendation.recommend_initial_batch_size(
                            system_info['available_memory_gb']
                        )
                        print(f"   可用内存: {system_info['available_memory_gb']:.1f} GB")
                        print(f"   CPU核心数: {system_info['cpu_count']}")
                        print(f"   推荐批大小: {recommended_size:,}")

                        apply = input("是否应用推荐的批大小？ (y/N): ").strip().lower()
                        if apply == 'y':
                            self.batch_optimizer.force_batch_size(recommended_size)
                            print(f"✅ 已应用推荐批大小: {recommended_size:,}")
                    else:
                        print(f"❌ 无法获取系统信息: {system_info['error']}")

                else:
                    print("❌ 无效选择")

                input("\n按回车键继续...")

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"❌ 操作失败: {e}")

    # === 交互式配置菜单 ===

    def interactive_menu(self):
        """交互式菜单 - 增加配置调优选项"""
        while True:
            print("\n" + "=" * 80)
            print("🚀 高性能ETL控制器 - 回归高性能版本")
            print("=" * 80)
            print("1. 🔥 处理所有未处理的日志")
            print("2. 📅 处理指定日期的日志")
            print("3. 📊 查看性能统计")
            print("4. 🧪 测试模式处理")
            print("5. ⚙️ 性能参数调优")
            print("6. 🔍 查看详细错误日志")
            print("7. 🔧 动态批大小优化管理")
            print("0. 👋 退出")
            print("-" * 80)
            current_batch_size = self.batch_optimizer.get_current_batch_size()
            print(f"📊 当前配置: 批量{current_batch_size} (动态) | 线程{self.max_workers} | 连接池{self.connection_pool_size}")

            try:
                choice = input("请选择操作 [0-7]: ").strip()

                if choice == '0':
                    print("👋 再见！")
                    break

                elif choice == '1':
                    print("\n🔥 高性能处理所有未处理的日志...")
                    limit_input = input("限制每个文件的处理行数 (留空表示不限制): ").strip()
                    limit = int(limit_input) if limit_input.isdigit() else None

                    result = self.process_all_parallel(test_mode=False, limit=limit)
                    if result['success']:
                        print(f"\n✅ 处理完成: {result['total_records']:,} 记录, 速度 {result['processing_speed']:.1f} rec/s")
                    else:
                        print(f"\n❌ 处理失败: {result.get('error', '未知错误')}")

                elif choice == '2':
                    date_str = input("\n请输入日期 (YYYYMMDD格式): ").strip()
                    if len(date_str) != 8 or not date_str.isdigit():
                        print("❌ 日期格式错误")
                        continue

                    force = input("强制重新处理？(y/N): ").strip().lower() == 'y'
                    limit_input = input("限制每个文件的处理行数 (留空表示不限制): ").strip()
                    limit = int(limit_input) if limit_input.isdigit() else None

                    result = self.process_date_parallel(date_str, force, test_mode=False, limit=limit)
                    if result['success']:
                        print(f"\n✅ 处理完成: {result['total_records']:,} 记录, 速度 {result['processing_speed']:.1f} rec/s")
                    else:
                        print(f"\n❌ 处理失败: {result.get('error', '未知错误')}")

                elif choice == '3':
                    self.show_performance_stats()

                elif choice == '4':
                    date_str = input("\n请输入测试日期 (YYYYMMDD格式): ").strip()
                    limit_input = input("限制处理行数 (建议100-1000): ").strip()
                    limit = int(limit_input) if limit_input.isdigit() else 100

                    result = self.process_date_parallel(date_str, False, test_mode=True, limit=limit)
                    if result['success']:
                        print(f"\n✅ 测试完成: {result['total_records']:,} 记录, 速度 {result['processing_speed']:.1f} rec/s")

                elif choice == '5':
                    print("\n⚙️ 性能参数调优")
                    print(f"当前配置:")
                    print(f"  批量大小: {self.batch_size}")
                    print(f"  线程数: {self.max_workers}")
                    print(f"  连接池: {self.connection_pool_size}")

                    new_batch = input(f"\n新的批量大小 (当前{self.batch_size}, 推荐1000-5000): ").strip()
                    new_workers = input(f"新的线程数 (当前{self.max_workers}, 推荐2-8): ").strip()
                    new_pool = input(f"新的连接池大小 (当前{self.connection_pool_size}, 推荐=线程数): ").strip()

                    if new_batch.isdigit():
                        self.batch_size = int(new_batch)
                        print(f"✅ 批量大小调整为: {self.batch_size}")

                    if new_workers.isdigit():
                        old_workers = self.max_workers
                        self.max_workers = int(new_workers)
                        print(f"✅ 线程数调整为: {self.max_workers}")

                        # 重新初始化组件池
                        if self.max_workers != old_workers:
                            print("🔄 重新初始化组件池...")
                            self.parser_pool = [BaseLogParser() for _ in range(self.max_workers)]
                            self.mapper_pool = [FieldMapper() for _ in range(self.max_workers)]

                    if new_pool.isdigit():
                        self.connection_pool_size = int(new_pool)
                        print(f"✅ 连接池大小调整为: {self.connection_pool_size}")
                        print("⚠️  连接池调整需要重启程序生效")

                elif choice == '6':
                    self.print_detailed_error_log()

                elif choice == '7':
                    self._dynamic_batch_optimization_menu()

                else:
                    print("❌ 无效选择")

                input("\n按回车键继续...")

            except KeyboardInterrupt:
                print("\n\n👋 用户中断，再见！")
                break
            except Exception as e:
                print(f"\n❌ 操作异常: {e}")
                if self.enable_detailed_logging:
                    self._log_error('critical_errors', f"菜单操作异常: {e}", {
                        'exception_type': type(e).__name__,
                        'traceback': traceback.format_exc()
                    })

def main():
    """主函数"""
    import logging

    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(
        description='高性能ETL控制器 - 回归高性能版本 (目标1200+ rps)'
    )

    parser.add_argument('--date', help='处理指定日期 (YYYYMMDD格式)')
    parser.add_argument('--all', action='store_true', help='处理所有未处理日志')
    parser.add_argument('--force', action='store_true', help='强制重新处理')
    parser.add_argument('--test', action='store_true', help='测试模式')
    parser.add_argument('--limit', type=int, help='每个文件的行数限制')
    parser.add_argument('--batch-size', type=int, default=2000, help='批处理大小 (可调优)')
    parser.add_argument('--workers', type=int, default=6, help='工作线程数 (可调优)')
    parser.add_argument('--pool-size', type=int, help='连接池大小 (可调优，默认=线程数)')
    parser.add_argument('--detailed-logging', action='store_true', default=True, help='启用详细错误日志')

    args = parser.parse_args()

    try:
        with HighPerformanceETLController(
            batch_size=args.batch_size,
            max_workers=args.workers,
            connection_pool_size=args.pool_size,
            enable_detailed_logging=args.detailed_logging
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
            controller.print_detailed_error_log()

    except KeyboardInterrupt:
        print("\n👋 用户中断")
    except Exception as e:
        print(f"\n❌ 执行错误: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()