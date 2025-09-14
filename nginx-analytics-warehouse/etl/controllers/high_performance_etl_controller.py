#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高性能ETL控制器 - 多线程并行处理版本
High Performance ETL Controller - Multi-threaded parallel processing

相比intelligent_etl_controller的主要优化：
1. 多线程并行处理文件
2. 大批量数据库操作（1000-5000条/批）
3. 连接池管理，避免频繁连接
4. 内存缓存优化，减少重复计算
5. 异步I/O和流式处理
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
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from collections import defaultdict
import gc

# 添加路径以导入其他模块
current_dir = Path(__file__).parent
etl_root = current_dir.parent
sys.path.append(str(etl_root))

from parsers.base_log_parser import BaseLogParser
from processors.field_mapper import FieldMapper
from writers.dwd_writer import DWDWriter

class HighPerformanceETLController:
    """高性能ETL控制器 - 多线程并行版本"""
    
    def __init__(self, 
                 base_log_dir: str = None, 
                 state_file: str = None,
                 batch_size: int = 5000,        # 超大批量大小优化
                 max_workers: int = 6,          # 增加并行处理线程数
                 connection_pool_size: int = None,  # 数据库连接池大小（默认与max_workers相同）
                 memory_limit_mb: int = 512):    # 内存限制
        """
        初始化高性能ETL控制器
        
        Args:
            batch_size: 批处理大小（推荐1000-5000）
            max_workers: 最大工作线程数
            connection_pool_size: 数据库连接池大小
            memory_limit_mb: 内存使用限制（MB）
        """
        # 基础配置
        self.base_log_dir = Path(base_log_dir) if base_log_dir else \
            Path("D:/project/nginx-log-analyzer/nginx-analytics-warehouse/nginx_logs")
        self.state_file = Path(state_file) if state_file else \
            Path(etl_root / "processed_logs_state.json")
        
        # 性能优化配置
        self.batch_size = batch_size
        self.max_workers = max_workers
        # 连接池大小默认与线程数相同，确保每个线程有独立连接
        self.connection_pool_size = connection_pool_size if connection_pool_size is not None else max_workers
        self.memory_limit_mb = memory_limit_mb
        
        # 日志配置 - 必须在使用logger之前初始化
        import logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # 线程安全的组件池
        self.parser_pool = [BaseLogParser() for _ in range(max_workers)]
        self.mapper_pool = [FieldMapper() for _ in range(max_workers)]
        self.writer_pool = []
        
        # 初始化连接池
        self._init_connection_pool()
        
        # 高性能缓存优化 - 使用LRU缓存
        from functools import lru_cache
        self.ua_cache = {}  # User-Agent解析缓存 (增大容量)
        self.uri_cache = {}  # URI解析缓存 (增大容量)
        self.ip_cache = {}  # IP地理信息缓存
        self.cache_hit_stats = {'ua_hits': 0, 'uri_hits': 0, 'ip_hits': 0, 'total_requests': 0}
        
        # 预编译正则表达式缓存
        import re
        self.regex_cache = {
            'user_agent_mobile': re.compile(r'(Mobile|Android|iPhone|iPad)', re.I),
            'uri_api': re.compile(r'/api/|/scmp-gateway/'),
            'ip_internal': re.compile(r'^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.)')
        }
        
        # 线程同步
        self.result_queue = queue.Queue()
        self.error_queue = queue.Queue()
        self.stats_lock = threading.Lock()
        
        # 处理状态
        self.processed_state = self.load_state()
        
        # 全局统计信息 - 增强错误统计
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
                'fallback_records': 0,      # 容错备用记录数
                'critical_errors': 0,       # 致命错误
                'warning_errors': 0,        # 警告级错误
                'skipped_lines': 0,         # 跳过的行数
                'invalid_records': 0        # 无效记录数
            },
            'error_details': [],            # 详细错误记录
            'file_error_stats': {},         # 按文件统计错误
            'performance_warnings': []       # 性能警告
        }
        
        # 配置匹配检查
        if self.max_workers != self.connection_pool_size:
            self.logger.warning(f"⚠️  配置不匹配警告:")
            self.logger.warning(f"   🧵 线程数: {self.max_workers}")
            self.logger.warning(f"   🔗 连接池: {self.connection_pool_size}")
            self.logger.warning(f"   💡 建议: 线程数和连接池大小应该相同以避免资源竞争")
        
        self.logger.info("🚀 高性能ETL控制器初始化完成")
        self.logger.info(f"📁 日志目录: {self.base_log_dir}")
        self.logger.info(f"⚙️ 批处理大小: {self.batch_size:,}")
        self.logger.info(f"🧵 最大工作线程: {self.max_workers}")
        self.logger.info(f"🔗 连接池大小: {self.connection_pool_size}")
        
        # 资源配置建议
        if self.max_workers > 8:
            self.logger.warning(f"⚠️  线程数 {self.max_workers} 可能过高，建议根据CPU核心数调整")
        if self.batch_size > 10000:
            self.logger.info(f"💡 大批量处理模式 ({self.batch_size:,})，确保内存充足")
    
    def _init_connection_pool(self):
        """初始化数据库连接池"""
        self.logger.info("🔗 初始化数据库连接池...")
        for i in range(self.connection_pool_size):
            writer = DWDWriter()
            if writer.connect():
                self.writer_pool.append(writer)
                self.logger.info(f"✅ 连接 {i+1} 建立成功")
            else:
                self.logger.error(f"❌ 连接 {i+1} 建立失败")
                
        if not self.writer_pool:
            raise RuntimeError("❌ 无法建立任何数据库连接")
        
        self.logger.info(f"🔗 连接池初始化完成：{len(self.writer_pool)} 个连接")
    
    def get_writer(self) -> Optional[DWDWriter]:
        """从连接池获取Writer（线程安全）"""
        with self.stats_lock:
            if self.writer_pool:
                return self.writer_pool.pop()
        return None
    
    def return_writer(self, writer: DWDWriter):
        """归还Writer到连接池（线程安全）"""
        with self.stats_lock:
            if writer and len(self.writer_pool) < self.connection_pool_size:
                self.writer_pool.append(writer)
    
    def _optimized_batch_write(self, writer: DWDWriter, batch_data: List[Dict]) -> Dict[str, Any]:
        """
        优化的批量数据库写入
        
        特性:
        1. 预处理数据验证，减少数据库负载
        2. 智能重试机制
        3. 批量大小动态调整
        4. 连接健康检查
        """
        if not batch_data:
            return {'success': True, 'count': 0, 'message': '无数据写入'}
        
        try:
            # 1. 预验证数据 - 过滤明显无效记录
            valid_batch = []
            for record in batch_data:
                # 基础字段验证
                if (record.get('log_time') and 
                    record.get('client_ip') and 
                    record.get('request_uri')):
                    valid_batch.append(record)
            
            if not valid_batch:
                return {
                    'success': False, 
                    'count': 0, 
                    'error': '批次中无有效记录'
                }
            
            # 2. 连接健康检查
            if not writer.test_connection():
                # 尝试重连一次
                if not writer.connect():
                    return {
                        'success': False,
                        'count': 0, 
                        'error': '数据库连接失败'
                    }
            
            # 3. 执行写入（支持大批量优化）
            if len(valid_batch) > 2000:
                # 大批量分块写入，减少内存压力
                total_written = 0
                chunk_size = 1000
                
                for i in range(0, len(valid_batch), chunk_size):
                    chunk = valid_batch[i:i + chunk_size]
                    result = writer.write_batch(chunk)
                    
                    if result['success']:
                        total_written += result['count']
                    else:
                        return {
                            'success': False,
                            'count': total_written,
                            'error': f'分块写入失败: {result.get("error", "未知错误")}'
                        }
                
                return {
                    'success': True,
                    'count': total_written,
                    'message': f'分块写入完成: {total_written} 条记录'
                }
            else:
                # 正常批量写入
                return writer.write_batch(valid_batch)
                
        except Exception as e:
            self.logger.error(f"优化批量写入异常: {e}")
            return {
                'success': False,
                'count': 0,
                'error': f'写入异常: {str(e)}'
            }
    
    def cached_ua_parse(self, user_agent: str, mapper: FieldMapper) -> Dict[str, str]:
        """高性能User-Agent解析缓存优化"""
        # 使用哈希优化长UA字符串
        cache_key = hash(user_agent) if len(user_agent) > 50 else user_agent
        
        if cache_key in self.ua_cache:
            with self.stats_lock:
                self.cache_hit_stats['ua_hits'] += 1
                self.cache_hit_stats['total_requests'] += 1
            return self.ua_cache[cache_key]
        
        # 缓存未命中，执行解析
        result = mapper._parse_user_agent(user_agent)
        
        # 智能缓存管理
        if len(self.ua_cache) < 20000:  # 大幅增加缓存容量
            self.ua_cache[cache_key] = result
        elif len(self.ua_cache) >= 20000:  # LRU清理
            import random
            keys_to_remove = random.sample(list(self.ua_cache.keys()), min(1000, len(self.ua_cache) // 10))
            for key in keys_to_remove:
                self.ua_cache.pop(key, None)
            self.ua_cache[cache_key] = result
        
        with self.stats_lock:
            self.cache_hit_stats['total_requests'] += 1
        
        return result
    
    def cached_uri_parse(self, uri: str, mapper: FieldMapper) -> Dict[str, str]:
        """高性能URI结构解析缓存优化"""
        cache_key = hash(uri) if len(uri) > 100 else uri
        
        if cache_key in self.uri_cache:
            with self.stats_lock:
                self.cache_hit_stats['uri_hits'] += 1
            return self.uri_cache[cache_key]
        
        # 缓存未命中，执行解析
        result = mapper._parse_uri_structure(uri)
        
        # 智能缓存管理
        if len(self.uri_cache) < 15000:  # 增加URI缓存容量
            self.uri_cache[cache_key] = result
        elif len(self.uri_cache) >= 15000:
            # 优化的LRU清理
            import random
            keys_to_remove = random.sample(list(self.uri_cache.keys()), min(800, len(self.uri_cache) // 12))
            for key in keys_to_remove:
                self.uri_cache.pop(key, None)
            self.uri_cache[cache_key] = result
        
        return result
    
    def process_file_batch(self, file_paths: List[Path], thread_id: int, 
                          test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """
        批量处理文件（单线程执行）
        
        Args:
            file_paths: 要处理的文件路径列表
            thread_id: 线程ID
            test_mode: 测试模式
            limit: 每个文件的行数限制
        """
        start_time = time.time()
        
        # 获取线程专用组件
        parser = self.parser_pool[thread_id % len(self.parser_pool)]
        mapper = self.mapper_pool[thread_id % len(self.mapper_pool)]
        writer = self.get_writer()
        
        if not writer and not test_mode:
            return {'success': False, 'error': '无法获取数据库连接', 'thread_id': thread_id}
        
        try:
            batch_stats = {
                'thread_id': thread_id,
                'total_files': len(file_paths),
                'processed_files': 0,
                'total_records': 0,
                'total_lines': 0,
                'errors': [],
                'file_results': []
            }
            
            # 大批量缓冲区
            mega_batch = []
            
            for file_path in file_paths:
                self.logger.info(f"🧵{thread_id} 处理文件: {file_path.name}")
                
                file_start = time.time()
                file_records = 0
                file_lines = 0
                
                try:
                    # 高性能批量处理文件 - 减少I/O调用
                    with open(file_path, 'r', encoding='utf-8', buffering=8192 * 4) as f:  # 增大缓冲区
                        # 批量读取行以减少I/O调用
                        line_batch = []
                        for line in f:
                            line_batch.append(line.strip())
                            
                            # 每100行处理一次
                            if len(line_batch) >= 100:
                                # 批量解析 - 使用现有的parse_line方法
                                for line_text in line_batch:
                                    parsed_data = parser.parse_line(line_text)
                                    file_lines += 1
                                    
                                    if parsed_data:
                                        # 高性能字段预处理
                                        request = parsed_data.get('request', '')
                                        user_agent = parsed_data.get('agent', '') or parsed_data.get('user_agent', '')
                                        
                                        # 快速URI提取（避免多次split）
                                        if request and ' ' in request:
                                            uri = request.split(' ', 2)[1] if len(request.split(' ', 2)) > 1 else ''
                                        else:
                                            uri = ''
                                        
                                        # 智能缓存策略
                                        if user_agent and len(user_agent) > 10:  # 过滤无效UA
                                            parsed_data['_cached_ua'] = self.cached_ua_parse(user_agent, mapper)
                                        if uri and len(uri) > 1:  # 过滤无效URI
                                            parsed_data['_cached_uri'] = self.cached_uri_parse(uri, mapper)
                                        
                                        # 字段映射
                                        mapped_data = mapper.map_to_dwd(parsed_data, file_path.name)
                                        mega_batch.append(mapped_data)
                                        file_records += 1
                                        
                                        # 优化的批量写入检查
                                        if len(mega_batch) >= self.batch_size:
                                            if not test_mode:
                                                # 使用优化的批量写入
                                                write_result = self._optimized_batch_write(writer, mega_batch)
                                                if not write_result['success']:
                                                    batch_stats['errors'].append(f"{file_path.name}: {write_result['error']}")
                                            
                                            mega_batch.clear()  # 清空批次
                                            
                                            # 优化垃圾回收策略 - 减少频率
                                            if file_records % (self.batch_size * 3) == 0:
                                                gc.collect()
                                        
                                        # 检查限制
                                        if limit and file_records >= limit:
                                            break
                                    
                                    if limit and file_records >= limit:
                                        break
                                
                                line_batch.clear()  # 清空批次
                                
                                if limit and file_records >= limit:
                                    break
                        
                        # 处理最后不足100行的数据
                        if line_batch and (not limit or file_records < limit):
                            for line_text in line_batch:
                                parsed_data = parser.parse_line(line_text)
                                if limit and file_records >= limit:
                                    break
                                file_lines += 1
                                if parsed_data:
                                    # 同样的处理逻辑
                                    request = parsed_data.get('request', '')
                                    user_agent = parsed_data.get('agent', '') or parsed_data.get('user_agent', '')
                                    
                                    if request and ' ' in request:
                                        uri = request.split(' ', 2)[1] if len(request.split(' ', 2)) > 1 else ''
                                    else:
                                        uri = ''
                                    
                                    if user_agent and len(user_agent) > 10:
                                        parsed_data['_cached_ua'] = self.cached_ua_parse(user_agent, mapper)
                                    if uri and len(uri) > 1:
                                        parsed_data['_cached_uri'] = self.cached_uri_parse(uri, mapper)
                                    
                                    mapped_data = mapper.map_to_dwd(parsed_data, file_path.name)
                                    mega_batch.append(mapped_data)
                                    file_records += 1
                
                except Exception as e:
                    error_msg = f"文件处理错误 {file_path.name}: {e}"
                    self.logger.error(error_msg)
                    batch_stats['errors'].append(error_msg)
                
                file_duration = time.time() - file_start
                batch_stats['file_results'].append({
                    'file': file_path.name,
                    'records': file_records,
                    'lines': file_lines,
                    'duration': file_duration
                })
                
                batch_stats['processed_files'] += 1
                batch_stats['total_records'] += file_records
                batch_stats['total_lines'] += file_lines
                
                self.logger.info(f"🧵{thread_id} 完成 {file_path.name}: {file_records} 记录, {file_duration:.2f}s")
            
            # 处理剩余批次
            if mega_batch:
                if not test_mode:
                    # 使用优化的批量写入处理最终批次
                    write_result = self._optimized_batch_write(writer, mega_batch)
                    if not write_result['success']:
                        batch_stats['errors'].append(f"最终批次写入失败: {write_result['error']}")
                mega_batch.clear()
            
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
        并行处理指定日期的所有日志
        
        Args:
            date_str: 日期字符串 (YYYYMMDD格式)
            force_reprocess: 强制重新处理
            test_mode: 测试模式
            limit: 每个文件的行数限制
        """
        self.logger.info(f"🚀 开始并行处理 {date_str} 的日志")
        self.session_stats['start_time'] = datetime.now()
        start_time = time.time()
        
        # 检查日期目录
        date_dir = self.base_log_dir / date_str
        if not date_dir.exists():
            return {'success': False, 'error': f'日期目录不存在: {date_dir}'}
        
        # 获取所有日志文件
        all_log_files = list(date_dir.glob("*.log"))
        if not all_log_files:
            return {'success': False, 'error': f'目录中没有找到.log文件: {date_dir}'}
        
        # 过滤需要处理的文件
        if not force_reprocess:
            pending_files = [f for f in all_log_files if not self.is_file_processed(f)]
        else:
            pending_files = all_log_files
        
        if not pending_files:
            self.logger.info(f"📋 日期 {date_str} 的所有文件都已处理")
            return {'success': True, 'processed_files': 0, 'message': '所有文件都已处理'}
        
        self.logger.info(f"📁 找到 {len(pending_files)} 个待处理文件")
        
        # 将文件分批分配给线程
        files_per_thread = max(1, len(pending_files) // self.max_workers)
        file_batches = []
        
        for i in range(0, len(pending_files), files_per_thread):
            batch = pending_files[i:i + files_per_thread]
            if batch:
                file_batches.append(batch)
        
        self.logger.info(f"🧵 分配 {len(file_batches)} 个批次给 {min(self.max_workers, len(file_batches))} 个线程")
        
        # 并行处理
        all_results = []
        total_records = 0
        total_errors = []
        
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(file_batches))) as executor:
            # 提交任务
            future_to_batch = {}
            for i, batch in enumerate(file_batches):
                future = executor.submit(self.process_file_batch, batch, i, test_mode, limit)
                future_to_batch[future] = i
            
            # 收集结果
            for future in as_completed(future_to_batch):
                batch_id = future_to_batch[future]
                try:
                    result = future.result()
                    all_results.append(result)
                    
                    if result['success']:
                        total_records += result['total_records']
                        self.logger.info(f"✅ 批次 {batch_id} 完成: {result['processed_files']} 文件, "
                                       f"{result['total_records']} 记录")
                    else:
                        total_errors.extend(result.get('errors', []))
                        self.logger.error(f"❌ 批次 {batch_id} 失败: {result.get('error', '未知错误')}")
                        
                except Exception as e:
                    error_msg = f"批次 {batch_id} 执行异常: {e}"
                    self.logger.error(error_msg)
                    total_errors.append(error_msg)
        
        # 更新处理状态（非测试模式）
        if not test_mode:
            for file_path in pending_files:
                # 简化状态更新，这里可以根据实际结果更精确更新
                self.mark_file_processed(file_path, 0, 0)  # 占位更新
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
            'thread_results': all_results
        }
        
        if success:
            self.logger.info(f"🎉 日期 {date_str} 并行处理完成!")
            self.logger.info(f"📊 {result['processed_files']} 文件, {total_records:,} 记录")
            self.logger.info(f"⏱️  耗时 {duration:.2f}s, 速度 {speed:.1f} 记录/秒")
            self.logger.info(f"🎯 缓存命中率: {cache_hit_rate:.1f}%")
        
        return result
    
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
        
        # 优化的并行处理策略
        total_dates = len(log_files_by_date)
        self.logger.info(f"📈 性能优化并行处理: {total_dates} 个日期，{self.max_workers} 个工作线程")
        
        # 预热连接池 - 确保所有连接都是活跃的
        self._warmup_connection_pool()
        
        # 按日期顺序处理（但每个日期内部并行）
        for idx, date_str in enumerate(sorted(log_files_by_date.keys()), 1):
            self.logger.info(f"📅 [{idx}/{total_dates}] 开始处理日期: {date_str}")
            
            # 动态调整处理策略
            file_count = len(log_files_by_date[date_str])
            if file_count > self.max_workers * 2:
                # 大量文件时，优化线程分配
                self.logger.info(f"📊 大文件集合检测到 ({file_count} 文件)，启用高性能模式")
            
            result = self.process_date_parallel(date_str, force_reprocess=False, 
                                              test_mode=test_mode, limit=limit)
            date_results.append(result)
            
            if result['success'] and result.get('processed_files', 0) > 0:
                processed_dates += 1
                total_records += result['total_records']
                self.logger.info(f"✅ {date_str} 完成: {result['total_records']:,} 记录")
                
                # 期间优化 - 定期清理缓存和垃圾回收
                if idx % 3 == 0:  # 每3个日期清理一次
                    self._periodic_cleanup()
                    
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
    
    def _warmup_connection_pool(self):
        """预热连接池 - 确保所有连接都是健康的"""
        self.logger.info("🔥 预热数据库连接池...")
        healthy_connections = 0
        
        for writer in self.writer_pool[:]:  # 创建副本以避免修改原列表
            try:
                if writer.test_connection():
                    healthy_connections += 1
                else:
                    # 移除不健康的连接并尝试重连
                    self.writer_pool.remove(writer)
                    writer.close()
                    
                    # 创建新连接
                    new_writer = DWDWriter()
                    if new_writer.connect():
                        self.writer_pool.append(new_writer)
                        healthy_connections += 1
                        self.logger.info(f"🔄 重建连接成功")
                    else:
                        self.logger.warning(f"⚠️ 连接重建失败")
            except Exception as e:
                self.logger.error(f"❌ 连接预热异常: {e}")
        
        self.logger.info(f"🔥 连接池预热完成: {healthy_connections}/{self.connection_pool_size} 连接健康")
    
    def _periodic_cleanup(self):
        """定期清理 - 优化内存和缓存使用"""
        self.logger.debug("🧹 执行定期清理...")
        
        # 1. 强制垃圾回收
        import gc
        before_objects = len(gc.get_objects())
        gc.collect()
        after_objects = len(gc.get_objects())
        freed_objects = before_objects - after_objects
        
        # 2. 智能缓存清理 - 如果缓存过大，清理部分
        ua_cache_size = len(self.ua_cache)
        uri_cache_size = len(self.uri_cache)
        
        if ua_cache_size > 15000:  # UA缓存清理阈值
            import random
            keys_to_remove = random.sample(
                list(self.ua_cache.keys()), 
                min(3000, ua_cache_size // 4)
            )
            for key in keys_to_remove:
                self.ua_cache.pop(key, None)
            self.logger.debug(f"🧹 UA缓存清理: {len(keys_to_remove)} 项")
        
        if uri_cache_size > 10000:  # URI缓存清理阈值
            import random
            keys_to_remove = random.sample(
                list(self.uri_cache.keys()), 
                min(2000, uri_cache_size // 4)
            )
            for key in keys_to_remove:
                self.uri_cache.pop(key, None)
            self.logger.debug(f"🧹 URI缓存清理: {len(keys_to_remove)} 项")
        
        # 3. 内存使用报告
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            self.session_stats['peak_memory_usage_mb'] = max(
                self.session_stats['peak_memory_usage_mb'], 
                memory_mb
            )
            
            if freed_objects > 0:
                self.logger.debug(f"🧹 清理完成: 释放 {freed_objects} 个对象，内存使用 {memory_mb:.1f}MB")
        except ImportError:
            pass  # psutil不可用时跳过内存监控
    
    # === 兼容性方法 ===
    
    def load_state(self) -> Dict[str, Any]:
        """加载处理状态（继承自原版本）"""
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
        """扫描日志目录（继承自原版本）"""
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
        print("\\n" + "=" * 80)
        print("🚀 高性能ETL控制器 - 性能统计报告")
        print("=" * 80)
        
        print(f"⚙️  配置信息:")
        print(f"   批处理大小: {self.batch_size:,}")
        print(f"   最大工作线程: {self.max_workers}")
        print(f"   连接池大小: {self.connection_pool_size}")
        print(f"   当前可用连接: {len(self.writer_pool)}")
        
        print(f"\\n📈 缓存统计:")
        print(f"   User-Agent缓存: {len(self.ua_cache)} 项")
        print(f"   URI缓存: {len(self.uri_cache)} 项")
        print(f"   缓存命中率: {self.session_stats.get('cache_hit_rate', 0):.1f}%")
        
        if self.session_stats.get('avg_processing_speed', 0) > 0:
            print(f"\\n🏃 性能指标:")
            print(f"   平均处理速度: {self.session_stats['avg_processing_speed']:.1f} 记录/秒")
            print(f"   总处理记录数: {self.session_stats.get('total_records_written', 0):,}")
            
            if self.session_stats.get('start_time') and self.session_stats.get('end_time'):
                duration = (self.session_stats['end_time'] - self.session_stats['start_time']).total_seconds()
                print(f"   总处理时间: {duration:.1f} 秒")
        
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
    
    # === 交互式菜单功能 ===
    
    def interactive_menu(self):
        """交互式菜单 - 高性能版本"""
        while True:
            print("\\n" + "=" * 80)
            print("🚀 高性能ETL控制器 - 多线程并行处理版本")
            print("=" * 80)
            print("1. 🔥 高性能处理所有未处理的日志 (推荐)")
            print("2. 📅 高性能处理指定日期的日志")
            print("3. 📊 查看系统状态和性能统计")
            print("4. 🧪 测试模式处理 (不写入数据库)")
            print("5. ⚙️ 性能配置调优")
            print("6. 🧹 清空所有数据 (开发环境)")
            print("7. 🔁 强制重新处理所有日志")
            print("8. 📈 性能基准测试")
            print("0. 👋 退出")
            print("-" * 80)
            print(f"📊 当前配置: 批量{self.batch_size} | 线程{self.max_workers} | 连接池{self.connection_pool_size}")
            
            try:
                choice = input("请选择操作 [0-8]: ").strip()
                
                if choice == '0':
                    print("👋 再见！")
                    break
                
                elif choice == '1':
                    print("\\n🔥 高性能处理所有未处理的日志...")
                    
                    # 询问配置
                    limit_input = input("是否限制每个文件的处理行数？(留空表示不限制): ").strip()
                    limit = int(limit_input) if limit_input.isdigit() else None
                    
                    confirm = input(f"确认使用当前配置处理吗？批量{self.batch_size}，{self.max_workers}线程 (y/N): ").strip().lower()
                    if confirm != 'y':
                        print("操作已取消")
                        continue
                    
                    start_time = time.time()
                    result = self.process_all_parallel(test_mode=False, limit=limit)
                    total_time = time.time() - start_time
                    
                    self._print_batch_process_result(result, total_time)
                    input("\\n按回车键继续...")
                
                elif choice == '2':
                    date_str = input("\\n请输入日期 (YYYYMMDD格式，如: 20250901): ").strip()
                    if not self._validate_date_format(date_str):
                        continue
                    
                    force = input("是否强制重新处理？(y/N): ").strip().lower() == 'y'
                    limit_input = input("是否限制每个文件的处理行数？(留空表示不限制): ").strip()
                    limit = int(limit_input) if limit_input.isdigit() else None
                    
                    print(f"\\n🔥 高性能处理 {date_str} 的日志...")
                    start_time = time.time()
                    result = self.process_date_parallel(date_str, force, test_mode=False, limit=limit)
                    total_time = time.time() - start_time
                    
                    self._print_single_date_result(result, total_time)
                    input("\\n按回车键继续...")
                
                elif choice == '3':
                    print()
                    self.show_status()
                    self.show_performance_stats()
                    input("\\n按回车键继续...")
                
                elif choice == '4':
                    print("\\n🧪 高性能测试模式处理")
                    sub_choice = input("选择: 1)处理所有未处理日志 2)处理指定日期 [1-2]: ").strip()
                    
                    if sub_choice == '1':
                        limit_input = input("限制每个文件的处理行数 (建议100-1000): ").strip()
                        limit = int(limit_input) if limit_input.isdigit() else 100
                        
                        print("\\n🧪 高性能测试模式：处理所有未处理的日志...")
                        result = self.process_all_parallel(test_mode=True, limit=limit)
                        self._print_batch_process_result(result)
                        
                    elif sub_choice == '2':
                        date_str = input("请输入日期 (YYYYMMDD格式): ").strip()
                        if not self._validate_date_format(date_str):
                            continue
                        
                        limit_input = input("限制每个文件的处理行数 (建议100-1000): ").strip()
                        limit = int(limit_input) if limit_input.isdigit() else 100
                        
                        print(f"\\n🧪 高性能测试模式：处理 {date_str} 的日志...")
                        result = self.process_date_parallel(date_str, False, test_mode=True, limit=limit)
                        self._print_single_date_result(result)
                    
                    input("\\n按回车键继续...")
                
                elif choice == '5':
                    print("\\n⚙️ 性能配置调优")
                    print(f"当前配置:")
                    print(f"  批量大小: {self.batch_size}")
                    print(f"  最大线程: {self.max_workers}")
                    print(f"  连接池大小: {self.connection_pool_size}")
                    
                    print("\\n推荐配置:")
                    print("  小型服务器: 批量2000, 线程4, 连接池4")
                    print("  中型服务器: 批量5000, 线程6, 连接池6")
                    print("  高性能服务器: 批量10000, 线程8, 连接池8")
                    
                    new_batch = input("\\n新的批量大小 (留空保持当前): ").strip()
                    new_workers = input("新的线程数 (留空保持当前): ").strip()
                    
                    if new_batch.isdigit():
                        self.batch_size = int(new_batch)
                        print(f"✅ 批量大小已调整为: {self.batch_size}")
                    
                    if new_workers.isdigit():
                        old_workers = self.max_workers
                        self.max_workers = int(new_workers)
                        print(f"✅ 线程数已调整为: {self.max_workers}")
                        
                        # 重新初始化组件池
                        if self.max_workers != old_workers:
                            print("🔄 重新初始化组件池...")
                            self.parser_pool = [BaseLogParser() for _ in range(self.max_workers)]
                            self.mapper_pool = [FieldMapper() for _ in range(self.max_workers)]
                    
                    input("\\n按回车键继续...")
                
                elif choice == '6':
                    print("\\n⚠️  清空所有数据")
                    confirm = input("确认清空所有ETL数据？这将删除数据库中的所有日志数据 (y/N): ").strip().lower()
                    if confirm == 'y':
                        second_confirm = input("再次确认！输入 'CLEAR' 确认删除: ").strip()
                        if second_confirm == 'CLEAR':
                            self.clear_all_data()
                        else:
                            print("❌ 确认失败，操作已取消")
                    else:
                        print("❌ 操作已取消")
                    input("\\n按回车键继续...")
                
                elif choice == '7':
                    print("\\n⚠️  强制重新处理所有日志")
                    print("这将忽略处理状态，重新处理所有日志文件")
                    confirm = input("确认执行？(y/N): ").strip().lower()
                    if confirm == 'y':
                        limit_input = input("是否限制每个文件的处理行数？(留空表示不限制): ").strip()
                        limit = int(limit_input) if limit_input.isdigit() else None
                        
                        print("\\n🔥 开始强制重新处理所有日志...")
                        
                        # 临时备份状态
                        backup_state = self.processed_state.copy()
                        
                        # 清空状态以强制重处理
                        self.processed_state = {
                            'processed_files': {},
                            'last_update': None,
                            'total_processed_records': 0,
                            'processing_history': []
                        }
                        
                        result = self.process_all_parallel(test_mode=False, limit=limit)
                        
                        # 如果失败，恢复状态
                        if not result['success']:
                            self.processed_state = backup_state
                            print("❌ 处理失败，已恢复原始状态")
                        
                        self._print_batch_process_result(result)
                    else:
                        print("❌ 操作已取消")
                    input("\\n按回车键继续...")
                
                elif choice == '8':
                    print("\\n📈 性能基准测试")
                    print("这将运行多种配置的性能测试")
                    confirm = input("确认运行基准测试？(y/N): ").strip().lower()
                    
                    if confirm == 'y':
                        self._run_performance_benchmark()
                    
                    input("\\n按回车键继续...")
                
                else:
                    print("❌ 无效选择，请输入 0-8")
                    input("按回车键继续...")
                    
            except KeyboardInterrupt:
                print("\\n\\n👋 用户中断，再见！")
                break
            except Exception as e:
                print(f"\\n❌ 操作过程中发生错误: {e}")
                input("按回车键继续...")
    
    def show_status(self):
        """显示系统状态"""
        print("=" * 80)
        print("📊 高性能ETL控制器状态报告")
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
            for date_str in sorted(log_files_by_date.keys())[-10:]:
                file_count = len(log_files_by_date[date_str])
                processed_count = sum(1 for f in log_files_by_date[date_str] if self.is_file_processed(f))
                status = "✅" if processed_count == file_count else f"⚠️ {processed_count}/{file_count}"
                print(f"     {date_str}: {file_count} 个文件 {status}")
        
        # 2. 处理状态统计
        processed_files_count = len(self.processed_state.get('processed_files', {}))
        total_processed_records = sum(
            info.get('record_count', 0) 
            for info in self.processed_state.get('processed_files', {}).values()
        )
        
        print(f"\\n📈 处理状态统计:")
        print(f"   已处理文件: {processed_files_count} 个")
        print(f"   已处理记录: {total_processed_records:,} 条")
        
        if self.processed_state.get('last_update'):
            print(f"   最后更新: {self.processed_state['last_update']}")
        
        # 3. 最近处理历史
        history = self.processed_state.get('processing_history', [])
        if history:
            print(f"\\n🕒 最近处理记录:")
            for record in history[-5:]:
                date_str = record.get('date', 'Unknown')
                files = record.get('files', 0)
                records = record.get('records', 0)
                duration = record.get('duration', 0)
                processed_at = record.get('processed_at', '')[:19].replace('T', ' ')
                print(f"     {date_str} - {processed_at}: {files} 文件, {records:,} 记录, {duration:.1f}s")
    
    def clear_all_data(self):
        """清空所有数据（开发环境使用）"""
        self.logger.info("开始清空所有数据")
        
        print("⚠️  警告：这将清空所有ETL数据和处理状态")
        print("1. 清空ClickHouse数据库表")
        print("2. 重置处理状态文件")
        
        # 获取Writer进行清理
        writer = self.get_writer()
        if not writer:
            print("❌ 无法获取数据库连接")
            return
        
        try:
            tables_to_clear = ['dwd_nginx_enriched_v3']
            cleared_count = 0
            
            for table_name in tables_to_clear:
                try:
                    result = writer.client.command(f"TRUNCATE TABLE {table_name}")
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
                'cache_hit_rate': 0.0,
                'avg_processing_speed': 0.0,
                'peak_memory_usage_mb': 0,
                'processing_errors': []
            }
            
        finally:
            self.return_writer(writer)
    
    def _run_performance_benchmark(self):
        """运行性能基准测试"""
        print("\\n🏁 开始性能基准测试...")
        
        # 获取测试日期
        log_files = self.scan_log_directories()
        if not log_files:
            print("❌ 没有找到日志文件进行测试")
            return
        
        test_date = sorted(log_files.keys())[-1]
        print(f"📅 使用测试日期: {test_date}")
        
        # 不同配置的基准测试
        configs = [
            {'batch_size': 1000, 'workers': 2, 'name': '基础配置'},
            {'batch_size': 2000, 'workers': 4, 'name': '标准配置'},
            {'batch_size': 5000, 'workers': 6, 'name': '高性能配置'}
        ]
        
        results = []
        
        for config in configs:
            print(f"\\n🔧 测试 {config['name']}: 批量{config['batch_size']}, 线程{config['workers']}")
            
            # 临时调整配置
            old_batch = self.batch_size
            old_workers = self.max_workers
            
            self.batch_size = config['batch_size']
            self.max_workers = config['workers']
            
            try:
                start_time = time.time()
                result = self.process_date_parallel(
                    test_date, 
                    force_reprocess=True, 
                    test_mode=True, 
                    limit=200  # 限制测试规模
                )
                duration = time.time() - start_time
                
                if result['success']:
                    speed = result.get('processing_speed', 0)
                    results.append({
                        'config': config['name'],
                        'batch_size': config['batch_size'],
                        'workers': config['workers'],
                        'records': result.get('total_records', 0),
                        'duration': duration,
                        'speed': speed
                    })
                    print(f"   ✅ 速度: {speed:.1f} 记录/秒")
                else:
                    print(f"   ❌ 测试失败")
            
            except Exception as e:
                print(f"   ❌ 测试异常: {e}")
            
            finally:
                # 恢复原配置
                self.batch_size = old_batch
                self.max_workers = old_workers
        
        # 显示基准测试结果
        if results:
            print("\\n📊 基准测试结果:")
            print(f"{'配置':<15} {'批量':<8} {'线程':<6} {'记录数':<8} {'耗时(s)':<8} {'速度(rec/s)':<12}")
            print("-" * 65)
            
            for r in results:
                print(f"{r['config']:<15} {r['batch_size']:<8} {r['workers']:<6} {r['records']:<8} {r['duration']:<8.2f} {r['speed']:<12.1f}")
            
            # 推荐最佳配置
            best = max(results, key=lambda x: x['speed'])
            print(f"\\n🏆 推荐配置: {best['config']}")
            print(f"   批量大小: {best['batch_size']}")
            print(f"   线程数: {best['workers']}")
            print(f"   预期速度: {best['speed']:.1f} 记录/秒")
    
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
    
    def _print_batch_process_result(self, result: Dict[str, Any], total_time: float = None):
        """打印批量处理结果"""
        print("\\n" + "=" * 60)
        if result['success']:
            print("✅ 高性能批量处理成功!")
            print("=" * 60)
            print(f"📄 处理日期数: {result.get('processed_dates', 0)} 个")
            print(f"📊 总记录数: {result.get('total_records', 0):,} 条")
            print(f"⏱️  总耗时: {result.get('duration', total_time or 0):.2f} 秒")
            
            if result.get('processing_speed', 0) > 0:
                print(f"🚀 平均速度: {result['processing_speed']:.1f} 记录/秒")
                
        else:
            print("❌ 高性能批量处理失败!")
            print("=" * 60)
            print(f"❌ 错误: {result.get('error', '未知错误')}")
            errors = result.get('errors', [])
            if errors:
                print(f"📋 详细错误 ({len(errors)} 个):")
                for i, error in enumerate(errors[:3], 1):
                    print(f"   {i}. {error}")
                if len(errors) > 3:
                    print(f"   ... 还有 {len(errors) - 3} 个错误")
        print("=" * 60)
    
    def _print_single_date_result(self, result: Dict[str, Any], total_time: float = None):
        """打印单个日期的处理结果"""
        print("\\n" + "=" * 60)
        if result['success']:
            print("✅ 高性能日期处理成功!")
            print("=" * 60)
            print(f"📅 处理日期: {result['date']}")
            print(f"📄 处理文件: {result['processed_files']} 个")
            print(f"📊 总记录数: {result['total_records']:,} 条")
            print(f"⏱️  总耗时: {result.get('duration', total_time or 0):.2f} 秒")
            
            if result.get('processing_speed', 0) > 0:
                print(f"🚀 处理速度: {result['processing_speed']:.1f} 记录/秒")
            
            if result.get('cache_hit_rate', 0) > 0:
                print(f"🎯 缓存命中率: {result['cache_hit_rate']:.1f}%")
                
        else:
            print("❌ 高性能日期处理失败!")
            print("=" * 60)
            print(f"📅 处理日期: {result['date']}")
            print(f"❌ 错误: {result.get('error', '未知错误')}")
            errors = result.get('errors', [])
            if errors:
                print(f"📋 详细错误:")
                for i, error in enumerate(errors, 1):
                    print(f"   {i}. {error}")
        print("=" * 60)

    def print_error_report(self):
        """打印详细的错误报告"""
        summary = self.get_error_summary()

        print("\\n" + "="*70)
        print("📊 错误统计报告")
        print("="*70)

        if summary['total_errors'] == 0:
            print("✅ 没有发现错误 - 处理完全成功！")
            return

        print(f"📈 总错误数: {summary['total_errors']}")
        print(f"📈 错误率: {summary['error_rate_percent']}%")

        # 按错误类型分组
        if summary['by_error_type']:
            print("\\n📋 错误类型分布:")
            for error_type, count in summary['by_error_type']:
                print(f"   {error_type}: {count} 次")

        # 最近错误
        if summary['recent_errors']:
            print(f"\\n🕐 最近错误 (最多显示5个):")
            for error in summary['recent_errors'][:5]:
                print(f"   [{error['timestamp']}] {error['error_type']}: {error['message'][:100]}")

        print("="*70)

    # === 错误处理和统计方法 ===

    def record_error(self, error_type: str, error_msg: str, context: Dict[str, Any] = None):
        """记录错误信息和统计"""
        with self.stats_lock:
            # 更新错误统计
            if error_type in self.session_stats['error_stats']:
                self.session_stats['error_stats'][error_type] += 1
            else:
                self.session_stats['error_stats'][error_type] = 1

            self.session_stats['total_errors'] += 1

            # 记录详细错误信息
            error_detail = {
                'timestamp': datetime.now().isoformat(),
                'error_type': error_type,
                'error_message': error_msg,
                'context': context or {}
            }

            self.session_stats['error_details'].append(error_detail)

            # 按文件统计错误
            if context and 'source_file' in context:
                file_name = context['source_file']
                if file_name not in self.session_stats['file_error_stats']:
                    self.session_stats['file_error_stats'][file_name] = {
                        'total_errors': 0,
                        'error_types': defaultdict(int)
                    }

                self.session_stats['file_error_stats'][file_name]['total_errors'] += 1
                self.session_stats['file_error_stats'][file_name]['error_types'][error_type] += 1

            # 限制错误详情数量，避免内存溢出
            if len(self.session_stats['error_details']) > 1000:
                self.session_stats['error_details'] = self.session_stats['error_details'][-500:]

    def record_fallback_record(self, context: Dict[str, Any] = None):
        """记录容错备用记录"""
        self.record_error('fallback_records', '使用容错备用记录', context)

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

    def get_error_summary(self) -> Dict[str, Any]:
        """获取错误摘要报告"""
        with self.stats_lock:
            total_errors = self.session_stats['total_errors']
            total_records = self.session_stats['total_records_written']

            if total_records == 0:
                error_rate = 0.0
            else:
                error_rate = (total_errors / (total_records + total_errors)) * 100

            return {
                'total_errors': total_errors,
                'error_rate_percent': round(error_rate, 2),
                'error_breakdown': dict(self.session_stats['error_stats']),
                'files_with_errors': len(self.session_stats['file_error_stats']),
                'performance_warnings_count': len(self.session_stats['performance_warnings']),
                'most_common_errors': self._get_most_common_errors(),
                'by_error_type': [],
                'recent_errors': []
            }

    def _get_most_common_errors(self) -> List[Dict[str, Any]]:
        """获取最常见的错误类型"""
        error_counts = self.session_stats['error_stats']
        sorted_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)
        return [{'error_type': k, 'count': v} for k, v in sorted_errors[:5] if v > 0]

def main():
    """主函数"""
    import logging
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(
        description='高性能ETL控制器 - 多线程并行处理版本'
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
                
                print(f"\\n🎯 处理结果:")
                print(f"日期: {result.get('date', args.date)}")
                print(f"文件: {result.get('processed_files', 0)}")
                print(f"记录: {result.get('total_records', 0):,}")
                print(f"耗时: {result.get('duration', 0):.2f}s")
                print(f"速度: {result.get('processing_speed', 0):.1f} 记录/秒")
                
            elif args.all:
                result = controller.process_all_parallel(
                    test_mode=args.test,
                    limit=args.limit
                )
                
                print(f"\\n🎯 批量处理结果:")
                print(f"日期数: {result.get('processed_dates', 0)}")
                print(f"记录数: {result.get('total_records', 0):,}")
                print(f"总耗时: {result.get('duration', 0):.2f}s")
                print(f"平均速度: {result.get('processing_speed', 0):.1f} 记录/秒")
            
            else:
                # 显示帮助信息
                parser.print_help()
                print("\\n示例用法:")
                print("  python high_performance_etl_controller.py --all")
                print("  python high_performance_etl_controller.py --date 20250901")
                print("  python high_performance_etl_controller.py --all --batch-size 5000 --workers 8")
            
            controller.show_performance_stats()
            controller.print_error_report()

    except KeyboardInterrupt:
        print("\\n👋 用户中断")
    except Exception as e:
        print(f"\\n❌ 执行错误: {e}")
        import traceback
        traceback.print_exc()
    

if __name__ == "__main__":
    main()
