#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
集成超高性能ETL控制器 - 完全集成原有功能 + 自动文件发现
Integrated Ultra Performance ETL Controller with Auto Discovery

核心改进：
1. 完全集成原有交互式流程和状态管理
2. 解决性能衰减问题 (300->140 RPS)
3. 优化进度刷新频率 (1-5分钟可配置)
4. 保留所有原有功能特性
5. 增强缓存管理防止内存膨胀
6. 新增自动文件发现和监控功能
"""

import sys
import os
import json
import time
import argparse
import threading
import queue
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict, deque
import gc
import traceback
import logging

# 添加路径以导入其他模块
current_dir = Path(__file__).parent
etl_root = current_dir.parent
sys.path.append(str(etl_root))

from parsers.base_log_parser import BaseLogParser
# ========== FieldMapper选择 ==========
# 方案一：使用增强版 (推荐) - 包含政务应用识别和性能监控
# from processors.enhanced_field_mapper_with_monitoring import EnhancedFieldMapperWithMonitoring as FieldMapper
from processors.enhanced_field_mapper_v2 import EnhancedFieldMapperV2 as FieldMapper

# 方案二：使用兼容性包装器 - 保持原有接口
# from processors.field_mapper_enhanced import FieldMapper

# 方案三：使用原版 (当前)
#from processors.field_mapper import FieldMapper
# =====================================
from writers.dwd_writer import DWDWriter

# 尝试导入可选依赖
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# 安全打印函数 - 处理 Windows 控制台编码问题
import platform
import builtins

# 保存原始 print 函数
_original_print = builtins.print

def safe_print(*args, **kwargs):
    """安全打印函数，自动处理编码错误"""
    try:
        _original_print(*args, **kwargs)
    except UnicodeEncodeError:
        # 如果遇到编码错误，移除 emoji 和特殊字符
        message = ' '.join(str(arg) for arg in args)
        # 移除所有 emoji（Unicode > 0xFFFF）
        safe_message = ''.join(
            char if ord(char) < 0x10000 else '[EMOJI]'
            for char in message
        )
        _original_print(safe_message, **kwargs)
    except Exception as e:
        # 最后的保护：如果还是失败，输出 ASCII 版本
        try:
            message = ' '.join(str(arg) for arg in args)
            ascii_message = message.encode('ascii', errors='replace').decode('ascii')
            _original_print(ascii_message, **kwargs)
        except:
            # 完全失败时的最后防线
            _original_print("[OUTPUT ERROR]", **kwargs)

# 替换内置 print（仅在 Windows 上）
if platform.system() == 'Windows':
    builtins.print = safe_print

class AutoFileDiscovery:
    """自动文件发现器"""

    def __init__(self, log_dir: Path, scan_interval: int = 180):
        self.log_dir = log_dir
        self.scan_interval = scan_interval  # 默认3分钟
        self.last_scan_time = 0
        self.known_files = set()
        self.logger = logging.getLogger(__name__)

    def discover_new_files(self) -> List[Path]:
        """发现新增的日志文件"""
        current_time = time.time()
        if current_time - self.last_scan_time < self.scan_interval:
            return []

        new_files = []
        try:
            if not self.log_dir.exists():
                return []

            # 扫描所有日期目录下的.log文件
            for date_dir in self.log_dir.iterdir():
                if date_dir.is_dir() and date_dir.name.isdigit() and len(date_dir.name) == 8:
                    try:
                        datetime.strptime(date_dir.name, '%Y%m%d')
                        for log_file in date_dir.glob("*.log"):
                            if log_file not in self.known_files:
                                # 检查文件是否稳定（最后修改时间超过30秒）
                                if time.time() - log_file.stat().st_mtime > 30:
                                    new_files.append(log_file)
                                    self.known_files.add(log_file)
                    except (ValueError, OSError):
                        continue

            self.last_scan_time = current_time
            if new_files:
                self.logger.info(f"发现 {len(new_files)} 个新日志文件")

        except Exception as e:
            self.logger.error(f"文件发现过程出错: {e}")

        return new_files

    def initialize_known_files(self, existing_files: set = None):
        """初始化已知文件列表"""
        try:
            if existing_files:
                self.known_files.update(existing_files)

            if not self.log_dir.exists():
                return

            for date_dir in self.log_dir.iterdir():
                if date_dir.is_dir() and date_dir.name.isdigit() and len(date_dir.name) == 8:
                    try:
                        datetime.strptime(date_dir.name, '%Y%m%d')
                        for log_file in date_dir.glob("*.log"):
                            self.known_files.add(log_file)
                    except (ValueError, OSError):
                        continue

            self.logger.info(f"初始化已知文件列表，共 {len(self.known_files)} 个文件")
        except Exception as e:
            self.logger.error(f"初始化已知文件列表失败: {e}")

class PerformanceOptimizer:
    """性能优化器 - 解决性能衰减问题"""

    def __init__(self):
        self.performance_history = deque(maxlen=50)
        self.cache_optimization_interval = 300  # 5分钟
        self.gc_optimization_interval = 180     # 3分钟
        self.last_cache_cleanup = time.time()
        self.last_gc_cleanup = time.time()

    def monitor_performance(self, current_speed: float):
        """监控性能并记录历史"""
        self.performance_history.append({
            'timestamp': time.time(),
            'speed': current_speed
        })

    def should_optimize_cache(self) -> bool:
        """判断是否需要缓存优化"""
        return time.time() - self.last_cache_cleanup > self.cache_optimization_interval

    def should_optimize_gc(self) -> bool:
        """判断是否需要GC优化"""
        return time.time() - self.last_gc_cleanup > self.gc_optimization_interval

    def optimize_cache(self, cache_dict: dict, max_size: int = 5000):
        """优化缓存大小"""
        if len(cache_dict) > max_size:
            # 保留最近使用的一半
            items = list(cache_dict.items())
            keep_count = max_size // 2
            cache_dict.clear()
            cache_dict.update(items[-keep_count:])

        self.last_cache_cleanup = time.time()
        return len(cache_dict)

    def optimize_gc(self) -> int:
        """执行垃圾回收优化"""
        collected = gc.collect()
        self.last_gc_cleanup = time.time()
        return collected

    def get_performance_trend(self) -> str:
        """获取性能趋势"""
        if len(self.performance_history) < 10:
            return "insufficient_data"

        recent_speeds = [h['speed'] for h in list(self.performance_history)[-10:]]
        early_speeds = [h['speed'] for h in list(self.performance_history)[:10]]

        recent_avg = sum(recent_speeds) / len(recent_speeds)
        early_avg = sum(early_speeds) / len(early_speeds)

        if recent_avg < early_avg * 0.8:
            return "declining"
        elif recent_avg > early_avg * 1.1:
            return "improving"
        else:
            return "stable"

class IntegratedProgressTracker:
    """集成进度追踪器 - 完全兼容原有状态管理"""

    def __init__(self, logger, refresh_interval_minutes: int = 3):
        self.logger = logger
        self.refresh_interval = refresh_interval_minutes * 60  # 转换为秒
        self.last_display_time = 0

        # 全局统计 (兼容原有格式)
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
            'error_stats': {
                'parsing_errors': 0,
                'mapping_errors': 0,
                'database_errors': 0,
                'critical_errors': 0
            },
            'detailed_error_log': []
        }

        # 实时统计 (新增)
        self.real_time_stats = {
            'total_written_records': 0,
            'total_written_batches': 0,
            'last_batch_time': time.time()
        }

        # 当前处理状态
        self.current_files = {}
        self.completed_files = []
        self.performance_samples = deque(maxlen=100)

        # 文件级别进度 (兼容原有格式)
        self.file_progress = {}

    def start_session(self, total_files: int = 0):
        """开始处理会话"""
        self.session_stats['start_time'] = time.time()
        self.session_stats['total_files_to_process'] = total_files
        self.logger.info(f"🚀 开始处理会话，预计处理 {total_files} 个文件")

    def start_file_processing(self, thread_id: int, file_path: Path, estimated_lines: int = None):
        """开始文件处理"""
        self.current_files[thread_id] = {
            'file_path': file_path,
            'start_time': time.time(),
            'processed_lines': 0,
            'processed_records': 0,
            'estimated_lines': estimated_lines or 10000,
            'errors': 0
        }

        self.file_progress[str(file_path)] = {
            'status': 'processing',
            'start_time': time.time(),
            'thread_id': thread_id,
            'processed_lines': 0,
            'estimated_lines': estimated_lines or 10000
        }

    def update_file_progress(self, thread_id: int, lines_processed: int, records_processed: int, errors: int = 0):
        """更新文件处理进度"""
        if thread_id in self.current_files:
            file_info = self.current_files[thread_id]
            file_info['processed_lines'] = lines_processed
            file_info['processed_records'] = records_processed
            file_info['errors'] = errors

            # 计算当前速度
            elapsed = time.time() - file_info['start_time']
            if elapsed > 0:
                current_speed = records_processed / elapsed
                self.performance_samples.append(current_speed)

            # 更新文件级别进度
            file_path = str(file_info['file_path'])
            if file_path in self.file_progress:
                self.file_progress[file_path]['processed_lines'] = lines_processed

    def update_batch_written(self, records_count: int):
        """更新批次写入统计"""
        self.real_time_stats['total_written_records'] += records_count
        self.real_time_stats['total_written_batches'] += 1
        self.real_time_stats['last_batch_time'] = time.time()

    def complete_file_processing(self, thread_id: int, final_records: int, final_errors: int):
        """完成文件处理"""
        if thread_id in self.current_files:
            file_info = self.current_files[thread_id]
            processing_time = time.time() - file_info['start_time']

            # 更新会话统计
            self.session_stats['total_files_processed'] += 1
            self.session_stats['total_lines_processed'] += file_info['processed_lines']
            self.session_stats['total_records_written'] += final_records
            self.session_stats['total_errors'] += final_errors

            # 记录完成的文件
            completed_file = {
                'file_path': str(file_info['file_path']),
                'processing_time': processing_time,
                'records_processed': final_records,
                'errors': final_errors,
                'speed': final_records / processing_time if processing_time > 0 else 0
            }
            self.completed_files.append(completed_file)

            # 更新文件进度状态
            file_path = str(file_info['file_path'])
            if file_path in self.file_progress:
                self.file_progress[file_path]['status'] = 'completed'
                self.file_progress[file_path]['end_time'] = time.time()
                self.file_progress[file_path]['records_processed'] = final_records

            # 清理当前处理状态
            del self.current_files[thread_id]

    def should_display_progress(self) -> bool:
        """判断是否应该显示进度"""
        current_time = time.time()
        return current_time - self.last_display_time >= self.refresh_interval

    def display_progress(self, force: bool = False):
        """显示进度信息"""
        current_time = time.time()

        if not force and not self.should_display_progress():
            return

        self.last_display_time = current_time

        if not self.session_stats['start_time']:
            return

        elapsed = current_time - self.session_stats['start_time']

        # 计算平均速度 - 使用实时统计
        total_records = self.real_time_stats['total_written_records']
        if elapsed > 0:
            self.session_stats['avg_processing_speed'] = total_records / elapsed

        # 计算当前速度 - 基于最近的批次写入
        if self.performance_samples:
            # 使用最近几个样本的平均值
            recent_samples = list(self.performance_samples)[-5:]  # 最近5个样本
            current_speed = sum(recent_samples) / len(recent_samples)
        else:
            # 如果没有性能样本，基于当前正在处理的记录计算
            current_processing_records = sum(f['processed_records'] for f in self.current_files.values())
            if elapsed > 0 and current_processing_records > 0:
                current_speed = current_processing_records / elapsed
            else:
                current_speed = 0

        print("\\n" + "=" * 80)
        print("🚀 ETL处理进度报告")
        print("=" * 80)
        print(f"📁 已完成文件: {self.session_stats['total_files_processed']} 个")
        print(f"📊 处理记录数: {total_records:,} 条")
        print(f"❌ 错误记录数: {self.session_stats['total_errors']:,} 条")
        print(f"⏱️ 运行时间: {str(timedelta(seconds=int(elapsed)))}")
        print(f"⚡ 平均速度: {self.session_stats['avg_processing_speed']:.0f} RPS")
        print(f"📈 当前速度: {current_speed:.0f} RPS")

        # 显示当前处理文件
        if self.current_files:
            print("\\n📂 当前处理文件:")
            for tid, file_info in self.current_files.items():
                progress = file_info['processed_lines'] / file_info['estimated_lines']
                progress_bar = "█" * int(progress * 20) + "░" * (20 - int(progress * 20))
                print(f"  线程{tid}: {file_info['file_path'].name}")
                print(f"           [{progress_bar}] {progress*100:.1f}% ({file_info['processed_records']} 条)")

        print("=" * 80)

    def get_session_summary(self) -> Dict[str, Any]:
        """获取会话总结"""
        if self.session_stats['start_time']:
            elapsed = time.time() - self.session_stats['start_time']
            self.session_stats['avg_processing_speed'] = self.session_stats['total_records_written'] / elapsed if elapsed > 0 else 0

        return self.session_stats.copy()

class IntegratedUltraETLController:
    """集成超高性能ETL控制器 - 完全兼容原有功能"""

    def __init__(self,
                 base_log_dir: str = None,
                 state_file: str = None,
                 batch_size: int = 2000,
                 max_workers: int = 4,
                 connection_pool_size: int = None,
                 memory_limit_mb: int = 512,
                 enable_detailed_logging: bool = True,
                 progress_refresh_minutes: int = 3):
        """
        初始化集成超高性能ETL控制器

        Args:
            progress_refresh_minutes: 进度刷新间隔(分钟) 1-5分钟
        """
        # 基础配置 (完全兼容原有) - 使用相对路径
        if base_log_dir:
            self.base_log_dir = Path(base_log_dir)
        else:
            # 自动检测nginx_logs目录位置
            etl_root_parent = etl_root.parent  # nginx-analytics-warehouse
            default_log_dir = etl_root_parent / "nginx_logs"
            self.base_log_dir = default_log_dir
        self.state_file = Path(state_file) if state_file else \
            Path(etl_root / "processed_logs_state.json")

        # 性能配置
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.connection_pool_size = connection_pool_size if connection_pool_size is not None else max_workers
        self.memory_limit_mb = memory_limit_mb
        self.enable_detailed_logging = enable_detailed_logging

        # 日志配置
        self.logger = self._setup_logger()

        # 集成组件
        self.progress_tracker = IntegratedProgressTracker(self.logger, progress_refresh_minutes)
        self.performance_optimizer = PerformanceOptimizer()

        # 线程安全的组件池 (原有设计)
        self.parser_pool = [BaseLogParser() for _ in range(max_workers)]
        self.mapper_pool = [FieldMapper(geoip_db_path=str(etl_root / "data" / "GeoLite2-City.mmdb")) for _ in range(max_workers)]
        self.writer_pool = []

        # 优化的缓存机制 (解决性能衰减)
        self.ua_cache = {}
        self.uri_cache = {}
        self.cache_stats = {'ua_hits': 0, 'ua_misses': 0, 'uri_hits': 0, 'uri_misses': 0}

        # 处理状态 (兼容原有格式)
        self.processed_state = self.load_state()

        # 异步写入优化 - 增加缓冲区大小
        self.write_buffer = []
        self.write_buffer_lock = threading.Lock()
        self.async_write_queue = queue.Queue(maxsize=500)  # 增加队列大小
        self.buffer_flush_threshold = batch_size * 2  # 动态刷新阈值

        # 性能统计 (兼容原有)
        self.write_stats = {
            'total_writes': 0,
            'total_records': 0,
            'total_write_time': 0,
            'buffer_flushes': 0
        }

        # 初始化系统
        self._init_connection_pool()

        # 自动文件发现功能 (新增)
        self.auto_discovery = AutoFileDiscovery(self.base_log_dir)
        self.monitoring_enabled = False
        self.monitoring_thread = None
        self.stop_monitoring = threading.Event()
        self.is_processing = False
        self.processing_lock = threading.Lock()

        # 初始化已知文件列表
        existing_files = set(Path(f) for f in self.processed_state.get('processed_files', {}))
        self.auto_discovery.initialize_known_files(existing_files)

        self.logger.info("🚀 集成超高性能ETL控制器初始化完成")
        self.logger.info(f"📁 日志目录: {self.base_log_dir}")
        self.logger.info(f"⚙️ 批处理大小: {self.batch_size:,}")
        self.logger.info(f"🧵 工作线程数: {self.max_workers}")
        self.logger.info(f"🔗 连接池大小: {self.connection_pool_size}")
        self.logger.info(f"📊 进度刷新间隔: {progress_refresh_minutes} 分钟")
        self.logger.info(f"🔍 自动扫描间隔: {self.auto_discovery.scan_interval} 秒")

    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    def _init_connection_pool(self):
        """初始化数据库连接池"""
        self.logger.info("🔗 初始化数据库连接池...")
        success_count = 0

        for i in range(self.connection_pool_size):
            try:
                writer = DWDWriter()
                if writer.connect():
                    self.writer_pool.append(writer)
                    success_count += 1
                else:
                    self.logger.error(f"❌ 数据库连接 {i+1} 失败")
            except Exception as e:
                self.logger.error(f"❌ 创建数据库连接 {i+1} 异常: {e}")

        if success_count == 0:
            raise RuntimeError("无法创建任何数据库连接")

        self.logger.info(f"✅ 数据库连接池初始化完成: {success_count}/{self.connection_pool_size}")

    def get_writer(self) -> Optional[DWDWriter]:
        """获取可用的写入器"""
        if self.writer_pool:
            return self.writer_pool.pop(0)
        return None

    def return_writer(self, writer: DWDWriter):
        """归还写入器"""
        if writer:
            self.writer_pool.append(writer)

    def cached_ua_parse(self, user_agent: str, mapper: FieldMapper) -> Dict:
        """优化的缓存用户代理解析"""
        if user_agent in self.ua_cache:
            self.cache_stats['ua_hits'] += 1
            return self.ua_cache[user_agent]

        self.cache_stats['ua_misses'] += 1

        # 执行解析
        try:
            parsed = mapper._parse_user_agent_enhanced(user_agent)
        except:
            parsed = {'browser': 'Unknown', 'os': 'Unknown'}

        # 缓存管理 (防止内存膨胀) - 更积极的清理
        if len(self.ua_cache) > 3000:  # 降低阈值，更频繁清理
            cache_size = self.performance_optimizer.optimize_cache(self.ua_cache, 3000)
            if self.enable_detailed_logging:
                self.logger.info(f"🧹 UA缓存优化完成，当前大小: {cache_size}")

        self.ua_cache[user_agent] = parsed
        return parsed

    def cached_uri_parse(self, uri: str, mapper: FieldMapper) -> Dict:
        """优化的缓存URI解析"""
        if uri in self.uri_cache:
            self.cache_stats['uri_hits'] += 1
            return self.uri_cache[uri]

        self.cache_stats['uri_misses'] += 1

        # 执行解析
        try:
            parsed = mapper._parse_uri_components(uri)
        except:
            parsed = {'path': uri, 'query_count': 0}

        # 缓存管理 - 优化清理策略
        if len(self.uri_cache) > 3000:  # 降低阈值
            # 保留最常用的缓存
            items = list(self.uri_cache.items())
            self.uri_cache.clear()
            self.uri_cache.update(items[-1500:])  # 保留更少项目

        self.uri_cache[uri] = parsed
        return parsed

    def process_file_batch(self, file_paths: List[Path], thread_id: int,
                          test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """处理文件批次 - 集成性能优化"""
        results = []

        for file_path in file_paths:
            try:
                result = self.process_single_file(file_path, thread_id, test_mode, limit)
                results.append(result)

                # 性能监控
                if result.get('success') and 'speed_rps' in result:
                    self.performance_optimizer.monitor_performance(result['speed_rps'])

                # 定期GC优化
                if self.performance_optimizer.should_optimize_gc():
                    collected = self.performance_optimizer.optimize_gc()
                    if collected > 0:
                        self.logger.info(f"🧹 GC优化完成，清理对象: {collected}")

            except Exception as e:
                self.logger.error(f"处理文件 {file_path} 失败: {e}")
                results.append({
                    'success': False,
                    'file_path': str(file_path),
                    'error': str(e)
                })

        return {
            'success': all(r.get('success', False) for r in results),
            'thread_id': thread_id,
            'file_results': results
        }

    def process_single_file(self, file_path: Path, thread_id: int,
                           test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """处理单个文件 - 完全兼容原有逻辑"""
        start_time = time.time()

        # 估算文件行数
        estimated_lines = self._estimate_file_lines(file_path)

        # 开始文件处理追踪
        self.progress_tracker.start_file_processing(thread_id, file_path, estimated_lines)

        try:
            # 获取组件
            parser = self.parser_pool[thread_id % len(self.parser_pool)]
            mapper = self.mapper_pool[thread_id % len(self.mapper_pool)]
            writer = self.get_writer() if not test_mode else None

            if not test_mode and not writer:
                error_msg = f"线程 {thread_id} 无法获取数据库连接"
                self.logger.error(error_msg)
                return {'success': False, 'error': error_msg}

            batch = []
            file_records = 0
            file_lines = 0
            file_errors = 0

            try:
                # 流式处理文件
                for parsed_data in parser.parse_file(file_path):
                    file_lines += 1

                    # 更新进度 (减少频率以提高性能)
                    if file_lines % 5000 == 0:  # 从1000改为5000，减少进度更新开销
                        self.progress_tracker.update_file_progress(thread_id, file_lines, file_records, file_errors)

                        # 显示进度 (根据配置的刷新间隔)
                        self.progress_tracker.display_progress()

                        # 执行GC优化
                        if self.performance_optimizer.should_optimize_gc():
                            collected = self.performance_optimizer.optimize_gc()
                            if self.enable_detailed_logging:
                                self.logger.info(f"🗞️ GC优化完成，回收对象: {collected}")

                    if parsed_data:
                        try:
                            # 缓存解析
                            user_agent = parsed_data.get('user_agent', '')
                            if user_agent:
                                parsed_data['_cached_ua'] = self.cached_ua_parse(user_agent, mapper)

                            # 安全的URI解析
                            request = parsed_data.get('request', '')
                            uri = ''
                            if request:
                                request_parts = request.split(' ')
                                if len(request_parts) >= 2:
                                    uri = request_parts[1]
                                else:
                                    uri = request  # 如果分割失败，使用原始请求
                            if uri:
                                parsed_data['_cached_uri'] = self.cached_uri_parse(uri, mapper)

                            # 字段映射
                            mapped_data = mapper.map_to_dwd(parsed_data, file_path.name)
                            batch.append(mapped_data)
                            file_records += 1

                            # 批量写入 - 增加异步处理
                            if len(batch) >= self.batch_size:
                                if not test_mode and writer:
                                    try:
                                        write_start = time.time()

                                        # 使用更高效的写入方式
                                        result = writer.write_batch_optimized(batch)  # 使用优化版本
                                        write_time = time.time() - write_start

                                        # 检查写入结果
                                        if result and result.get('success', False):
                                            # 更新写入统计
                                            self.write_stats['total_writes'] += 1
                                            self.write_stats['total_records'] += len(batch)
                                            self.write_stats['total_write_time'] += write_time

                                            # 更新实时统计 (新增)
                                            self.progress_tracker.update_batch_written(len(batch))

                                            # 记录性能指标
                                            current_speed = len(batch) / write_time if write_time > 0 else 0
                                            self.performance_optimizer.monitor_performance(current_speed)

                                            if self.enable_detailed_logging:
                                                self.logger.info(f"✅ [线程{thread_id}] 批次写入成功: {len(batch)} 条记录, {write_time:.2f}秒")
                                        else:
                                            self.logger.error(f"写入返回失败结果: {result}")
                                            raise Exception("写入失败")

                                    except Exception as e:
                                        # 如果优化方法失败，回退到普通方法
                                        try:
                                            self.logger.warning(f"优化写入失败，回退到标准方法: {e}")
                                            result = writer.write_batch(batch)
                                            if result and result.get('success', False):
                                                self.write_stats['total_writes'] += 1
                                                self.write_stats['total_records'] += len(batch)
                                                # 更新实时统计
                                                self.progress_tracker.update_batch_written(len(batch))
                                            else:
                                                raise Exception("标准写入也失败")
                                        except Exception as e2:
                                            self.logger.error(f"批量写入完全失败: {e2}")
                                            file_errors += len(batch)  # 记录失败的记录数

                                batch.clear()

                            # 检查限制
                            if limit and file_records >= limit:
                                break

                        except Exception as e:
                            file_errors += 1
                            if file_errors <= 5:
                                self.logger.error(f"记录处理错误: {e}")

                # 处理剩余批次
                if batch and not test_mode and writer:
                    try:
                        result = writer.write_batch(batch)
                        if result and result.get('success', False):
                            self.write_stats['total_writes'] += 1
                            self.write_stats['total_records'] += len(batch)
                            # 更新实时统计
                            self.progress_tracker.update_batch_written(len(batch))
                    except Exception as e:
                        self.logger.error(f"最终批量写入失败: {e}")
                        file_errors += 1

            finally:
                # 归还写入器
                if writer:
                    self.return_writer(writer)

            # 完成文件处理
            processing_time = time.time() - start_time
            self.progress_tracker.complete_file_processing(thread_id, file_records, file_errors)

            # 标记文件已处理
            if not test_mode:
                self.mark_file_processed(file_path, file_records, processing_time)

            result = {
                'success': True,
                'file_path': str(file_path),
                'lines_processed': file_lines,
                'records_processed': file_records,
                'errors': file_errors,
                'processing_time': processing_time,
                'speed_rps': file_records / processing_time if processing_time > 0 else 0
            }

            self.logger.info(
                f"✅ {file_path.name}: {file_records:,} 条记录, "
                f"{processing_time:.1f}秒, {result['speed_rps']:.0f} RPS"
            )

            return result

        except Exception as e:
            error_msg = f"处理文件 {file_path.name} 失败: {e}"
            self.logger.error(error_msg)
            self.progress_tracker.complete_file_processing(thread_id, 0, 1)
            return {
                'success': False,
                'file_path': str(file_path),
                'error': error_msg
            }

    def _estimate_file_lines(self, file_path: Path) -> int:
        """估算文件行数"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                sample_lines = []
                for i, line in enumerate(f):
                    sample_lines.append(len(line))
                    if i >= 999:
                        break

            if sample_lines:
                avg_line_length = sum(sample_lines) / len(sample_lines)
                file_size = file_path.stat().st_size
                estimated_lines = int(file_size / avg_line_length)
                return max(1000, estimated_lines)
            else:
                return 10000
        except Exception:
            return 10000

    # === 完全兼容原有的状态管理 ===

    def load_state(self) -> Dict[str, Any]:
        """加载处理状态 - 完全兼容原有格式"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"加载状态文件失败: {e}")

        return {
            'processed_files': {},
            'last_update': None,
            'total_processed_records': 0,
            'processing_history': []
        }

    def save_state(self):
        """保存处理状态 - 完全兼容原有格式"""
        try:
            self.processed_state['last_update'] = datetime.now().isoformat()
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.processed_state, f, indent=2, ensure_ascii=False, default=str)
        except Exception as e:
            self.logger.error(f"保存状态文件失败: {e}")

    def scan_log_directories(self) -> Dict[str, List[Path]]:
        """扫描日志目录 - 完全兼容原有逻辑"""
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
            # 定期保存状态
            if len(self.processed_state['processed_files']) % 10 == 0:
                self.save_state()
        except Exception as e:
            self.logger.error(f"标记文件状态失败 {file_path}: {e}")

    # === 完全兼容原有的处理方法 ===

    def process_date_parallel(self, date_str: str, force_reprocess: bool = False,
                             test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """处理指定日期的日志 - 完全兼容原有逻辑"""
        start_time = time.time()
        date_dir = self.base_log_dir / date_str

        if not date_dir.exists():
            return {
                'success': False,
                'error': f'日期目录不存在: {date_dir}',
                'date': date_str
            }

        # 获取日志文件
        log_files = list(date_dir.glob("*.log"))
        if not log_files:
            return {
                'success': True,
                'message': f'日期 {date_str} 没有日志文件',
                'date': date_str,
                'processed_files': 0,
                'total_records': 0
            }

        # 过滤已处理文件
        if not force_reprocess:
            unprocessed_files = [f for f in log_files if not self.is_file_processed(f)]
            if not unprocessed_files:
                return {
                    'success': True,
                    'message': f'日期 {date_str} 的所有文件已处理',
                    'date': date_str,
                    'processed_files': 0,
                    'total_records': 0
                }
            log_files = unprocessed_files

        self.logger.info(f"📅 处理日期 {date_str}: {len(log_files)} 个文件")

        # 启动进度追踪
        self.progress_tracker.start_session(len(log_files))

        # 文件分组处理
        files_per_thread = max(1, len(log_files) // self.max_workers)
        file_groups = [log_files[i:i + files_per_thread]
                      for i in range(0, len(log_files), files_per_thread)]

        # 确保不超过最大线程数
        while len(file_groups) > self.max_workers:
            last_group = file_groups.pop()
            file_groups[-1].extend(last_group)

        # 并行处理
        total_records = 0
        total_errors = 0
        processed_files = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_group = {
                executor.submit(self.process_file_batch, group, thread_id, test_mode, limit): thread_id
                for thread_id, group in enumerate(file_groups)
            }

            for future in as_completed(future_to_group):
                thread_id = future_to_group[future]
                try:
                    result = future.result()
                    if result['success']:
                        for file_result in result['file_results']:
                            if file_result.get('success'):
                                total_records += file_result.get('records_processed', 0)
                                processed_files += 1
                            total_errors += file_result.get('errors', 0)
                except Exception as e:
                    self.logger.error(f"线程 {thread_id} 处理异常: {e}")
                    total_errors += 1

        # 最终进度显示
        self.progress_tracker.display_progress(force=True)

        # 保存状态
        self.save_state()

        processing_time = time.time() - start_time
        processing_speed = total_records / processing_time if processing_time > 0 else 0

        return {
            'success': total_errors == 0,
            'date': date_str,
            'processed_files': processed_files,
            'total_records': total_records,
            'total_errors': total_errors,
            'duration': processing_time,
            'processing_speed': processing_speed
        }

    def process_all_parallel(self, test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """处理所有日志 - 完全兼容原有逻辑"""
        start_time = time.time()
        log_files_by_date = self.scan_log_directories()

        if not log_files_by_date:
            return {
                'success': False,
                'error': '未找到任何日志文件'
            }

        self.logger.info(f"📁 发现 {len(log_files_by_date)} 个日期的日志文件")

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
                if result.get('error'):
                    all_errors.append(result['error'])

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

    # === 完全兼容原有的性能统计和错误日志 ===

    def show_performance_stats(self):
        """显示性能统计信息 - 完全兼容原有格式"""
        print("\\n" + "=" * 80)
        print("🚀 集成超高性能ETL控制器 - 性能统计报告")
        print("=" * 80)

        print(f"⚙️  配置信息:")
        print(f"   批处理大小: {self.batch_size:,}")
        print(f"   最大工作线程: {self.max_workers}")
        print(f"   连接池大小: {self.connection_pool_size}")
        print(f"   详细日志: {'启用' if self.enable_detailed_logging else '禁用'}")

        # 缓存统计
        total_ua = self.cache_stats['ua_hits'] + self.cache_stats['ua_misses']
        total_uri = self.cache_stats['uri_hits'] + self.cache_stats['uri_misses']
        ua_hit_rate = self.cache_stats['ua_hits'] / total_ua * 100 if total_ua > 0 else 0
        uri_hit_rate = self.cache_stats['uri_hits'] / total_uri * 100 if total_uri > 0 else 0

        print(f"\\n📈 缓存统计:")
        print(f"   User-Agent缓存: {len(self.ua_cache)} 项 (命中率: {ua_hit_rate:.1f}%)")
        print(f"   URI缓存: {len(self.uri_cache)} 项 (命中率: {uri_hit_rate:.1f}%)")

        # 性能趋势
        trend = self.performance_optimizer.get_performance_trend()
        trend_icon = {"declining": "📉", "improving": "📈", "stable": "📊"}.get(trend, "❓")
        print(f"   性能趋势: {trend_icon} {trend}")

        # 会话统计
        session_stats = self.progress_tracker.get_session_summary()
        if session_stats['avg_processing_speed'] > 0:
            print(f"\\n🏃 性能指标:")
            print(f"   平均处理速度: {session_stats['avg_processing_speed']:.1f} 记录/秒")
            print(f"   总处理记录数: {session_stats.get('total_records_written', 0):,}")
            print(f"   总处理文件数: {session_stats.get('total_files_processed', 0)}")

        # 写入统计
        if self.write_stats['total_writes'] > 0:
            avg_write_time = self.write_stats['total_write_time'] / self.write_stats['total_writes']
            print(f"\\n💾 写入统计:")
            print(f"   总写入次数: {self.write_stats['total_writes']}")
            print(f"   总写入记录: {self.write_stats['total_records']:,}")
            print(f"   平均写入时间: {avg_write_time:.3f}秒")

        print("=" * 80)

    def print_detailed_error_log(self):
        """打印详细错误日志 - 完全兼容原有格式"""
        if not self.enable_detailed_logging:
            print("详细错误日志未启用，请在初始化时设置 enable_detailed_logging=True")
            return

        session_stats = self.progress_tracker.get_session_summary()
        error_log = session_stats.get('detailed_error_log', [])

        if not error_log:
            print("✅ 没有详细错误记录")
            return

        print("\\n" + "="*80)
        print("🔍 详细错误日志 (最近10个)")
        print("="*80)

        for error in error_log[-10:]:
            print(f"\\n📅 时间: {error['timestamp']}")
            print(f"🏷️  类型: {error['type']}")
            print(f"📝 消息: {error['message']}")
            if error.get('context'):
                print(f"🔍 上下文: {error['context']}")
            print("-" * 40)

        print("="*80)

    # === 自动文件发现和监控功能 (新增) ===

    def auto_process_new_files(self) -> Dict[str, Any]:
        """自动处理新发现的文件"""
        with self.processing_lock:
            if self.is_processing:
                return {'success': False, 'error': 'Already processing', 'skipped': True}
            self.is_processing = True

        try:
            start_time = time.time()
            new_files = self.auto_discovery.discover_new_files()

            if not new_files:
                return {'success': True, 'new_files': 0, 'message': 'No new files found'}

            self.logger.info(f"开始自动处理 {len(new_files)} 个新文件")

            # 使用原有的高性能处理逻辑
            total_records = 0
            processed_files = 0
            errors = []

            for file_path in new_files:
                try:
                    # 使用原有的处理逻辑
                    result = self.process_single_file(file_path, 0, test_mode=False)
                    if result['success'] and not result.get('skipped', False):
                        total_records += result.get('records_processed', 0)
                        processed_files += 1
                    elif not result['success']:
                        errors.append(f"{file_path}: {result.get('error', 'Unknown error')}")
                except Exception as e:
                    errors.append(f"{file_path}: {str(e)}")

            processing_time = time.time() - start_time

            result = {
                'success': True,
                'new_files': len(new_files),
                'processed_files': processed_files,
                'total_records': total_records,
                'processing_time': processing_time,
                'processing_speed': total_records / processing_time if processing_time > 0 else 0,
                'errors': errors
            }

            if processed_files > 0:
                self.logger.info(
                    f"自动处理完成: {processed_files} 文件, {total_records:,} 记录, "
                    f"速度 {result['processing_speed']:.1f} rec/s"
                )

            return result

        except Exception as e:
            self.logger.error(f"自动处理过程出错: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            with self.processing_lock:
                self.is_processing = False

    def monitoring_loop(self):
        """监控循环线程"""
        self.logger.info(f"自动监控已启动，扫描间隔 {self.auto_discovery.scan_interval} 秒")

        while not self.stop_monitoring.is_set():
            try:
                # 等待扫描间隔
                if self.stop_monitoring.wait(self.auto_discovery.scan_interval):
                    break  # 收到停止信号

                # 执行自动处理
                result = self.auto_process_new_files()

                if result.get('new_files', 0) > 0:
                    print(f"\n🔍 自动发现并处理了 {result['processed_files']} 个新文件，"
                          f"共 {result['total_records']:,} 条记录")
                    print("👆 按回车键返回菜单，或等待继续监控...")

            except Exception as e:
                self.logger.error(f"监控循环出错: {e}")
                time.sleep(60)  # 出错后等待1分钟再继续

    def start_auto_monitoring(self):
        """启动自动监控"""
        if self.monitoring_enabled:
            print("⚠️ 自动监控已经在运行中")
            return False

        self.monitoring_enabled = True
        self.stop_monitoring.clear()
        self.monitoring_thread = threading.Thread(target=self.monitoring_loop, daemon=True)
        self.monitoring_thread.start()
        print(f"✅ 自动监控已启动，扫描间隔 {self.auto_discovery.scan_interval} 秒")
        print("💡 提示: 按 Ctrl+C 或输入任意键停止监控")
        return True

    def stop_auto_monitoring(self):
        """停止自动监控"""
        if not self.monitoring_enabled:
            return False

        self.monitoring_enabled = False
        self.stop_monitoring.set()

        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=5)

        print("✅ 自动监控已停止")
        return True

    # === 完全兼容原有的交互式菜单 ===

    def interactive_menu(self):
        """交互式菜单 - 完全兼容原有功能"""
        while True:
            print("\\n" + "=" * 80)
            print("🚀 集成超高性能ETL控制器 - 交互式菜单")
            print("=" * 80)
            print("1. 🔥 处理所有未处理的日志 (推荐)")
            print("2. 📅 处理指定日期的日志")
            print("3. 📊 查看性能统计")
            print("4. 🧪 测试模式处理")
            print("5. ⚙️ 性能参数调优")
            print("6. 🔍 查看详细错误日志")
            print("7. 🗂️ 查看处理状态")
            print("0. 👋 退出")
            print("-" * 80)
            print(f"📊 当前配置: 批量{self.batch_size} | 线程{self.max_workers} | 连接池{self.connection_pool_size}")

            try:
                choice = input("请选择操作 [0-7]: ").strip()

                if choice == '0':
                    print("👋 再见！")
                    break

                elif choice == '1':
                    print("\\n🔥 高性能处理所有未处理的日志...")
                    limit_input = input("限制每个文件的处理行数 (留空表示不限制): ").strip()
                    limit = int(limit_input) if limit_input.isdigit() else None

                    result = self.process_all_parallel(test_mode=False, limit=limit)
                    if result['success']:
                        print(f"\\n✅ 处理完成: {result['total_records']:,} 记录, 速度 {result['processing_speed']:.1f} rec/s")

                        # 处理完成后询问是否进入自动监控模式
                        print("\\n🤖 处理完成！现在可以进入自动监控模式")
                        print(f"📊 将每 {self.auto_discovery.scan_interval} 秒自动检查新文件并处理")
                        auto_monitor = input("是否启动自动监控？(Y/n): ").strip().lower()

                        if auto_monitor != 'n':
                            self.start_auto_monitoring()
                            try:
                                print("\\n🔍 自动监控中...")
                                print("💡 提示: 按回车键停止监控并返回菜单")
                                input()  # 等待用户输入
                            except KeyboardInterrupt:
                                pass
                            finally:
                                self.stop_auto_monitoring()
                    else:
                        print(f"\\n❌ 处理失败: {result.get('error', '未知错误')}")

                elif choice == '2':
                    date_str = input("\\n请输入日期 (YYYYMMDD格式): ").strip()
                    if len(date_str) != 8 or not date_str.isdigit():
                        print("❌ 日期格式错误")
                        continue

                    force = input("强制重新处理？(y/N): ").strip().lower() == 'y'
                    limit_input = input("限制每个文件的处理行数 (留空表示不限制): ").strip()
                    limit = int(limit_input) if limit_input.isdigit() else None

                    result = self.process_date_parallel(date_str, force, test_mode=False, limit=limit)
                    if result['success']:
                        print(f"\\n✅ 处理完成: {result['total_records']:,} 记录, 速度 {result['processing_speed']:.1f} rec/s")
                    else:
                        print(f"\\n❌ 处理失败: {result.get('error', '未知错误')}")

                elif choice == '3':
                    self.show_performance_stats()

                elif choice == '4':
                    date_str = input("\\n请输入测试日期 (YYYYMMDD格式): ").strip()
                    limit_input = input("限制处理行数 (建议100-1000): ").strip()
                    limit = int(limit_input) if limit_input.isdigit() else 100

                    result = self.process_date_parallel(date_str, False, test_mode=True, limit=limit)
                    if result['success']:
                        print(f"\\n✅ 测试完成: {result['total_records']:,} 记录, 速度 {result['processing_speed']:.1f} rec/s")

                elif choice == '5':
                    self._interactive_performance_tuning()

                elif choice == '6':
                    self.print_detailed_error_log()

                elif choice == '7':
                    self._show_processing_status()

                else:
                    print("❌ 无效选择")

                input("\\n按回车键继续...")

            except KeyboardInterrupt:
                print("\\n\\n👋 用户中断，再见！")
                break
            except Exception as e:
                print(f"\\n❌ 操作异常: {e}")

    def _interactive_performance_tuning(self):
        """交互式性能调优 - 扩展原有功能"""
        print("\\n⚙️ 性能参数调优")
        print(f"当前配置:")
        print(f"  批量大小: {self.batch_size}")
        print(f"  线程数: {self.max_workers}")
        print(f"  连接池: {self.connection_pool_size}")
        print(f"  进度刷新间隔: {self.progress_tracker.refresh_interval // 60} 分钟")
        print(f"  自动扫描间隔: {self.auto_discovery.scan_interval} 秒")

        # 获取系统信息推荐
        if PSUTIL_AVAILABLE:
            try:
                cpu_count = psutil.cpu_count()
                memory_gb = psutil.virtual_memory().total / (1024**3)
                print(f"\\n💻 系统信息:")
                print(f"  CPU核心数: {cpu_count}")
                print(f"  总内存: {memory_gb:.1f}GB")
                print(f"\\n📊 推荐配置:")
                print(f"  推荐线程数: {min(cpu_count, 8)}")
                print(f"  推荐批量大小: {min(4000, int(memory_gb * 500))}")
            except:
                pass

        # 交互式配置
        new_batch = input(f"\\n新的批量大小 (当前{self.batch_size}, 推荐1000-5000): ").strip()
        new_workers = input(f"新的线程数 (当前{self.max_workers}, 推荐2-8): ").strip()
        new_pool = input(f"新的连接池大小 (当前{self.connection_pool_size}, 推荐=线程数): ").strip()
        new_refresh = input(f"新的进度刷新间隔(分钟) (当前{self.progress_tracker.refresh_interval // 60}, 推荐1-5): ").strip()
        new_scan_interval = input(f"新的自动扫描间隔(秒) (当前{self.auto_discovery.scan_interval}, 推荐180-600): ").strip()

        # 应用配置
        if new_batch.isdigit():
            self.batch_size = max(500, min(10000, int(new_batch)))
            print(f"✅ 批量大小调整为: {self.batch_size}")

        if new_workers.isdigit():
            old_workers = self.max_workers
            self.max_workers = max(1, min(16, int(new_workers)))
            print(f"✅ 线程数调整为: {self.max_workers}")

            if self.max_workers != old_workers:
                print("🔄 重新初始化组件池...")
                self.parser_pool = [BaseLogParser() for _ in range(self.max_workers)]
                self.mapper_pool = [FieldMapper(geoip_db_path=str(etl_root / "data" / "GeoLite2-City.mmdb")) for _ in range(self.max_workers)]

        if new_pool.isdigit():
            self.connection_pool_size = max(1, min(16, int(new_pool)))
            print(f"✅ 连接池大小调整为: {self.connection_pool_size}")
            print("⚠️  连接池调整需要重启程序生效")

        if new_refresh.isdigit():
            refresh_minutes = max(1, min(30, int(new_refresh)))
            self.progress_tracker.refresh_interval = refresh_minutes * 60
            print(f"✅ 进度刷新间隔调整为: {refresh_minutes} 分钟")

        if new_scan_interval.isdigit():
            scan_interval = max(60, min(3600, int(new_scan_interval)))  # 1分钟到1小时
            self.auto_discovery.scan_interval = scan_interval
            print(f"✅ 自动扫描间隔调整为: {scan_interval} 秒")
            if self.monitoring_enabled:
                print("⚠️  自动监控正在运行，新设置将在下次扫描时生效")

    def _show_processing_status(self):
        """显示处理状态"""
        print("\\n📋 文件处理状态")
        print("-" * 50)

        processed_files = self.processed_state.get('processed_files', {})
        if not processed_files:
            print("暂无已处理文件记录")
            return

        print(f"已处理文件总数: {len(processed_files)}")
        print(f"最后更新时间: {self.processed_state.get('last_update', 'Unknown')}")

        # 显示最近处理的10个文件
        recent_files = sorted(processed_files.items(),
                            key=lambda x: x[1].get('processed_at', ''),
                            reverse=True)[:10]

        print("\\n📄 最近处理的文件:")
        for file_path, info in recent_files:
            file_name = Path(file_path).name
            record_count = info.get('record_count', 0)
            processing_time = info.get('processing_time', 0)
            speed = record_count / processing_time if processing_time > 0 else 0
            processed_at = info.get('processed_at', '')[:19]  # 只显示到秒

            print(f"  {file_name}: {record_count:,} 条记录, {speed:.0f} RPS, {processed_at}")

    def cleanup(self):
        """清理资源"""
        self.logger.info("🧹 开始清理资源...")

        # 保存最终状态
        self.save_state()

        # 关闭数据库连接
        for writer in self.writer_pool:
            try:
                writer.close()
            except Exception as e:
                self.logger.warning(f"关闭数据库连接失败: {e}")

        # 清理缓存
        self.ua_cache.clear()
        self.uri_cache.clear()

        self.logger.info("✅ 资源清理完成")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 停止自动监控线程
        if hasattr(self, 'monitoring_enabled') and self.monitoring_enabled:
            self.stop_auto_monitoring()

        self.cleanup()

def main():
    """主函数 - 完全兼容原有命令行参数"""
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(
        description='集成超高性能ETL控制器 - 解决性能衰减问题'
    )

    parser.add_argument('--date', help='处理指定日期 (YYYYMMDD格式)')
    parser.add_argument('--all', action='store_true', help='处理所有未处理日志')
    parser.add_argument('--force', action='store_true', help='强制重新处理')
    parser.add_argument('--test', action='store_true', help='测试模式')
    parser.add_argument('--limit', type=int, help='每个文件的行数限制')
    parser.add_argument('--batch-size', type=int, default=2000, help='批处理大小')
    parser.add_argument('--workers', type=int, default=4, help='工作线程数')
    parser.add_argument('--pool-size', type=int, help='连接池大小')
    parser.add_argument('--detailed-logging', action='store_true', default=True, help='启用详细错误日志')
    parser.add_argument('--refresh-minutes', type=int, default=3, help='进度刷新间隔(分钟)')
    parser.add_argument('--auto-monitor', action='store_true', help='启动自动监控模式（非交互式）')
    parser.add_argument('--monitor-duration', type=int, default=7200, help='自动监控持续时间（秒），默认2小时')

    args = parser.parse_args()

    try:
        with IntegratedUltraETLController(
            batch_size=args.batch_size,
            max_workers=args.workers,
            connection_pool_size=args.pool_size,
            enable_detailed_logging=args.detailed_logging,
            progress_refresh_minutes=args.refresh_minutes
        ) as controller:

            # 自动监控模式（非交互式）
            if args.auto_monitor:
                print(f"🤖 启动自动监控模式，持续时间: {args.monitor_duration}秒")

                # 首先处理所有未处理的日志
                print("📋 首先处理所有未处理的日志...")
                result = controller.process_all_parallel(test_mode=args.test, limit=args.limit)
                if result['success'] and result.get('total_records', 0) > 0:
                    print(f"✅ 初始处理完成: {result['total_records']:,} 记录")
                else:
                    print("📝 没有发现未处理的文件")

                # 启动自动监控
                controller.start_auto_monitoring()

                try:
                    start_time = time.time()
                    print(f"🔍 自动监控中... 将运行 {args.monitor_duration} 秒")

                    while time.time() - start_time < args.monitor_duration:
                        remaining = args.monitor_duration - (time.time() - start_time)
                        print(f"⏰ 剩余监控时间: {remaining:.0f} 秒", end='\\r')
                        time.sleep(30)  # 每30秒显示一次剩余时间

                        # 检查是否有新文件被处理
                        if controller.monitoring_enabled:
                            continue
                        else:
                            print("\\n⚠️ 监控线程已停止，退出自动监控模式")
                            break

                    print(f"\\n✅ 自动监控完成，运行了 {time.time() - start_time:.0f} 秒")

                except KeyboardInterrupt:
                    print("\\n👋 用户中断自动监控")
                finally:
                    controller.stop_auto_monitoring()

                return

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

                print(f"\\n🎯 批量处理结果:")
                print(f"日期数: {result.get('processed_dates', 0)}")
                print(f"记录数: {result.get('total_records', 0):,}")
                print(f"总耗时: {result.get('duration', 0):.2f}s")
                print(f"平均速度: {result.get('processing_speed', 0):.1f} 记录/秒")

            controller.show_performance_stats()

    except KeyboardInterrupt:
        print("\\n👋 用户中断")
    except Exception as e:
        print(f"\\n❌ 执行错误: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()