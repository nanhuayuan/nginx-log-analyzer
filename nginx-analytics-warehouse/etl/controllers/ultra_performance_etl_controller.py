#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
超高性能ETL控制器 - 增强监控版本
Ultra Performance ETL Controller - Enhanced Monitoring Version

核心改进：
1. 实时统计信息显示 - 防止界面假死，实时了解处理进展
2. 进度监控和自动恢复 - 防止系统卡死
3. 多级性能优化 - 进一步提升处理速度
4. 智能资源管理 - 自适应调节避免系统过载
5. 详细的文件级别进度追踪
"""

import sys
import os
import json
import time
import argparse
import threading
import queue
import psutil
import signal
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
from processors.field_mapper import FieldMapper
from writers.dwd_writer import DWDWriter

class PerformanceMonitor:
    """性能监控器 - 实时监控系统性能防止卡死"""

    def __init__(self, warning_memory_percent=80, critical_memory_percent=90):
        self.warning_memory_percent = warning_memory_percent
        self.critical_memory_percent = critical_memory_percent
        self.start_time = time.time()

        # 性能历史记录 (保留最近100个数据点)
        self.performance_history = {
            'timestamps': deque(maxlen=100),
            'memory_usage': deque(maxlen=100),
            'cpu_usage': deque(maxlen=100),
            'processing_speed': deque(maxlen=100)
        }

    def check_system_health(self) -> Dict[str, Any]:
        """检查系统健康状况"""
        try:
            # 获取当前系统信息
            memory = psutil.virtual_memory()
            cpu_percent = psutil.cpu_percent(interval=0.1)
            process = psutil.Process()
            process_memory = process.memory_info()

            current_time = time.time()

            # 更新历史记录
            self.performance_history['timestamps'].append(current_time)
            self.performance_history['memory_usage'].append(memory.percent)
            self.performance_history['cpu_usage'].append(cpu_percent)

            health_status = {
                'timestamp': current_time,
                'system_memory_percent': memory.percent,
                'system_memory_available_gb': memory.available / (1024**3),
                'process_memory_mb': process_memory.rss / (1024**2),
                'cpu_percent': cpu_percent,
                'uptime_seconds': current_time - self.start_time,
                'status': 'healthy'
            }

            # 判断健康状况
            if memory.percent > self.critical_memory_percent:
                health_status['status'] = 'critical'
                health_status['warning'] = f"系统内存使用率过高: {memory.percent:.1f}%"
            elif memory.percent > self.warning_memory_percent:
                health_status['status'] = 'warning'
                health_status['warning'] = f"系统内存使用率较高: {memory.percent:.1f}%"
            elif cpu_percent > 95:
                health_status['status'] = 'warning'
                health_status['warning'] = f"CPU使用率过高: {cpu_percent:.1f}%"

            return health_status

        except Exception as e:
            return {
                'timestamp': time.time(),
                'status': 'error',
                'error': str(e)
            }

class ProgressTracker:
    """进度追踪器 - 实时显示处理进度"""

    def __init__(self, logger):
        self.logger = logger
        self.lock = threading.Lock()

        # 全局统计
        self.start_time = None
        self.total_files = 0
        self.completed_files = 0
        self.total_records = 0
        self.total_errors = 0

        # 当前处理状态
        self.current_files = {}  # {thread_id: {'file': path, 'progress': 0.5, 'speed': records/sec}}
        self.recent_speeds = deque(maxlen=50)  # 最近的处理速度记录

        # 文件级别详细进度
        self.file_progress = {}  # {file_path: {'total_lines': int, 'processed_lines': int, 'start_time': float}}

        # 性能统计
        self.performance_stats = {
            'avg_speed_rps': 0.0,
            'peak_speed_rps': 0.0,
            'current_speed_rps': 0.0,
            'estimated_completion': None,
            'efficiency_score': 0.0
        }

    def start_processing(self, total_files: int):
        """开始处理"""
        with self.lock:
            self.start_time = time.time()
            self.total_files = total_files
            self.completed_files = 0
            self.total_records = 0
            self.total_errors = 0

    def start_file(self, thread_id: int, file_path: Path, estimated_lines: int = None):
        """开始处理文件"""
        with self.lock:
            self.current_files[thread_id] = {
                'file': file_path,
                'start_time': time.time(),
                'processed_lines': 0,
                'estimated_lines': estimated_lines or 10000,
                'speed_rps': 0.0
            }

            self.file_progress[str(file_path)] = {
                'total_lines': estimated_lines or 10000,
                'processed_lines': 0,
                'start_time': time.time(),
                'thread_id': thread_id
            }

    def update_file_progress(self, thread_id: int, processed_lines: int, file_records: int):
        """更新文件处理进度"""
        with self.lock:
            if thread_id in self.current_files:
                current_time = time.time()
                file_info = self.current_files[thread_id]
                file_info['processed_lines'] = processed_lines

                # 计算处理速度
                elapsed = current_time - file_info['start_time']
                if elapsed > 0:
                    speed_rps = file_records / elapsed
                    file_info['speed_rps'] = speed_rps
                    self.recent_speeds.append(speed_rps)

                    # 更新性能统计
                    if speed_rps > self.performance_stats['peak_speed_rps']:
                        self.performance_stats['peak_speed_rps'] = speed_rps

                # 更新文件级别进度
                file_path = str(file_info['file'])
                if file_path in self.file_progress:
                    self.file_progress[file_path]['processed_lines'] = processed_lines

    def complete_file(self, thread_id: int, record_count: int, error_count: int = 0):
        """完成文件处理"""
        with self.lock:
            if thread_id in self.current_files:
                file_info = self.current_files[thread_id]
                file_path = str(file_info['file'])

                # 更新统计
                self.completed_files += 1
                self.total_records += record_count
                self.total_errors += error_count

                # 完成文件级别进度
                if file_path in self.file_progress:
                    self.file_progress[file_path]['completed'] = True
                    self.file_progress[file_path]['record_count'] = record_count
                    self.file_progress[file_path]['error_count'] = error_count

                # 移除当前处理状态
                del self.current_files[thread_id]

    def get_progress_summary(self) -> Dict[str, Any]:
        """获取进度摘要"""
        with self.lock:
            if not self.start_time:
                return {'status': 'not_started'}

            current_time = time.time()
            elapsed = current_time - self.start_time

            # 计算平均速度
            avg_speed = self.total_records / elapsed if elapsed > 0 else 0

            # 计算当前速度 (最近10秒)
            recent_speeds_list = list(self.recent_speeds)
            current_speed = sum(recent_speeds_list[-10:]) / min(10, len(recent_speeds_list)) if recent_speeds_list else 0

            # 估算完成时间
            remaining_files = self.total_files - self.completed_files
            estimated_completion = None
            if current_speed > 0 and remaining_files > 0:
                # 假设每个文件平均10000条记录
                remaining_records = remaining_files * 10000
                eta_seconds = remaining_records / current_speed
                estimated_completion = current_time + eta_seconds

            # 计算效率评分 (0-100)
            efficiency_score = min(100, (current_speed / 1000) * 100) if current_speed > 0 else 0

            progress_percent = (self.completed_files / self.total_files * 100) if self.total_files > 0 else 0

            return {
                'status': 'processing',
                'progress_percent': progress_percent,
                'completed_files': self.completed_files,
                'total_files': self.total_files,
                'total_records': self.total_records,
                'total_errors': self.total_errors,
                'elapsed_seconds': elapsed,
                'avg_speed_rps': avg_speed,
                'current_speed_rps': current_speed,
                'peak_speed_rps': self.performance_stats['peak_speed_rps'],
                'estimated_completion': estimated_completion,
                'efficiency_score': efficiency_score,
                'active_threads': len(self.current_files),
                'current_files': {
                    tid: {
                        'file': info['file'].name,
                        'progress': info['processed_lines'] / info['estimated_lines'],
                        'speed_rps': info['speed_rps']
                    } for tid, info in self.current_files.items()
                }
            }

class UltraPerformanceETLController:
    """超高性能ETL控制器 - 增强监控版本"""

    def __init__(self,
                 base_log_dir: str = None,
                 state_file: str = None,
                 batch_size: int = 3000,        # 增加批处理大小
                 max_workers: int = 6,          # 增加线程数
                 memory_limit_mb: int = 1024,   # 增加内存限制
                 enable_realtime_stats: bool = True,  # 启用实时统计
                 auto_optimize: bool = True):   # 自动优化
        """
        初始化超高性能ETL控制器

        Args:
            batch_size: 批处理大小（推荐2000-5000）
            max_workers: 最大工作线程数（推荐4-8）
            memory_limit_mb: 内存使用限制（MB）
            enable_realtime_stats: 是否启用实时统计显示
            auto_optimize: 是否启用自动性能优化
        """
        # 基础配置
        self.base_log_dir = Path(base_log_dir) if base_log_dir else \
            Path("D:/project/nginx-log-analyzer/nginx-analytics-warehouse/nginx_logs")
        self.state_file = Path(state_file) if state_file else \
            Path(etl_root / "processed_logs_state.json")

        # 性能配置
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.memory_limit_mb = memory_limit_mb
        self.enable_realtime_stats = enable_realtime_stats
        self.auto_optimize = auto_optimize

        # 日志配置
        self.logger = self._setup_logger()

        # 监控组件
        self.performance_monitor = PerformanceMonitor()
        self.progress_tracker = ProgressTracker(self.logger)

        # 优化的组件池
        self.parser_pool = [BaseLogParser() for _ in range(max_workers)]
        self.mapper_pool = [FieldMapper() for _ in range(max_workers)]
        self.writer_pool = []

        # 缓存优化
        self.ua_cache = {}
        self.uri_cache = {}
        self.ip_cache = {}  # 新增IP解析缓存
        self.cache_stats = {'hits': 0, 'misses': 0}

        # 智能批量写入
        self.write_buffer = []
        self.write_buffer_lock = threading.Lock()
        self.auto_flush_interval = 2.0  # 自动刷新间隔
        self.last_flush_time = time.time()

        # 控制信号
        self.shutdown_event = threading.Event()
        self.pause_event = threading.Event()

        # 实时统计线程
        self.stats_thread = None
        if self.enable_realtime_stats:
            self.stats_thread = threading.Thread(target=self._stats_monitor_loop, daemon=True)

        # 自动优化参数
        self.optimization_history = deque(maxlen=20)
        self.last_optimization_time = time.time()

        # 初始化系统
        self._init_system()

        # 注册信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            # 控制台处理器
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)

            # 文件处理器
            log_file = etl_root / f"etl_ultra_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)

            # 格式化器
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)

            logger.addHandler(console_handler)
            logger.addHandler(file_handler)

        return logger

    def _init_system(self):
        """初始化系统组件"""
        self.logger.info("🚀 超高性能ETL控制器初始化开始")

        # 初始化数据库连接池
        self._init_connection_pool()

        # 启动实时统计
        if self.enable_realtime_stats and self.stats_thread:
            self.stats_thread.start()

        self.logger.info("✅ 超高性能ETL控制器初始化完成")
        self.logger.info(f"📁 日志目录: {self.base_log_dir}")
        self.logger.info(f"⚙️ 批处理大小: {self.batch_size:,}")
        self.logger.info(f"🧵 最大工作线程: {self.max_workers}")
        self.logger.info(f"💾 内存限制: {self.memory_limit_mb}MB")
        self.logger.info(f"📊 实时统计: {'启用' if self.enable_realtime_stats else '禁用'}")
        self.logger.info(f"🔧 自动优化: {'启用' if self.auto_optimize else '禁用'}")

    def _init_connection_pool(self):
        """初始化数据库连接池"""
        self.logger.info("🔗 初始化数据库连接池...")
        success_count = 0

        for i in range(self.max_workers):
            try:
                writer = DWDWriter()
                if writer.connect():
                    self.writer_pool.append(writer)
                    success_count += 1
                    self.logger.info(f"✅ 数据库连接 {i+1}/{self.max_workers} 成功")
                else:
                    self.logger.error(f"❌ 数据库连接 {i+1}/{self.max_workers} 失败")
            except Exception as e:
                self.logger.error(f"❌ 创建数据库连接 {i+1} 异常: {e}")

        if success_count == 0:
            raise RuntimeError("无法创建任何数据库连接")

        self.logger.info(f"🎯 数据库连接池初始化完成: {success_count}/{self.max_workers}")

    def _stats_monitor_loop(self):
        """实时统计监控循环"""
        self.logger.info("📊 启动实时统计监控线程")

        last_display_time = 0
        display_interval = 5.0  # 每5秒显示一次统计

        while not self.shutdown_event.is_set():
            try:
                current_time = time.time()

                # 检查系统健康状况
                health = self.performance_monitor.check_system_health()

                # 获取进度摘要
                progress = self.progress_tracker.get_progress_summary()

                # 自动优化检查
                if self.auto_optimize:
                    self._auto_optimize_performance(health, progress)

                # 显示统计信息
                if current_time - last_display_time >= display_interval:
                    self._display_realtime_stats(health, progress)
                    last_display_time = current_time

                # 检查是否需要暂停
                if health.get('status') == 'critical':
                    self.logger.warning("⚠️ 系统资源紧张，暂停处理...")
                    self.pause_event.set()
                    time.sleep(5)  # 等待5秒
                    self.pause_event.clear()
                    self.logger.info("▶️ 恢复处理")

                time.sleep(1)  # 每秒检查一次

            except Exception as e:
                self.logger.error(f"📊 统计监控线程异常: {e}")
                time.sleep(5)

    def _display_realtime_stats(self, health: Dict, progress: Dict):
        """显示实时统计信息"""
        if progress.get('status') != 'processing':
            return

        # 清屏并显示统计
        os.system('cls' if os.name == 'nt' else 'clear')

        print("=" * 80)
        print("🚀 Nginx日志ETL处理器 - 实时监控面板")
        print("=" * 80)

        # 进度信息
        print(f"📁 总体进度: {progress['completed_files']}/{progress['total_files']} 文件 " +
              f"({progress['progress_percent']:.1f}%)")
        print(f"📊 处理记录: {progress['total_records']:,} 条 " +
              f"(错误: {progress['total_errors']:,} 条)")

        # 性能信息
        elapsed_str = str(timedelta(seconds=int(progress['elapsed_seconds'])))
        print(f"⏱️ 运行时间: {elapsed_str}")
        print(f"⚡ 当前速度: {progress['current_speed_rps']:.0f} RPS")
        print(f"📈 平均速度: {progress['avg_speed_rps']:.0f} RPS")
        print(f"🏆 峰值速度: {progress['peak_speed_rps']:.0f} RPS")
        print(f"💯 效率评分: {progress['efficiency_score']:.0f}/100")

        # 完成时间估算
        if progress['estimated_completion']:
            eta = datetime.fromtimestamp(progress['estimated_completion'])
            print(f"🎯 预计完成: {eta.strftime('%H:%M:%S')}")

        print("-" * 40)

        # 当前处理文件
        print("📂 当前处理文件:")
        for tid, file_info in progress['current_files'].items():
            progress_bar = "█" * int(file_info['progress'] * 20) + "░" * (20 - int(file_info['progress'] * 20))
            print(f"  线程{tid}: {file_info['file'][:30]}")
            print(f"          [{progress_bar}] {file_info['progress']*100:.1f}% ({file_info['speed_rps']:.0f} RPS)")

        print("-" * 40)

        # 系统健康状况
        status_icon = "🟢" if health['status'] == 'healthy' else "🟡" if health['status'] == 'warning' else "🔴"
        print(f"{status_icon} 系统状态: {health['status'].upper()}")
        print(f"💾 内存使用: {health['system_memory_percent']:.1f}% " +
              f"(可用: {health['system_memory_available_gb']:.1f}GB)")
        print(f"🖥️ CPU使用率: {health['cpu_percent']:.1f}%")
        print(f"📦 进程内存: {health['process_memory_mb']:.0f}MB")

        if 'warning' in health:
            print(f"⚠️ {health['warning']}")

        print("=" * 80)
        print("按 Ctrl+C 停止处理")

    def _auto_optimize_performance(self, health: Dict, progress: Dict):
        """自动性能优化"""
        if not self.auto_optimize:
            return

        current_time = time.time()
        if current_time - self.last_optimization_time < 30:  # 30秒优化一次
            return

        try:
            current_speed = progress.get('current_speed_rps', 0)
            memory_percent = health.get('system_memory_percent', 0)
            cpu_percent = health.get('cpu_percent', 0)

            # 记录当前性能
            self.optimization_history.append({
                'time': current_time,
                'speed': current_speed,
                'memory': memory_percent,
                'cpu': cpu_percent,
                'batch_size': self.batch_size
            })

            # 自动调优逻辑
            if memory_percent > 85 and self.batch_size > 1000:
                # 内存紧张，减小批大小
                self.batch_size = max(1000, int(self.batch_size * 0.8))
                self.logger.info(f"🔧 自动优化: 降低批处理大小至 {self.batch_size}")

            elif memory_percent < 60 and cpu_percent < 70 and current_speed > 0:
                # 资源充足，可以增加批大小
                if len(self.optimization_history) >= 3:
                    recent_speeds = [h['speed'] for h in list(self.optimization_history)[-3:]]
                    if all(speed >= 500 for speed in recent_speeds):  # 速度稳定且良好
                        self.batch_size = min(5000, int(self.batch_size * 1.2))
                        self.logger.info(f"🔧 自动优化: 提升批处理大小至 {self.batch_size}")

            self.last_optimization_time = current_time

        except Exception as e:
            self.logger.error(f"🔧 自动优化失败: {e}")

    def _signal_handler(self, signum, frame):
        """信号处理器"""
        self.logger.info(f"📡 接收到信号 {signum}，开始优雅停止...")
        self.shutdown_event.set()

    def cached_ua_parse(self, user_agent: str, mapper: FieldMapper) -> Dict:
        """缓存的用户代理解析"""
        if user_agent in self.ua_cache:
            self.cache_stats['hits'] += 1
            return self.ua_cache[user_agent]

        self.cache_stats['misses'] += 1
        # 这里应该调用实际的解析方法
        parsed = mapper._parse_user_agent_enhanced(user_agent)

        # 限制缓存大小
        if len(self.ua_cache) > 10000:
            # 移除最旧的1000个条目
            for _ in range(1000):
                self.ua_cache.pop(next(iter(self.ua_cache)))

        self.ua_cache[user_agent] = parsed
        return parsed

    def process_all_logs(self, test_mode: bool = False, limit: int = None,
                        skip_processed: bool = True) -> Dict[str, Any]:
        """
        处理所有日志文件 - 增强版本

        Args:
            test_mode: 测试模式，不写入数据库
            limit: 每个文件处理的记录限制
            skip_processed: 是否跳过已处理的文件

        Returns:
            处理结果统计
        """
        try:
            self.logger.info("🚀 开始处理所有日志文件")

            # 获取所有日志文件
            log_files = list(self.base_log_dir.glob("**/*.log"))
            if not log_files:
                return {'success': False, 'error': '未找到日志文件'}

            # 过滤已处理的文件
            if skip_processed:
                processed_state = self.load_state()
                unprocessed_files = [f for f in log_files if str(f) not in processed_state.get('processed_files', {})]
                self.logger.info(f"📁 发现 {len(log_files)} 个日志文件，{len(unprocessed_files)} 个未处理")
                log_files = unprocessed_files
            else:
                self.logger.info(f"📁 发现 {len(log_files)} 个日志文件")

            if not log_files:
                self.logger.info("✅ 所有文件已处理完成")
                return {'success': True, 'message': '所有文件已处理完成'}

            # 启动进度追踪
            self.progress_tracker.start_processing(len(log_files))

            # 文件分组处理
            file_groups = self._group_files_for_processing(log_files)

            # 多线程处理
            results = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_group = {
                    executor.submit(self.process_file_group, group, thread_id, test_mode, limit): (group, thread_id)
                    for thread_id, group in enumerate(file_groups)
                }

                for future in as_completed(future_to_group):
                    group, thread_id = future_to_group[future]
                    try:
                        result = future.result()
                        results.append(result)

                        if not result.get('success', False):
                            self.logger.error(f"线程 {thread_id} 处理失败: {result.get('error', '未知错误')}")

                    except Exception as e:
                        self.logger.error(f"线程 {thread_id} 异常: {e}")
                        results.append({'success': False, 'error': str(e), 'thread_id': thread_id})

            # 汇总结果
            return self._summarize_results(results)

        except Exception as e:
            self.logger.error(f"处理所有日志文件失败: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            # 确保资源清理
            self.cleanup()

    def _group_files_for_processing(self, files: List[Path]) -> List[List[Path]]:
        """将文件分组以便并行处理"""
        files_per_thread = max(1, len(files) // self.max_workers)
        groups = []

        for i in range(0, len(files), files_per_thread):
            group = files[i:i + files_per_thread]
            groups.append(group)

        # 确保不超过最大线程数
        while len(groups) > self.max_workers:
            # 将最后一组合并到前一组
            last_group = groups.pop()
            groups[-1].extend(last_group)

        return groups

    def process_file_group(self, file_paths: List[Path], thread_id: int,
                          test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """处理文件组"""
        results = []

        for file_path in file_paths:
            if self.shutdown_event.is_set():
                break

            # 等待暂停状态结束
            while self.pause_event.is_set() and not self.shutdown_event.is_set():
                time.sleep(1)

            result = self.process_single_file(file_path, thread_id, test_mode, limit)
            results.append(result)

        return {
            'success': all(r.get('success', False) for r in results),
            'thread_id': thread_id,
            'file_results': results
        }

    def process_single_file(self, file_path: Path, thread_id: int,
                           test_mode: bool = False, limit: int = None) -> Dict[str, Any]:
        """处理单个文件 - 增强版本"""
        start_time = time.time()

        # 估算文件行数
        estimated_lines = self._estimate_file_lines(file_path)

        # 开始文件处理追踪
        self.progress_tracker.start_file(thread_id, file_path, estimated_lines)

        try:
            # 获取组件
            parser = self.parser_pool[thread_id % len(self.parser_pool)]
            mapper = self.mapper_pool[thread_id % len(self.mapper_pool)]
            writer = self.writer_pool[thread_id % len(self.writer_pool)] if not test_mode else None

            batch = []
            file_records = 0
            file_lines = 0
            file_errors = 0

            self.logger.info(f"🧵{thread_id} 开始处理: {file_path.name} (预估 {estimated_lines:,} 行)")

            # 流式处理文件
            for parsed_data in parser.parse_file(file_path):
                if self.shutdown_event.is_set():
                    break

                file_lines += 1

                # 更新进度
                if file_lines % 1000 == 0:
                    self.progress_tracker.update_file_progress(thread_id, file_lines, file_records)

                if parsed_data:
                    try:
                        # 缓存解析
                        user_agent = parsed_data.get('user_agent', '')
                        if user_agent:
                            parsed_data['_cached_ua'] = self.cached_ua_parse(user_agent, mapper)

                        # 字段映射
                        mapped_data = mapper.map_to_dwd(parsed_data, file_path.name)
                        batch.append(mapped_data)
                        file_records += 1

                        # 批量写入
                        if len(batch) >= self.batch_size:
                            if not test_mode and writer:
                                try:
                                    writer.write_batch(batch)
                                except Exception as e:
                                    self.logger.error(f"批量写入失败: {e}")
                                    file_errors += 1

                            batch.clear()
                            gc.collect()

                        # 检查限制
                        if limit and file_records >= limit:
                            break

                    except Exception as e:
                        file_errors += 1
                        if file_errors <= 5:  # 只记录前5个错误
                            self.logger.error(f"记录处理错误: {e}")

            # 处理剩余批次
            if batch and not test_mode and writer:
                try:
                    writer.write_batch(batch)
                except Exception as e:
                    self.logger.error(f"最终批量写入失败: {e}")
                    file_errors += 1

            # 完成文件处理
            processing_time = time.time() - start_time
            self.progress_tracker.complete_file(thread_id, file_records, file_errors)

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
                f"✅ 文件完成: {file_path.name} - "
                f"{file_records:,} 条记录，{file_errors} 个错误，"
                f"{processing_time:.1f}秒 ({result['speed_rps']:.0f} RPS)"
            )

            return result

        except Exception as e:
            error_msg = f"处理文件 {file_path.name} 失败: {e}"
            self.logger.error(error_msg)
            self.progress_tracker.complete_file(thread_id, 0, 1)
            return {
                'success': False,
                'file_path': str(file_path),
                'error': error_msg
            }

    def _estimate_file_lines(self, file_path: Path) -> int:
        """估算文件行数"""
        try:
            # 快速估算：读取前1000行，计算平均行长，估算总行数
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                sample_lines = []
                for i, line in enumerate(f):
                    sample_lines.append(len(line))
                    if i >= 999:  # 读取1000行样本
                        break

            if sample_lines:
                avg_line_length = sum(sample_lines) / len(sample_lines)
                file_size = file_path.stat().st_size
                estimated_lines = int(file_size / avg_line_length)
                return max(1000, estimated_lines)  # 至少1000行
            else:
                return 10000  # 默认估算

        except Exception:
            return 10000  # 默认估算

    def _summarize_results(self, results: List[Dict]) -> Dict[str, Any]:
        """汇总处理结果"""
        total_files = len(results)
        successful_files = sum(1 for r in results if r.get('success', False))

        total_records = 0
        total_errors = 0
        total_time = 0

        for result in results:
            if 'file_results' in result:
                for file_result in result['file_results']:
                    total_records += file_result.get('records_processed', 0)
                    total_errors += file_result.get('errors', 0)
                    total_time += file_result.get('processing_time', 0)
            else:
                total_records += result.get('records_processed', 0)
                total_errors += result.get('errors', 0)
                total_time += result.get('processing_time', 0)

        avg_speed = total_records / total_time if total_time > 0 else 0

        summary = {
            'success': successful_files > 0,
            'total_files': total_files,
            'successful_files': successful_files,
            'failed_files': total_files - successful_files,
            'total_records_processed': total_records,
            'total_errors': total_errors,
            'total_processing_time': total_time,
            'average_speed_rps': avg_speed,
            'cache_hit_rate': self.cache_stats['hits'] / (self.cache_stats['hits'] + self.cache_stats['misses']) if (self.cache_stats['hits'] + self.cache_stats['misses']) > 0 else 0
        }

        self.logger.info("=" * 60)
        self.logger.info("🎯 处理完成总结:")
        self.logger.info(f"📁 文件: {successful_files}/{total_files} 成功")
        self.logger.info(f"📊 记录: {total_records:,} 条 (错误: {total_errors:,})")
        self.logger.info(f"⏱️ 时间: {total_time:.1f} 秒")
        self.logger.info(f"⚡ 平均速度: {avg_speed:.0f} RPS")
        self.logger.info(f"💾 缓存命中率: {summary['cache_hit_rate']:.1%}")
        self.logger.info("=" * 60)

        return summary

    def load_state(self) -> Dict:
        """加载处理状态"""
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.warning(f"加载状态文件失败: {e}")
        return {'processed_files': {}}

    def mark_file_processed(self, file_path: Path, record_count: int, processing_time: float):
        """标记文件已处理"""
        try:
            state = self.load_state()
            state['processed_files'][str(file_path)] = {
                'timestamp': datetime.now().isoformat(),
                'record_count': record_count,
                'processing_time': processing_time
            }

            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False)

        except Exception as e:
            self.logger.error(f"保存状态文件失败: {e}")

    def cleanup(self):
        """清理资源"""
        self.logger.info("🧹 开始清理资源...")

        # 停止统计线程
        self.shutdown_event.set()

        # 关闭数据库连接
        for writer in self.writer_pool:
            try:
                writer.close()
            except Exception as e:
                self.logger.error(f"关闭数据库连接失败: {e}")

        # 清理缓存
        self.ua_cache.clear()
        self.uri_cache.clear()
        self.ip_cache.clear()

        self.logger.info("✅ 资源清理完成")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='超高性能ETL处理器')
    parser.add_argument('--test', action='store_true', help='测试模式')
    parser.add_argument('--limit', type=int, help='每个文件处理记录限制')
    parser.add_argument('--batch-size', type=int, default=3000, help='批处理大小')
    parser.add_argument('--workers', type=int, default=6, help='工作线程数')
    parser.add_argument('--memory-limit', type=int, default=1024, help='内存限制(MB)')
    parser.add_argument('--no-stats', action='store_true', help='禁用实时统计')
    parser.add_argument('--no-optimize', action='store_true', help='禁用自动优化')

    args = parser.parse_args()

    # 创建控制器
    controller = UltraPerformanceETLController(
        batch_size=args.batch_size,
        max_workers=args.workers,
        memory_limit_mb=args.memory_limit,
        enable_realtime_stats=not args.no_stats,
        auto_optimize=not args.no_optimize
    )

    try:
        # 处理所有日志
        result = controller.process_all_logs(
            test_mode=args.test,
            limit=args.limit
        )

        if result['success']:
            print("\n🎉 处理完成!")
        else:
            print(f"\n❌ 处理失败: {result.get('error', '未知错误')}")

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断处理")
    except Exception as e:
        print(f"\n💥 处理异常: {e}")
    finally:
        controller.cleanup()

if __name__ == "__main__":
    main()