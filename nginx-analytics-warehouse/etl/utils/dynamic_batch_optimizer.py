#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
动态批大小优化器
根据系统性能和数据量动态调整批处理大小以获得最佳性能
"""

import time
import logging
from typing import Dict, List, Tuple, Optional
from statistics import mean, median
from collections import deque

# 尝试导入psutil
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None


class DynamicBatchOptimizer:
    """动态批大小优化器"""

    def __init__(self,
                 initial_batch_size: int = 25000,
                 min_batch_size: int = 1000,
                 max_batch_size: int = 100000,
                 memory_threshold: float = 0.8,
                 cpu_threshold: float = 0.9,
                 optimization_window: int = 10):
        """
        初始化动态批大小优化器

        Args:
            initial_batch_size: 初始批大小
            min_batch_size: 最小批大小
            max_batch_size: 最大批大小
            memory_threshold: 内存使用阈值 (0-1)
            cpu_threshold: CPU使用阈值 (0-1)
            optimization_window: 性能统计窗口大小
        """
        self.current_batch_size = initial_batch_size
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.memory_threshold = memory_threshold
        self.cpu_threshold = cpu_threshold
        self.optimization_window = optimization_window

        # 性能历史记录
        self.performance_history = deque(maxlen=optimization_window)
        self.batch_history = deque(maxlen=optimization_window)

        # 系统监控
        self.memory_history = deque(maxlen=20)
        self.cpu_history = deque(maxlen=20)

        # 优化状态
        self.last_optimization_time = time.time()
        self.optimization_interval = 30  # 30秒优化一次
        self.consecutive_bad_performance = 0
        self.consecutive_good_performance = 0

        self.logger = logging.getLogger(__name__)

    def record_batch_performance(self,
                                batch_size: int,
                                records_count: int,
                                duration: float,
                                memory_used_mb: Optional[float] = None) -> None:
        """
        记录批处理性能数据

        Args:
            batch_size: 批大小
            records_count: 处理记录数
            duration: 处理耗时(秒)
            memory_used_mb: 内存使用量(MB)
        """
        if duration > 0:
            throughput = records_count / duration  # 记录/秒

            performance_data = {
                'batch_size': batch_size,
                'records_count': records_count,
                'duration': duration,
                'throughput': throughput,
                'memory_used_mb': memory_used_mb,
                'timestamp': time.time()
            }

            self.performance_history.append(performance_data)
            self.batch_history.append(batch_size)

            # 记录系统资源使用
            self._record_system_metrics()

    def _record_system_metrics(self) -> None:
        """记录系统指标"""
        if not PSUTIL_AVAILABLE:
            return

        try:
            # 内存使用率
            memory = psutil.virtual_memory()
            self.memory_history.append(memory.percent / 100.0)

            # CPU使用率
            cpu_percent = psutil.cpu_percent(interval=0.1)
            self.cpu_history.append(cpu_percent / 100.0)

        except Exception as e:
            self.logger.warning(f"无法获取系统指标: {e}")

    def should_optimize(self) -> bool:
        """判断是否应该进行优化"""
        current_time = time.time()

        # 时间间隔检查
        if current_time - self.last_optimization_time < self.optimization_interval:
            return False

        # 性能数据充足检查
        if len(self.performance_history) < 3:
            return False

        return True

    def optimize_batch_size(self) -> Tuple[int, str]:
        """
        优化批大小

        Returns:
            Tuple[新的批大小, 优化原因]
        """
        if not self.should_optimize():
            return self.current_batch_size, "无需优化"

        # 分析性能趋势
        optimization_reason = self._analyze_performance_trend()
        new_batch_size = self._calculate_optimal_batch_size()

        # 应用约束
        new_batch_size = max(self.min_batch_size, min(self.max_batch_size, new_batch_size))

        if new_batch_size != self.current_batch_size:
            self.logger.info(f"批大小优化: {self.current_batch_size} -> {new_batch_size}, 原因: {optimization_reason}")
            self.current_batch_size = new_batch_size

        self.last_optimization_time = time.time()
        return self.current_batch_size, optimization_reason

    def _analyze_performance_trend(self) -> str:
        """分析性能趋势"""
        if len(self.performance_history) < 3:
            return "数据不足"

        recent_data = list(self.performance_history)[-3:]
        throughputs = [data['throughput'] for data in recent_data]

        # 检查系统资源压力
        if self._is_system_under_pressure():
            self.consecutive_bad_performance += 1
            self.consecutive_good_performance = 0
            return "系统资源压力过大"

        # 检查性能趋势
        if len(throughputs) >= 2:
            trend = throughputs[-1] - throughputs[0]

            if trend < -100:  # 性能下降明显
                self.consecutive_bad_performance += 1
                self.consecutive_good_performance = 0
                return "性能下降趋势"
            elif trend > 100:  # 性能提升明显
                self.consecutive_good_performance += 1
                self.consecutive_bad_performance = 0
                return "性能提升趋势"

        # 重置计数器
        self.consecutive_bad_performance = max(0, self.consecutive_bad_performance - 1)
        self.consecutive_good_performance = max(0, self.consecutive_good_performance - 1)

        return "性能稳定"

    def _is_system_under_pressure(self) -> bool:
        """检查系统是否处于压力状态"""
        if not self.memory_history or not self.cpu_history:
            return False

        # 检查内存压力
        recent_memory = list(self.memory_history)[-5:] if len(self.memory_history) >= 5 else list(self.memory_history)
        avg_memory = mean(recent_memory)

        # 检查CPU压力
        recent_cpu = list(self.cpu_history)[-5:] if len(self.cpu_history) >= 5 else list(self.cpu_history)
        avg_cpu = mean(recent_cpu)

        return avg_memory > self.memory_threshold or avg_cpu > self.cpu_threshold

    def _calculate_optimal_batch_size(self) -> int:
        """计算最优批大小"""
        recent_data = list(self.performance_history)[-5:]

        if len(recent_data) < 2:
            return self.current_batch_size

        # 系统压力检查
        if self._is_system_under_pressure():
            # 系统压力大，减少批大小
            return int(self.current_batch_size * 0.8)

        # 性能趋势分析
        if self.consecutive_bad_performance >= 2:
            # 连续性能不佳，减少批大小
            return int(self.current_batch_size * 0.7)
        elif self.consecutive_good_performance >= 3:
            # 连续性能良好，尝试增加批大小
            return int(self.current_batch_size * 1.3)

        # 基于吞吐量的优化
        throughputs = [data['throughput'] for data in recent_data]
        batch_sizes = [data['batch_size'] for data in recent_data]

        # 寻找吞吐量最高的批大小
        best_idx = throughputs.index(max(throughputs))
        best_batch_size = batch_sizes[best_idx]

        # 渐进式调整
        if best_batch_size > self.current_batch_size:
            return int(self.current_batch_size * 1.2)
        elif best_batch_size < self.current_batch_size:
            return int(self.current_batch_size * 0.9)

        return self.current_batch_size

    def get_current_batch_size(self) -> int:
        """获取当前批大小"""
        return self.current_batch_size

    def get_performance_stats(self) -> Dict:
        """获取性能统计信息"""
        if not self.performance_history:
            return {}

        recent_data = list(self.performance_history)
        throughputs = [data['throughput'] for data in recent_data]

        stats = {
            'current_batch_size': self.current_batch_size,
            'total_measurements': len(recent_data),
            'avg_throughput': mean(throughputs) if throughputs else 0,
            'median_throughput': median(throughputs) if throughputs else 0,
            'max_throughput': max(throughputs) if throughputs else 0,
            'min_throughput': min(throughputs) if throughputs else 0,
            'consecutive_bad_performance': self.consecutive_bad_performance,
            'consecutive_good_performance': self.consecutive_good_performance
        }

        # 系统资源统计
        if self.memory_history:
            stats['avg_memory_usage'] = mean(self.memory_history)
            stats['max_memory_usage'] = max(self.memory_history)

        if self.cpu_history:
            stats['avg_cpu_usage'] = mean(self.cpu_history)
            stats['max_cpu_usage'] = max(self.cpu_history)

        return stats

    def reset_optimizer(self) -> None:
        """重置优化器状态"""
        self.performance_history.clear()
        self.batch_history.clear()
        self.memory_history.clear()
        self.cpu_history.clear()
        self.consecutive_bad_performance = 0
        self.consecutive_good_performance = 0
        self.last_optimization_time = time.time()

        self.logger.info("动态批大小优化器已重置")

    def force_batch_size(self, batch_size: int) -> None:
        """强制设置批大小（跳过优化）"""
        self.current_batch_size = max(self.min_batch_size, min(self.max_batch_size, batch_size))
        self.logger.info(f"强制设置批大小为: {self.current_batch_size}")


class BatchSizeRecommendation:
    """批大小推荐器"""

    @staticmethod
    def recommend_initial_batch_size(available_memory_gb: float,
                                   record_size_kb: float = 1.0,
                                   target_memory_usage: float = 0.3) -> int:
        """
        根据系统配置推荐初始批大小

        Args:
            available_memory_gb: 可用内存(GB)
            record_size_kb: 单条记录大小(KB)
            target_memory_usage: 目标内存使用率

        Returns:
            推荐的初始批大小
        """
        # 计算可用于批处理的内存
        batch_memory_mb = available_memory_gb * 1024 * target_memory_usage

        # 计算可容纳的记录数
        records_per_mb = 1024 / record_size_kb
        max_records = int(batch_memory_mb * records_per_mb)

        # 应用合理范围
        recommended_size = max(1000, min(100000, max_records))

        # 调整到常用的批大小
        if recommended_size < 5000:
            return 5000
        elif recommended_size < 15000:
            return 10000
        elif recommended_size < 35000:
            return 25000
        elif recommended_size < 75000:
            return 50000
        else:
            return 100000

    @staticmethod
    def get_system_info() -> Dict:
        """获取系统信息用于批大小推荐"""
        if not PSUTIL_AVAILABLE:
            return {'error': 'psutil模块未安装，无法获取系统信息'}

        try:
            memory = psutil.virtual_memory()
            cpu_count = psutil.cpu_count()

            return {
                'total_memory_gb': memory.total / (1024**3),
                'available_memory_gb': memory.available / (1024**3),
                'memory_usage_percent': memory.percent,
                'cpu_count': cpu_count,
                'cpu_usage_percent': psutil.cpu_percent(interval=1)
            }
        except Exception as e:
            return {'error': str(e)}


def main():
    """演示动态批大小优化器的使用"""
    print("🔧 动态批大小优化器演示")
    print("=" * 60)

    # 创建优化器
    optimizer = DynamicBatchOptimizer(
        initial_batch_size=25000,
        min_batch_size=5000,
        max_batch_size=100000
    )

    # 获取系统信息
    system_info = BatchSizeRecommendation.get_system_info()
    print("📊 系统信息:")
    for key, value in system_info.items():
        if isinstance(value, float):
            print(f"   {key}: {value:.2f}")
        else:
            print(f"   {key}: {value}")

    # 推荐初始批大小
    if 'available_memory_gb' in system_info:
        recommended = BatchSizeRecommendation.recommend_initial_batch_size(
            system_info['available_memory_gb']
        )
        print(f"\n💡 推荐初始批大小: {recommended:,}")

    # 模拟性能数据
    print(f"\n🚀 当前批大小: {optimizer.get_current_batch_size():,}")

    # 模拟几次性能记录
    import random
    for i in range(5):
        batch_size = optimizer.get_current_batch_size()
        records = batch_size
        duration = random.uniform(1.0, 3.0)  # 模拟1-3秒处理时间
        memory_used = random.uniform(100, 500)  # 模拟内存使用

        optimizer.record_batch_performance(batch_size, records, duration, memory_used)

        # 尝试优化
        new_size, reason = optimizer.optimize_batch_size()
        print(f"轮次 {i+1}: 批大小={new_size:,}, 原因={reason}")

    # 显示性能统计
    stats = optimizer.get_performance_stats()
    print(f"\n📈 性能统计:")
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"   {key}: {value:.2f}")
        else:
            print(f"   {key}: {value}")


if __name__ == "__main__":
    main()