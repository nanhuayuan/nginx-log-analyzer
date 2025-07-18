"""
高级采样算法实现模块
包含T-Digest、蓄水池采样、Count-Min Sketch等算法
用于nginx日志分析的流式统计计算

Author: Claude Code
Date: 2025-07-18
"""

import random
import math
import hashlib
from collections import defaultdict
from typing import List, Dict, Any, Optional
import numpy as np


class TDigest:
    """
    T-Digest算法实现
    用于高效计算分位数，特别适合流式数据处理
    """
    
    def __init__(self, compression: int = 100):
        """
        初始化T-Digest
        
        Args:
            compression: 压缩参数，控制精度和内存使用的平衡
        """
        self.compression = compression
        self.centroids = []  # [(mean, weight), ...]
        self.count = 0
        self.min_value = float('inf')
        self.max_value = float('-inf')
    
    def add(self, value: float, weight: int = 1):
        """添加单个值"""
        if math.isnan(value):
            return
            
        self.count += weight
        self.min_value = min(self.min_value, value)
        self.max_value = max(self.max_value, value)
        
        # 简化版本：直接添加到centroids中
        self.centroids.append((value, weight))
        
        # 当centroids数量超过压缩参数时进行压缩
        if len(self.centroids) > self.compression * 2:
            self._compress()
    
    def add_batch(self, values: List[float]):
        """批量添加值"""
        for value in values:
            self.add(value)
    
    def _compress(self):
        """压缩centroids"""
        if not self.centroids:
            return
        
        # 按均值排序
        self.centroids.sort(key=lambda x: x[0])
        
        compressed = []
        current_mean, current_weight = self.centroids[0]
        
        for mean, weight in self.centroids[1:]:
            # 简化的合并逻辑
            if len(compressed) < self.compression:
                # 合并相邻的centroids
                combined_weight = current_weight + weight
                combined_mean = (current_mean * current_weight + mean * weight) / combined_weight
                current_mean, current_weight = combined_mean, combined_weight
            else:
                compressed.append((current_mean, current_weight))
                current_mean, current_weight = mean, weight
        
        compressed.append((current_mean, current_weight))
        self.centroids = compressed
    
    def percentile(self, p: float) -> float:
        """
        计算百分位数
        
        Args:
            p: 百分位数 (0-100)
            
        Returns:
            对应的值
        """
        if not self.centroids or self.count == 0:
            return 0.0
        
        if p <= 0:
            return self.min_value
        if p >= 100:
            return self.max_value
        
        # 确保centroids已排序
        self.centroids.sort(key=lambda x: x[0])
        
        target = p / 100.0 * self.count
        cumulative = 0
        
        for i, (mean, weight) in enumerate(self.centroids):
            cumulative += weight
            if cumulative >= target:
                return mean
        
        return self.max_value
    
    def merge(self, other: 'TDigest') -> 'TDigest':
        """合并两个T-Digest"""
        result = TDigest(self.compression)
        result.centroids = self.centroids + other.centroids
        result.count = self.count + other.count
        result.min_value = min(self.min_value, other.min_value)
        result.max_value = max(self.max_value, other.max_value)
        result._compress()
        return result


class ReservoirSampler:
    """
    蓄水池采样算法实现
    保证每个元素被选中的概率相等，适合需要原始数据的场景
    """
    
    def __init__(self, max_size: int = 1000):
        """
        初始化蓄水池采样器
        
        Args:
            max_size: 采样池的最大大小
        """
        self.max_size = max_size
        self.samples = []
        self.count = 0
    
    def add(self, value):
        """添加单个值"""
        self.count += 1
        
        if len(self.samples) < self.max_size:
            # 蓄水池未满，直接添加
            self.samples.append(value)
        else:
            # 蓄水池已满，以概率 max_size/count 替换
            j = random.randint(1, self.count)
            if j <= self.max_size:
                self.samples[j - 1] = value
    
    def add_batch(self, values: List):
        """批量添加值"""
        for value in values:
            self.add(value)
    
    def get_samples(self) -> List:
        """获取当前采样结果"""
        return self.samples.copy()
    
    def percentile(self, p: float) -> float:
        """计算百分位数"""
        if not self.samples:
            return 0.0
        return float(np.percentile(self.samples, p))
    
    def mean(self) -> float:
        """计算均值"""
        if not self.samples:
            return 0.0
        return float(np.mean(self.samples))
    
    def std(self) -> float:
        """计算标准差"""
        if not self.samples:
            return 0.0
        return float(np.std(self.samples))


class CountMinSketch:
    """
    Count-Min Sketch算法实现
    用于估计元素频率，特别适合热点检测
    """
    
    def __init__(self, width: int = 1000, depth: int = 5, seed: int = 42):
        """
        初始化Count-Min Sketch
        
        Args:
            width: 哈希表宽度
            depth: 哈希函数个数
            seed: 随机种子
        """
        self.width = width
        self.depth = depth
        self.table = [[0] * width for _ in range(depth)]
        self.hash_seeds = [seed + i for i in range(depth)]
    
    def _hash(self, item: str, seed: int) -> int:
        """计算哈希值"""
        return int(hashlib.md5(f"{item}{seed}".encode()).hexdigest(), 16) % self.width
    
    def increment(self, item: str, count: int = 1):
        """增加元素计数"""
        for i, seed in enumerate(self.hash_seeds):
            j = self._hash(item, seed)
            self.table[i][j] += count
    
    def estimate(self, item: str) -> int:
        """估计元素频率"""
        estimates = []
        for i, seed in enumerate(self.hash_seeds):
            j = self._hash(item, seed)
            estimates.append(self.table[i][j])
        return min(estimates)
    
    def top_k(self, k: int = 10) -> List[tuple]:
        """获取估计的top-k元素（需要额外维护候选集）"""
        # 简化实现：返回所有非零位置的最小值
        candidates = {}
        for i in range(self.depth):
            for j in range(self.width):
                if self.table[i][j] > 0:
                    # 这里需要反向映射，实际使用中需要维护候选集
                    key = f"pos_{i}_{j}"
                    candidates[key] = min(candidates.get(key, float('inf')), self.table[i][j])
        
        return sorted(candidates.items(), key=lambda x: x[1], reverse=True)[:k]


class HyperLogLog:
    """
    HyperLogLog算法实现
    用于估计集合基数（唯一元素个数）
    """
    
    def __init__(self, precision: int = 12):
        """
        初始化HyperLogLog
        
        Args:
            precision: 精度参数，决定桶的数量 (2^precision)
        """
        self.precision = precision
        self.m = 2 ** precision
        self.buckets = [0] * self.m
        self.alpha = self._get_alpha()
    
    def _get_alpha(self) -> float:
        """获取偏差修正常数"""
        if self.m >= 128:
            return 0.7213 / (1 + 1.079 / self.m)
        elif self.m >= 64:
            return 0.709
        elif self.m >= 32:
            return 0.697
        else:
            return 0.673
    
    def _hash(self, item: str) -> int:
        """计算哈希值"""
        return int(hashlib.md5(item.encode()).hexdigest(), 16)
    
    def add(self, item: str):
        """添加元素"""
        hash_value = self._hash(item)
        
        # 取前precision位作为桶索引
        bucket = hash_value & ((1 << self.precision) - 1)
        
        # 取剩余位，计算前导零个数
        w = hash_value >> self.precision
        leading_zeros = self._leading_zeros(w) + 1
        
        # 更新桶中的最大值
        self.buckets[bucket] = max(self.buckets[bucket], leading_zeros)
    
    def _leading_zeros(self, w: int) -> int:
        """计算前导零个数"""
        if w == 0:
            return 32
        return (w ^ (w - 1)).bit_length() - 1
    
    def cardinality(self) -> int:
        """估计基数"""
        raw_estimate = self.alpha * (self.m ** 2) / sum(2 ** (-x) for x in self.buckets)
        
        # 小范围修正
        if raw_estimate <= 2.5 * self.m:
            zeros = self.buckets.count(0)
            if zeros != 0:
                return int(self.m * math.log(self.m / float(zeros)))
        
        # 大范围修正
        if raw_estimate <= (1.0/30.0) * (2**32):
            return int(raw_estimate)
        else:
            return int(-2**32 * math.log(1 - raw_estimate / 2**32))


class StratifiedSampler:
    """
    分层采样器
    按照指定的分层键进行采样，确保各层都有代表性
    """
    
    def __init__(self, samples_per_stratum: int = 100):
        """
        初始化分层采样器
        
        Args:
            samples_per_stratum: 每层的采样数量
        """
        self.samples_per_stratum = samples_per_stratum
        self.strata = defaultdict(lambda: ReservoirSampler(samples_per_stratum))
    
    def add(self, value, stratum_key: str):
        """添加值到指定层"""
        self.strata[stratum_key].add(value)
    
    def get_stratum_samples(self, stratum_key: str) -> List:
        """获取指定层的采样"""
        return self.strata[stratum_key].get_samples()
    
    def get_all_samples(self) -> List:
        """获取所有层的采样"""
        all_samples = []
        for sampler in self.strata.values():
            all_samples.extend(sampler.get_samples())
        return all_samples
    
    def get_stratum_percentile(self, stratum_key: str, p: float) -> float:
        """获取指定层的百分位数"""
        return self.strata[stratum_key].percentile(p)
    
    def get_overall_percentile(self, p: float) -> float:
        """获取整体百分位数"""
        all_samples = self.get_all_samples()
        if not all_samples:
            return 0.0
        return float(np.percentile(all_samples, p))
    
    def get_strata_stats(self) -> Dict[str, Dict[str, float]]:
        """获取各层统计信息"""
        stats = {}
        for key, sampler in self.strata.items():
            stats[key] = {
                'count': sampler.count,
                'sample_size': len(sampler.samples),
                'mean': sampler.mean(),
                'std': sampler.std(),
                'p50': sampler.percentile(50),
                'p95': sampler.percentile(95),
                'p99': sampler.percentile(99)
            }
        return stats


class AdaptiveSampler:
    """
    自适应采样器
    根据数据分布自动调整采样策略
    """
    
    def __init__(self, initial_sample_size: int = 1000, adaptation_threshold: int = 10000):
        """
        初始化自适应采样器
        
        Args:
            initial_sample_size: 初始采样大小
            adaptation_threshold: 自适应阈值
        """
        self.sample_size = initial_sample_size
        self.adaptation_threshold = adaptation_threshold
        self.reservoir = ReservoirSampler(initial_sample_size)
        self.total_count = 0
        self.variance_history = []
    
    def add(self, value: float):
        """添加值"""
        self.total_count += 1
        self.reservoir.add(value)
        
        # 定期评估并调整采样大小
        if self.total_count % self.adaptation_threshold == 0:
            self._adapt_sample_size()
    
    def _adapt_sample_size(self):
        """自适应调整采样大小"""
        if len(self.reservoir.samples) < 100:
            return
        
        current_std = self.reservoir.std()
        self.variance_history.append(current_std)
        
        # 如果方差较大，增加采样大小
        if len(self.variance_history) >= 3:
            recent_variance = np.mean(self.variance_history[-3:])
            if len(self.variance_history) >= 6:
                older_variance = np.mean(self.variance_history[-6:-3])
                
                # 如果方差增长，增加采样大小
                if recent_variance > older_variance * 1.2:
                    new_size = min(self.sample_size * 2, 5000)
                    if new_size > self.sample_size:
                        self.sample_size = new_size
                        # 创建新的更大的蓄水池
                        new_reservoir = ReservoirSampler(new_size)
                        new_reservoir.samples = self.reservoir.samples.copy()
                        new_reservoir.count = self.reservoir.count
                        self.reservoir = new_reservoir
    
    def get_samples(self) -> List:
        """获取采样结果"""
        return self.reservoir.get_samples()
    
    def percentile(self, p: float) -> float:
        """计算百分位数"""
        return self.reservoir.percentile(p)