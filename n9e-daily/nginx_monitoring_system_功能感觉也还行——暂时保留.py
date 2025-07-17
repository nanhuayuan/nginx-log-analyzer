#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nginx监控报告生成系统
支持日报、周报、月报以及自定义时间段的监控报告生成
包含GUI界面和Excel导出功能
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.ttk import *
import datetime
import json
import logging
import os
import sys
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Tuple, Optional, Any
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import threading
import queue

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('monitoring_report.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# 枚举和数据类定义
class ReportType(Enum):
    DAILY = "日报"
    WEEKLY = "周报"
    MONTHLY = "月报"
    CUSTOM = "自定义"


@dataclass
class MetricResult:
    value: float
    unit: str = ""
    timestamp: str = ""
    status: str = "normal"


@dataclass
class ConnectionConfig:
    url: str
    username: str
    password: str
    datasource_ids: List[str]


# 指标配置集中管理
class MetricsConfig:
    """指标配置集中管理类"""

    @staticmethod
    def get_core_metrics_queries() -> Dict[str, Dict[str, Any]]:
        """核心业务指标查询配置"""
        return {
            "availability": {
                "query": 'count(nginx_active{{cluster="{cluster}"}} > 0) / count(nginx_active{{cluster="{cluster}"}}) * 100',
                "type": "instant",
                "unit": "%",
                "thresholds": {"critical": 99.0, "warning": 99.9}
            },
            "total_requests": {
                "query": 'sum(increase(nginx_requests{{cluster="{cluster}"}}[{duration}]))',
                "type": "instant",
                "unit": "次"
            },
            "qps": {
                "query": 'sum(rate(nginx_requests{{cluster="{cluster}"}}[2m]))',
                "type": "range",
                "unit": "req/s"
            },
            "bandwidth_in": {
                "query": 'sum(rate(net_bytes_recv{{cluster="{cluster}"}}[2m]) * 8) / 1000000000',
                "type": "range",
                "unit": "Gbps"
            },
            "bandwidth_out": {
                "query": 'sum(rate(net_bytes_sent{{cluster="{cluster}"}}[2m]) * 8) / 1000000000',
                "type": "range",
                "unit": "Gbps"
            },
            "traffic_in": {
                "query": 'sum(increase(net_bytes_recv{{cluster="{cluster}"}}[{duration}])) / 1024 / 1024 / 1024 / 1024',
                "type": "instant",
                "unit": "TB"
            },
            "traffic_out": {
                "query": 'sum(increase(net_bytes_sent{{cluster="{cluster}"}}[{duration}])) / 1024 / 1024 / 1024 / 1024',
                "type": "instant",
                "unit": "TB"
            }
        }

    @staticmethod
    def get_system_metrics_queries() -> Dict[str, Dict[str, Any]]:
        """系统资源指标查询配置"""
        return {
            "cpu_usage": {
                "query": 'avg(cpu_usage_active{{cluster="{cluster}"}})',
                "type": "range",
                "unit": "%",
                "thresholds": {"critical": 85, "warning": 70}
            },
            "memory_usage": {
                "query": 'avg(mem_used_percent{{cluster="{cluster}"}})',
                "type": "range",
                "unit": "%",
                "thresholds": {"critical": 90, "warning": 75}
            },
            "system_load": {
                "query": 'avg(system_load5{{cluster="{cluster}"}})',
                "type": "range",
                "unit": ""
            },
            "cpu_cores": {
                "query": 'system_n_cpus{{cluster="{cluster}"}}',
                "type": "instant",
                "unit": "核"
            },
            "node_count": {
                "query": 'count(up{{cluster="{cluster}", job=~".*node.*"}} == 1 or system_uptime{{cluster="{cluster}"}} > 0)',
                "type": "instant",
                "unit": "个"
            }
        }

    @staticmethod
    def get_connection_metrics_queries() -> Dict[str, Dict[str, Any]]:
        """连接状态指标查询配置"""
        return {
            "conn_active": {
                "query": 'sum(nginx_active{{cluster="{cluster}"}})',
                "type": "range",
                "unit": "个"
            },
            "conn_waiting": {
                "query": 'sum(nginx_waiting{{cluster="{cluster}"}})',
                "type": "range",
                "unit": "个"
            },
            "conn_reading": {
                "query": 'sum(nginx_reading{{cluster="{cluster}"}})',
                "type": "range",
                "unit": "个"
            },
            "conn_writing": {
                "query": 'sum(nginx_writing{{cluster="{cluster}"}})',
                "type": "range",
                "unit": "个"
            }
        }

    @staticmethod
    def get_anomaly_metrics_queries() -> Dict[str, Dict[str, Any]]:
        """异常事件指标查询配置"""
        return {
            "oom_events": {
                "query": 'sum(increase(kernel_vmstat_oom_kill{{cluster="{cluster}"}}[{duration}]))',
                "type": "instant",
                "unit": "次",
                "thresholds": {"critical": 1}
            },
            "high_cpu_nodes": {
                "query": 'count(cpu_usage_active{{cluster="{cluster}"}} > {cpu_threshold})',
                "type": "instant",
                "unit": "个",
                "thresholds": {"warning": 1}
            },
            "high_memory_nodes": {
                "query": 'count(mem_used_percent{{cluster="{cluster}"}} > {memory_threshold})',
                "type": "instant",
                "unit": "个",
                "thresholds": {"warning": 1}
            },
            "network_errors": {
                "query": 'sum(increase(net_err_in{{cluster="{cluster}"}}[{duration}]) + increase(net_err_out{{cluster="{cluster}"}}[{duration}]))',
                "type": "instant",
                "unit": "个",
                "thresholds": {"critical": 1000, "warning": 100}
            }
        }


class NightingaleClient:
    """夜莺监控客户端"""

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

    def query_instant(self, query: str, timestamp: int) -> Dict:
        """执行即时查询"""
        # url = f"{self.base_url}/api/v1/query"
        url = f"{self.base_url}/api/n9e/proxy/1/api/v1/query"
        params = {
            'query': query,
            'time': timestamp
        }

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"即时查询失败: {query}, 错误: {e}")
            return {}

    def query_range(self, query: str, start_time: int, end_time: int, step: str = "1m") -> Dict:
        """执行范围查询"""
        # url = f"{self.base_url}/api/v1/query_range"
        url = f"{self.base_url}/api/n9e/proxy/1/api/v1/query_range"
        params = {
            'query': query,
            'start': start_time,
            'end': end_time,
            'step': step
        }

        try:
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"范围查询失败: {query}, 错误: {e}")
            return {}


class MetricsExtractor:
    """指标数据提取器"""

    @staticmethod
    def extract_max_with_time(result: Dict) -> Tuple[float, str]:
        """提取最大值和对应的时间戳"""
        max_value = 0.0
        max_time = "N/A"

        for series in result.get("data", {}).get("result", []):
            for timestamp, value in series.get("values", []):
                try:
                    val = float(value)
                    if val > max_value:
                        max_value = val
                        max_time = datetime.datetime.fromtimestamp(timestamp).strftime("%m-%d %H:%M")
                except (ValueError, TypeError):
                    continue

        return max_value, max_time

    @staticmethod
    def extract_min_with_time(result: Dict) -> Tuple[float, str]:
        """提取最小值和对应的时间戳"""
        min_value = float('inf')
        min_time = "N/A"

        for series in result.get("data", {}).get("result", []):
            for timestamp, value in series.get("values", []):
                try:
                    val = float(value)
                    if val < min_value:
                        min_value = val
                        min_time = datetime.datetime.fromtimestamp(timestamp).strftime("%m-%d %H:%M")
                except (ValueError, TypeError):
                    continue

        return min_value if min_value != float('inf') else 0.0, min_time

    @staticmethod
    def extract_avg(result: Dict) -> float:
        """提取平均值"""
        total_sum = 0.0
        count = 0

        for series in result.get("data", {}).get("result", []):
            for timestamp, value in series.get("values", []):
                try:
                    total_sum += float(value)
                    count += 1
                except (ValueError, TypeError):
                    continue

        return total_sum / count if count > 0 else 0.0

    @staticmethod
    def extract_instant_value(result: Dict) -> float:
        """提取即时值"""
        for series in result.get("data", {}).get("result", []):
            try:
                return float(series["value"][1])
            except (KeyError, ValueError, TypeError, IndexError):
                continue
        return 0.0


class NginxReportGenerator:
    """Nginx监控报告生成器"""

    def __init__(self, client: NightingaleClient, cpu_threshold: float = 85, memory_threshold: float = 90):
        self.client = client
        self.extractor = MetricsExtractor()
        self.cpu_threshold = cpu_threshold
        self.memory_threshold = memory_threshold
        self.metrics_config = MetricsConfig()

    def get_time_ranges(self, target_date: str, report_type: ReportType = ReportType.DAILY,
                        end_date: str = None) -> Dict[str, Tuple[int, int]]:
        """获取时间范围"""
        try:
            base_date = datetime.datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            logger.error(f"日期格式错误: {target_date}")
            raise ValueError(f"日期格式错误: {target_date}")

        ranges = {}

        if report_type == ReportType.CUSTOM and end_date:
            try:
                end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
                current_start = base_date.replace(hour=0, minute=0, second=0, microsecond=0)
                current_end = end_dt.replace(hour=23, minute=59, second=59, microsecond=0)
                ranges["current"] = (int(current_start.timestamp()), int(current_end.timestamp()))
            except ValueError:
                logger.error(f"结束日期格式错误: {end_date}")
                raise ValueError(f"结束日期格式错误: {end_date}")

        elif report_type == ReportType.DAILY:
            current_start = base_date.replace(hour=0, minute=0, second=0, microsecond=0)
            current_end = base_date.replace(hour=23, minute=59, second=59, microsecond=0)
            ranges["current"] = (int(current_start.timestamp()), int(current_end.timestamp()))

            prev_date = base_date - datetime.timedelta(days=1)
            prev_start = prev_date.replace(hour=0, minute=0, second=0, microsecond=0)
            prev_end = prev_date.replace(hour=23, minute=59, second=59, microsecond=0)
            ranges["previous"] = (int(prev_start.timestamp()), int(prev_end.timestamp()))

        elif report_type == ReportType.WEEKLY:
            monday = base_date - datetime.timedelta(days=base_date.weekday())
            current_start = monday.replace(hour=0, minute=0, second=0, microsecond=0)
            current_end = (monday + datetime.timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=0)
            ranges["current"] = (int(current_start.timestamp()), int(current_end.timestamp()))

            prev_monday = monday - datetime.timedelta(days=7)
            prev_start = prev_monday.replace(hour=0, minute=0, second=0, microsecond=0)
            prev_end = (prev_monday + datetime.timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=0)
            ranges["previous"] = (int(prev_start.timestamp()), int(prev_end.timestamp()))

        elif report_type == ReportType.MONTHLY:
            first_day = base_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if base_date.month == 12:
                next_month = first_day.replace(year=base_date.year + 1, month=1)
            else:
                next_month = first_day.replace(month=base_date.month + 1)
            last_day = next_month - datetime.timedelta(days=1)
            current_end = last_day.replace(hour=23, minute=59, second=59, microsecond=0)
            ranges["current"] = (int(first_day.timestamp()), int(current_end.timestamp()))

            if first_day.month == 1:
                prev_first = first_day.replace(year=first_day.year - 1, month=12, day=1)
            else:
                prev_first = first_day.replace(month=first_day.month - 1, day=1)
            prev_last = first_day - datetime.timedelta(days=1)
            prev_end = prev_last.replace(hour=23, minute=59, second=59, microsecond=0)
            ranges["previous"] = (int(prev_first.timestamp()), int(prev_end.timestamp()))

        return ranges

    def _execute_query(self, query_config: Dict[str, Any], cluster: str, start_time: int,
                       end_time: int, **kwargs) -> Dict:
        """执行查询"""
        duration = f"{end_time - start_time}s"
        query = query_config["query"].format(
            cluster=cluster,
            duration=duration,
            cpu_threshold=kwargs.get('cpu_threshold', self.cpu_threshold),
            memory_threshold=kwargs.get('memory_threshold', self.memory_threshold)
        )

        if query_config["type"] == "instant":
            return self.client.query_instant(query, end_time)
        else:
            return self.client.query_range(query, start_time, end_time)

    def collect_metrics_by_config(self, metrics_config: Dict[str, Dict[str, Any]],
                                  cluster: str, start_time: int, end_time: int) -> Dict[str, MetricResult]:
        """根据配置收集指标"""
        metrics = {}

        for metric_name, config in metrics_config.items():
            try:
                result = self._execute_query(config, cluster, start_time, end_time)

                if config["type"] == "instant":
                    value = self.extractor.extract_instant_value(result)
                    metrics[metric_name] = MetricResult(
                        value=max(0, value),
                        unit=config["unit"],
                        status=self._get_status(value, config.get("thresholds", {}))
                    )
                else:
                    # 范围查询，提取平均值、峰值等
                    avg_value = self.extractor.extract_avg(result)
                    peak_value, peak_time = self.extractor.extract_max_with_time(result)
                    min_value, min_time = self.extractor.extract_min_with_time(result)

                    metrics[f"{metric_name}_avg"] = MetricResult(
                        value=max(0, avg_value),
                        unit=config["unit"],
                        status=self._get_status(avg_value, config.get("thresholds", {}))
                    )
                    metrics[f"{metric_name}_peak"] = MetricResult(
                        value=max(0, peak_value),
                        unit=config["unit"],
                        timestamp=peak_time
                    )
                    metrics[f"{metric_name}_min"] = MetricResult(
                        value=max(0, min_value),
                        unit=config["unit"],
                        timestamp=min_time
                    )

            except Exception as e:
                logger.error(f"收集指标 {metric_name} 失败: {e}")
                metrics[metric_name] = MetricResult(value=0.0, unit=config["unit"])

        return metrics

    def _get_status(self, value: float, thresholds: Dict[str, float]) -> str:
        """根据阈值判断状态"""
        if not thresholds:
            return "normal"

        if "critical" in thresholds and value >= thresholds["critical"]:
            return "critical"
        elif "warning" in thresholds and value >= thresholds["warning"]:
            return "warning"
        else:
            return "normal"

    def generate_report(self, target_date: str, clusters: List[str],
                        report_type: ReportType = ReportType.DAILY,
                        end_date: str = None, include_comparison: bool = True) -> List[str]:
        """生成监控报告"""
        logger.info(f"🚀 开始生成 {target_date} 的{report_type.value}监控报告...")

        time_ranges = self.get_time_ranges(target_date, report_type, end_date)
        current_start, current_end = time_ranges["current"]

        cluster_reports = []

        for cluster in clusters:
            logger.info(f"📊 处理集群: {cluster}")

            # 收集各类指标
            core_metrics = self.collect_metrics_by_config(
                self.metrics_config.get_core_metrics_queries(),
                cluster, current_start, current_end
            )

            system_metrics = self.collect_metrics_by_config(
                self.metrics_config.get_system_metrics_queries(),
                cluster, current_start, current_end
            )

            connection_metrics = self.collect_metrics_by_config(
                self.metrics_config.get_connection_metrics_queries(),
                cluster, current_start, current_end
            )

            anomaly_metrics = self.collect_metrics_by_config(
                self.metrics_config.get_anomaly_metrics_queries(),
                cluster, current_start, current_end
            )

            # 对比数据
            comparison_data = {}
            if include_comparison and "previous" in time_ranges:
                prev_start, prev_end = time_ranges["previous"]
                prev_core = self.collect_metrics_by_config(
                    self.metrics_config.get_core_metrics_queries(),
                    cluster, prev_start, prev_end
                )
                prev_system = self.collect_metrics_by_config(
                    self.metrics_config.get_system_metrics_queries(),
                    cluster, prev_start, prev_end
                )

                comparison_data = self._calculate_comparisons(core_metrics, system_metrics,
                                                              prev_core, prev_system)

            # 格式化报告
            cluster_report = self._format_cluster_report(
                target_date, report_type, cluster,
                core_metrics, system_metrics, connection_metrics, anomaly_metrics,
                comparison_data
            )

            cluster_reports.append(cluster_report)

        logger.info(f"✅ {target_date} {report_type.value}监控报告生成完成，共生成{len(cluster_reports)}个集群报告")
        return cluster_reports

    def _calculate_comparisons(self, current_core: Dict, current_system: Dict,
                               prev_core: Dict, prev_system: Dict) -> Dict:
        """计算对比数据"""
        comparisons = {}

        compare_pairs = [
            ("total_requests", "total_requests"),
            ("traffic_in", "traffic_in"),
            ("qps_peak", "qps_peak"),
            ("cpu_usage_avg", "cpu_usage_avg"),
            ("memory_usage_avg", "memory_usage_avg")
        ]

        for current_key, prev_key in compare_pairs:
            current_val = 0
            prev_val = 0

            # 从当前数据中获取值
            if current_key in current_core:
                current_val = current_core[current_key].value
            elif current_key in current_system:
                current_val = current_system[current_key].value

            # 从历史数据中获取值
            if prev_key in prev_core:
                prev_val = prev_core[prev_key].value
            elif prev_key in prev_system:
                prev_val = prev_system[prev_key].value

            comparisons[current_key] = self.calculate_comparison(current_val, prev_val)

        return comparisons

    def calculate_comparison(self, current: float, previous: float) -> Tuple[float, str]:
        """计算对比变化"""
        if previous == 0:
            if current == 0:
                return 0.0, "持平"
            else:
                return 100.0, "新增"

        change_ratio = ((current - previous) / previous) * 100

        if abs(change_ratio) < 0.1:
            trend = "持平"
        elif change_ratio > 0:
            trend = f"上升{abs(change_ratio):.1f}%"
        else:
            trend = f"下降{abs(change_ratio):.1f}%"

        return change_ratio, trend

    def _format_cluster_report(self, target_date: str, report_type: ReportType, cluster: str,
                               core_metrics: Dict, system_metrics: Dict, connection_metrics: Dict,
                               anomaly_metrics: Dict, comparison_data: Dict) -> str:
        """格式化集群报告"""
        report_lines = []

        # 报告头部
        report_lines.append(f"# {cluster} 集群 {target_date} {report_type.value}监控报告")
        report_lines.append("=" * 60)
        report_lines.append("")

        # 核心业务指标
        report_lines.append("## 📊 核心业务指标")
        report_lines.append("")

        if "availability" in core_metrics:
            availability = core_metrics["availability"]
            status_icon = "🟢" if availability.status == "normal" else "🟡" if availability.status == "warning" else "🔴"
            report_lines.append(f"**服务可用性**: {status_icon} {availability.value:.2f}%")

        if "total_requests" in core_metrics:
            requests = core_metrics["total_requests"]
            comparison = comparison_data.get("total_requests", (0, ""))
            report_lines.append(f"**总请求数**: {self.format_requests(requests.value)} ({comparison[1]})")

        if "qps_peak" in core_metrics:
            qps = core_metrics["qps_peak"]
            comparison = comparison_data.get("qps_peak", (0, ""))
            report_lines.append(f"**峰值QPS**: {qps.value:.2f} {qps.unit} (时间: {qps.timestamp}) ({comparison[1]})")

        # 系统资源指标
        report_lines.append("")
        report_lines.append("## 🖥️ 系统资源指标")
        report_lines.append("")

        if "cpu_usage_avg" in system_metrics:
            cpu = system_metrics["cpu_usage_avg"]
            cpu_peak = system_metrics.get("cpu_usage_peak", MetricResult(0, ""))
            status_icon = "🟢" if cpu.status == "normal" else "🟡" if cpu.status == "warning" else "🔴"
            comparison = comparison_data.get("cpu_usage_avg", (0, ""))
            report_lines.append(
                f"**CPU使用率**: {status_icon} 平均{cpu.value:.1f}%, 峰值{cpu_peak.value:.1f}% (时间: {cpu_peak.timestamp}) ({comparison[1]})")

        if "memory_usage_avg" in system_metrics:
            memory = system_metrics["memory_usage_avg"]
            memory_peak = system_metrics.get("memory_usage_peak", MetricResult(0, ""))
            comparison = comparison_data.get("memory_usage_avg", (0, ""))
            status_icon = "🟢" if memory.status == "normal" else "🟡" if memory.status == "warning" else "🔴"
            report_lines.append(
                f"**内存使用率**: {status_icon} 平均{memory.value:.1f}%, 峰值{memory_peak.value:.1f}% (时间: {memory_peak.timestamp}) ({comparison[1]})")

        # 连接状态指标
        if any(key.startswith("conn_") for key in connection_metrics.keys()):
            report_lines.append("")
            report_lines.append("## 🔗 连接状态指标")
            report_lines.append("")

            for conn_type in ["active", "waiting", "reading", "writing"]:
                avg_key = f"conn_{conn_type}_avg"
                peak_key = f"conn_{conn_type}_peak"
                if avg_key in connection_metrics:
                    avg_conn = connection_metrics[avg_key]
                    peak_conn = connection_metrics.get(peak_key, MetricResult(0, ""))
                    report_lines.append(
                        f"**{conn_type.title()}连接**: 平均{avg_conn.value:.0f}个, 峰值{peak_conn.value:.0f}个 (时间: {peak_conn.timestamp})")

        # 异常事件
        if any(metric.value > 0 for metric in anomaly_metrics.values()):
            report_lines.append("")
            report_lines.append("## ⚠️ 异常事件")
            report_lines.append("")

            for metric_name, metric in anomaly_metrics.items():
                if metric.value > 0:
                    status_icon = "🔴" if metric.status == "critical" else "🟡"
                    report_lines.append(f"**{metric_name}**: {status_icon} {metric.value:.0f}{metric.unit}")

        report_lines.append("")
        report_lines.append("---")
        report_lines.append(f"报告生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")

        return "\n".join(report_lines)

    def export_to_excel(self, target_date: str, clusters: List[str],
                        report_type: ReportType, end_date: str = None,
                        output_path: str = "monitoring_report.xlsx") -> bool:
        """导出Excel报告"""
        try:
            time_ranges = self.get_time_ranges(target_date, report_type, end_date)
            current_start, current_end = time_ranges["current"]

            all_data = []

            for cluster in clusters:
                logger.info(f"📊 导出集群 {cluster} 的Excel数据...")

                # 收集各类指标
                all_metrics = {}
                all_metrics.update(self.collect_metrics_by_config(
                    self.metrics_config.get_core_metrics_queries(),
                    cluster, current_start, current_end
                ))
                all_metrics.update(self.collect_metrics_by_config(
                    self.metrics_config.get_system_metrics_queries(),
                    cluster, current_start, current_end
                ))
                all_metrics.update(self.collect_metrics_by_config(
                    self.metrics_config.get_connection_metrics_queries(),
                    cluster, current_start, current_end
                ))
                all_metrics.update(self.collect_metrics_by_config(
                    self.metrics_config.get_anomaly_metrics_queries(),
                    cluster, current_start, current_end
                ))

                # 格式化数据为Excel格式
                for metric_name, metric in all_metrics.items():
                    row_data = {
                        '日期': target_date,
                        '集群': cluster,
                        '指标名称': metric_name,
                        '数值': metric.value,
                        '单位': metric.unit,
                        '状态': metric.status,
                        '时间戳': metric.timestamp if metric.timestamp else '',
                        '报告类型': report_type.value
                    }
                    all_data.append(row_data)

            # 创建DataFrame并导出Excel
            df = pd.DataFrame(all_data)

            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # 原始数据表
                df.to_excel(writer, sheet_name='监控数据', index=False)

                # 统计汇总表
                summary_data = []
                for cluster in clusters:
                    cluster_data = df[df['集群'] == cluster]
                    for metric_name in cluster_data['指标名称'].unique():
                        metric_data = cluster_data[cluster_data['指标名称'] == metric_name]
                        if not metric_data.empty:
                            values = metric_data['数值'].dropna()
                            if not values.empty:
                                summary_row = {
                                    '集群': cluster,
                                    '指标名称': metric_name,
                                    '最大值': values.max(),
                                    '最小值': values.min(),
                                    '平均值': values.mean(),
                                    '总和': values.sum(),
                                    '单位': metric_data['单位'].iloc[0] if not metric_data['单位'].empty else ''
                                }
                                summary_data.append(summary_row)

                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='统计汇总', index=False)

            logger.info(f"✅ Excel报告已导出到: {output_path}")
            return True

        except Exception as e:
            logger.error(f"❌ Excel导出失败: {e}")
            return False

    @staticmethod
    def format_requests(value: float) -> str:
        """格式化请求数"""
        if value >= 1000000000:
            return f"{value / 1000000000:.2f}G"
        elif value >= 1000000:
            return f"{value / 1000000:.2f}M"
        elif value >= 1000:
            return f"{value / 1000:.2f}K"
        else:
            return f"{value:.0f}"


class NginxMonitoringGUI:
    """Nginx监控系统GUI界面"""

    def __init__(self, root):
        self.root = root
        self.root.title("Nginx监控报告生成系统")
        self.root.geometry("800x700")
        self.root.resizable(True, True)

        # 配置样式
        self.setup_styles()

        # 初始化变量
        self.init_variables()

        # 创建界面
        self.create_widgets()

        # 状态
        self.client = None
        self.generator = None
        self.report_queue = queue.Queue()

    def setup_styles(self):
        """配置界面样式"""
        style = ttk.Style()
        style.theme_use('clam')

        # 配置颜色
        style.configure('Title.TLabel', font=('Arial', 12, 'bold'))
        style.configure('Section.TLabel', font=('Arial', 10, 'bold'))
        style.configure('Success.TLabel', foreground='green')
        style.configure('Error.TLabel', foreground='red')

    def init_variables(self):
        """初始化界面变量"""
        # 连接配置
        self.url_var = tk.StringVar(value="http://localhost:8080")
        self.username_var = tk.StringVar(value="admin")
        self.password_var = tk.StringVar(value="")
        self.datasource_var = tk.StringVar(value="prometheus")

        # 报告配置
        self.report_type_var = tk.StringVar(value="日报")
        self.start_date_var = tk.StringVar(value=datetime.datetime.now().strftime("%Y-%m-%d"))
        self.end_date_var = tk.StringVar(value=datetime.datetime.now().strftime("%Y-%m-%d"))
        self.clusters_var = tk.StringVar(value="default")

        # 阈值配置
        self.cpu_threshold_var = tk.DoubleVar(value=85.0)
        self.memory_threshold_var = tk.DoubleVar(value=90.0)

        # 输出配置
        self.output_text_var = tk.BooleanVar(value=True)
        self.output_excel_var = tk.BooleanVar(value=True)
        self.output_path_var = tk.StringVar(value="./reports/")

        # 状态
        self.status_var = tk.StringVar(value="就绪")

    def create_widgets(self):
        """创建界面组件"""
        # 主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 配置权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)

        current_row = 0

        # 标题
        title_label = ttk.Label(main_frame, text="Nginx监控报告生成系统", style='Title.TLabel')
        title_label.grid(row=current_row, column=0, columnspan=3, pady=(0, 20))
        current_row += 1

        # 连接配置区域
        self.create_connection_section(main_frame, current_row)
        current_row += 6

        # 报告配置区域
        self.create_report_section(main_frame, current_row)
        current_row += 8

        # 阈值配置区域
        self.create_threshold_section(main_frame, current_row)
        current_row += 3

        # 输出配置区域
        self.create_output_section(main_frame, current_row)
        current_row += 4

        # 操作按钮区域
        self.create_action_section(main_frame, current_row)
        current_row += 2

        # 状态栏
        self.create_status_section(main_frame, current_row)

    def create_connection_section(self, parent, start_row):
        """创建连接配置区域"""
        # 标题
        conn_label = ttk.Label(parent, text="🔗 连接配置", style='Section.TLabel')
        conn_label.grid(row=start_row, column=0, columnspan=3, sticky=tk.W, pady=(10, 5))

        # 服务器地址
        ttk.Label(parent, text="服务器地址:").grid(row=start_row + 1, column=0, sticky=tk.W, pady=2)
        url_entry = ttk.Entry(parent, textvariable=self.url_var, width=40)
        url_entry.grid(row=start_row + 1, column=1, sticky=(tk.W, tk.E), pady=2, padx=(5, 0))

        # 用户名
        ttk.Label(parent, text="用户名:").grid(row=start_row + 2, column=0, sticky=tk.W, pady=2)
        username_entry = ttk.Entry(parent, textvariable=self.username_var, width=40)
        username_entry.grid(row=start_row + 2, column=1, sticky=(tk.W, tk.E), pady=2, padx=(5, 0))

        # 密码
        ttk.Label(parent, text="密码:").grid(row=start_row + 3, column=0, sticky=tk.W, pady=2)
        password_entry = ttk.Entry(parent, textvariable=self.password_var, show="*", width=40)
        password_entry.grid(row=start_row + 3, column=1, sticky=(tk.W, tk.E), pady=2, padx=(5, 0))

        # 数据源ID
        ttk.Label(parent, text="数据源ID:").grid(row=start_row + 4, column=0, sticky=tk.W, pady=2)
        datasource_entry = ttk.Entry(parent, textvariable=self.datasource_var, width=40)
        datasource_entry.grid(row=start_row + 4, column=1, sticky=(tk.W, tk.E), pady=2, padx=(5, 0))

        # 连接测试按钮
        test_btn = ttk.Button(parent, text="测试连接", command=self.test_connection)
        test_btn.grid(row=start_row + 1, column=2, rowspan=2, padx=(10, 0), pady=2)

    def create_report_section(self, parent, start_row):
        """创建报告配置区域"""
        # 标题
        report_label = ttk.Label(parent, text="📊 报告配置", style='Section.TLabel')
        report_label.grid(row=start_row, column=0, columnspan=3, sticky=tk.W, pady=(10, 5))

        # 报告类型
        ttk.Label(parent, text="报告类型:").grid(row=start_row + 1, column=0, sticky=tk.W, pady=2)
        report_type_combo = ttk.Combobox(parent, textvariable=self.report_type_var,
                                         values=["日报", "周报", "月报", "自定义"], state="readonly")
        report_type_combo.grid(row=start_row + 1, column=1, sticky=(tk.W, tk.E), pady=2, padx=(5, 0))
        report_type_combo.bind('<<ComboboxSelected>>', self.on_report_type_changed)

        # 开始日期
        ttk.Label(parent, text="开始日期:").grid(row=start_row + 2, column=0, sticky=tk.W, pady=2)
        start_date_entry = ttk.Entry(parent, textvariable=self.start_date_var, width=40)
        start_date_entry.grid(row=start_row + 2, column=1, sticky=(tk.W, tk.E), pady=2, padx=(5, 0))

        # 结束日期（自定义时间段时显示）
        self.end_date_label = ttk.Label(parent, text="结束日期:")
        self.end_date_entry = ttk.Entry(parent, textvariable=self.end_date_var, width=40)

        # 集群选择
        ttk.Label(parent, text="集群列表:").grid(row=start_row + 4, column=0, sticky=tk.W, pady=2)
        clusters_entry = ttk.Entry(parent, textvariable=self.clusters_var, width=40)
        clusters_entry.grid(row=start_row + 4, column=1, sticky=(tk.W, tk.E), pady=2, padx=(5, 0))

        # 说明标签
        help_label = ttk.Label(parent, text="提示: 集群名称用逗号分隔，日期格式: YYYY-MM-DD",
                               font=('Arial', 8), foreground='gray')
        help_label.grid(row=start_row + 5, column=1, sticky=tk.W, pady=2, padx=(5, 0))

    def create_threshold_section(self, parent, start_row):
        """创建阈值配置区域"""
        # 标题
        threshold_label = ttk.Label(parent, text="⚠️ 阈值配置", style='Section.TLabel')
        threshold_label.grid(row=start_row, column=0, columnspan=3, sticky=tk.W, pady=(10, 5))

        # CPU阈值
        ttk.Label(parent, text="CPU告警阈值(%):").grid(row=start_row + 1, column=0, sticky=tk.W, pady=2)
        cpu_spinbox = ttk.Spinbox(parent, from_=0, to=100, textvariable=self.cpu_threshold_var,
                                  increment=5, width=10)
        cpu_spinbox.grid(row=start_row + 1, column=1, sticky=tk.W, pady=2, padx=(5, 0))

        # 内存阈值
        ttk.Label(parent, text="内存告警阈值(%):").grid(row=start_row + 2, column=0, sticky=tk.W, pady=2)
        memory_spinbox = ttk.Spinbox(parent, from_=0, to=100, textvariable=self.memory_threshold_var,
                                     increment=5, width=10)
        memory_spinbox.grid(row=start_row + 2, column=1, sticky=tk.W, pady=2, padx=(5, 0))

    def create_output_section(self, parent, start_row):
        """创建输出配置区域"""
        # 标题
        output_label = ttk.Label(parent, text="📁 输出配置", style='Section.TLabel')
        output_label.grid(row=start_row, column=0, columnspan=3, sticky=tk.W, pady=(10, 5))

        # 输出格式
        format_frame = ttk.Frame(parent)
        format_frame.grid(row=start_row + 1, column=1, sticky=(tk.W, tk.E), pady=2, padx=(5, 0))

        ttk.Label(parent, text="输出格式:").grid(row=start_row + 1, column=0, sticky=tk.W, pady=2)
        ttk.Checkbutton(format_frame, text="文本报告", variable=self.output_text_var).grid(row=0, column=0, sticky=tk.W)
        ttk.Checkbutton(format_frame, text="Excel报表", variable=self.output_excel_var).grid(row=0, column=1,
                                                                                             sticky=tk.W, padx=(20, 0))

        # 输出路径
        ttk.Label(parent, text="输出路径:").grid(row=start_row + 2, column=0, sticky=tk.W, pady=2)
        path_frame = ttk.Frame(parent)
        path_frame.grid(row=start_row + 2, column=1, sticky=(tk.W, tk.E), pady=2, padx=(5, 0))
        path_frame.columnconfigure(0, weight=1)

        path_entry = ttk.Entry(path_frame, textvariable=self.output_path_var)
        path_entry.grid(row=0, column=0, sticky=(tk.W, tk.E))

        browse_btn = ttk.Button(path_frame, text="浏览", command=self.browse_output_path)
        browse_btn.grid(row=0, column=1, padx=(5, 0))

    def create_action_section(self, parent, start_row):
        """创建操作按钮区域"""
        action_frame = ttk.Frame(parent)
        action_frame.grid(row=start_row, column=0, columnspan=3, pady=(20, 10))

        # 生成报告按钮
        generate_btn = ttk.Button(action_frame, text="🚀 生成报告",
                                  command=self.generate_report, style='Accent.TButton')
        generate_btn.pack(side=tk.LEFT, padx=(0, 10))

        # 停止按钮
        self.stop_btn = ttk.Button(action_frame, text="⏹️ 停止",
                                   command=self.stop_generation, state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT, padx=(0, 10))

        # 清空日志按钮
        clear_btn = ttk.Button(action_frame, text="🗑️ 清空日志", command=self.clear_log)
        clear_btn.pack(side=tk.LEFT)

    def create_status_section(self, parent, start_row):
        """创建状态栏区域"""
        status_frame = ttk.LabelFrame(parent, text="运行状态", padding="5")
        status_frame.grid(row=start_row, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(10, 0))
        status_frame.columnconfigure(0, weight=1)
        status_frame.rowconfigure(1, weight=1)

        # 状态标签
        status_label = ttk.Label(status_frame, textvariable=self.status_var)
        status_label.grid(row=0, column=0, sticky=tk.W, pady=(0, 5))

        # 进度条
        self.progress = ttk.Progressbar(status_frame, mode='indeterminate')
        self.progress.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(10, 0))

        # 日志文本框
        log_frame = ttk.Frame(status_frame)
        log_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(5, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(log_frame, height=8, wrap=tk.WORD)
        log_scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=log_scrollbar.set)

        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        log_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))

    def on_report_type_changed(self, event=None):
        """报告类型改变时的处理"""
        if self.report_type_var.get() == "自定义":
            # 显示结束日期输入
            self.end_date_label.grid(row=3, column=0, sticky=tk.W, pady=2)
            self.end_date_entry.grid(row=3, column=1, sticky=(tk.W, tk.E), pady=2, padx=(5, 0))
        else:
            # 隐藏结束日期输入
            self.end_date_label.grid_remove()
            self.end_date_entry.grid_remove()

    def browse_output_path(self):
        """浏览输出路径"""
        path = filedialog.askdirectory(initialdir=self.output_path_var.get())
        if path:
            self.output_path_var.set(path)

    def test_connection(self):
        """测试连接"""
        try:
            self.log_message("🔄 正在测试连接...")
            self.status_var.set("测试连接中...")
            self.progress.start()

            # 在后台线程中测试连接
            def test_worker():
                try:
                    client = NightingaleClient(
                        self.url_var.get().strip(),
                        self.username_var.get().strip(),
                        self.password_var.get().strip()
                    )

                    # 执行简单查询测试
                    result = client.query_instant("up", int(datetime.datetime.now().timestamp()))

                    if result and "data" in result:
                        self.root.after(0, lambda: self.on_connection_success())
                    else:
                        self.root.after(0, lambda: self.on_connection_error("连接成功但查询返回空结果"))

                except Exception as e:
                    self.root.after(0, lambda: self.on_connection_error(str(e)))

            threading.Thread(target=test_worker, daemon=True).start()

        except Exception as e:
            self.on_connection_error(f"连接测试失败: {e}")

    def on_connection_success(self):
        """连接成功回调"""
        self.progress.stop()
        self.status_var.set("连接测试成功")
        self.log_message("✅ 连接测试成功")
        messagebox.showinfo("连接测试", "连接测试成功!")

    def on_connection_error(self, error_msg):
        """连接错误回调"""
        self.progress.stop()
        self.status_var.set("连接测试失败")
        self.log_message(f"❌ 连接测试失败: {error_msg}")
        messagebox.showerror("连接测试", f"连接测试失败:\n{error_msg}")

    def generate_report(self):
        """生成报告"""
        try:
            # 验证输入
            if not self.validate_inputs():
                return

            self.log_message("🚀 开始生成监控报告...")
            self.status_var.set("正在生成报告...")
            self.progress.start()
            self.stop_btn.config(state=tk.NORMAL)

            # 在后台线程中生成报告
            def generate_worker():
                try:
                    # 创建客户端和生成器
                    client = NightingaleClient(
                        self.url_var.get().strip(),
                        self.username_var.get().strip(),
                        self.password_var.get().strip()
                    )

                    generator = NginxReportGenerator(
                        client,
                        self.cpu_threshold_var.get(),
                        self.memory_threshold_var.get()
                    )

                    # 解析参数
                    clusters = [c.strip() for c in self.clusters_var.get().split(',') if c.strip()]
                    report_type = ReportType(self.report_type_var.get())
                    start_date = self.start_date_var.get()
                    end_date = self.end_date_var.get() if report_type == ReportType.CUSTOM else None

                    # 生成文本报告
                    if self.output_text_var.get():
                        self.root.after(0, lambda: self.log_message("📝 生成文本报告..."))
                        reports = generator.generate_report(
                            start_date, clusters, report_type, end_date
                        )

                        # 保存文本报告
                        output_dir = self.output_path_var.get().strip()
                        if not os.path.exists(output_dir):
                            os.makedirs(output_dir)

                        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                        text_file = os.path.join(output_dir, f"monitoring_report_{timestamp}.txt")

                        with open(text_file, 'w', encoding='utf-8') as f:
                            f.write("\n\n".join(reports))

                        self.root.after(0, lambda: self.log_message(f"✅ 文本报告已保存: {text_file}"))

                    # 生成Excel报告
                    if self.output_excel_var.get():
                        self.root.after(0, lambda: self.log_message("📊 生成Excel报告..."))
                        output_dir = self.output_path_var.get().strip()
                        if not os.path.exists(output_dir):
                            os.makedirs(output_dir)

                        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                        excel_file = os.path.join(output_dir, f"monitoring_report_{timestamp}.xlsx")

                        success = generator.export_to_excel(
                            start_date, clusters, report_type, end_date, excel_file
                        )

                        if success:
                            self.root.after(0, lambda: self.log_message(f"✅ Excel报告已保存: {excel_file}"))
                        else:
                            self.root.after(0, lambda: self.log_message("❌ Excel报告生成失败"))

                    self.root.after(0, lambda: self.on_generation_complete())

                except Exception as e:
                    self.root.after(0, lambda: self.on_generation_error(str(e)))

            threading.Thread(target=generate_worker, daemon=True).start()

        except Exception as e:
            self.on_generation_error(f"报告生成失败: {e}")

    def validate_inputs(self):
        """验证输入参数"""
        if not self.url_var.get().strip():
            messagebox.showerror("输入错误", "请输入服务器地址")
            return False

        if not self.username_var.get().strip():
            messagebox.showerror("输入错误", "请输入用户名")
            return False

        if not self.clusters_var.get().strip():
            messagebox.showerror("输入错误", "请输入集群列表")
            return False

        # 验证日期格式
        try:
            datetime.datetime.strptime(self.start_date_var.get(), "%Y-%m-%d")
            if self.report_type_var.get() == "自定义":
                datetime.datetime.strptime(self.end_date_var.get(), "%Y-%m-%d")
        except ValueError:
            messagebox.showerror("输入错误", "日期格式错误，请使用 YYYY-MM-DD 格式")
            return False

        if not self.output_text_var.get() and not self.output_excel_var.get():
            messagebox.showerror("输入错误", "请至少选择一种输出格式")
            return False

        return True

    def on_generation_complete(self):
        """报告生成完成回调"""
        self.progress.stop()
        self.status_var.set("报告生成完成")
        self.stop_btn.config(state=tk.DISABLED)
        self.log_message("🎉 报告生成完成!")
        messagebox.showinfo("生成完成", "监控报告生成完成!")

    def on_generation_error(self, error_msg):
        """报告生成错误回调"""
        self.progress.stop()
        self.status_var.set("报告生成失败")
        self.stop_btn.config(state=tk.DISABLED)
        self.log_message(f"❌ 报告生成失败: {error_msg}")
        messagebox.showerror("生成失败", f"报告生成失败:\n{error_msg}")

    def stop_generation(self):
        """停止报告生成"""
        self.progress.stop()
        self.status_var.set("操作已停止")
        self.stop_btn.config(state=tk.DISABLED)
        self.log_message("⏹️ 操作已停止")

    def clear_log(self):
        """清空日志"""
        self.log_text.delete(1.0, tk.END)
        self.log_message("日志已清空")

    def log_message(self, message):
        """添加日志消息"""
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"

        self.log_text.insert(tk.END, log_entry)
        self.log_text.see(tk.END)

        # 限制日志行数
        lines = self.log_text.get(1.0, tk.END).split('\n')
        if len(lines) > 100:
            self.log_text.delete(1.0, f"{len(lines) - 100}.0")


def main():
    """主函数 - 启动GUI应用程序"""
    try:
        # 创建主窗口
        root = tk.Tk()

        # 尝试设置图标（如果存在的话）
        try:
            root.iconbitmap('icon.ico')
        except:
            # 如果图标文件不存在，忽略错误
            pass

        # 创建应用程序实例
        app = NginxMonitoringGUI(root)

        # 设置窗口关闭时的处理函数
        def on_closing():
            """窗口关闭时的清理工作"""
            try:
                # 停止进度条（如果正在运行）
                app.progress.stop()

                # 记录关闭日志
                app.log_message("🔄 正在关闭应用程序...")

                # 确保所有线程结束
                root.quit()  # 退出主循环
                root.destroy()  # 销毁窗口

            except Exception as e:
                print(f"关闭应用程序时出错: {e}")
                root.destroy()

        # 绑定窗口关闭事件
        root.protocol("WM_DELETE_WINDOW", on_closing)

        # 设置窗口最小尺寸
        root.minsize(600, 500)

        # 居中显示窗口
        center_window(root)

        # 启动应用程序日志
        app.log_message("🚀 Nginx监控报告生成系统已启动")
        app.log_message("📝 请先配置连接参数并测试连接")

        # 启动主事件循环
        root.mainloop()

    except Exception as e:
        # 如果启动失败，显示错误信息
        import traceback
        error_msg = f"启动应用程序失败: {e}\n\n详细错误:\n{traceback.format_exc()}"
        print(error_msg)

        # 尝试显示错误对话框
        try:
            import tkinter.messagebox as messagebox
            messagebox.showerror("启动错误", error_msg)
        except:
            pass


def center_window(window):
    """将窗口居中显示"""
    try:
        # 更新窗口以获取实际尺寸
        window.update_idletasks()

        # 获取窗口尺寸
        width = window.winfo_width()
        height = window.winfo_height()

        # 获取屏幕尺寸
        screen_width = window.winfo_screenwidth()
        screen_height = window.winfo_screenheight()

        # 计算居中位置
        x = (screen_width - width) // 2
        y = (screen_height - height) // 2

        # 设置窗口位置
        window.geometry(f"{width}x{height}+{x}+{y}")

    except Exception as e:
        print(f"窗口居中失败: {e}")


def setup_logging():
    """设置日志配置"""
    import logging

    # 创建日志目录
    log_dir = "./logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 配置日志格式
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # 设置日志文件名（包含日期）
    log_filename = os.path.join(log_dir, f"nginx_monitoring_{datetime.datetime.now().strftime('%Y%m%d')}.log")

    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()  # 同时输出到控制台
        ]
    )

    return logging.getLogger(__name__)


if __name__ == "__main__":
    """程序入口点"""


    # 设置全局异常处理
    def handle_exception(exc_type, exc_value, exc_traceback):
        """全局异常处理函数"""
        if issubclass(exc_type, KeyboardInterrupt):
            # 处理 Ctrl+C 中断
            print("\n程序被用户中断")
            sys.exit(0)

        # 记录其他异常
        import traceback
        error_msg = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        print(f"未处理的异常: {error_msg}")

        # 尝试写入日志文件
        try:
            with open("error.log", "a", encoding="utf-8") as f:
                f.write(f"\n[{datetime.datetime.now()}] 未处理的异常:\n{error_msg}\n")
        except:
            pass


    # 导入必要的模块
    import sys
    import os

    # 设置异常处理
    sys.excepthook = handle_exception

    try:
        # 设置日志
        logger.info("=" * 50)
        logger.info("Nginx监控报告生成系统启动")
        logger.info("=" * 50)

        # 检查Python版本
        if sys.version_info < (3, 6):
            error_msg = "此程序需要Python 3.6或更高版本"
            print(error_msg)
            logger.error(error_msg)
            sys.exit(1)

        # 检查必要的模块
        required_modules = [
            'tkinter', 'requests', 'pandas', 'openpyxl',
            'datetime', 'json', 'threading', 'queue'
        ]

        missing_modules = []
        for module in required_modules:
            try:
                __import__(module)
            except ImportError:
                missing_modules.append(module)

        if missing_modules:
            error_msg = f"缺少必要的Python模块: {', '.join(missing_modules)}"
            print(error_msg)
            print("请使用以下命令安装:")
            print(f"pip install {' '.join(missing_modules)}")
            logger.error(error_msg)
            sys.exit(1)

        # 创建必要的目录
        directories = ["./reports", "./logs", "./temp"]
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory)
                logger.info(f"创建目录: {directory}")

        # 启动主程序
        logger.info("启动GUI界面...")
        main()

    except KeyboardInterrupt:
        print("\n程序被用户中断")
        logger.info("程序被用户中断")
        sys.exit(0)

    except Exception as e:
        error_msg = f"程序启动失败: {e}"
        print(error_msg)
        try:
            logger.error(error_msg, exc_info=True)
        except:
            pass
        sys.exit(1)

    finally:
        try:
            logger.info("程序正常退出")
            logger.info("=" * 50)
        except:
            pass