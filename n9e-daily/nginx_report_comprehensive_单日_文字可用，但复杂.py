#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nginx集群监控日报自动化生成器 - 综合版
基于夜莺(Nightingale) v8.0.0监控数据生成全面的系统运行汇报

功能特性:
- 支持多集群监控统计
- 自动生成日报/周报/月报
- 同比环比数据对比
- 面向技术和非技术领导的双重汇报格式
- 异常事件和告警信息统计
- 灵活的日期选择和输出格式
"""

import requests
import json
import datetime
import argparse
from typing import Dict, List, Optional, Tuple, Any
import sys
import logging
from dataclasses import dataclass
from enum import Enum

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('../nginx_report.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class ReportType(Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


@dataclass
class MetricResult:
    """监控指标结果数据类"""
    value: float
    timestamp: Optional[int] = None
    unit: str = ""
    status: str = "normal"  # normal, warning, critical


class NightingaleClient:
    """夜莺API客户端"""

    def __init__(self, base_url: str, datasource_id: int, username: str = None, password: str = None):
        """
        初始化夜莺客户端

        Args:
            base_url: 夜莺服务地址
            datasource_id: Prometheus数据源ID
            username: 用户名（可选）
            password: 密码（可选）
        """
        self.base_url = base_url.rstrip('/')
        self.datasource_id = datasource_id
        self.session = requests.Session()

        # 设置默认超时和重试
        self.session.timeout = 30

        # 如果提供认证信息则登录
        if username and password:
            self._authenticate(username, password)

    def _authenticate(self, username: str, password: str) -> bool:
        """用户认证登录"""
        login_url = f"{self.base_url}/api/n9e/auth/login"
        login_data = {"username": username, "password": password}

        try:
            response = self.session.post(login_url, json=login_data, timeout=10)
            if response.status_code == 200:
                return True
            else:
                return False
        except Exception as e:
            return False

    def query_range(self, query: str, start_time: int, end_time: int, step: str = "60s") -> Dict:
        """区间查询"""
        url = f"{self.base_url}/api/n9e/proxy/1/api/v1/query_range"
        params = {
            "query": query,
            "start": start_time,
            "end": end_time,
            "step": step,
            "datasource_ids": [self.datasource_id]
        }

        try:
            response = self.session.get(url, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"查询失败: {query[:50]}... - {response.status_code}")
                return {"data": {"result": []}}
        except Exception as e:
            logger.error(f"查询异常: {query[:50]}... - {e}")
            return {"data": {"result": []}}

    def query_instant(self, query: str, timestamp: int = None) -> Dict:
        """即时查询"""
        url = f"{self.base_url}/api/n9e/proxy/1/api/v1/query"
        params = {
            "query": query,
            "datasource_ids": [self.datasource_id]
        }

        if timestamp:
            params["time"] = timestamp

        try:
            response = self.session.get(url, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"即时查询失败: {query[:50]}...")
                return {"data": {"result": []}}
        except Exception as e:
            logger.error(f"即时查询异常: {query[:50]}... - {e}")
            return {"data": {"result": []}}


class MetricsExtractor:
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
                        # 格式化时间为更详细的格式
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
    def extract_sum(result: Dict) -> float:
        """提取总和值"""
        total = 0.0
        for series in result.get("data", {}).get("result", []):
            values = series.get("values", [])
            if values:
                try:
                    total += float(values[-1][1])
                except (ValueError, TypeError, IndexError):
                    continue
        return total

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

    def __init__(self, client: NightingaleClient):
        self.client = client
        self.extractor = MetricsExtractor()

        # 默认集群配置（可通过参数覆盖）
        self.default_clusters = ["self-prod-nginx", "zgtapp-prod-nginx"]

    def get_time_ranges(self, target_date: str, report_type: ReportType = ReportType.DAILY) -> Dict[
        str, Tuple[int, int]]:
        """
        获取报告时间范围

        Returns:
            {
                "current": (start, end),     # 当前周期
                "previous": (start, end),    # 上一周期（用于环比）
                "same_period_last": (start, end)  # 同期（用于同比）
            }
        """
        try:
            base_date = datetime.datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            logger.error(f"日期格式错误: {target_date}")
            sys.exit(1)

        ranges = {}

        if report_type == ReportType.DAILY:
            # 当前日期
            current_start = base_date.replace(hour=0, minute=0, second=0, microsecond=0)
            current_end = base_date.replace(hour=23, minute=59, second=59, microsecond=0)
            ranges["current"] = (int(current_start.timestamp()), int(current_end.timestamp()))

            # 前一天（环比）
            prev_date = base_date - datetime.timedelta(days=1)
            prev_start = prev_date.replace(hour=0, minute=0, second=0, microsecond=0)
            prev_end = prev_date.replace(hour=23, minute=59, second=59, microsecond=0)
            ranges["previous"] = (int(prev_start.timestamp()), int(prev_end.timestamp()))

            # 上周同一天（同比）
            same_date = base_date - datetime.timedelta(days=7)
            same_start = same_date.replace(hour=0, minute=0, second=0, microsecond=0)
            same_end = same_date.replace(hour=23, minute=59, second=59, microsecond=0)
            ranges["same_period_last"] = (int(same_start.timestamp()), int(same_end.timestamp()))

        elif report_type == ReportType.WEEKLY:
            # 当前周（周一到周日）
            monday = base_date - datetime.timedelta(days=base_date.weekday())
            current_start = monday.replace(hour=0, minute=0, second=0, microsecond=0)
            current_end = (monday + datetime.timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=0)
            ranges["current"] = (int(current_start.timestamp()), int(current_end.timestamp()))

            # 前一周
            prev_monday = monday - datetime.timedelta(days=7)
            prev_start = prev_monday.replace(hour=0, minute=0, second=0, microsecond=0)
            prev_end = (prev_monday + datetime.timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=0)
            ranges["previous"] = (int(prev_start.timestamp()), int(prev_end.timestamp()))

            # 去年同期
            same_monday = monday - datetime.timedelta(days=365)
            same_start = same_monday.replace(hour=0, minute=0, second=0, microsecond=0)
            same_end = (same_monday + datetime.timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=0)
            ranges["same_period_last"] = (int(same_start.timestamp()), int(same_end.timestamp()))

        return ranges

    def collect_core_metrics(self, cluster: str, start_time: int, end_time: int) -> Dict[str, MetricResult]:
        duration = end_time - start_time
        metrics = {}

        logger.info(f"📊 收集集群 {cluster} 的核心业务指标...")

        # 可用性计算保持不变
        uptime_query = f'count(nginx_active{{cluster="{cluster}"}} > 0) / count(nginx_active{{cluster="{cluster}"}}) '
        uptime_result = self.client.query_instant(uptime_query, end_time)
        min_uptime = self.extractor.extract_instant_value(uptime_result)
        availability = min(99.99, 100.0 - (1.0 / max(min_uptime, 1)) * 100)
        metrics["availability"] = MetricResult(
            value=availability,
            unit="%",
            status="normal" if availability >= 99.9 else "warning" if availability >= 99.0 else "critical"
        )

        # 使用deriv()替代increase()来避免幽灵计数器重置
        total_requests_query = f'sum(deriv(nginx_requests{{cluster="{cluster}"}}[{duration}s])) * {duration}'
        requests_result = self.client.query_instant(total_requests_query, end_time)
        total_requests = max(0, self.extractor.extract_instant_value(requests_result))  # 确保非负值
        metrics["total_requests"] = MetricResult(value=total_requests, unit="次")

        # 使用deriv()替代irate()
        qps_query = f'sum(deriv(nginx_requests{{cluster="{cluster}"}}[2m]))'
        qps_result = self.client.query_range(qps_query, start_time, end_time)
        qps_peak, qps_time = self.extractor.extract_max_with_time(qps_result)
        metrics["qps_peak"] = MetricResult(value=max(0, qps_peak), unit="req/s", timestamp=qps_time)

        qps_min, qps_min_time = self.extractor.extract_min_with_time(qps_result)
        metrics["qps_min"] = MetricResult(value=max(0, qps_min), unit="req/s", timestamp=qps_min_time)

        qps_avg = self.extractor.extract_avg(qps_result)
        metrics["qps_avg"] = MetricResult(value=max(0, qps_avg), unit="req/s")

        # 网络带宽使用deriv()
        bandwidth_in_query = f'sum(deriv(net_bytes_recv{{cluster="{cluster}"}}[2m]) * 8) / 1000 / 1000 / 1000'
        bandwidth_out_query = f'sum(deriv(net_bytes_sent{{cluster="{cluster}"}}[2m]) * 8) / 1000 / 1000 / 1000'

        bandwidth_in_result = self.client.query_range(bandwidth_in_query, start_time, end_time)
        bandwidth_out_result = self.client.query_range(bandwidth_out_query, start_time, end_time)

        bandwidth_in_peak, bandwidth_in_time = self.extractor.extract_max_with_time(bandwidth_in_result)
        bandwidth_out_peak, bandwidth_out_time = self.extractor.extract_max_with_time(bandwidth_out_result)
        bandwidth_in_avg = self.extractor.extract_avg(bandwidth_in_result)
        bandwidth_out_avg = self.extractor.extract_avg(bandwidth_out_result)

        bandwidth_total_query = f'''sum(
            deriv(net_bytes_recv{{cluster="{cluster}"}}[2m]) * 8 + 
            deriv(net_bytes_sent{{cluster="{cluster}"}}[2m]) * 8
        ) / 1000 / 1000 / 1000'''
        bandwidth_total_result = self.client.query_range(bandwidth_total_query, start_time, end_time)
        bandwidth_peak, bandwidth_time = self.extractor.extract_max_with_time(bandwidth_total_result)
        bandwidth_avg = self.extractor.extract_avg(bandwidth_total_result)

        metrics["bandwidth_peak"] = MetricResult(value=max(0, bandwidth_peak), unit="Gbps", timestamp=bandwidth_time)
        metrics["bandwidth_avg"] = MetricResult(value=max(0, bandwidth_avg), unit="Gbps")
        metrics["bandwidth_in_peak"] = MetricResult(value=max(0, bandwidth_in_peak), unit="Gbps",
                                                    timestamp=bandwidth_in_time)
        metrics["bandwidth_out_peak"] = MetricResult(value=max(0, bandwidth_out_peak), unit="Gbps",
                                                     timestamp=bandwidth_out_time)
        metrics["bandwidth_in_avg"] = MetricResult(value=max(0, bandwidth_in_avg), unit="Gbps")
        metrics["bandwidth_out_avg"] = MetricResult(value=max(0, bandwidth_out_avg), unit="Gbps")

        # 流量统计使用deriv()
        traffic_in_query = f'sum(deriv(net_bytes_recv{{cluster="{cluster}"}}[{duration}s])) * {duration} / 1024 / 1024 / 1024 / 1024'
        traffic_out_query = f'sum(deriv(net_bytes_sent{{cluster="{cluster}"}}[{duration}s])) * {duration} / 1024 / 1024 / 1024 / 1024'

        traffic_in_result = self.client.query_instant(traffic_in_query, end_time)
        traffic_out_result = self.client.query_instant(traffic_out_query, end_time)

        traffic_in = max(0, self.extractor.extract_instant_value(traffic_in_result))
        traffic_out = max(0, self.extractor.extract_instant_value(traffic_out_result))
        total_traffic = traffic_in + traffic_out

        metrics["total_traffic"] = MetricResult(value=total_traffic, unit="TB")
        metrics["traffic_in"] = MetricResult(value=traffic_in, unit="TB")
        metrics["traffic_out"] = MetricResult(value=traffic_out, unit="TB")

        return metrics

    def collect_anomaly_metrics(self, cluster: str, start_time: int, end_time: int) -> Dict[str, MetricResult]:
        duration = end_time - start_time
        metrics = {}

        logger.info(f"⚠️ 收集集群 {cluster} 的异常事件指标...")

        # OOM事件统计使用deriv()替代increase()
        oom_query = f'sum(deriv(kernel_vmstat_oom_kill{{cluster="{cluster}"}}[{duration}s])) * {duration}'
        oom_result = self.client.query_instant(oom_query, end_time)
        oom_count = max(0, self.extractor.extract_instant_value(oom_result))
        metrics["oom_events"] = MetricResult(
            value=oom_count,
            unit="次",
            status="normal" if oom_count == 0 else "critical"
        )

        # 高CPU节点统计
        high_cpu_query = f'count(cpu_usage_active{{cluster="{cluster}"}} > 85)'
        high_cpu_result = self.client.query_instant(high_cpu_query, end_time)
        high_cpu_nodes = self.extractor.extract_instant_value(high_cpu_result)
        metrics["high_cpu_nodes"] = MetricResult(
            value=high_cpu_nodes,
            unit="个",
            status="normal" if high_cpu_nodes == 0 else "warning"
        )

        # 高内存节点统计
        high_mem_query = f'count(mem_used_percent{{cluster="{cluster}"}} > 90)'
        high_mem_result = self.client.query_instant(high_mem_query, end_time)
        high_mem_nodes = self.extractor.extract_instant_value(high_mem_result)
        metrics["high_memory_nodes"] = MetricResult(
            value=high_mem_nodes,
            unit="个",
            status="normal" if high_mem_nodes == 0 else "warning"
        )

        # 网络错误统计使用deriv()
        net_errors_query = f'sum(deriv(net_err_in{{cluster="{cluster}"}}[{duration}s]) + deriv(net_err_out{{cluster="{cluster}"}}[{duration}s])) * {duration}'
        net_errors_result = self.client.query_instant(net_errors_query, end_time)
        net_errors = max(0, self.extractor.extract_instant_value(net_errors_result))
        metrics["network_errors"] = MetricResult(
            value=net_errors,
            unit="个",
            status="normal" if net_errors < 100 else "warning" if net_errors < 1000 else "critical"
        )

        return metrics

    def collect_system_metrics(self, cluster: str, start_time: int, end_time: int) -> Dict[str, MetricResult]:
        duration = end_time - start_time
        metrics = {}

        logger.info(f"🖥️ 收集集群 {cluster} 的系统资源指标...")

        # CPU使用率
        cpu_query = f'avg(cpu_usage_active{{cluster="{cluster}"}})'
        cpu_result = self.client.query_range(cpu_query, start_time, end_time)
        avg_cpu = self.extractor.extract_avg(cpu_result)
        cpu_peak, cpu_peak_time = self.extractor.extract_max_with_time(cpu_result)
        cpu_min, cpu_min_time = self.extractor.extract_min_with_time(cpu_result)

        metrics["cpu_usage"] = MetricResult(
            value=avg_cpu,
            unit="%",
            status="normal" if avg_cpu < 70 else "warning" if avg_cpu < 85 else "critical"
        )
        metrics["cpu_peak"] = MetricResult(value=cpu_peak, unit="%", timestamp=cpu_peak_time)
        metrics["cpu_min"] = MetricResult(value=cpu_min, unit="%", timestamp=cpu_min_time)

        # 内存使用率
        memory_query = f'avg(mem_used_percent{{cluster="{cluster}"}})'
        memory_result = self.client.query_range(memory_query, start_time, end_time)
        avg_memory = self.extractor.extract_avg(memory_result)
        memory_peak, memory_peak_time = self.extractor.extract_max_with_time(memory_result)
        memory_min, memory_min_time = self.extractor.extract_min_with_time(memory_result)

        metrics["memory_usage"] = MetricResult(
            value=avg_memory,
            unit="%",
            status="normal" if avg_memory < 75 else "warning" if avg_memory < 90 else "critical"
        )
        metrics["memory_peak"] = MetricResult(value=memory_peak, unit="%", timestamp=memory_peak_time)
        metrics["memory_min"] = MetricResult(value=memory_min, unit="%", timestamp=memory_min_time)

        # 系统负载
        load_query = f'avg(system_load5{{cluster="{cluster}"}})'
        load_result = self.client.query_range(load_query, start_time, end_time)
        avg_load = self.extractor.extract_avg(load_result)
        load_peak, load_peak_time = self.extractor.extract_max_with_time(load_result)

        # 获取CPU核心数统计信息
        cpu_cores_query = f'system_n_cpus{{cluster="{cluster}"}}'
        cores_result = self.client.query_instant(cpu_cores_query, end_time)

        # 计算CPU核心数的统计信息
        cores_values = []
        if cores_result and 'data' in cores_result and 'result' in cores_result['data']:
            for item in cores_result['data']['result']:
                if 'value' in item and len(item['value']) > 1:
                    cores_values.append(float(item['value'][1]))

        if cores_values:
            total_cores = sum(cores_values)
            avg_cores = sum(cores_values) / len(cores_values)
            min_cores = min(cores_values)
            max_cores = max(cores_values)
            node_count = len(cores_values)
        else:
            total_cores = avg_cores = min_cores = max_cores = node_count = 0

        load_ratio = (avg_load / max(avg_cores, 1)) * 100

        metrics["system_load"] = MetricResult(
            value=avg_load,
            unit="",
            status="normal" if load_ratio < 70 else "warning" if load_ratio < 100 else "critical"
        )
        metrics["load_peak"] = MetricResult(value=load_peak, unit="", timestamp=load_peak_time)
        metrics["total_cpu_cores"] = MetricResult(value=total_cores, unit="核")
        metrics["avg_cpu_cores"] = MetricResult(value=avg_cores, unit="核")
        metrics["min_cpu_cores"] = MetricResult(value=min_cores, unit="核")
        metrics["max_cpu_cores"] = MetricResult(value=max_cores, unit="核")

        # 节点状态统计
        # 修改节点查询逻辑，使用更准确的指标
        online_nodes_query = f'count(up{{cluster="{cluster}", job=~".*node.*"}} == 1 or system_uptime{{cluster="{cluster}"}} > 0)'
        total_nodes_query = f'count(up{{cluster="{cluster}", job=~".*node.*"}} or system_uptime{{cluster="{cluster}"}})'

        online_nodes_result = self.client.query_instant(online_nodes_query, end_time)
        total_nodes_result = self.client.query_instant(total_nodes_query, end_time)

        online_nodes = self.extractor.extract_instant_value(online_nodes_result)
        total_nodes = self.extractor.extract_instant_value(total_nodes_result)

        # 如果查询结果为0，使用备用查询
        if total_nodes == 0:
            backup_query = f'count(group by (instance) ({{cluster="{cluster}"}}))'
            backup_result = self.client.query_instant(backup_query, end_time)
            total_nodes = max(1, self.extractor.extract_instant_value(backup_result))
            online_nodes = total_nodes  # 假设都在线，如果有更精确的指标可以替换

        availability_ratio = (online_nodes / max(total_nodes, 1)) * 100
        metrics["node_availability"] = MetricResult(
            value=availability_ratio,
            unit="%",
            status="normal" if availability_ratio >= 100 else "warning" if availability_ratio >= 90 else "critical"
        )
        metrics["online_nodes"] = MetricResult(value=online_nodes, unit="个")
        metrics["total_nodes"] = MetricResult(value=total_nodes, unit="个")
        metrics["node_count"] = MetricResult(value=node_count, unit="台")

        return metrics

    def format_number(self, value: float, precision: int = 2) -> str:
        """改进的数字格式化函数，特别处理大数值"""
        if value >= 1e12:
            return f"{value / 1e12:.{precision}f}万亿"
        elif value >= 1e8:  # 1亿
            return f"{value / 1e8:.{precision}f}亿"
        elif value >= 1e4:  # 1万
            return f"{value / 1e4:.{precision}f}万"
        elif value >= 1e3:
            return f"{value / 1e3:.{precision}f}千"
        else:
            return f"{value:.{precision}f}"

    def format_requests(self, value: float, precision: int = 2) -> str:
        """专门用于格式化请求数的函数"""
        if value >= 1e8:  # 1亿次
            return f"{value / 1e8:.{precision}f}亿次"
        elif value >= 1e4:  # 1万次
            return f"{value / 1e4:.{precision}f}万次"
        elif value >= 1e3:  # 1千次
            return f"{value / 1e3:.{precision}f}千次"
        else:
            return f"{value:.{precision}f}次"

    def calculate_comparison(self, current: float, previous: float) -> Tuple[float, str]:
        """计算环比/同比变化"""
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

    def collect_connection_metrics(self, cluster: str, start_time: int, end_time: int) -> Dict[str, MetricResult]:
        """收集单个集群的连接状态指标"""
        duration = end_time - start_time
        metrics = {}

        logger.info(f"🔗 收集集群 {cluster} 的连接状态指标...")

        connection_types = ["active", "waiting", "reading", "writing"]
        for conn_type in connection_types:
            query = f'sum(nginx_{conn_type}{{cluster="{cluster}"}})'
            result = self.client.query_range(query, start_time, end_time)

            avg_conn = self.extractor.extract_avg(result)
            peak_conn, peak_time = self.extractor.extract_max_with_time(result)

            metrics[f"conn_{conn_type}"] = MetricResult(value=avg_conn, unit="个")
            metrics[f"conn_{conn_type}_peak"] = MetricResult(value=peak_conn, unit="个", timestamp=peak_time)

        # 计算总连接峰值
        total_conn_query = f'sum(nginx_active{{cluster="{cluster}"}} + nginx_waiting{{cluster="{cluster}"}})'
        total_conn_result = self.client.query_range(total_conn_query, start_time, end_time)
        total_conn_peak, total_conn_time = self.extractor.extract_max_with_time(total_conn_result)
        metrics["conn_total_peak"] = MetricResult(value=total_conn_peak, unit="个", timestamp=total_conn_time)

        return metrics

    def collect_anomaly_metrics(self, cluster: str, start_time: int, end_time: int) -> Dict[str, MetricResult]:
        """收集单个集群的异常事件指标"""
        duration = end_time - start_time
        metrics = {}

        logger.info(f"⚠️ 收集集群 {cluster} 的异常事件指标...")

        # OOM事件统计
        oom_query = f'sum(increase(kernel_vmstat_oom_kill{{cluster="{cluster}"}}[{duration}s]))'
        oom_result = self.client.query_instant(oom_query, end_time)
        oom_count = self.extractor.extract_instant_value(oom_result)
        metrics["oom_events"] = MetricResult(
            value=oom_count,
            unit="次",
            status="normal" if oom_count == 0 else "critical"
        )

        # 高CPU使用率节点统计
        high_cpu_query = f'count(cpu_usage_active{{cluster="{cluster}"}} > 85)'
        high_cpu_result = self.client.query_instant(high_cpu_query, end_time)
        high_cpu_nodes = self.extractor.extract_instant_value(high_cpu_result)
        metrics["high_cpu_nodes"] = MetricResult(
            value=high_cpu_nodes,
            unit="个",
            status="normal" if high_cpu_nodes == 0 else "warning"
        )

        # 高内存使用率节点统计
        high_mem_query = f'count(mem_used_percent{{cluster="{cluster}"}} > 90)'
        high_mem_result = self.client.query_instant(high_mem_query, end_time)
        high_mem_nodes = self.extractor.extract_instant_value(high_mem_result)
        metrics["high_memory_nodes"] = MetricResult(
            value=high_mem_nodes,
            unit="个",
            status="normal" if high_mem_nodes == 0 else "warning"
        )

        # 网络错误统计
        net_errors_query = f'sum(increase(net_err_in{{cluster="{cluster}"}}[{duration}s]) + increase(net_err_out{{cluster="{cluster}"}}[{duration}s]))'
        net_errors_result = self.client.query_instant(net_errors_query, end_time)
        net_errors = self.extractor.extract_instant_value(net_errors_result)
        metrics["network_errors"] = MetricResult(
            value=net_errors,
            unit="个",
            status="normal" if net_errors < 100 else "warning" if net_errors < 1000 else "critical"
        )

        return metrics

    def generate_report(self, target_date: str, clusters: List[str] = None,
                        report_type: ReportType = ReportType.DAILY,
                        include_comparison: bool = True) -> List[str]:  # 注意：返回类型改为List[str]
        """生成分集群报告，每个集群单独生成一个报告"""
        if not clusters:
            clusters = self.default_clusters

        logger.info(f"🚀 开始生成 {target_date} 的{report_type.value}监控报告...")
        time_ranges = self.get_time_ranges(target_date, report_type)
        current_start, current_end = time_ranges["current"]
        cluster_reports = []

        for cluster in clusters:
            logger.info(f"📊 处理集群: {cluster}")

            # 收集当前时段的指标
            core_metrics = self.collect_core_metrics(cluster, current_start, current_end)
            system_metrics = self.collect_system_metrics(cluster, current_start, current_end)
            connection_metrics = self.collect_connection_metrics(cluster, current_start, current_end)
            anomaly_metrics = self.collect_anomaly_metrics(cluster, current_start, current_end)

            # 收集对比数据
            comparison_data = {}
            if include_comparison:
                logger.info(f"📈 收集集群 {cluster} 的对比数据...")
                prev_start, prev_end = time_ranges["previous"]
                prev_core = self.collect_core_metrics(cluster, prev_start, prev_end)
                prev_system = self.collect_system_metrics(cluster, prev_start, prev_end)

                comparison_data = {
                    "requests": self.calculate_comparison(
                        core_metrics["total_requests"].value,
                        prev_core["total_requests"].value
                    ),
                    "traffic": self.calculate_comparison(
                        core_metrics["total_traffic"].value,
                        prev_core["total_traffic"].value
                    ),
                    "qps_peak": self.calculate_comparison(
                        core_metrics["qps_peak"].value,
                        prev_core["qps_peak"].value
                    ),
                    "cpu_avg": self.calculate_comparison(
                        system_metrics["cpu_usage"].value,
                        prev_system["cpu_usage"].value
                    ),
                    "memory_avg": self.calculate_comparison(
                        system_metrics["memory_usage"].value,
                        prev_system["memory_usage"].value
                    )
                }

            # 生成单个集群的报告
            cluster_report = self._format_cluster_report(
                target_date, report_type, cluster,
                core_metrics, system_metrics, connection_metrics, anomaly_metrics,
                comparison_data
            )

            cluster_reports.append(cluster_report)

        logger.info(f"✅ {target_date} {report_type.value}监控报告生成完成，共生成{len(cluster_reports)}个集群报告")
        return cluster_reports  # 返回报告列表，每个集群一个报告

    def _generate_cluster_recommendations(self, cluster: str, core_metrics: Dict, system_metrics: Dict,
                                          connection_metrics: Dict, anomaly_metrics: Dict) -> Dict[str, List[str]]:
        """为单个集群生成运维建议"""
        recommendations = {
            "性能优化": [],
            "资源管理": [],
            "安全加固": [],
            "容灾备份": []
        }

        # 性能优化建议
        if system_metrics["cpu_usage"].value > 70:
            recommendations["性能优化"].append(
                f"集群{cluster}: CPU使用率达到{system_metrics['cpu_usage'].value:.1f}%，建议优化应用性能或扩容")

        if system_metrics["memory_usage"].value > 75:
            recommendations["性能优化"].append(
                f"集群{cluster}: 内存使用率达到{system_metrics['memory_usage'].value:.1f}%，建议优化内存使用或增加内存")

        # 检查连接数峰值
        if connection_metrics["conn_active_peak"].value > 10000:
            recommendations["性能优化"].append(
                f"集群{cluster}: 活跃连接数峰值达到{int(connection_metrics['conn_active_peak'].value)}，建议优化连接池配置")

        # 资源管理建议
        if system_metrics["node_availability"].value < 100:
            offline_nodes = int(system_metrics["total_nodes"].value - system_metrics["online_nodes"].value)
            recommendations["资源管理"].append(f"集群{cluster}: 有{offline_nodes}个节点离线，请检查节点状态")

        if anomaly_metrics["oom_events"].value > 0:
            recommendations["资源管理"].append(
                f"集群{cluster}: 发生{int(anomaly_metrics['oom_events'].value)}次OOM事件，请检查内存配置")

        # 安全加固建议
        if anomaly_metrics["network_errors"].value > 1000:
            recommendations["安全加固"].append(
                f"集群{cluster}: 网络错误数量较高({int(anomaly_metrics['network_errors'].value)}个)，请检查网络安全")

        # 容灾备份建议
        if core_metrics["availability"].value < 99.9:
            recommendations["容灾备份"].append(
                f"集群{cluster}: 服务可用性为{core_metrics['availability'].value:.3f}%，建议完善容灾机制")

        # 如果没有特别的建议，给出通用建议
        if not any(recommendations.values()):
            recommendations["性能优化"].append(f"集群{cluster}: 系统运行状态良好，建议继续保持当前运维水平")
            recommendations["资源管理"].append(f"集群{cluster}: 资源使用合理，可考虑制定容量规划")
            recommendations["安全加固"].append(f"集群{cluster}: 建议定期进行安全检查和漏洞扫描")
            recommendations["容灾备份"].append(f"集群{cluster}: 建议定期演练容灾恢复流程")

        return recommendations

    def _format_cluster_report(self, date: str, report_type: ReportType, cluster: str,
                               core_metrics: Dict, system_metrics: Dict,
                               connection_metrics: Dict, anomaly_metrics: Dict,
                               comparison_data: Dict) -> str:
        type_names = {
            ReportType.DAILY: "日报",
            ReportType.WEEKLY: "周报",
            ReportType.MONTHLY: "月报"
        }

        # 计算告警统计
        warning_count = sum(1 for m in {**core_metrics, **system_metrics, **anomaly_metrics}.values()
                            if hasattr(m, 'status') and m.status == "warning")
        critical_count = sum(1 for m in {**core_metrics, **system_metrics, **anomaly_metrics}.values()
                             if hasattr(m, 'status') and m.status == "critical")

        if critical_count > 0:
            overall_status = "🚨 需要紧急关注"
        elif warning_count > 0:
            overall_status = "⚠️ 需要关注"
        else:
            overall_status = "✅ 运行正常"

        lines = []
        lines.extend([
            "=" * 100,
            f"🏢 【{cluster}】集群 Nginx 系统运行{type_names[report_type]} - {date}",
            "=" * 100,
            f"🎯 集群状态: {overall_status}",
            f"📅 生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ""
        ])

        # 执行摘要
        lines.extend([
            "【📋 一、执行摘要】",
            ""
        ])

        availability = core_metrics["availability"].value
        total_requests = core_metrics["total_requests"].value
        qps_peak = core_metrics["qps_peak"].value
        qps_avg = core_metrics["qps_avg"].value
        bandwidth_peak = core_metrics["bandwidth_peak"].value
        bandwidth_avg = core_metrics["bandwidth_avg"].value

        lines.extend([
            f"🎯 服务可用性: {availability:.3f}% {'(优秀)' if availability >= 99.95 else '(良好)' if availability >= 99.9 else '(需改进)'}",
            f"📊 业务处理: 总计 {self.format_requests(total_requests)}",  # 使用新的格式化函数
            f"⚡ QPS表现: 峰值 {self.format_number(qps_peak)} ({core_metrics['qps_peak'].timestamp})",
            f"🌐 网络带宽: 峰值 {bandwidth_peak:.2f} Gbps ({core_metrics['bandwidth_peak'].timestamp}), 平均 {bandwidth_avg:.2f} Gbps",
            f"📦 数据流量: 总计 {core_metrics['total_traffic'].value:.2f} TB (上行 {core_metrics['traffic_out'].value:.2f} TB, 下行 {core_metrics['traffic_in'].value:.2f} TB)",
            f"💻 资源使用: CPU平均 {system_metrics['cpu_usage'].value:.1f}%, 内存平均 {system_metrics['memory_usage'].value:.1f}%",
            f"🖥️ 硬件配置: {int(system_metrics['node_count'].value)} 台服务器, 总计 {int(system_metrics['total_cpu_cores'].value)} 核 CPU",
            ""
        ])

        # 环比变化
        if comparison_data:
            lines.extend([
                "📈 环比变化:",
                f"  • 请求量: {comparison_data['requests'][1]}",
                f"  • 流量: {comparison_data['traffic'][1]}",
                f"  • 峰值QPS: {comparison_data['qps_peak'][1]}",
                f"  • CPU平均: {comparison_data.get('cpu_avg', ('', '暂无数据'))[1]}",
                f"  • 内存平均: {comparison_data.get('memory_avg', ('', '暂无数据'))[1]}",
                ""
            ])

        # 异常事件摘要
        total_anomalies = int(anomaly_metrics["oom_events"].value +
                              anomaly_metrics["high_cpu_nodes"].value +
                              anomaly_metrics["high_memory_nodes"].value)

        if total_anomalies > 0:
            lines.extend([
                f"⚠️ 异常事件: 发现 {total_anomalies} 项异常需要关注",
                f"  • OOM事件: {int(anomaly_metrics['oom_events'].value)} 次",
                f"  • 高CPU节点: {int(anomaly_metrics['high_cpu_nodes'].value)} 个",
                f"  • 高内存节点: {int(anomaly_metrics['high_memory_nodes'].value)} 个",
                f"  • 网络错误: {int(anomaly_metrics['network_errors'].value)} 个",
                ""
            ])
        else:
            lines.extend([
                "✅ 异常事件: 无异常事件发生",
                ""
            ])

        # 详细技术指标
        lines.extend([
            "【🔧 二、详细技术指标】",
            ""
        ])

        # 业务指标
        lines.extend([
            "📊 业务指标:",
            f"  • 总请求数: {self.format_number(total_requests)} 次",
            f"  • QPS峰值: {self.format_number(qps_peak)} (时间: {core_metrics['qps_peak'].timestamp})",
            f"  • QPS最低: {self.format_number(core_metrics['qps_min'].value)} (时间: {core_metrics['qps_min'].timestamp})",
            f"  • QPS平均: {self.format_number(qps_avg)}",
            f"  • 带宽峰值: {bandwidth_peak:.2f} Gbps (时间: {core_metrics['bandwidth_peak'].timestamp})",
            f"    - 上行峰值: {core_metrics['bandwidth_out_peak'].value:.2f} Gbps (时间: {core_metrics['bandwidth_out_peak'].timestamp})",
            f"    - 下行峰值: {core_metrics['bandwidth_in_peak'].value:.2f} Gbps (时间: {core_metrics['bandwidth_in_peak'].timestamp})",
            f"  • 带宽平均: {bandwidth_avg:.2f} Gbps",
            f"    - 上行平均: {core_metrics['bandwidth_out_avg'].value:.2f} Gbps",
            f"    - 下行平均: {core_metrics['bandwidth_in_avg'].value:.2f} Gbps",
            f"  • 总流量: {core_metrics['total_traffic'].value:.2f} TB",
            f"    - 上行流量: {core_metrics['traffic_out'].value:.2f} TB",
            f"    - 下行流量: {core_metrics['traffic_in'].value:.2f} TB",
            f"  • 服务可用性: {availability:.3f}%",
            ""
        ])

        # 系统资源
        lines.extend([
            "💻 系统资源:",
            f"  • CPU使用率: 平均 {system_metrics['cpu_usage'].value:.1f}% {self._get_status_icon(system_metrics['cpu_usage'].status)}",
            f"    - 峰值: {system_metrics['cpu_peak'].value:.1f}% (时间: {system_metrics['cpu_peak'].timestamp})",
            f"    - 最低: {system_metrics['cpu_min'].value:.1f}% (时间: {system_metrics['cpu_min'].timestamp})",
            f"  • 内存使用率: 平均 {system_metrics['memory_usage'].value:.1f}% {self._get_status_icon(system_metrics['memory_usage'].status)}",
            f"    - 峰值: {system_metrics['memory_peak'].value:.1f}% (时间: {system_metrics['memory_peak'].timestamp})",
            f"    - 最低: {system_metrics['memory_min'].value:.1f}% (时间: {system_metrics['memory_min'].timestamp})",
            f"  • 系统负载: 平均 {system_metrics['system_load'].value:.2f}",
            f"    - 峰值: {system_metrics['load_peak'].value:.2f} (时间: {system_metrics['load_peak'].timestamp})",
            f"  • 硬件配置:",
            f"    - 服务器数量: {int(system_metrics['node_count'].value)} 台",
            f"    - CPU核心总数: {int(system_metrics['total_cpu_cores'].value)} 核",
            f"    - 单机CPU核心数: {int(system_metrics['min_cpu_cores'].value)}-{int(system_metrics['max_cpu_cores'].value)} 核 (平均 {system_metrics['avg_cpu_cores'].value:.1f} 核)",
            f"  • 在线节点: {int(system_metrics['online_nodes'].value)}/{int(system_metrics['total_nodes'].value)} 个",
            f"  • 节点可用率: {system_metrics['node_availability'].value:.1f}% {self._get_status_icon(system_metrics['node_availability'].status)}",
            ""
        ])

        # 连接状态
        lines.extend([
            "🔗 连接状态:",
            f"  • 活跃连接: 平均 {int(connection_metrics['conn_active'].value)} 个",
            f"    - 峰值: {int(connection_metrics['conn_active_peak'].value)} 个 (时间: {connection_metrics['conn_active_peak'].timestamp})",
            f"  • 等待连接: 平均 {int(connection_metrics['conn_waiting'].value)} 个",
            f"    - 峰值: {int(connection_metrics['conn_waiting_peak'].value)} 个 (时间: {connection_metrics['conn_waiting_peak'].timestamp})",
            f"  • 读取连接: 平均 {int(connection_metrics['conn_reading'].value)} 个",
            f"    - 峰值: {int(connection_metrics['conn_reading_peak'].value)} 个 (时间: {connection_metrics['conn_reading_peak'].timestamp})",
            f"  • 写入连接: 平均 {int(connection_metrics['conn_writing'].value)} 个",
            f"    - 峰值: {int(connection_metrics['conn_writing_peak'].value)} 个 (时间: {connection_metrics['conn_writing_peak'].timestamp})",
            f"  • 总连接峰值: {int(connection_metrics['conn_total_peak'].value)} 个 (时间: {connection_metrics['conn_total_peak'].timestamp})",
            ""
        ])

        # 连接状态说明
        if connection_metrics['conn_reading'].value == 0 and connection_metrics['conn_reading_peak'].value <= 1:
            lines.append("  📝 说明: 读取连接数较低属于正常现象，表示请求处理速度较快")
            lines.append("")

        # 异常事件统计
        lines.extend([
            "⚠️ 异常事件统计:",
            f"  • OOM事件: {int(anomaly_metrics['oom_events'].value)} 次 {self._get_status_icon(anomaly_metrics['oom_events'].status)}",
            f"  • 高CPU使用率节点: {int(anomaly_metrics['high_cpu_nodes'].value)} 个 {self._get_status_icon(anomaly_metrics['high_cpu_nodes'].status)}",
            f"  • 高内存使用率节点: {int(anomaly_metrics['high_memory_nodes'].value)} 个 {self._get_status_icon(anomaly_metrics['high_memory_nodes'].status)}",
            f"  • 网络错误: {int(anomaly_metrics['network_errors'].value)} 个 {self._get_status_icon(anomaly_metrics['network_errors'].status)}",
            ""
        ])

        # 运维建议
        lines.extend([
            "【💡 三、运维建议】",
            ""
        ])

        recommendations = self._generate_cluster_recommendations(
            cluster, core_metrics, system_metrics, connection_metrics, anomaly_metrics
        )

        for category, recs in recommendations.items():
            if recs:
                lines.append(f"🔍 {category}:")
                for rec in recs:
                    lines.append(f"  • {rec}")
                lines.append("")

        lines.extend([
            "=" * 100,
            ""
        ])

        return "\n".join(lines)

    def _get_status_icon(self, status: str) -> str:
        """获取状态图标"""
        icons = {
            "normal": "✅",
            "warning": "⚠️",
            "critical": "🚨"
        }
        return icons.get(status, "❓")

    def _generate_recommendations(self, core_metrics: Dict, system_metrics: Dict,
                                  connection_metrics: Dict, anomaly_metrics: Dict) -> Dict[str, List[str]]:
        """生成运维建议"""
        recommendations = {
            "性能优化": [],
            "资源管理": [],
            "安全加固": [],
            "容灾备份": []
        }

        # 性能相关建议
        if system_metrics["cpu_usage"].value > 70:
            recommendations["性能优化"].append(
                f"CPU使用率达到{system_metrics['cpu_usage'].value:.1f}%，建议优化应用性能或扩容")

        if system_metrics["memory_usage"].value > 75:
            recommendations["性能优化"].append(
                f"内存使用率达到{system_metrics['memory_usage'].value:.1f}%，建议优化内存使用或增加内存")

        if connection_metrics["conn_peak"].value > 10000:
            recommendations["性能优化"].append(
                f"连接数峰值达到{int(connection_metrics['conn_peak'].value)}，建议优化连接池配置")

        # 资源管理建议
        if system_metrics["node_availability"].value < 100:
            offline_nodes = int(system_metrics["total_nodes"].value - system_metrics["online_nodes"].value)
            recommendations["资源管理"].append(f"有{offline_nodes}个节点离线，请检查节点状态")

        if anomaly_metrics["oom_events"].value > 0:
            recommendations["资源管理"].append(
                f"发生{int(anomaly_metrics['oom_events'].value)}次OOM事件，请检查内存配置")

        # 安全相关建议
        if anomaly_metrics["network_errors"].value > 1000:
            recommendations["安全加固"].append(
                f"网络错误数量较高({int(anomaly_metrics['network_errors'].value)}个)，请检查网络安全")

        # 容灾相关建议
        if core_metrics["availability"].value < 99.9:
            recommendations["容灾备份"].append(
                f"服务可用性为{core_metrics['availability'].value:.3f}%，建议完善容灾机制")

        # 如果没有问题，给出积极建议
        if not any(recommendations.values()):
            recommendations["性能优化"].append("系统运行状态良好，建议继续保持当前运维水平")
            recommendations["资源管理"].append("资源使用合理，可考虑制定容量规划")
            recommendations["安全加固"].append("建议定期进行安全检查和漏洞扫描")
            recommendations["容灾备份"].append("建议定期演练容灾恢复流程")

        return recommendations


def create_nightingale_dashboard():
    """创建夜莺监控大屏配置"""
    dashboard_config = {
        "name": "Nginx日报监控大屏",
        "tags": ["nginx", "daily-report"],
        "configs": {
            "panels": [
                {
                    "title": "服务可用性",
                    "type": "stat",
                    "targets": [
                        {
                            "expr": "min(system_uptime{cluster=~\"$cluster\"}) / 86400 * 99.99",
                            "legend": "可用性"
                        }
                    ],
                    "options": {
                        "valueName": "current",
                        "unit": "percent",
                        "thresholds": [
                            {"color": "red", "value": 99.0},
                            {"color": "yellow", "value": 99.9},
                            {"color": "green", "value": 99.95}
                        ]
                    }
                },
                {
                    "title": "总请求数",
                    "type": "stat",
                    "targets": [
                        {
                            "expr": "sum(increase(nginx_requests{cluster=~\"$cluster\"}[24h]))",
                            "legend": "总请求数"
                        }
                    ],
                    "options": {
                        "valueName": "current",
                        "unit": "short"
                    }
                },
                {
                    "title": "QPS趋势",
                    "type": "graph",
                    "targets": [
                        {
                            "expr": "sum(irate(nginx_requests{cluster=~\"$cluster\"}[2m]))",
                            "legend": "QPS"
                        }
                    ],
                    "options": {
                        "unit": "reqps"
                    }
                },
                {
                    "title": "带宽使用趋势",
                    "type": "graph",
                    "targets": [
                        {
                            "expr": "sum(irate(net_bytes_recv{cluster=~\"$cluster\"}[2m]) * 8 + irate(net_bytes_sent{cluster=~\"$cluster\"}[2m]) * 8) / 1024 / 1024 / 1024",
                            "legend": "带宽"
                        }
                    ],
                    "options": {
                        "unit": "bps"
                    }
                },
                {
                    "title": "系统资源概览",
                    "type": "table",
                    "targets": [
                        {
                            "expr": "avg(cpu_usage_active{cluster=~\"$cluster\"})",
                            "legend": "CPU使用率"
                        },
                        {
                            "expr": "avg(mem_used_percent{cluster=~\"$cluster\"})",
                            "legend": "内存使用率"
                        },
                        {
                            "expr": "avg(system_load5{cluster=~\"$cluster\"})",
                            "legend": "系统负载"
                        }
                    ]
                },
                {
                    "title": "连接状态分布",
                    "type": "piechart",
                    "targets": [
                        {
                            "expr": "sum(nginx_active{cluster=~\"$cluster\"})",
                            "legend": "活跃"
                        },
                        {
                            "expr": "sum(nginx_waiting{cluster=~\"$cluster\"})",
                            "legend": "等待"
                        },
                        {
                            "expr": "sum(nginx_reading{cluster=~\"$cluster\"})",
                            "legend": "读取"
                        },
                        {
                            "expr": "sum(nginx_writing{cluster=~\"$cluster\"})",
                            "legend": "写入"
                        }
                    ]
                },
                {
                    "title": "异常事件统计",
                    "type": "table",
                    "targets": [
                        {
                            "expr": "sum(increase(kernel_vmstat_oom_kill{cluster=~\"$cluster\"}[24h]))",
                            "legend": "OOM事件"
                        },
                        {
                            "expr": "count(cpu_usage_active{cluster=~\"$cluster\"} > 85)",
                            "legend": "高CPU节点"
                        },
                        {
                            "expr": "count(mem_used_percent{cluster=~\"$cluster\"} > 90)",
                            "legend": "高内存节点"
                        },
                        {
                            "expr": "sum(increase(net_err_in{cluster=~\"$cluster\"}[24h]) + increase(net_err_out{cluster=~\"$cluster\"}[24h]))",
                            "legend": "网络错误"
                        }
                    ]
                }
            ],
            "templating": {
                "list": [
                    {
                        "name": "cluster",
                        "type": "query",
                        "query": "label_values(nginx_requests, cluster)",
                        "multi": True,
                        "includeAll": True,
                        "current": {
                            "value": "self-prod-nginx|zgtapp-prod-nginx"
                        }
                    },
                    {
                        "name": "date",
                        "type": "textbox",
                        "current": {
                            "value": (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                        }
                    }
                ]
            },
            "time": {
                "from": "now-1d",
                "to": "now"
            },
            "refresh": "1m"
        }
    }

    return json.dumps(dashboard_config, indent=2, ensure_ascii=False)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="Nginx集群监控日报自动化生成器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    使用示例:
      %(prog)s --url http://nightingale.company.com --datasource-id 1
      %(prog)s --url http://nightingale.company.com --datasource-id 1 --date 2024-01-15
      %(prog)s --url http://nightingale.company.com --datasource-id 1 --clusters prod-web,prod-api --weekly
      %(prog)s --url http://nightingale.company.com --datasource-id 1 --username admin --password secret
      %(prog)s --generate-dashboard > nginx_dashboard.json
            """
    )

    # 必需参数
    parser.add_argument('--url', default='http://11.11.11.11:17000',
                        help='夜莺服务地址 (默认: http://11.11.11.11:17000)')
    parser.add_argument('--datasource-id', type=int, default=1,
                        help='Prometheus数据源ID (默认: 1)')

    # 认证参数
    parser.add_argument('--username', default='root',
                        help='登录用户名 (默认: root)')
    parser.add_argument('--password', default='root',
                        help='登录密码 (默认: root)')

    # 报告参数
    parser.add_argument('--date',
                        default=(datetime.datetime.now() - datetime.timedelta(days=6)).strftime("%Y-%m-%d"),
                        help='统计日期 (格式: YYYY-MM-DD, 默认: 昨天)')
    parser.add_argument('--clusters',
                        default='self-prod-nginx,zgtapp-prod-nginx',
                        help='监控集群列表，逗号分隔 (默认: self-prod-nginx,zgtapp-prod-nginx)')

    # 报告类型
    report_group = parser.add_mutually_exclusive_group()
    report_group.add_argument('--daily', action='store_true', default=True,
                              help='生成日报 (默认)')
    report_group.add_argument('--weekly', action='store_true',
                              help='生成周报')
    report_group.add_argument('--monthly', action='store_true',
                              help='生成月报')

    # 输出选项
    parser.add_argument('--no-comparison', action='store_true',
                        help='不包含环比对比数据')
    parser.add_argument('--output', '-o',
                        help='输出文件路径 (默认: 输出到控制台)')
    parser.add_argument('--generate-dashboard', action='store_true',
                        help='生成夜莺监控大屏配置')

    # 调试选项
    parser.add_argument('--debug', action='store_true',
                        help='启用调试模式')

    args = parser.parse_args()

    # 设置日志级别
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # 生成监控大屏配置
    if args.generate_dashboard:
        print("# 夜莺监控大屏配置 - Nginx日报")
        print("# 使用方法: 在夜莺管理界面导入此配置")
        print(create_nightingale_dashboard())
        return

    try:
        # 确定报告类型
        if args.weekly:
            report_type = ReportType.WEEKLY
        elif args.monthly:
            report_type = ReportType.MONTHLY
        else:
            report_type = ReportType.DAILY

        # 解析集群列表
        clusters = [c.strip() for c in args.clusters.split(',') if c.strip()]

        # 创建客户端
        logger.info(f"🔗 连接夜莺服务: {args.url}")
        client = NightingaleClient(
            base_url=args.url,
            datasource_id=args.datasource_id,
            username=args.username,
            password=args.password
        )

        # 创建报告生成器
        generator = NginxReportGenerator(client)

        # 生成报告
        report_list = generator.generate_report(
            target_date=args.date,
            clusters=clusters,
            report_type=report_type,
            include_comparison=not args.no_comparison
        )

        # 输出报告
        for report in report_list:
            if args.output:
                with open(args.output, 'a', encoding='utf-8') as f:
                    f.write(report)
                    f.write("\n")
                logger.info(f"📄 报告已保存至: {args.output}")
            else:
                print(report)

    except KeyboardInterrupt:
        logger.info("👋 用户取消操作")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ 程序执行失败: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()