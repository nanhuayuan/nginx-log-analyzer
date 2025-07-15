import gc
import numpy as np
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font
from datetime import datetime

from self_00_04_excel_processor import format_excel_sheet, add_dataframe_to_excel_with_grouped_headers
from self_00_01_constants import DEFAULT_CHUNK_SIZE, DEFAULT_SLOW_THRESHOLD, DEFAULT_SLOW_REQUESTS_THRESHOLD, \
    TIME_METRICS, SIZE_METRICS, HIGHLIGHT_FILL
from self_00_02_utils import log_info, get_distribution_stats, calculate_time_percentages
import random

# 尝试导入scipy，如果失败则使用近似计算
try:
    from scipy.stats import norm
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False


def analyze_api_performance(csv_path, output_path, success_codes=None, slow_threshold=DEFAULT_SLOW_THRESHOLD):
    """分析API性能数据并生成Excel报告"""
    log_info(f"开始分析API性能数据: {csv_path}", show_memory=True)

    if success_codes is None:
        from self_00_01_constants import DEFAULT_SUCCESS_CODES
        success_codes = DEFAULT_SUCCESS_CODES

    # 优化后的字段映射
    field_mapping = {
        'uri': 'request_full_uri',
        'app': 'application_name',
        'service': 'service_name',
        'status': 'response_status_code',
        'request_time': 'total_request_duration',
        'header_time': 'upstream_header_time',
        'connect_time': 'upstream_connect_time',
        'response_time': 'upstream_response_time',
        'body_bytes_kb': 'response_body_size_kb',  # 已转换为KB
        'bytes_sent_kb': 'total_bytes_sent_kb',  # 已转换为KB
        # 新增的阶段分析字段
        'backend_connect_phase': 'backend_connect_phase',
        'backend_process_phase': 'backend_process_phase',
        'backend_transfer_phase': 'backend_transfer_phase',
        'nginx_transfer_phase': 'nginx_transfer_phase',
        # 新增的性能分析字段
        'backend_efficiency': 'backend_efficiency',
        'network_overhead': 'network_overhead',
        'transfer_ratio': 'transfer_ratio',
        'response_transfer_speed': 'response_transfer_speed',
        'total_transfer_speed': 'total_transfer_speed',
        'processing_efficiency_index': 'processing_efficiency_index'
    }

    chunk_size = max(DEFAULT_CHUNK_SIZE // 2, 10000)  # 减小chunk大小
    api_stats = {}
    total_requests = 0
    success_requests = 0
    total_slow_requests = 0
    
    # 使用采样策略减少内存使用
    SAMPLE_SIZE = 100000  # 最多采雇10万个样本
    response_times = []
    body_sizes_kb = []
    bytes_sizes_kb = []
    transfer_speeds = []
    efficiency_scores = []

    # 阶段时间统计
    phase_times = {
        'backend_connect_phase': 0,
        'backend_process_phase': 0,
        'backend_transfer_phase': 0,
        'nginx_transfer_phase': 0
    }

    success_codes = [str(code) for code in success_codes]
    chunks_processed = 0
    start_time = datetime.now()

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        chunks_processed += 1
        chunk_rows = len(chunk)
        total_requests += chunk_rows

        # 按API分组统计请求总数
        for api, group in chunk.groupby(field_mapping['uri']):
            if api not in api_stats:
                initialize_api_stats(api_stats, api, group, field_mapping)
            api_stats[api]['total_requests'] += len(group)

        # 筛选成功请求
        successful_requests = chunk[chunk[field_mapping['status']].astype(str).isin(success_codes)]
        success_count = len(successful_requests)
        success_requests += success_count

        if success_count > 0:
            # 处理响应时间（使用采样）
            request_times = pd.to_numeric(successful_requests[field_mapping['request_time']], errors='coerce')
            valid_times = request_times.dropna()
            
            # 采样策略：如果数据量超过限制，随机采样
            if len(response_times) + len(valid_times) > SAMPLE_SIZE:
                remaining_slots = SAMPLE_SIZE - len(response_times)
                if remaining_slots > 0:
                    sample_indices = random.sample(range(len(valid_times)), min(remaining_slots, len(valid_times)))
                    sample_times = valid_times.iloc[sample_indices]
                    response_times.extend(sample_times.tolist())
            else:
                response_times.extend(valid_times.tolist())

            # 统计慢请求
            chunk_slow_requests = (valid_times > slow_threshold).sum()
            total_slow_requests += chunk_slow_requests

            # 统计阶段时间
            for phase_key, phase_field in phase_times.items():
                if phase_field in successful_requests.columns:
                    phase_data = pd.to_numeric(successful_requests[phase_field], errors='coerce')
                    phase_times[phase_key] += phase_data.dropna().sum()

            # 统计数据大小 (使用采样)
            body_data_kb = pd.to_numeric(successful_requests[field_mapping['body_bytes_kb']], errors='coerce').dropna()
            if len(body_sizes_kb) + len(body_data_kb) > SAMPLE_SIZE:
                remaining_slots = SAMPLE_SIZE - len(body_sizes_kb)
                if remaining_slots > 0:
                    sample_indices = random.sample(range(len(body_data_kb)), min(remaining_slots, len(body_data_kb)))
                    sample_body = body_data_kb.iloc[sample_indices]
                    body_sizes_kb.extend(sample_body.tolist())
            else:
                body_sizes_kb.extend(body_data_kb.tolist())

            bytes_data_kb = pd.to_numeric(successful_requests[field_mapping['bytes_sent_kb']], errors='coerce').dropna()
            if len(bytes_sizes_kb) + len(bytes_data_kb) > SAMPLE_SIZE:
                remaining_slots = SAMPLE_SIZE - len(bytes_sizes_kb)
                if remaining_slots > 0:
                    sample_indices = random.sample(range(len(bytes_data_kb)), min(remaining_slots, len(bytes_data_kb)))
                    sample_bytes = bytes_data_kb.iloc[sample_indices]
                    bytes_sizes_kb.extend(sample_bytes.tolist())
            else:
                bytes_sizes_kb.extend(bytes_data_kb.tolist())

            # 统计传输速度（使用采样）
            if field_mapping['response_transfer_speed'] in successful_requests.columns:
                speed_data = pd.to_numeric(successful_requests[field_mapping['response_transfer_speed']],
                                           errors='coerce').dropna()
                if len(transfer_speeds) + len(speed_data) > SAMPLE_SIZE:
                    remaining_slots = SAMPLE_SIZE - len(transfer_speeds)
                    if remaining_slots > 0:
                        sample_indices = random.sample(range(len(speed_data)), min(remaining_slots, len(speed_data)))
                        sample_speed = speed_data.iloc[sample_indices]
                        transfer_speeds.extend(sample_speed.tolist())
                else:
                    transfer_speeds.extend(speed_data.tolist())

            # 统计效率指标（使用采样）
            if field_mapping['processing_efficiency_index'] in successful_requests.columns:
                efficiency_data = pd.to_numeric(successful_requests[field_mapping['processing_efficiency_index']],
                                                errors='coerce').dropna()
                if len(efficiency_scores) + len(efficiency_data) > SAMPLE_SIZE:
                    remaining_slots = SAMPLE_SIZE - len(efficiency_scores)
                    if remaining_slots > 0:
                        sample_indices = random.sample(range(len(efficiency_data)), min(remaining_slots, len(efficiency_data)))
                        sample_efficiency = efficiency_data.iloc[sample_indices]
                        efficiency_scores.extend(sample_efficiency.tolist())
                else:
                    efficiency_scores.extend(efficiency_data.tolist())

            # 按API分组处理详细统计
            for api, group in successful_requests.groupby(field_mapping['uri']):
                process_api_group(api_stats, api, group, slow_threshold, field_mapping)

        if chunks_processed % 5 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            log_info(f"已处理 {chunks_processed} 个数据块, {total_requests} 条记录, 耗时: {elapsed:.2f}秒", show_memory=True)
            del chunk
            gc.collect()

    log_info(f"数据处理完成，共处理 {total_requests} 条请求, 成功请求 {success_requests} 条", show_memory=True)

    # 生成统计报告
    results = generate_api_statistics(api_stats, success_requests, total_slow_requests)

    if results:
        results_df = pd.DataFrame(results)
        if not results_df.empty and '平均请求时长(秒)' in results_df.columns:
            results_df = results_df.sort_values(by='平均请求时长(秒)', ascending=False)

        # 创建Excel报告
        create_api_performance_excel(
            results_df, output_path, total_requests, success_requests,
            total_slow_requests, response_times, body_sizes_kb, bytes_sizes_kb,
            phase_times, transfer_speeds, efficiency_scores
        )

        log_info(f"API性能分析报告已生成: {output_path}", show_memory=True)
        return results_df.head(5)
    else:
        log_info("没有找到任何API数据，返回空DataFrame", show_memory=True)
        return pd.DataFrame()


def initialize_api_stats(api_stats, api, group, field_mapping):
    """初始化API统计数据结构"""
    if api in api_stats:
        return

    app_name = group[field_mapping['app']].iloc[0] if not group[field_mapping['app']].empty else ""
    service_name = group[field_mapping['service']].iloc[0] if not group[field_mapping['service']].empty else ""

    api_stats[api] = {
        'request_uri': api,
        'app_name': app_name,
        'service_name': service_name,
        'total_requests': 0,
        'success_requests': 0,
        'slow_requests_count': 0
    }

    # 时间相关字段
    time_fields = [
        'request_time', 'connect_time', 'header_time', 'response_time',
        'backend_connect_phase', 'backend_process_phase',
        'backend_transfer_phase', 'nginx_transfer_phase'
    ]

    for field in time_fields:
        api_stats[api][f'{field}_min'] = float('inf')
        api_stats[api][f'{field}_max'] = 0
        api_stats[api][f'{field}_total'] = 0
        # 不再存储原始数据，使用流式统计
        api_stats[api][f'{field}_count'] = 0
        api_stats[api][f'{field}_sum'] = 0.0
        api_stats[api][f'{field}_sum_sq'] = 0.0  # 用于计算方差

    # 大小相关字段 (KB单位)
    size_fields = ['body_kb', 'bytes_kb']
    for field in size_fields:
        api_stats[api][f'{field}_min'] = float('inf')
        api_stats[api][f'{field}_max'] = 0
        api_stats[api][f'{field}_total'] = 0
        api_stats[api][f'{field}_count'] = 0
        api_stats[api][f'{field}_sum'] = 0.0
        api_stats[api][f'{field}_sum_sq'] = 0.0

    # 性能指标字段
    performance_fields = ['transfer_speed', 'efficiency_index']
    for field in performance_fields:
        api_stats[api][f'{field}_count'] = 0
        api_stats[api][f'{field}_sum'] = 0.0
        api_stats[api][f'{field}_sum_sq'] = 0.0


def process_api_group(api_stats, api, group, slow_threshold, field_mapping):
    """处理单个API组的详细统计"""
    if api not in api_stats:
        initialize_api_stats(api_stats, api, group, field_mapping)

    # 时间字段映射
    time_fields = {
        'request_time': field_mapping['request_time'],
        'connect_time': field_mapping['connect_time'],
        'header_time': field_mapping['header_time'],
        'response_time': field_mapping['response_time'],
        'backend_connect_phase': field_mapping['backend_connect_phase'],
        'backend_process_phase': field_mapping['backend_process_phase'],
        'backend_transfer_phase': field_mapping['backend_transfer_phase'],
        'nginx_transfer_phase': field_mapping['nginx_transfer_phase']
    }

    # 转换时间数据
    numeric_data = {}
    for field_key, field_name in time_fields.items():
        if field_name in group.columns:
            numeric_data[field_key] = pd.to_numeric(group[field_name], errors='coerce')

    # 转换大小数据 (KB单位)
    body_sizes_kb = pd.to_numeric(group[field_mapping['body_bytes_kb']], errors='coerce')
    bytes_sizes_kb = pd.to_numeric(group[field_mapping['bytes_sent_kb']], errors='coerce')

    # 转换性能指标
    transfer_speeds = pd.to_numeric(group[field_mapping['response_transfer_speed']], errors='coerce') if field_mapping[
                                                                                                             'response_transfer_speed'] in group.columns else pd.Series(
        [])
    efficiency_scores = pd.to_numeric(group[field_mapping['processing_efficiency_index']], errors='coerce') if \
    field_mapping['processing_efficiency_index'] in group.columns else pd.Series([])

    # 统计慢请求
    slow_requests_count = (numeric_data.get('request_time', pd.Series([])) > slow_threshold).sum()

    # 更新统计数据
    api_stats[api]['success_requests'] += len(group)
    api_stats[api]['slow_requests_count'] += slow_requests_count

    # 更新时间统计
    for field, series in numeric_data.items():
        update_stats(api_stats[api], field, series)

    # 更新大小统计
    update_stats(api_stats[api], 'body_kb', body_sizes_kb)
    update_stats(api_stats[api], 'bytes_kb', bytes_sizes_kb)

    # 更新性能指标统计（使用增量统计）
    if len(transfer_speeds) > 0:
        update_incremental_stats(api_stats[api], 'transfer_speed', transfer_speeds.dropna())
    if len(efficiency_scores) > 0:
        update_incremental_stats(api_stats[api], 'efficiency_index', efficiency_scores.dropna())


def update_stats(stats_dict, field, series):
    """更新统计数据的通用函数（优化版）"""
    valid_values = series.dropna()
    if len(valid_values) == 0:
        return

    min_field = f"{field}_min"
    max_field = f"{field}_max"
    total_field = f"{field}_total"
    count_field = f"{field}_count"
    sum_field = f"{field}_sum"
    sum_sq_field = f"{field}_sum_sq"

    if all(key in stats_dict for key in [min_field, max_field, total_field]):
        min_val = valid_values.min()
        max_val = valid_values.max()
        total_val = valid_values.sum()
        count_val = len(valid_values)
        sum_sq_val = (valid_values ** 2).sum()

        stats_dict[min_field] = min(stats_dict[min_field], min_val) if stats_dict[min_field] != float(
            'inf') else min_val
        stats_dict[max_field] = max(stats_dict[max_field], max_val)
        stats_dict[total_field] += total_val
        
        # 增量统计
        if count_field in stats_dict:
            stats_dict[count_field] += count_val
        if sum_field in stats_dict:
            stats_dict[sum_field] += total_val
        if sum_sq_field in stats_dict:
            stats_dict[sum_sq_field] += sum_sq_val


def update_incremental_stats(stats_dict, field, series):
    """更新增量统计数据"""
    valid_values = series.dropna()
    if len(valid_values) == 0:
        return
    
    count_field = f"{field}_count"
    sum_field = f"{field}_sum"
    sum_sq_field = f"{field}_sum_sq"
    
    count_val = len(valid_values)
    sum_val = valid_values.sum()
    sum_sq_val = (valid_values ** 2).sum()
    
    if count_field in stats_dict:
        stats_dict[count_field] += count_val
    if sum_field in stats_dict:
        stats_dict[sum_field] += sum_val
    if sum_sq_field in stats_dict:
        stats_dict[sum_sq_field] += sum_sq_val


def generate_api_statistics(api_stats, success_requests, total_slow_requests):
    """生成API统计报告"""
    results = []
    processed_apis = 0

    for api, stats in api_stats.items():
        # 使用统计量计算百分位数（近似值）
        def calculate_percentile_from_stats(count, sum_val, sum_sq, percentile):
            if count == 0:
                return 0
            # 使用正态分布近似百分位数
            mean = sum_val / count
            variance = max(0, (sum_sq / count) - (mean ** 2))
            std = variance ** 0.5
            
            # 正态分布的百分位数近似
            if SCIPY_AVAILABLE:
                z_score = norm.ppf(percentile / 100)
                return max(0, mean + z_score * std)
            else:
                # 简单的线性插值近似（当scipy不可用时）
                if percentile <= 50:
                    return max(0, mean - std * (50 - percentile) / 25)
                else:
                    return max(0, mean + std * (percentile - 50) / 25)

        # 计算各个时间指标的百分位数
        time_fields = ['request_time', 'connect_time', 'header_time', 'response_time',
                      'backend_connect_phase', 'backend_process_phase', 
                      'backend_transfer_phase', 'nginx_transfer_phase']
        
        time_percentiles = {}
        for field in time_fields:
            count = stats.get(f'{field}_count', 0)
            sum_val = stats.get(f'{field}_sum', 0)
            sum_sq = stats.get(f'{field}_sum_sq', 0)
            
            if count > 0:
                time_percentiles[field] = {
                    'median': calculate_percentile_from_stats(count, sum_val, sum_sq, 50),
                    'p90': calculate_percentile_from_stats(count, sum_val, sum_sq, 90),
                    'p95': calculate_percentile_from_stats(count, sum_val, sum_sq, 95),
                    'p99': calculate_percentile_from_stats(count, sum_val, sum_sq, 99)
                }
            else:
                time_percentiles[field] = {'median': 0, 'p90': 0, 'p95': 0, 'p99': 0}

        # 计算大小指标的百分位数
        size_percentiles = {}
        for field in ['body_kb', 'bytes_kb']:
            count = stats.get(f'{field}_count', 0)
            sum_val = stats.get(f'{field}_sum', 0)
            sum_sq = stats.get(f'{field}_sum_sq', 0)
            
            if count > 0:
                size_percentiles[field] = {
                    'median': calculate_percentile_from_stats(count, sum_val, sum_sq, 50),
                    'p90': calculate_percentile_from_stats(count, sum_val, sum_sq, 90),
                    'p95': calculate_percentile_from_stats(count, sum_val, sum_sq, 95),
                    'p99': calculate_percentile_from_stats(count, sum_val, sum_sq, 99)
                }
            else:
                size_percentiles[field] = {'median': 0, 'p90': 0, 'p95': 0, 'p99': 0}

        # 计算性能指标的百分位数  
        transfer_speed_avg = 0
        efficiency_avg = 0
        if stats.get('transfer_speed_count', 0) > 0:
            transfer_speed_avg = stats['transfer_speed_sum'] / stats['transfer_speed_count']
        if stats.get('efficiency_index_count', 0) > 0:
            efficiency_avg = stats['efficiency_index_sum'] / stats['efficiency_index_count']

        # 计算基本指标
        avg_time = stats['request_time_total'] / stats['success_requests'] if stats['success_requests'] > 0 else 0
        slow_ratio = stats['slow_requests_count'] / stats['success_requests'] if stats['success_requests'] > 0 else 0
        is_slow_api = "Y" if (
                    avg_time > DEFAULT_SLOW_THRESHOLD or slow_ratio > DEFAULT_SLOW_REQUESTS_THRESHOLD) else "N"
        global_slow_ratio = (stats['slow_requests_count'] / total_slow_requests * 100) if total_slow_requests > 0 else 0

        # 构建结果字典
        result = {
            '请求URI': stats['request_uri'],
            '应用名称': stats['app_name'],
            '服务名称': stats['service_name'],
            '请求总数': stats['total_requests'],
            '成功请求数': stats['success_requests'],
            '占总请求比例(%)': round(stats['success_requests'] / success_requests * 100, 2) if success_requests > 0 else 0,
            '成功率(%)': round(stats['success_requests'] / stats['total_requests'] * 100, 2) if stats[
                                                                                                 'total_requests'] > 0 else 0,
            '慢请求数': stats['slow_requests_count'],
            '慢请求比例(%)': round(slow_ratio * 100, 2),
            '全局慢请求占比(%)': round(global_slow_ratio, 2),
            '是否慢接口': is_slow_api,
        }

        # 请求时长统计
        result.update({
            '平均请求时长(秒)': round(avg_time, 3),
            '最小请求时长(秒)': round(stats['request_time_min'] if stats['request_time_min'] != float('inf') else 0, 3),
            '最大请求时长(秒)': round(stats['request_time_max'], 3),
            '请求时长中位数(秒)': round(time_percentiles['request_time']['median'], 3),
            'P90请求时长(秒)': round(time_percentiles['request_time']['p90'], 3),
            'P95请求时长(秒)': round(time_percentiles['request_time']['p95'], 3),
            'P99请求时长(秒)': round(time_percentiles['request_time']['p99'], 3),
        })

        # 阶段时长统计
        backend_connect_avg = stats['backend_connect_phase_sum'] / stats['backend_connect_phase_count'] if stats.get('backend_connect_phase_count', 0) > 0 else 0
        backend_process_avg = stats['backend_process_phase_sum'] / stats['backend_process_phase_count'] if stats.get('backend_process_phase_count', 0) > 0 else 0
        backend_transfer_avg = stats['backend_transfer_phase_sum'] / stats['backend_transfer_phase_count'] if stats.get('backend_transfer_phase_count', 0) > 0 else 0
        nginx_transfer_avg = stats['nginx_transfer_phase_sum'] / stats['nginx_transfer_phase_count'] if stats.get('nginx_transfer_phase_count', 0) > 0 else 0
        
        result.update({
            '后端连接时长(秒)': round(backend_connect_avg, 3),
            '后端处理时长(秒)': round(backend_process_avg, 3),
            '后端传输时长(秒)': round(backend_transfer_avg, 3),
            'Nginx传输时长(秒)': round(nginx_transfer_avg, 3),
        })

        # 阶段占比统计
        if avg_time > 0:
            result.update({
                '后端连接占比(%)': round((backend_connect_avg / avg_time) * 100, 2),
                '后端处理占比(%)': round((backend_process_avg / avg_time) * 100, 2),
                '后端传输占比(%)': round((backend_transfer_avg / avg_time) * 100, 2),
                'Nginx传输占比(%)': round((nginx_transfer_avg / avg_time) * 100, 2),
            })
        else:
            result.update({
                '后端连接占比(%)': 0,
                '后端处理占比(%)': 0,
                '后端传输占比(%)': 0,
                'Nginx传输占比(%)': 0,
            })

        # 响应体大小统计 (KB)
        body_avg = stats['body_kb_sum'] / stats['body_kb_count'] if stats.get('body_kb_count', 0) > 0 else 0
        result.update({
            '平均响应体大小(KB)': round(body_avg, 2),
            '最小响应体大小(KB)': round(stats['body_kb_min'] if stats['body_kb_min'] != float('inf') else 0, 2),
            '最大响应体大小(KB)': round(stats['body_kb_max'], 2),
            '响应体大小中位数(KB)': round(size_percentiles['body_kb']['median'], 2),
            'P90响应体大小(KB)': round(size_percentiles['body_kb']['p90'], 2),
            'P95响应体大小(KB)': round(size_percentiles['body_kb']['p95'], 2),
            'P99响应体大小(KB)': round(size_percentiles['body_kb']['p99'], 2),
        })

        # 传输大小统计 (KB)
        bytes_avg = stats['bytes_kb_sum'] / stats['bytes_kb_count'] if stats.get('bytes_kb_count', 0) > 0 else 0
        result.update({
            '平均传输大小(KB)': round(bytes_avg, 2),
            '最小传输大小(KB)': round(stats['bytes_kb_min'] if stats['bytes_kb_min'] != float('inf') else 0, 2),
            '最大传输大小(KB)': round(stats['bytes_kb_max'], 2),
            '传输大小中位数(KB)': round(size_percentiles['bytes_kb']['median'], 2),
            'P90传输大小(KB)': round(size_percentiles['bytes_kb']['p90'], 2),
            'P95传输大小(KB)': round(size_percentiles['bytes_kb']['p95'], 2),
            'P99传输大小(KB)': round(size_percentiles['bytes_kb']['p99'], 2),
        })

        # 传输速度统计 (KB/s) - 使用近似值
        result.update({
            '平均传输速度(KB/s)': round(transfer_speed_avg, 2),
            '最低传输速度(KB/s)': 0,  # 无法从统计量计算最小值
            '最高传输速度(KB/s)': 0,  # 无法从统计量计算最大值
            'P90传输速度(KB/s)': 0,  # 使用近似计算有误差，略过
        })

        # 效率指标统计
        result.update({
            '平均处理效率指数': round(efficiency_avg, 2),
            '最低处理效率指数': 0,  # 无法从统计量计算最小值
            '最高处理效率指数': 0,  # 无法从统计量计算最大值
        })

        results.append(result)

        # 清理内存（不再需要，因为没有存储原始数据）
        pass

        processed_apis += 1
        if processed_apis % 100 == 0:
            log_info(f"已处理 {processed_apis}/{len(api_stats)} 个API的统计数据", show_memory=True)

    return results


def create_api_performance_excel(results_df, output_path, total_requests, success_requests,
                                 total_slow_requests, response_times, body_sizes_kb, bytes_sizes_kb,
                                 phase_times, transfer_speeds, efficiency_scores):
    """创建API性能分析Excel报告"""
    log_info(f"开始创建Excel报告: {output_path}", show_memory=True)

    wb = Workbook()
    if 'Sheet' in wb.sheetnames:
        del wb['Sheet']

    # 定义分组表头
    main_headers = {
        "请求URI": ["请求URI"],
        "应用信息": ["应用名称", "服务名称"],
        "请求统计": ["请求总数", "成功请求数", "占总请求比例(%)", "成功率(%)"],
        "慢请求统计": ["慢请求数", "慢请求比例(%)", "全局慢请求占比(%)", "是否慢接口"],
        "请求时长(秒)": ["平均", "最小", "最大", "中位数", "P90", "P95", "P99"],
        "阶段时长(秒)": ["后端连接", "后端处理", "后端传输", "Nginx传输"],
        "阶段占比(%)": ["后端连接占比", "后端处理占比", "后端传输占比", "Nginx传输占比"],
        "响应体大小(KB)": ["平均", "最小", "最大", "中位数", "P90", "P95", "P99"],
        "传输大小(KB)": ["平均", "最小", "最大", "中位数", "P90", "P95", "P99"],
        "传输速度(KB/s)": ["平均", "最低", "最高", "P90"],
        "效率指标": ["平均处理效率指数", "最低处理效率指数", "最高处理效率指数"]
    }

    # 列名映射
    column_mapping = {
        '请求URI': '请求URI',
        '应用名称': '应用名称',
        '服务名称': '服务名称',
        '请求总数': '请求总数',
        '成功请求数': '成功请求数',
        '占总请求比例(%)': '占总请求比例(%)',
        '成功率(%)': '成功率(%)',
        '慢请求数': '慢请求数',
        '慢请求比例(%)': '慢请求比例(%)',
        '全局慢请求占比(%)': '全局慢请求占比(%)',
        '是否慢接口': '是否慢接口',
        '平均请求时长(秒)': '平均',
        '最小请求时长(秒)': '最小',
        '最大请求时长(秒)': '最大',
        '请求时长中位数(秒)': '中位数',
        'P90请求时长(秒)': 'P90',
        'P95请求时长(秒)': 'P95',
        'P99请求时长(秒)': 'P99',
        '后端连接时长(秒)': '后端连接',
        '后端处理时长(秒)': '后端处理',
        '后端传输时长(秒)': '后端传输',
        'Nginx传输时长(秒)': 'Nginx传输',
        '后端连接占比(%)': '后端连接占比',
        '后端处理占比(%)': '后端处理占比',
        '后端传输占比(%)': '后端传输占比',
        'Nginx传输占比(%)': 'Nginx传输占比',
        '平均响应体大小(KB)': '平均',
        '最小响应体大小(KB)': '最小',
        '最大响应体大小(KB)': '最大',
        '响应体大小中位数(KB)': '中位数',
        'P90响应体大小(KB)': 'P90',
        'P95响应体大小(KB)': 'P95',
        'P99响应体大小(KB)': 'P99',
        '平均传输大小(KB)': '平均',
        '最小传输大小(KB)': '最小',
        '最大传输大小(KB)': '最大',
        '传输大小中位数(KB)': '中位数',
        'P90传输大小(KB)': 'P90',
        'P95传输大小(KB)': 'P95',
        'P99传输大小(KB)': 'P99',
        '平均传输速度(KB/s)': '平均',
        '最低传输速度(KB/s)': '最低',
        '最高传输速度(KB/s)': '最高',
        'P90传输速度(KB/s)': 'P90'
    }

    # 重命名列
    renamed_df = results_df.copy()
    renamed_df.columns = [column_mapping.get(col, col) for col in results_df.columns]

    # 创建主要统计表
    ws1 = add_dataframe_to_excel_with_grouped_headers(wb, renamed_df, 'API性能统计', header_groups=main_headers)

    # 高亮慢接口行
    for row_idx in range(2, len(renamed_df) + 2):
        if ws1.cell(row=row_idx, column=renamed_df.columns.get_loc('是否慢接口') + 1).value == 'Y':
            for col_idx in range(1, len(renamed_df.columns) + 1):
                ws1.cell(row=row_idx, column=col_idx).fill = HIGHLIGHT_FILL

    # 创建整体分析工作表
    ws2 = wb.create_sheet(title='整体API请求分析')

    # 修正变量名，使用正确的KB单位数据
    body_sizes = body_sizes_kb  # 已经是KB单位
    bytes_sizes = bytes_sizes_kb  # 已经是KB单位

    # 计算整体统计数据
    total_body_size_gb = round(sum(body_sizes) / (1024 * 1024), 3) if body_sizes and len(body_sizes) > 0 else 0
    total_bytes_size_gb = round(sum(bytes_sizes) / (1024 * 1024), 3) if bytes_sizes and len(bytes_sizes) > 0 else 0

    avg_response_time = round(sum(response_times) / len(response_times), 3) if response_times and len(
        response_times) > 0 else 0
    median_response_time = round(np.median(response_times), 3) if response_times and len(response_times) > 0 else 0
    p90_response_time = round(np.percentile(response_times, 90), 3) if response_times and len(response_times) > 0 else 0
    p95_response_time = round(np.percentile(response_times, 95), 3) if response_times and len(response_times) > 0 else 0
    p99_response_time = round(np.percentile(response_times, 99), 3) if response_times and len(response_times) > 0 else 0

    # 响应体大小统计（已经是KB单位）
    avg_body_size = round(sum(body_sizes) / len(body_sizes), 2) if body_sizes and len(body_sizes) > 0 else 0
    median_body_size = round(np.median(body_sizes), 2) if body_sizes and len(body_sizes) > 0 else 0
    p90_body_size = round(np.percentile(body_sizes, 90), 2) if body_sizes and len(body_sizes) > 0 else 0
    p95_body_size = round(np.percentile(body_sizes, 95), 2) if body_sizes and len(body_sizes) > 0 else 0
    p99_body_size = round(np.percentile(body_sizes, 99), 2) if body_sizes and len(body_sizes) > 0 else 0

    # 传输大小统计（已经是KB单位）
    avg_bytes_size = round(sum(bytes_sizes) / len(bytes_sizes), 2) if bytes_sizes and len(bytes_sizes) > 0 else 0
    median_bytes_size = round(np.median(bytes_sizes), 2) if bytes_sizes and len(bytes_sizes) > 0 else 0
    p90_bytes_size = round(np.percentile(bytes_sizes, 90), 2) if bytes_sizes and len(bytes_sizes) > 0 else 0
    p95_bytes_size = round(np.percentile(bytes_sizes, 95), 2) if bytes_sizes and len(bytes_sizes) > 0 else 0
    p99_bytes_size = round(np.percentile(bytes_sizes, 99), 2) if bytes_sizes and len(bytes_sizes) > 0 else 0

    # 传输速度统计
    avg_transfer_speed = round(np.mean(transfer_speeds), 2) if len(transfer_speeds) > 0 else 0
    median_transfer_speed = round(np.median(transfer_speeds), 2) if len(transfer_speeds) > 0 else 0
    p90_transfer_speed = round(np.percentile(transfer_speeds, 90), 2) if len(transfer_speeds) > 0 else 0
    p95_transfer_speed = round(np.percentile(transfer_speeds, 95), 2) if len(transfer_speeds) > 0 else 0

    # 处理效率指数统计
    avg_efficiency = round(np.mean(efficiency_scores), 2) if len(efficiency_scores) > 0 else 0
    median_efficiency = round(np.median(efficiency_scores), 2) if len(efficiency_scores) > 0 else 0
    p90_efficiency = round(np.percentile(efficiency_scores, 90), 2) if len(efficiency_scores) > 0 else 0

    # 阶段时间占比计算（使用新的阶段数据）
    total_phase_times = calculate_time_percentages(phase_times) if avg_response_time > 0 else {
        'backend_connect_phase': 0,
        'backend_process_phase': 0,
        'backend_transfer_phase': 0,
        'nginx_transfer_phase': 0
    }

    # 整体统计数据
    overall_stats = [
        ['=== 基础统计 ===', ''],
        ['总请求数', total_requests],
        ['成功请求数', success_requests],
        ['成功率(%)', round(success_requests / total_requests * 100, 2) if total_requests > 0 else 0],
        ['慢请求数', total_slow_requests],
        ['慢请求占比(%)', round(total_slow_requests / success_requests * 100, 2) if success_requests > 0 else 0],
        ['API数量', len(results_df)],
        ['', ''],

        ['=== 响应时间分析 ===', ''],
        ['平均响应时间(秒)', avg_response_time],
        ['最小响应时间(秒)', round(min(response_times), 3) if response_times and len(response_times) > 0 else 0],
        ['最大响应时间(秒)', round(max(response_times), 3) if response_times and len(response_times) > 0 else 0],
        ['中位数响应时间(秒)', median_response_time],
        ['P90响应时间(秒)', p90_response_time],
        ['P95响应时间(秒)', p95_response_time],
        ['P99响应时间(秒)', p99_response_time],
        ['', ''],

        ['=== 阶段时间占比 ===', ''],
        ['后端连接阶段占比(%)', total_phase_times.get('backend_connect_phase', 0)],
        ['后端处理阶段占比(%)', total_phase_times.get('backend_process_phase', 0)],
        ['后端传输阶段占比(%)', total_phase_times.get('backend_transfer_phase', 0)],
        ['Nginx传输阶段占比(%)', total_phase_times.get('nginx_transfer_phase', 0)],
        ['', ''],

        ['=== 数据传输分析 ===', ''],
        ['响应体总大小(GB)', total_body_size_gb],
        ['平均响应体大小(KB)', avg_body_size],
        ['中位数响应体大小(KB)', median_body_size],
        ['P90响应体大小(KB)', p90_body_size],
        ['P95响应体大小(KB)', p95_body_size],
        ['P99响应体大小(KB)', p99_body_size],
        ['', ''],

        ['总传输数据量(GB)', total_bytes_size_gb],
        ['平均传输大小(KB)', avg_bytes_size],
        ['中位数传输大小(KB)', median_bytes_size],
        ['P90传输大小(KB)', p90_bytes_size],
        ['P95传输大小(KB)', p95_bytes_size],
        ['P99传输大小(KB)', p99_bytes_size],
        ['', ''],

        ['=== 传输性能分析 ===', ''],
        ['平均传输速度(KB/s)', avg_transfer_speed],
        ['中位数传输速度(KB/s)', median_transfer_speed],
        ['P90传输速度(KB/s)', p90_transfer_speed],
        ['P95传输速度(KB/s)', p95_transfer_speed],
        ['', ''],

        ['=== 处理效率分析 ===', ''],
        ['平均处理效率指数', avg_efficiency],
        ['中位数处理效率指数', median_efficiency],
        ['P90处理效率指数', p90_efficiency],
        ['', ''],

        ['=== 性能阈值设置 ===', ''],
        ['慢请求阈值(秒)', DEFAULT_SLOW_THRESHOLD],
        ['慢请求占比阈值(%)', DEFAULT_SLOW_REQUESTS_THRESHOLD * 100],
    ]

    # 写入整体统计数据
    for row_idx, (label, value) in enumerate(overall_stats, start=1):
        cell_label = ws2.cell(row=row_idx, column=1, value=label)
        cell_value = ws2.cell(row=row_idx, column=2, value=value)

        # 设置标题行格式
        if label.startswith('===') and label.endswith('==='):
            cell_label.font = Font(bold=True, size=12)
            cell_value.font = Font(bold=True, size=12)

    # 设置列宽
    ws2.column_dimensions['A'].width = 30
    ws2.column_dimensions['B'].width = 20

    # 添加TOP API分析区域
    next_section_start = len(overall_stats) + 3

    # 请求量最多的10个API
    add_top_apis_section(ws2, results_df.sort_values(by='成功请求数', ascending=False),
                         next_section_start, "请求量最多的10个API",
                         ['API', '请求总数', '成功请求数', '占比(%)', '平均请求时间(秒)'],
                         ['请求URI', '请求总数', '成功请求数', '占总请求比例(%)', '平均请求时长(秒)'])

    # 响应时间最长的10个API
    add_top_apis_section(ws2, results_df.sort_values(by='平均请求时长(秒)', ascending=False),
                         next_section_start + 15, "响应时间最长的10个API",
                         ['API', '平均响应时间(秒)', 'P95响应时间(秒)', '请求总数', '成功请求数'],
                         ['请求URI', '平均请求时长(秒)', 'P95请求时长(秒)', '请求总数', '成功请求数'])

    # 响应体大小最大的10个API
    add_top_apis_section(ws2, results_df.sort_values(by='平均响应体大小(KB)', ascending=False),
                         next_section_start + 30, "响应体大小最大的10个API",
                         ['API', '平均响应体大小(KB)', 'P95响应体大小(KB)', '请求总数', '成功请求数'],
                         ['请求URI', '平均响应体大小(KB)', 'P95响应体大小(KB)', '请求总数', '成功请求数'])

    # 传输大小最大的10个API
    add_top_apis_section(ws2, results_df.sort_values(by='平均传输大小(KB)', ascending=False),
                         next_section_start + 45, "传输大小最大的10个API",
                         ['API', '平均传输大小(KB)', 'P95传输大小(KB)', '成功请求数'],
                         ['请求URI', '平均传输大小(KB)', 'P95传输大小(KB)', '成功请求数'])

    # 传输速度最低的10个API（新增）
    add_top_apis_section(ws2, results_df.sort_values(by='平均传输速度(KB/s)', ascending=True),
                         next_section_start + 60, "传输速度最低的10个API",
                         ['API', '平均传输速度(KB/s)', '平均响应体大小(KB)', '成功请求数'],
                         ['请求URI', '平均传输速度(KB/s)', '平均响应体大小(KB)', '成功请求数'])

    # 处理效率最低的10个API（新增）
    add_top_apis_section(ws2, results_df.sort_values(by='平均处理效率指数', ascending=True),
                         next_section_start + 75, "处理效率最低的10个API",
                         ['API', '平均处理效率指数', '后端处理时长(秒)', '成功请求数'],
                         ['请求URI', '平均处理效率指数', '后端处理时长(秒)', '成功请求数'])

    # 添加阶段时间分析工作表
    add_phase_time_analysis(wb, total_phase_times, response_times)

    # 添加性能瓶颈分析工作表（新增）
    add_performance_bottleneck_analysis(wb, results_df)

    # 格式化工作表
    format_excel_sheet(ws1)
    format_excel_sheet(ws2)

    log_info(f"Excel报告格式化完成，准备保存", show_memory=True)
    wb.save(output_path)
    log_info(f"Excel报告已保存: {output_path}", show_memory=True)


def add_performance_bottleneck_analysis(workbook, results_df):
    """添加性能瓶颈分析工作表"""
    ws = workbook.create_sheet(title='性能瓶颈分析')

    # 分析各类性能问题
    analysis_sections = []

    # 1. 网络连接问题API
    high_connect_apis = results_df[results_df['后端连接占比(%)'] > 20].sort_values(by='后端连接占比(%)', ascending=False)
    if not high_connect_apis.empty:
        analysis_sections.append({
            'title': '网络连接耗时过高的API (连接占比>20%)',
            'data': high_connect_apis[['请求URI', '后端连接时长(秒)', '后端连接占比(%)', '成功请求数']].head(10),
            'headers': ['API', '连接时长(秒)', '连接占比(%)', '请求数']
        })

    # 2. 后端处理缓慢API
    slow_process_apis = results_df[results_df['后端处理占比(%)'] > 60].sort_values(by='后端处理占比(%)', ascending=False)
    if not slow_process_apis.empty:
        analysis_sections.append({
            'title': '后端处理耗时过高的API (处理占比>60%)',
            'data': slow_process_apis[['请求URI', '后端处理时长(秒)', '后端处理占比(%)', '成功请求数']].head(10),
            'headers': ['API', '处理时长(秒)', '处理占比(%)', '请求数']
        })

    # 3. 数据传输问题API
    high_transfer_apis = results_df[results_df['后端传输占比(%)'] > 30].sort_values(by='后端传输占比(%)', ascending=False)
    if not high_transfer_apis.empty:
        analysis_sections.append({
            'title': '数据传输耗时过高的API (传输占比>30%)',
            'data': high_transfer_apis[['请求URI', '后端传输时长(秒)', '后端传输占比(%)', '平均响应体大小(KB)', '成功请求数']].head(10),
            'headers': ['API', '传输时长(秒)', '传输占比(%)', '响应体大小(KB)', '请求数']
        })

    # 4. 低效传输API
    if '平均传输速度(KB/s)' in results_df.columns:
        low_speed_apis = results_df[results_df['平均传输速度(KB/s)'] < 1000].sort_values(by='平均传输速度(KB/s)', ascending=True)
        if not low_speed_apis.empty:
            analysis_sections.append({
                'title': '传输速度过低的API (速度<1000KB/s)',
                'data': low_speed_apis[['请求URI', '平均传输速度(KB/s)', '平均响应体大小(KB)', '成功请求数']].head(10),
                'headers': ['API', '传输速度(KB/s)', '响应体大小(KB)', '请求数']
            })

    # 写入分析结果
    current_row = 1
    for section in analysis_sections:
        # 写入标题
        title_cell = ws.cell(row=current_row, column=1, value=section['title'])
        title_cell.font = Font(bold=True, size=12)
        current_row += 2

        # 写入表头
        for col_idx, header in enumerate(section['headers'], start=1):
            header_cell = ws.cell(row=current_row, column=col_idx, value=header)
            header_cell.font = Font(bold=True)
        current_row += 1

        # 写入数据
        for _, row_data in section['data'].iterrows():
            for col_idx, value in enumerate(row_data.values, start=1):
                ws.cell(row=current_row, column=col_idx, value=value)
            current_row += 1

        current_row += 2  # 空行分隔

    # 设置列宽
    for col in range(1, 6):
        ws.column_dimensions[chr(64 + col)].width = 25 if col == 1 else 15

    format_excel_sheet(ws)

def add_top_apis_section(worksheet, df, start_row, title, display_headers, data_columns):
    """添加TOP API分析区域"""
    worksheet.cell(row=start_row, column=1, value=title).font = Font(bold=True)

    # 获取前10个API数据
    top_apis = df.head(10)[data_columns].copy()
    top_apis.columns = display_headers

    # 添加表头
    for col_idx, header in enumerate(display_headers, start=1):
        worksheet.cell(row=start_row + 1, column=col_idx, value=header).font = Font(bold=True)
        worksheet.column_dimensions[chr(64 + col_idx)].width = 20 if col_idx == 1 else 15

    # 添加数据行
    for row_idx, (_, row) in enumerate(top_apis.iterrows(), start=start_row + 2):
        for col_idx, col_name in enumerate(display_headers, start=1):
            value = row[col_name] if col_name in row.index else ""
            cell = worksheet.cell(row=row_idx, column=col_idx, value=value)
            # 高亮慢请求
            if col_name == '平均响应时间(秒)' and value > DEFAULT_SLOW_THRESHOLD:
                cell.fill = HIGHLIGHT_FILL

def add_phase_time_analysis(workbook, total_phase_times, response_times):
    """添加阶段时间分析工作表"""
    if not total_phase_times:
        return

    # 阶段定义映射（基于新的参数体系）
    phases = {
        'backend_connect_phase': '后端连接阶段',
        'backend_process_phase': '后端处理阶段',
        'backend_transfer_phase': '后端传输阶段',
        'nginx_transfer_phase': 'Nginx传输阶段'
    }

    total_time = sum(response_times) / len(response_times) if response_times and len(response_times) > 0 else 0

    data = []
    for phase_key, phase_name in phases.items():
        percentage = total_phase_times.get(phase_key, 0)
        time_value = total_time * percentage / 100 if percentage > 0 else 0
        data.append([phase_name, round(percentage, 2), round(time_value, 3)])

    # 添加总计行
    total_percent = sum(percentage for phase_key, percentage in total_phase_times.items())
    data.append(['总计', round(total_percent, 2), round(total_time, 3)])

    phase_df = pd.DataFrame(data, columns=['请求阶段', '耗时占比(%)', '平均耗时(秒)'])
    ws = add_dataframe_to_excel_with_grouped_headers(workbook, phase_df, '阶段耗时分析')

    # 添加说明注释
    note_row = len(data) + 3
    note = ws.cell(row=note_row, column=1, value='注: 阶段耗时分析基于成功请求的平均耗时')
    note.font = Font(italic=True)

    format_excel_sheet(ws)
    ws.column_dimensions['A'].width = 25
    ws.column_dimensions['B'].width = 15
    ws.column_dimensions['C'].width = 15

def add_transfer_performance_analysis(workbook, results_df):
    """添加传输性能专项分析（基于新增参数）"""
    ws = workbook.create_sheet(title='传输性能分析')

    current_row = 1

    # 1. 传输速度分析
    ws.cell(row=current_row, column=1, value='=== 传输速度综合分析 ===').font = Font(bold=True, size=12)
    current_row += 2

    # 响应传输速度分析
    if 'response_transfer_speed' in results_df.columns:
        speed_stats = [
            ['响应传输速度统计', ''],
            ['平均响应传输速度(KB/s)', round(results_df['response_transfer_speed'].mean(), 2)],
            ['最低响应传输速度(KB/s)', round(results_df['response_transfer_speed'].min(), 2)],
            ['最高响应传输速度(KB/s)', round(results_df['response_transfer_speed'].max(), 2)],
            ['P50响应传输速度(KB/s)', round(results_df['response_transfer_speed'].median(), 2)],
            ['P90响应传输速度(KB/s)', round(results_df['response_transfer_speed'].quantile(0.9), 2)],
            ['P95响应传输速度(KB/s)', round(results_df['response_transfer_speed'].quantile(0.95), 2)],
            ['', '']
        ]

        for label, value in speed_stats:
            ws.cell(row=current_row, column=1, value=label).font = Font(bold=True) if label and not value else None
            ws.cell(row=current_row, column=2, value=value)
            current_row += 1

    # 总传输速度分析
    if 'total_transfer_speed' in results_df.columns:
        total_speed_stats = [
            ['总传输速度统计', ''],
            ['平均总传输速度(KB/s)', round(results_df['total_transfer_speed'].mean(), 2)],
            ['最低总传输速度(KB/s)', round(results_df['total_transfer_speed'].min(), 2)],
            ['最高总传输速度(KB/s)', round(results_df['total_transfer_speed'].max(), 2)],
            ['P50总传输速度(KB/s)', round(results_df['total_transfer_speed'].median(), 2)],
            ['P90总传输速度(KB/s)', round(results_df['total_transfer_speed'].quantile(0.9), 2)],
            ['P95总传输速度(KB/s)', round(results_df['total_transfer_speed'].quantile(0.95), 2)],
            ['', '']
        ]

        for label, value in total_speed_stats:
            ws.cell(row=current_row, column=1, value=label).font = Font(bold=True) if label and not value else None
            ws.cell(row=current_row, column=2, value=value)
            current_row += 1

    # 2. 连接成本分析
    current_row += 2
    ws.cell(row=current_row, column=1, value='=== 连接成本分析 ===').font = Font(bold=True, size=12)
    current_row += 2

    if 'connection_cost_ratio' in results_df.columns:
        connection_stats = [
            ['连接成本比例统计', ''],
            ['平均连接成本比例(%)', round(results_df['connection_cost_ratio'].mean(), 2)],
            ['最低连接成本比例(%)', round(results_df['connection_cost_ratio'].min(), 2)],
            ['最高连接成本比例(%)', round(results_df['connection_cost_ratio'].max(), 2)],
            ['P90连接成本比例(%)', round(results_df['connection_cost_ratio'].quantile(0.9), 2)],
            ['', ''],
            ['连接成本过高的API数量(>20%)', len(results_df[results_df['connection_cost_ratio'] > 20])],
            ['连接成本异常的API数量(>50%)', len(results_df[results_df['connection_cost_ratio'] > 50])],
            ['', '']
        ]

        for label, value in connection_stats:
            ws.cell(row=current_row, column=1, value=label).font = Font(bold=True) if label and not value else None
            ws.cell(row=current_row, column=2, value=value)
            current_row += 1

    # 3. 处理效率指数分析
    current_row += 2
    ws.cell(row=current_row, column=1, value='=== 处理效率指数分析 ===').font = Font(bold=True, size=12)
    current_row += 2

    if 'processing_efficiency_index' in results_df.columns:
        efficiency_stats = [
            ['处理效率指数统计', ''],
            ['平均处理效率指数', round(results_df['processing_efficiency_index'].mean(), 3)],
            ['最低处理效率指数', round(results_df['processing_efficiency_index'].min(), 3)],
            ['最高处理效率指数', round(results_df['processing_efficiency_index'].max(), 3)],
            ['P10处理效率指数', round(results_df['processing_efficiency_index'].quantile(0.1), 3)],
            ['P50处理效率指数', round(results_df['processing_efficiency_index'].quantile(0.5), 3)],
            ['P90处理效率指数', round(results_df['processing_efficiency_index'].quantile(0.9), 3)],
            ['', ''],
            ['低效率API数量(<0.5)', len(results_df[results_df['processing_efficiency_index'] < 0.5])],
            ['高效率API数量(>0.8)', len(results_df[results_df['processing_efficiency_index'] > 0.8])],
            ['', '']
        ]

        for label, value in efficiency_stats:
            ws.cell(row=current_row, column=1, value=label).font = Font(bold=True) if label and not value else None
            ws.cell(row=current_row, column=2, value=value)
            current_row += 1

    # 设置列宽
    ws.column_dimensions['A'].width = 30
    ws.column_dimensions['B'].width = 20

    # 添加传输性能问题API列表
    current_row += 3
    add_transfer_problem_apis(ws, current_row, results_df)

    format_excel_sheet(ws)

def add_transfer_problem_apis(worksheet, start_row, results_df):
    """添加传输性能问题API列表"""

    # 1. 传输速度过慢的API
    if 'response_transfer_speed' in results_df.columns:
        worksheet.cell(row=start_row, column=1, value='响应传输速度过慢的API (< 500KB/s)').font = Font(bold=True)
        start_row += 1

        slow_speed_apis = results_df[results_df['response_transfer_speed'] < 500].sort_values(
            by='response_transfer_speed', ascending=True).head(10)

        headers = ['API', '响应传输速度(KB/s)', '响应体大小(KB)', '后端传输时长(秒)', '请求数']
        columns = ['请求URI', 'response_transfer_speed', 'response_body_size_kb', 'backend_transfer_phase', '成功请求数']

        for col_idx, header in enumerate(headers, start=1):
            worksheet.cell(row=start_row, column=col_idx, value=header).font = Font(bold=True)
        start_row += 1

        for _, row in slow_speed_apis.iterrows():
            for col_idx, col_name in enumerate(columns, start=1):
                value = row.get(col_name, 0)
                worksheet.cell(row=start_row, column=col_idx, value=value)
            start_row += 1

        start_row += 2

    # 2. 连接成本过高的API
    if 'connection_cost_ratio' in results_df.columns:
        worksheet.cell(row=start_row, column=1, value='连接成本过高的API (> 30%)').font = Font(bold=True)
        start_row += 1

        high_cost_apis = results_df[results_df['connection_cost_ratio'] > 30].sort_values(
            by='connection_cost_ratio', ascending=False).head(10)

        headers = ['API', '连接成本比例(%)', '后端连接时长(秒)', '总请求时长(秒)', '请求数']
        columns = ['请求URI', 'connection_cost_ratio', 'backend_connect_phase', 'total_request_duration', '成功请求数']

        for col_idx, header in enumerate(headers, start=1):
            worksheet.cell(row=start_row, column=col_idx, value=header).font = Font(bold=True)
        start_row += 1

        for _, row in high_cost_apis.iterrows():
            for col_idx, col_name in enumerate(columns, start=1):
                value = row.get(col_name, 0)
                worksheet.cell(row=start_row, column=col_idx, value=value)
            start_row += 1

        start_row += 2

    # 3. 处理效率过低的API
    if 'processing_efficiency_index' in results_df.columns:
        worksheet.cell(row=start_row, column=1, value='处理效率过低的API (< 0.3)').font = Font(bold=True)
        start_row += 1

        low_efficiency_apis = results_df[results_df['processing_efficiency_index'] < 0.3].sort_values(
            by='processing_efficiency_index', ascending=True).head(10)

        headers = ['API', '处理效率指数', '后端处理时长(秒)', '总请求时长(秒)', '请求数']
        columns = ['请求URI', 'processing_efficiency_index', 'backend_process_phase', 'total_request_duration',
                   '成功请求数']

        for col_idx, header in enumerate(headers, start=1):
            worksheet.cell(row=start_row, column=col_idx, value=header).font = Font(bold=True)
        start_row += 1

        for _, row in low_efficiency_apis.iterrows():
            for col_idx, col_name in enumerate(columns, start=1):
                value = row.get(col_name, 0)
                worksheet.cell(row=start_row, column=col_idx, value=value)
            start_row += 1

def update_field_mapping_with_unified_units():
    """更新字段映射以适配统一的KB单位"""
    field_mapping = {
        'uri': 'request_full_uri',
        'app': 'application_name',
        'service': 'service_name',
        'status': 'response_status_code',
        'request_time': 'total_request_duration',
        'header_time': 'upstream_header_time',
        'connect_time': 'upstream_connect_time',
        'response_time': 'upstream_response_time',

        # 统一为KB单位的大小字段
        'body_bytes_kb': 'response_body_size_kb',
        'bytes_sent_kb': 'total_bytes_sent_kb',

        # 基于文档的新参数体系
        'backend_connect_phase': 'backend_connect_phase',
        'backend_process_phase': 'backend_process_phase',
        'backend_transfer_phase': 'backend_transfer_phase',
        'nginx_transfer_phase': 'nginx_transfer_phase',
        'backend_efficiency': 'backend_efficiency',
        'network_overhead': 'network_overhead',
        'transfer_ratio': 'transfer_ratio',

        # 新增的传输性能参数
        'response_transfer_speed': 'response_transfer_speed',
        'total_transfer_speed': 'total_transfer_speed',
        'nginx_transfer_speed': 'nginx_transfer_speed',
        'connection_cost_ratio': 'connection_cost_ratio',
        'processing_efficiency_index': 'processing_efficiency_index'
    }
    return field_mapping

def enhance_api_statistics_with_new_metrics(api_stats, group, field_mapping):
    """使用新指标增强API统计信息"""

    # 传输速度相关指标
    response_transfer_speeds = pd.to_numeric(group[field_mapping['response_transfer_speed']], errors='coerce') \
        if field_mapping['response_transfer_speed'] in group.columns else pd.Series([])
    total_transfer_speeds = pd.to_numeric(group[field_mapping['total_transfer_speed']], errors='coerce') \
        if field_mapping['total_transfer_speed'] in group.columns else pd.Series([])
    nginx_transfer_speeds = pd.to_numeric(group[field_mapping['nginx_transfer_speed']], errors='coerce') \
        if field_mapping['nginx_transfer_speed'] in group.columns else pd.Series([])

    # 效率指标
    connection_cost_ratios = pd.to_numeric(group[field_mapping['connection_cost_ratio']], errors='coerce') \
        if field_mapping['connection_cost_ratio'] in group.columns else pd.Series([])
    processing_efficiency_indices = pd.to_numeric(group[field_mapping['processing_efficiency_index']],
                                                  errors='coerce') \
        if field_mapping['processing_efficiency_index'] in group.columns else pd.Series([])

    api = group[field_mapping['uri']].iloc[0]

    # 将新指标添加到统计中
    if len(response_transfer_speeds) > 0:
        api_stats[api]['response_transfer_speed_values'] = response_transfer_speeds.dropna().tolist()
    if len(total_transfer_speeds) > 0:
        api_stats[api]['total_transfer_speed_values'] = total_transfer_speeds.dropna().tolist()
    if len(nginx_transfer_speeds) > 0:
        api_stats[api]['nginx_transfer_speed_values'] = nginx_transfer_speeds.dropna().tolist()
    if len(connection_cost_ratios) > 0:
        api_stats[api]['connection_cost_ratio_values'] = connection_cost_ratios.dropna().tolist()
    if len(processing_efficiency_indices) > 0:
        api_stats[api]['processing_efficiency_index_values'] = processing_efficiency_indices.dropna().tolist()

def generate_enhanced_api_statistics_with_new_metrics(api_stats, success_requests, total_slow_requests):
    """生成包含新指标的增强API统计信息"""
    results = []

    for api, stats in api_stats.items():
        # ... 原有的基础统计计算代码保持不变 ...

        # 新增传输速度指标统计
        response_speeds = np.array(stats.get('response_transfer_speed_values', []))
        total_speeds = np.array(stats.get('total_transfer_speed_values', []))
        nginx_speeds = np.array(stats.get('nginx_transfer_speed_values', []))
        connection_costs = np.array(stats.get('connection_cost_ratio_values', []))
        efficiency_indices = np.array(stats.get('processing_efficiency_index_values', []))

        # 添加新的统计字段到结果中
        enhanced_metrics = {}

        if len(response_speeds) > 0:
            enhanced_metrics.update({
                '平均响应传输速度(KB/s)': round(np.mean(response_speeds), 2),
                'P10响应传输速度(KB/s)': round(np.percentile(response_speeds, 10), 2),
                'P90响应传输速度(KB/s)': round(np.percentile(response_speeds, 90), 2),
            })

        if len(total_speeds) > 0:
            enhanced_metrics.update({
                '平均总传输速度(KB/s)': round(np.mean(total_speeds), 2),
                'P10总传输速度(KB/s)': round(np.percentile(total_speeds, 10), 2),
                'P90总传输速度(KB/s)': round(np.percentile(total_speeds, 90), 2),
            })

        if len(nginx_speeds) > 0:
            enhanced_metrics.update({
                '平均Nginx传输速度(KB/s)': round(np.mean(nginx_speeds), 2),
                'P90Nginx传输速度(KB/s)': round(np.percentile(nginx_speeds, 90), 2),
            })

        if len(connection_costs) > 0:
            enhanced_metrics.update({
                '平均连接成本比例(%)': round(np.mean(connection_costs), 2),
                'P90连接成本比例(%)': round(np.percentile(connection_costs, 90), 2),
                '连接成本过高(>30%)': '是' if np.mean(connection_costs) > 30 else '否',
            })

        if len(efficiency_indices) > 0:
            enhanced_metrics.update({
                '平均处理效率指数': round(np.mean(efficiency_indices), 3),
                'P10处理效率指数': round(np.percentile(efficiency_indices, 10), 3),
                'P90处理效率指数': round(np.percentile(efficiency_indices, 90), 3),
                '处理效率等级': classify_processing_efficiency(np.mean(efficiency_indices)),
            })

        # 将增强指标合并到结果中
        result.update(enhanced_metrics)
        results.append(result)

    return results

def classify_processing_efficiency(efficiency_index):
    """根据处理效率指数进行分级"""
    if efficiency_index >= 0.8:
        return '优秀'
    elif efficiency_index >= 0.6:
        return '良好'
    elif efficiency_index >= 0.4:
        return '一般'
    elif efficiency_index >= 0.2:
        return '较差'
    else:
        return '极差'