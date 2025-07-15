import os
import json
import glob
import csv
import re
from datetime import datetime, timedelta

from self_00_02_utils import log_info, extract_app_name, extract_service_from_path
from self_00_01_constants import (
    DEFAULT_BATCH_SIZE, DEFAULT_LOG_DIR, DEFAULT_底座_LOG_DIR,
    LOG_TYPE_SELF_DEVELOPED, LOG_TYPE_BASE, LOG_TYPE_AUTO,
    DEFAULT_START_DATE, DEFAULT_END_DATE, ESTIMATED_HEADER_SIZE
)


def is_date_in_range(date_str, start_date=None, end_date=None):
    """检查日期是否在指定范围内"""
    if not date_str or (not start_date and not end_date):
        return True

    try:
        # 尝试解析带时分秒的日期格式
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            # 如果失败，尝试只解析日期部分
            date = datetime.strptime(date_str.split()[0], '%Y-%m-%d')

        if start_date:
            try:
                start = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                # 如果只提供了日期，设置时间为当天开始
                start = datetime.strptime(start_date.split()[0], '%Y-%m-%d')
            if date < start:
                return False

        if end_date:
            try:
                end = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                # 如果只提供了日期，设置时间为当天结束
                end = datetime.strptime(end_date.split()[0], '%Y-%m-%d')
                end = end + timedelta(days=1, microseconds=-1)
            if date > end:
                return False

        return True
    except Exception as e:
        log_info(f"日期范围检查出错: {e}", level="ERROR")
        return True  # 如果解析出错，默认包含该记录


def detect_log_type(file_path):
    """自动检测日志类型"""
    with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
        for _ in range(5):  # 读取前5行进行检测
            line = file.readline().strip()
            if not line:
                continue

            # 尝试解析为JSON格式（自研日志）
            try:
                json.loads(line)
                return LOG_TYPE_SELF_DEVELOPED
            except json.JSONDecodeError:
                # 检查是否包含底座日志特征
                if 'http_host:' in line and 'remote_addr:' in line:
                    return LOG_TYPE_BASE

    # 无法确定类型时的默认处理
    log_info(f"无法确定日志类型，默认按自研处理: {file_path}", level="WARNING")
    return LOG_TYPE_SELF_DEVELOPED


def normalize_api_path(api_path):
    """标准化API路径，移除查询字符串"""
    return api_path.split('?')[0] if api_path else api_path


def extract_query_string(api_path):
    """从API路径中提取查询字符串"""
    parts = api_path.split('?', 1)
    return parts[1] if len(parts) > 1 else ""


def calculate_http_lifecycle_metrics(row_data):
    """
    计算HTTP生命周期相关的性能指标
    根据HTTP请求生命周期，计算各个阶段的时间和性能比率
    """
    # 获取基础时间参数（单位：秒）
    request_time = float(row_data.get('total_request_duration', 0) or 0)
    upstream_response_time = float(row_data.get('upstream_response_time', 0) or 0)
    upstream_header_time = float(row_data.get('upstream_header_time', 0) or 0)
    upstream_connect_time = float(row_data.get('upstream_connect_time', 0) or 0)

    # 获取数据大小参数（转换为KB）
    response_body_size_kb = float(row_data.get('response_body_size', 0) or 0) / 1024
    total_bytes_sent_kb = float(row_data.get('total_bytes_sent', 0) or 0) / 1024

    # === 核心阶段参数（按请求流程顺序）===
    # 后端连接阶段：建立与后端服务的连接时间
    backend_connect_phase = upstream_connect_time

    # 后端处理阶段：后端处理请求并生成响应头的时间
    backend_process_phase = max(0, upstream_header_time - upstream_connect_time)

    # 后端传输阶段：后端传输响应体的时间
    backend_transfer_phase = max(0, upstream_response_time - upstream_header_time)

    # Nginx传输阶段：Nginx向客户端传输响应的时间
    nginx_transfer_phase = max(0, request_time - upstream_response_time)

    # === 组合分析参数 ===
    # 后端总阶段：后端处理的全部时间
    backend_total_phase = upstream_response_time

    # 网络传输阶段：网络连接和传输的总时间
    network_phase = upstream_connect_time + nginx_transfer_phase

    # 纯处理阶段：业务逻辑处理时间（等同于backend_process_phase）
    processing_phase = backend_process_phase

    # 纯传输阶段：所有数据传输时间
    transfer_phase = backend_transfer_phase + nginx_transfer_phase

    # === 性能比率参数（百分比）===
    # 避免除零错误的安全计算函数
    def safe_ratio(numerator, denominator, default=0):
        return (numerator / denominator * 100) if denominator > 0 else default

    # 后端处理效率：后端处理时间占后端总时间的比例
    backend_efficiency = safe_ratio(backend_process_phase, backend_total_phase)

    # 网络开销占比：网络相关时间占总请求时间的比例
    network_overhead = safe_ratio(network_phase, request_time)

    # 传输时间占比：数据传输时间占总请求时间的比例
    transfer_ratio = safe_ratio(transfer_phase, request_time)

    # === 数据传输效率参数 ===
    # 响应体传输速度（KB/秒）
    response_transfer_speed = safe_ratio(response_body_size_kb, backend_transfer_phase, 0)

    # 总体传输速度（KB/秒）
    total_transfer_speed = safe_ratio(total_bytes_sent_kb, transfer_phase, 0)

    # Nginx传输效率（KB/秒）
    nginx_transfer_speed = safe_ratio(total_bytes_sent_kb, nginx_transfer_phase, 0)

    # === 连接复用效率参数 ===
    # 连接建立成本：连接时间占后端总时间的比例
    connection_cost_ratio = safe_ratio(backend_connect_phase, backend_total_phase)

    # 处理效率指数：处理时间与连接时间的比值
    processing_efficiency_index = safe_ratio(backend_process_phase, backend_connect_phase, 1)

    # 将计算结果添加到行数据中
    lifecycle_metrics = {
        # === 核心阶段参数 ===
        'backend_connect_phase': round(backend_connect_phase, 6),
        'backend_process_phase': round(backend_process_phase, 6),
        'backend_transfer_phase': round(backend_transfer_phase, 6),
        'nginx_transfer_phase': round(nginx_transfer_phase, 6),

        # === 组合分析参数 ===
        'backend_total_phase': round(backend_total_phase, 6),
        'network_phase': round(network_phase, 6),
        'processing_phase': round(processing_phase, 6),
        'transfer_phase': round(transfer_phase, 6),

        # === 性能比率参数（百分比）===
        'backend_efficiency': round(backend_efficiency, 2),
        'network_overhead': round(network_overhead, 2),
        'transfer_ratio': round(transfer_ratio, 2),

        # === 数据传输效率参数 ===
        'response_body_size_kb': round(response_body_size_kb, 3),
        'total_bytes_sent_kb': round(total_bytes_sent_kb, 3),
        'response_transfer_speed': round(response_transfer_speed, 2),
        'total_transfer_speed': round(total_transfer_speed, 2),
        'nginx_transfer_speed': round(nginx_transfer_speed, 2),

        # === 连接复用效率参数 ===
        'connection_cost_ratio': round(connection_cost_ratio, 2),
        'processing_efficiency_index': round(processing_efficiency_index, 2)
    }

    return lifecycle_metrics


def parse_log_line(line, source_file, app_name, log_type=LOG_TYPE_SELF_DEVELOPED):
    """解析日志行，统一输出格式"""
    if not line or not line.strip():
        return None

    try:
        if log_type == LOG_TYPE_SELF_DEVELOPED:
            return parse_self_developed_log(line, source_file, app_name)
        elif log_type == LOG_TYPE_BASE:
            return parse_base_log(line, source_file)
        else:
            # 如果未指定类型，根据内容尝试判断
            try:
                return parse_self_developed_log(line, source_file, app_name)
            except:
                return parse_base_log(line, source_file)
    except Exception as e:
        error_msg = str(e)
        log_info(f"解析日志行出错: {error_msg}, line:{line[:100]}...", level="ERROR")
        return None


def parse_self_developed_log(line, source_file, app_name):
    """解析自研日志格式"""
    log_data = json.loads(line)

    # 基础字段
    row_data = {
        'log_source_file': source_file,
        'application_name': app_name,
        'raw_time': None,
        'raw_timestamp': log_data.get('timestamp'),
        'total_request_duration': float(log_data.get('request_time', 0) or 0),
        'client_ip_address': log_data.get('client_ip'),
        'client_port_number': log_data.get('client_port'),
        'http_method': log_data.get('request_method'),
        'request_full_uri': normalize_api_path(log_data.get('request_uri')),
        'request_path': log_data.get('request_path'),
        'query_parameters': log_data.get('query_string'),
        'http_protocol_version': log_data.get('request_protocol'),
        'response_status_code': log_data.get('status'),
        'response_body_size': int(log_data.get('body_bytes_sent', 0) or 0),
        'total_bytes_sent': int(log_data.get('bytes_sent', 0) or 0),
        'response_content_type': log_data.get('content_type'),
        'upstream_connect_time': float(log_data.get('upstream_connect_time', 0) or 0),
        'upstream_header_time': float(log_data.get('upstream_header_time', 0) or 0),
        'upstream_response_time': float(log_data.get('upstream_response_time', 0) or 0),
        'upstream_server_address': log_data.get('upstream_addr'),
        'upstream_status_code': log_data.get('upstream_status'),
        'server_name': log_data.get('server_name'),
        'host_header': log_data.get('host'),
        'user_agent_string': log_data.get('user_agent'),
        'referer_url': log_data.get('referer'),
    }

    # 提取服务名称
    row_data['service_name'] = extract_service_from_path(row_data['request_path'])

    # 处理时间相关字段
    if 'time' in log_data and log_data['time']:
        time_str = log_data['time'].replace('T', ' ').replace('+08:00', '')
        row_data['raw_time'] = time_str

        try:
            dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')

            # 基本时间维度
            row_data['date'] = dt.strftime('%Y-%m-%d')
            row_data['hour'] = dt.hour
            row_data['minute'] = dt.minute
            row_data['second'] = dt.second

            # 组合时间维度
            row_data['date_hour'] = dt.strftime('%Y-%m-%d %H')
            row_data['date_hour_minute'] = dt.strftime('%Y-%m-%d %H:%M')
            row_data['date_hour_minute_second'] = dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            # 设置所有时间维度为空
            for field in ['date', 'hour', 'minute', 'second',
                          'date_hour', 'date_hour_minute', 'date_hour_minute_second']:
                row_data[field] = None

    # 计算请求到达时间
    if row_data['raw_timestamp'] and row_data['total_request_duration'] is not None:
        try:
            ts = float(row_data['raw_timestamp'])
            req_time = float(row_data['total_request_duration'])
            row_data['arrival_timestamp'] = ts - req_time
            arrival_dt = datetime.fromtimestamp(row_data['arrival_timestamp'])
            row_data['arrival_time'] = arrival_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

            # 到达时间维度
            row_data['arrival_date'] = arrival_dt.strftime('%Y-%m-%d')
            row_data['arrival_hour'] = arrival_dt.hour
            row_data['arrival_minute'] = arrival_dt.minute
            row_data['arrival_second'] = arrival_dt.second

            # 组合到达时间维度
            row_data['arrival_date_hour'] = arrival_dt.strftime('%Y-%m-%d %H')
            row_data['arrival_date_hour_minute'] = arrival_dt.strftime('%Y-%m-%d %H:%M')
            row_data['arrival_date_hour_minute_second'] = arrival_dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            # 设置所有到达时间维度为空
            for field in ['arrival_timestamp', 'arrival_time', 'arrival_date', 'arrival_hour',
                          'arrival_minute', 'arrival_second', 'arrival_date_hour',
                          'arrival_date_hour_minute', 'arrival_date_hour_minute_second']:
                row_data[field] = None

    # 计算原有的请求处理各阶段耗时（保持向后兼容）
    row_data['phase_upstream_connect'] = row_data.get('upstream_connect_time', 0)

    upstream_header = row_data.get('upstream_header_time', 0)
    upstream_connect = row_data.get('upstream_connect_time', 0)
    row_data['phase_upstream_header'] = max(0, upstream_header - upstream_connect)

    upstream_response = row_data.get('upstream_response_time', 0)
    row_data['phase_upstream_body'] = max(0, upstream_response - upstream_header)

    request_time = row_data.get('total_request_duration', 0)
    row_data['phase_client_transfer'] = max(0, request_time - upstream_response)

    # **新增：计算HTTP生命周期性能指标**
    lifecycle_metrics = calculate_http_lifecycle_metrics(row_data)
    row_data.update(lifecycle_metrics)

    return row_data


def extract_value(text, key):
    """从底座日志中提取特定键的值"""
    pattern = f'{key}:"([^"]*)"'
    match = re.search(pattern, text)
    return match.group(1) if match else None


def process_time(time_str):
    """处理时间字符串格式"""
    return time_str.replace('+08:00', '').replace('T', ' ')


def process_request(request_str):
    """处理请求字符串，分离方法、URI和协议"""
    parts = request_str.split()
    if len(parts) >= 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        return parts[0], parts[1], ''
    elif len(parts) == 1:
        return parts[0], '', ''
    return '', '', ''


def extract_service_info(api_path):
    """从API路径提取服务信息"""
    if not api_path or not api_path.startswith('/'):
        return "", ""

    parts = api_path.strip('/').split('/')
    if len(parts) > 0:
        primary_service = parts[0]
        secondary_service = parts[1] if len(parts) > 1 else ""
        return primary_service, secondary_service
    return "", ""


def parse_base_log(line, source_file):
    """解析底座日志格式，并转换为统一的格式"""
    if 'http_host' not in line:
        return None

    # 提取底座日志字段
    http_host = extract_value(line, 'http_host')
    remote_addr = extract_value(line, 'remote_addr')
    remote_port = extract_value(line, 'remote_port')
    time_value = extract_value(line, 'time')
    request_value = extract_value(line, 'request')
    code = extract_value(line, 'code')
    body = extract_value(line, 'body')
    http_referer = extract_value(line, 'http_referer')
    ar_time = extract_value(line, 'ar_time')
    real_ip = extract_value(line, 'RealIp')
    agent = extract_value(line, 'agent')

    # 处理时间相关字段
    raw_time = None
    date = None
    hour = None
    minute = None
    second = None
    timestamp = None
    date_hour = None
    date_hour_minute = None
    date_hour_minute_second = None

    if time_value:
        raw_time = process_time(time_value)
        try:
            dt = datetime.strptime(raw_time, '%Y-%m-%d %H:%M:%S')
            date = dt.strftime('%Y-%m-%d')
            hour = dt.hour
            minute = dt.minute
            second = dt.second
            timestamp = dt.timestamp()

            # 组合时间维度
            date_hour = dt.strftime('%Y-%m-%d %H')
            date_hour_minute = dt.strftime('%Y-%m-%d %H:%M')
            date_hour_minute_second = dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            pass

    # 处理请求
    request_method = None
    request_uri = None
    request_protocol = None

    if request_value:
        request_method, request_uri, request_protocol = process_request(request_value)

    # 提取服务信息
    normalized_uri = normalize_api_path(request_uri) if request_uri else ""
    query_string = extract_query_string(request_uri) if request_uri else ""
    primary_service, secondary_service = extract_service_info(normalized_uri)

    # 计算请求到达时间
    request_time = float(ar_time or 0)
    arrival_timestamp = None
    arrival_time = None
    arrival_date = None
    arrival_hour = None
    arrival_minute = None
    arrival_second = None
    arrival_date_hour = None
    arrival_date_hour_minute = None
    arrival_date_hour_minute_second = None

    if timestamp is not None and request_time > 0:
        arrival_timestamp = timestamp - request_time
        arrival_dt = datetime.fromtimestamp(arrival_timestamp)
        arrival_time = arrival_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        # 到达时间维度
        arrival_date = arrival_dt.strftime('%Y-%m-%d')
        arrival_hour = arrival_dt.hour
        arrival_minute = arrival_dt.minute
        arrival_second = arrival_dt.second

        # 组合到达时间维度
        arrival_date_hour = arrival_dt.strftime('%Y-%m-%d %H')
        arrival_date_hour_minute = arrival_dt.strftime('%Y-%m-%d %H:%M')
        arrival_date_hour_minute_second = arrival_dt.strftime('%Y-%m-%d %H:%M:%S')

    # 构建统一格式的数据
    row_data = {
        'log_source_file': source_file,
        'application_name': primary_service,
        'raw_time': raw_time,
        'raw_timestamp': str(timestamp) if timestamp else None,
        'total_request_duration': request_time,
        'client_ip_address': real_ip or remote_addr,
        'client_port_number': remote_port,
        'http_method': request_method,
        'request_full_uri': normalized_uri,
        'request_path': normalized_uri,
        'query_parameters': query_string,
        'http_protocol_version': request_protocol,
        'response_status_code': code,
        'response_body_size': int(body or 0),
        'total_bytes_sent': int(body or 0) + ESTIMATED_HEADER_SIZE,
        'response_content_type': "",
        'upstream_connect_time': 0,
        'upstream_header_time': 0,
        'upstream_response_time': request_time,
        'upstream_server_address': "",
        'upstream_status_code': "",
        'server_name': http_host,
        'host_header': http_host,
        'user_agent_string': agent,
        'referer_url': http_referer,
        'service_name': secondary_service,

        # 时间维度
        'date': date,
        'hour': hour,
        'minute': minute,
        'second': second,
        'date_hour': date_hour,
        'date_hour_minute': date_hour_minute,
        'date_hour_minute_second': date_hour_minute_second,

        # 到达时间维度
        'arrival_timestamp': arrival_timestamp,
        'arrival_time': arrival_time,
        'arrival_date': arrival_date,
        'arrival_hour': arrival_hour,
        'arrival_minute': arrival_minute,
        'arrival_second': arrival_second,
        'arrival_date_hour': arrival_date_hour,
        'arrival_date_hour_minute': arrival_date_hour_minute,
        'arrival_date_hour_minute_second': arrival_date_hour_minute_second,

        # 处理阶段耗时
        'phase_upstream_connect': 0,
        'phase_upstream_header': 0,
        'phase_upstream_body': request_time,
        'phase_client_transfer': 0
    }

    # **新增：计算HTTP生命周期性能指标**
    lifecycle_metrics = calculate_http_lifecycle_metrics(row_data)
    row_data.update(lifecycle_metrics)

    return row_data


def process_log_file_generator(file_path, log_type=LOG_TYPE_AUTO, start_date=None, end_date=None):
    """处理单个日志文件，生成统一格式的记录"""
    source_file = os.path.basename(file_path)
    app_name = extract_app_name(source_file)
    line_count = 0
    error_count = 0
    filtered_count = 0

    # 如果是自动检测，先确定日志类型
    if log_type == LOG_TYPE_AUTO:
        log_type = detect_log_type(file_path)
        log_info(f"日志类型自动检测结果: {file_path} -> {log_type}")

    with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue

            row_data = parse_log_line(line, source_file, app_name, log_type)
            if row_data:
                # 检查日期范围过滤
                if not is_date_in_range(row_data.get('date'), start_date, end_date):
                    filtered_count += 1
                    continue

                line_count += 1
                yield row_data
            else:
                error_count += 1

            if (line_count + error_count) % 100000 == 0:
                log_info(f"处理中: {source_file} - 已解析 {line_count:,} 行，跳过 {error_count:,} 行，过滤 {filtered_count:,} 行")

    log_info(f"从 {source_file} 中处理了 {line_count:,} 条记录，跳过了 {error_count:,} 条无效记录，过滤了 {filtered_count:,} 条范围外记录")


def collect_log_files(log_dir):
    """收集日志文件列表"""
    log_files = []

    if os.path.isdir(log_dir):
        log_files = glob.glob(os.path.join(log_dir, "*.log"))
        log_info(f"从目录 '{log_dir}' 中找到 {len(log_files)} 个日志文件")
    else:
        if os.path.isfile(log_dir) and log_dir.endswith('.log'):
            log_files = [log_dir]
            log_info(f"使用单个日志文件: {log_dir}")
        else:
            log_files = glob.glob("*.log")
            log_info(f"从当前目录找到 {len(log_files)} 个日志文件")

    return log_files


def batch_save_to_csv(data_iterator, csv_path, batch_size=DEFAULT_BATCH_SIZE):
    """批量保存数据到CSV文件"""
    total_count = 0
    header_written = False
    start_time = datetime.now()
    log_info(f"开始保存数据到CSV: {csv_path}")

    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = None
        batch = []

        for row in data_iterator:
            batch.append(row)
            total_count += 1

            if len(batch) >= batch_size:
                if not header_written:
                    writer = csv.DictWriter(csvfile, fieldnames=batch[0].keys())
                    writer.writeheader()
                    header_written = True

                writer.writerows(batch)
                batch = []

                if total_count % 100000 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    speed = total_count / elapsed if elapsed > 0 else 0
                    log_info(f"已处理 {total_count:,} 条记录 (速度: {speed:.2f} 条/秒)", show_memory=True)

        if batch:
            if not header_written and batch:
                writer = csv.DictWriter(csvfile, fieldnames=batch[0].keys())
                writer.writeheader()
            writer.writerows(batch)

    elapsed = (datetime.now() - start_time).total_seconds()
    log_info(f"CSV保存完成: {csv_path} (总计: {total_count:,} 条记录, 耗时: {elapsed:.2f} 秒)")
    return total_count


def process_log_files(log_files, output_csv, log_type=LOG_TYPE_AUTO, start_date=None, end_date=None):
    """处理多个日志文件并输出到CSV"""
    total_records = 0
    start_time = datetime.now()

    if start_date or end_date:
        date_range_info = f"日期范围过滤: {start_date or '不限'} 至 {end_date or '不限'}"
        log_info(date_range_info)

    for i, log_file in enumerate(log_files, 1):
        file_start_time = datetime.now()
        log_info(f"[{i}/{len(log_files)}] 处理文件: {os.path.basename(log_file)}")

        data_iterator = process_log_file_generator(log_file, log_type, start_date, end_date)

        if i == 1:
            count = batch_save_to_csv(data_iterator, output_csv)
        else:
            with open(output_csv, 'a', newline='', encoding='utf-8') as csvfile:
                writer = None
                batch = []
                batch_size = DEFAULT_BATCH_SIZE
                file_count = 0

                for row in data_iterator:
                    batch.append(row)
                    file_count += 1

                    if len(batch) >= batch_size:
                        if writer is None:
                            writer = csv.DictWriter(csvfile, fieldnames=batch[0].keys())
                        writer.writerows(batch)
                        batch = []

                        if file_count % 100000 == 0:
                            elapsed = (datetime.now() - file_start_time).total_seconds()
                            speed = file_count / elapsed if elapsed > 0 else 0
                            log_info(f"文件进度: {file_count:,} 条记录 (速度: {speed:.2f} 条/秒)", show_memory=True)

                if batch:
                    if writer is None and batch:
                        writer = csv.DictWriter(csvfile, fieldnames=batch[0].keys())
                    writer.writerows(batch)

                count = file_count

        file_elapsed = (datetime.now() - file_start_time).total_seconds()
        total_records += count
        log_info(f"文件处理完成: {os.path.basename(log_file)} ({count:,} 条记录, 耗时: {file_elapsed:.2f} 秒)")
        log_info(f"累计处理记录数: {total_records:,} 条", show_memory=True)

    total_elapsed = (datetime.now() - start_time).total_seconds()
    avg_speed = total_records / total_elapsed if total_elapsed > 0 else 0
    log_info(f"全部日志处理完成: {len(log_files)} 个文件, {total_records:,} 条记录 (平均速度: {avg_speed:.2f} 条/秒)")

    return total_records

def main(log_dir=None, log_type=LOG_TYPE_AUTO, output_dir=None, start_date=None, end_date=None):
    """主函数，处理日志文件"""
    script_start_time = datetime.now()
    log_info(f"开始执行统一日志分析任务 (版本: 3.0.0)", show_memory=True)

    # 使用参数或默认值
    log_dir = log_dir or DEFAULT_LOG_DIR

    # 如果未指定输出目录，创建基于日期时间的目录
    if not output_dir:
        timestamp = script_start_time.strftime('%Y%m%d_%H%M%S')
        output_dir = f"{log_dir}_统一分析结果_{timestamp}"

    temp_dir = f"{output_dir}_temp"  # 临时文件目录

    # 创建目录
    for directory in [output_dir, temp_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)
            log_info(f"创建目录: {directory}")

    # 收集日志文件
    log_files = collect_log_files(log_dir)
    if not log_files:
        log_info("未找到日志文件!", level="ERROR")
        return

    log_info(f"找到 {len(log_files)} 个日志文件，使用日志类型: {log_type}")

    # 设置输出CSV路径
    temp_csv = os.path.join(temp_dir, "processed_logs.csv")

    # 日期范围过滤
    start_date = start_date or DEFAULT_START_DATE
    end_date = end_date or DEFAULT_END_DATE

    # 处理日志文件
    total_records = process_log_files(log_files, temp_csv, log_type, start_date, end_date)

    log_info(f"日志处理完成，输出到: {temp_csv}")
    return temp_csv

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='统一日志分析工具 v3.0.0')
    parser.add_argument('--log_dir', '-d', type=str, help='日志文件目录或文件路径')
    parser.add_argument('--log_type', '-t', type=str,
                        choices=[LOG_TYPE_SELF_DEVELOPED, LOG_TYPE_BASE, LOG_TYPE_AUTO],
                        default=LOG_TYPE_AUTO, help='日志类型')
    parser.add_argument('--output_dir', '-o', type=str, help='输出目录')
    parser.add_argument('--start_date', '-s', type=str, help='开始日期 (YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end_date', '-e', type=str, help='结束日期 (YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS)')

    args = parser.parse_args()

    try:
        result_csv = main(
            log_dir=args.log_dir,
            log_type=args.log_type,
            output_dir=args.output_dir,
            start_date=args.start_date,
            end_date=args.end_date
        )
        if result_csv:
            log_info(f"任务执行成功，结果文件: {result_csv}")
    except Exception as e:
        log_info(f"任务执行失败: {str(e)}", level="ERROR")
        raise
