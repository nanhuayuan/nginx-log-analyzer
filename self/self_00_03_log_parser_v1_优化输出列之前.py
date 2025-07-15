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
    row_data = {
        'source': source_file,
        'app_name': app_name,
        'time': None,
        'timestamp': log_data.get('timestamp'),
        'request_time': float(log_data.get('request_time', 0) or 0),
        'client_ip': log_data.get('client_ip'),
        'client_port': log_data.get('client_port'),
        'request_method': log_data.get('request_method'),
        'request_uri': normalize_api_path(log_data.get('request_uri')),
        'request_path': log_data.get('request_path'),
        'query_string': log_data.get('query_string'),
        'request_protocol': log_data.get('request_protocol'),
        'status': log_data.get('status'),
        'body_bytes_sent': int(log_data.get('body_bytes_sent', 0) or 0),
        'bytes_sent': int(log_data.get('bytes_sent', 0) or 0),
        'content_type': log_data.get('content_type'),
        'upstream_connect_time': float(log_data.get('upstream_connect_time', 0) or 0),
        'upstream_header_time': float(log_data.get('upstream_header_time', 0) or 0),
        'upstream_response_time': float(log_data.get('upstream_response_time', 0) or 0),
        'upstream_addr': log_data.get('upstream_addr'),
        'upstream_status': log_data.get('upstream_status'),
        'server_name': log_data.get('server_name'),
        'host': log_data.get('host'),
        'user_agent': log_data.get('user_agent'),
        'referer': log_data.get('referer'),
    }
    row_data['service_name'] = extract_service_from_path(row_data['request_path'])

    if 'time' in log_data and log_data['time']:
        time_str = log_data['time'].replace('T', ' ').replace('+08:00', '')
        row_data['time'] = time_str
        try:
            dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            row_data['date'] = dt.strftime('%Y-%m-%d')
            row_data['hour'] = dt.hour
            row_data['minute'] = dt.minute
            row_data['second'] = dt.second
        except Exception:
            row_data['date'] = None
            row_data['hour'] = None
            row_data['minute'] = None
            row_data['second'] = None

    if row_data['timestamp'] and row_data['request_time'] is not None:
        try:
            ts = float(row_data['timestamp'])
            req_time = float(row_data['request_time'])
            row_data['arrival_timestamp'] = ts - req_time
            arrival_dt = datetime.fromtimestamp(row_data['arrival_timestamp'])
            row_data['arrival_time'] = arrival_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        except (ValueError, TypeError):
            row_data['arrival_timestamp'] = None
            row_data['arrival_time'] = None

    row_data['upstream_connect_phase'] = row_data.get('upstream_connect_time', 0)

    upstream_header = row_data.get('upstream_header_time', 0)
    upstream_connect = row_data.get('upstream_connect_time', 0)
    row_data['upstream_header_phase'] = max(0, upstream_header - upstream_connect)

    upstream_response = row_data.get('upstream_response_time', 0)
    row_data['upstream_body_phase'] = max(0, upstream_response - upstream_header)

    request_time = row_data.get('request_time', 0)
    row_data['client_transfer_phase'] = max(0, request_time - upstream_response)

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
    """解析底座日志格式，并转换为统一的自研格式"""
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

    # 处理时间
    time_str = None
    date = None
    hour = None
    minute = None
    second = None
    timestamp = None

    if time_value:
        time_str = process_time(time_value)
        try:
            dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            date = dt.strftime('%Y-%m-%d')
            hour = dt.hour
            minute = dt.minute
            second = dt.second
            # 将时间转换为时间戳
            timestamp = dt.timestamp()
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

    if timestamp is not None and request_time > 0:
        arrival_timestamp = timestamp - request_time
        arrival_dt = datetime.fromtimestamp(arrival_timestamp)
        arrival_time = arrival_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    # 构建统一格式的数据
    row_data = {
        'source': source_file,
        'app_name': primary_service,  # 底座中primary_service对应应用名称
        'time': time_str,
        'timestamp': str(timestamp) if timestamp else None,
        'request_time': request_time,
        'client_ip': real_ip or remote_addr,  # 优先使用RealIp
        'client_port': remote_port,
        'request_method': request_method,
        'request_uri': normalized_uri,
        'request_path': normalized_uri,  # 底座日志没有单独的path，使用normalized_uri
        'query_string': query_string,
        'request_protocol': request_protocol,
        'status': code,
        'body_bytes_sent': int(body or 0),
        'bytes_sent': int(body or 0) + ESTIMATED_HEADER_SIZE,  # 估算total bytes
        'content_type': "",  # 底座日志中没有这个字段
        'upstream_connect_time': 0,  # 底座日志中没有这些字段，设为默认值
        'upstream_header_time': 0,
        'upstream_response_time': request_time,  # ar_time作为upstream_response_time
        'upstream_addr': "",
        'upstream_status': "",
        'server_name': http_host,
        'host': http_host,
        'user_agent': agent,
        'referer': http_referer,
        'service_name': secondary_service,  # 底座中secondary_service对应服务名称
        'date': date,
        'hour': hour,
        'minute': minute,
        'second': second,
        'arrival_timestamp': arrival_timestamp,
        'arrival_time': arrival_time,
        'upstream_connect_phase': 0,  # 计算阶段时间
        'upstream_header_phase': 0,
        'upstream_body_phase': request_time,  # 将整个请求时间分配给body_phase
        'client_transfer_phase': 0
    }

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

