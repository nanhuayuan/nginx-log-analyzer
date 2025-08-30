# -*- coding: utf-8 -*-
"""
Nginx日志统一处理器 - 支持标准nginx格式
基于现有log_parser.py，专门用于ClickHouse数据管道
"""

import os
import sys
import glob
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent))

from self.self_00_01_constants import LOG_TYPE_BASE
from self.self_00_02_utils import log_info
from scripts.custom_log_parser import parse_custom_nginx_log

class NginxLogProcessor:
    """Nginx日志处理器"""
    
    def __init__(self):
        self.processed_files = set()
        self.status_file = "processing-status.json"
        self.load_status()
    
    def load_status(self):
        """加载处理状态"""
        try:
            if os.path.exists(self.status_file):
                with open(self.status_file, 'r', encoding='utf-8') as f:
                    status_data = json.load(f)
                    # 提取已处理的文件路径和哈希
                    for date_info in status_data.values():
                        if isinstance(date_info, dict) and 'files_processed' in date_info:
                            for file_info in date_info['files_processed']:
                                if file_info.get('status') == 'completed':
                                    self.processed_files.add(f"{file_info['file']}:{file_info['hash']}")
        except Exception as e:
            log_info(f"加载状态文件失败: {e}", level="WARN")
    
    def save_status(self, log_date, file_path, file_hash, records_count, status='completed', error_msg=None):
        """保存处理状态"""
        try:
            status_data = {}
            if os.path.exists(self.status_file):
                with open(self.status_file, 'r', encoding='utf-8') as f:
                    status_data = json.load(f)
            
            date_str = log_date.strftime('%Y-%m-%d')
            if date_str not in status_data:
                status_data[date_str] = {
                    'status': 'processing',
                    'files_processed': []
                }
            
            # 更新或添加文件信息
            file_info = {
                'file': os.path.basename(file_path),
                'full_path': file_path,
                'hash': file_hash,
                'records': records_count,
                'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'status': status,
                'error_message': error_msg
            }
            
            # 查找并更新现有记录，或添加新记录
            existing_found = False
            for i, existing in enumerate(status_data[date_str]['files_processed']):
                if existing['file'] == file_info['file']:
                    status_data[date_str]['files_processed'][i] = file_info
                    existing_found = True
                    break
            
            if not existing_found:
                status_data[date_str]['files_processed'].append(file_info)
            
            # 更新日期状态
            all_completed = all(f.get('status') == 'completed' 
                              for f in status_data[date_str]['files_processed'])
            status_data[date_str]['status'] = 'completed' if all_completed else 'processing'
            
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(status_data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            log_info(f"保存状态文件失败: {e}", level="ERROR")
    
    def get_file_hash(self, file_path):
        """计算文件哈希值"""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
        except Exception as e:
            log_info(f"计算文件哈希失败 {file_path}: {e}", level="ERROR")
            return None
        return hash_md5.hexdigest()
    
    def is_file_processed(self, file_path, file_hash):
        """检查文件是否已处理"""
        file_key = f"{os.path.basename(file_path)}:{file_hash}"
        return file_key in self.processed_files
    
    def collect_log_files_by_date(self, base_log_dir, target_date=None):
        """按日期收集日志文件"""
        log_files = []
        
        if target_date:
            # 指定日期的目录
            date_dir = os.path.join(base_log_dir, target_date.strftime('%Y-%m-%d'))
            if os.path.exists(date_dir):
                files = glob.glob(os.path.join(date_dir, "*.log"))
                log_files.extend([(f, target_date) for f in files])
                log_info(f"从目录 {date_dir} 找到 {len(files)} 个日志文件")
        else:
            # 扫描所有日期目录
            if os.path.exists(base_log_dir):
                for item in os.listdir(base_log_dir):
                    item_path = os.path.join(base_log_dir, item)
                    if os.path.isdir(item_path):
                        # 尝试解析日期目录名
                        try:
                            log_date = datetime.strptime(item, '%Y-%m-%d').date()
                            files = glob.glob(os.path.join(item_path, "*.log"))
                            log_files.extend([(f, log_date) for f in files])
                            log_info(f"从目录 {item_path} 找到 {len(files)} 个日志文件")
                        except ValueError:
                            continue
                            
        return log_files
    
    def detect_log_format(self, line):
        """检测日志格式类型"""
        if not line or not line.strip():
            return None
        
        # 检查是否是键值对格式 (自定义格式)
        if 'http_host:' in line and 'remote_addr:' in line and 'time:' in line:
            return 'custom'
        
        # 检查是否是标准Combined格式
        if re.match(r'^\d+\.\d+\.\d+\.\d+', line) and '[' in line and ']' in line and '"' in line:
            return 'combined'
        
        # 默认尝试Combined格式
        return 'combined'
    
    def parse_nginx_log_auto(self, line, source_file, log_date):
        """自动检测格式并解析日志"""
        format_type = self.detect_log_format(line)
        
        if format_type == 'custom':
            return self.parse_custom_nginx_log(line, source_file, log_date)
        else:
            return self.parse_nginx_base_log(line, source_file, log_date)
    
    def parse_custom_nginx_log(self, line, source_file, log_date):
        """解析自定义键值对格式的nginx日志"""
        record = parse_custom_nginx_log(line)
        if record:
            # 添加文件和日期信息
            record['source_file'] = source_file
            record['log_date'] = log_date.strftime('%Y-%m-%d')
        return record
    
    def parse_nginx_base_log(self, line, source_file, log_date):
        """解析标准nginx日志格式（底座格式）"""
        if not line or not line.strip():
            return None
            
        try:
            # 标准nginx日志格式解析
            # 示例: 192.168.1.1 - - [09/May/2025:11:16:11 +0800] "GET /api/v1/users HTTP/1.1" 200 1234 "-" "Mozilla/5.0..."
            
            # 正则表达式匹配nginx访问日志
            log_pattern = re.compile(
                r'(?P<client_ip>\S+) '
                r'(?P<identd>\S+) '
                r'(?P<userid>\S+) '
                r'\[(?P<timestamp>[^\]]+)\] '
                r'"(?P<request>[^"]*)" '
                r'(?P<status>\S+) '
                r'(?P<size>\S+) '
                r'"(?P<referer>[^"]*)" '
                r'"(?P<user_agent>[^"]*)"'
                r'(?:\s+"(?P<x_forwarded_for>[^"]*)")?'
                r'(?:\s+(?P<upstream_response_time>\S+))?'
                r'(?:\s+(?P<request_time>\S+))?'
            )
            
            match = log_pattern.match(line.strip())
            if not match:
                return None
                
            groups = match.groupdict()
            
            # 解析请求信息
            request = groups.get('request', '')
            request_parts = request.split(' ') if request else ['', '', '']
            method = request_parts[0] if len(request_parts) > 0 else ''
            uri = request_parts[1] if len(request_parts) > 1 else ''
            protocol = request_parts[2] if len(request_parts) > 2 else ''
            
            # 解析时间戳
            timestamp_str = groups.get('timestamp', '')
            try:
                # 解析nginx时间格式: 09/May/2025:11:16:11 +0800
                dt = datetime.strptime(timestamp_str, '%d/%b/%Y:%H:%M:%S %z')
                # 转换为本地时间字符串
                timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 构建标准化记录
            record = {
                'timestamp': timestamp,
                'client_ip': groups.get('client_ip', ''),
                'request_method': method,
                'request_full_uri': uri,
                'request_protocol': protocol,
                'response_status_code': groups.get('status', ''),
                'response_body_size_kb': self._safe_float(groups.get('size', '0')) / 1024,
                'total_bytes_sent_kb': self._safe_float(groups.get('size', '0')) / 1024,
                'referer': groups.get('referer', ''),
                'user_agent': groups.get('user_agent', ''),
                'total_request_duration': self._safe_float(groups.get('request_time', '0')),
                'upstream_response_time': self._safe_float(groups.get('upstream_response_time', '0')),
                'upstream_connect_time': 0.0,
                'upstream_header_time': 0.0,
                'application_name': self._extract_app_name(source_file),
                'service_name': self._extract_service_name(uri),
                'source_file': source_file,
                'log_date': log_date.strftime('%Y-%m-%d')
            }
            
            return record
            
        except Exception as e:
            log_info(f"解析nginx日志行失败: {e}, line: {line[:100]}...", level="ERROR")
            return None
    
    def _safe_float(self, value):
        """安全转换为浮点数"""
        try:
            if not value or value == '-':
                return 0.0
            return float(value)
        except:
            return 0.0
    
    def _extract_app_name(self, source_file):
        """从文件名提取应用名"""
        filename = os.path.basename(source_file)
        # 移除.log扩展名和日期部分
        app_name = re.sub(r'\.log$', '', filename)
        app_name = re.sub(r'_\d{4}-\d{2}-\d{2}$', '', app_name)
        return app_name or 'nginx'
    
    def _extract_service_name(self, uri):
        """从URI提取服务名"""
        if not uri:
            return 'unknown'
        
        # 提取URI路径的第一段作为服务名
        parts = uri.strip('/').split('/')
        if parts and parts[0]:
            return parts[0]
        return 'root'
    
    def process_log_file(self, file_path, log_date):
        """处理单个日志文件"""
        log_info(f"开始处理日志文件: {file_path}")
        
        # 计算文件哈希
        file_hash = self.get_file_hash(file_path)
        if not file_hash:
            return 0, "计算文件哈希失败"
        
        # 检查是否已处理
        if self.is_file_processed(file_path, file_hash):
            log_info(f"文件已处理，跳过: {file_path}")
            return 0, "文件已处理"
        
        processed_records = []
        error_count = 0
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line_no, line in enumerate(f, 1):
                    try:
                        record = self.parse_nginx_log_auto(line, file_path, log_date)
                        if record:
                            processed_records.append(record)
                        else:
                            error_count += 1
                    except Exception as e:
                        error_count += 1
                        if error_count <= 10:  # 只记录前10个错误
                            log_info(f"处理第{line_no}行失败: {e}", level="WARN")
            
            log_info(f"文件处理完成: {file_path}, 记录数: {len(processed_records)}, 错误数: {error_count}")
            
            # 更新处理状态
            self.save_status(log_date, file_path, file_hash, len(processed_records))
            self.processed_files.add(f"{os.path.basename(file_path)}:{file_hash}")
            
            return processed_records, None
            
        except Exception as e:
            error_msg = f"处理文件失败: {e}"
            log_info(error_msg, level="ERROR")
            self.save_status(log_date, file_path, file_hash, 0, 'failed', error_msg)
            return [], error_msg
    
    def process_logs_by_date(self, base_log_dir, target_date=None):
        """按日期处理日志"""
        log_info("开始Nginx日志处理任务", show_memory=True)
        
        # 收集日志文件
        log_files = self.collect_log_files_by_date(base_log_dir, target_date)
        
        if not log_files:
            log_info("未找到日志文件！", level="WARN")
            return []
        
        log_info(f"找到 {len(log_files)} 个日志文件")
        
        all_records = []
        
        for file_path, log_date in log_files:
            records, error = self.process_log_file(file_path, log_date)
            if records:
                all_records.extend(records)
        
        log_info(f"所有日志处理完成，总记录数: {len(all_records)}")
        return all_records


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Nginx日志处理器 v1.0.0')
    parser.add_argument('--log-dir', '-d', type=str, required=True, help='日志根目录')
    parser.add_argument('--date', type=str, help='处理指定日期 (YYYY-MM-DD)')
    parser.add_argument('--output', '-o', type=str, help='输出JSON文件路径')
    
    args = parser.parse_args()
    
    # 解析目标日期
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            log_info(f"日期格式错误: {args.date}, 应该是 YYYY-MM-DD", level="ERROR")
            return
    
    # 创建处理器并处理日志
    processor = NginxLogProcessor()
    records = processor.process_logs_by_date(args.log_dir, target_date)
    
    # 输出结果
    if args.output:
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=2, default=str)
            log_info(f"结果已保存到: {args.output}")
        except Exception as e:
            log_info(f"保存结果失败: {e}", level="ERROR")
    
    log_info(f"处理完成，总记录数: {len(records)}")


if __name__ == "__main__":
    main()