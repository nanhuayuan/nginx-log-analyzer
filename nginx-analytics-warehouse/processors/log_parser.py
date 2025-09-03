#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nginx日志解析器模块 - 解耦设计
Nginx Log Parser Module - Decoupled Design

专门负责解析nginx日志的原始数据，不涉及业务逻辑处理
只负责将日志文件转换为标准化的数据结构
"""

import re
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Iterator, Tuple
import logging

class NginxLogParser:
    """Nginx日志解析器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # 时间解析正则模式
        self.time_patterns = [
            r'time:"([^"]+)"',
            r'time:([^\s]+)',
            r'"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})"',
            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})'
        ]
        
        # 请求解析正则
        self.request_pattern = r'request:"([^"]*)"'
        
        # 基础字段映射
        self.field_patterns = {
            'http_host': [r'http_host:([^\s]+)', r'http_host:"([^"]*)"'],
            'remote_addr': [r'remote_addr:"([^"]*)"', r'remote_addr:([^\s]+)'],
            'request': [r'request:"([^"]*)"', r'request:([^\s]+)'],
            'code': [r'code:"?(\d+)"?', r'status:"?(\d+)"?'],
            'body': [r'body:"?(\d+)"?', r'body_bytes_sent:"?(\d+)"?'],
            'ar_time': [r'ar_time:"?([^"\s]+)"?', r'request_time:"?([^"\s]+)"?'],
            'agent': [r'agent:"([^"]*)"', r'user_agent:"([^"]*)"'],
            'referer': [r'referer:"([^"]*)"', r'http_referer:"([^"]*)"'],
            'xff': [r'xff:"([^"]*)"', r'x_forwarded_for:"([^"]*)"'],
            'upstream_response_time': [r'upstream_response_time:"?([^"\s]*)"?'],
            'upstream_connect_time': [r'upstream_connect_time:"?([^"\s]*)"?'],
            'upstream_header_time': [r'upstream_header_time:"?([^"\s]*)"?']
        }
    
    def parse_time(self, log_line: str) -> Optional[datetime]:
        """解析时间字段"""
        for pattern in self.time_patterns:
            match = re.search(pattern, log_line)
            if match:
                time_str = match.group(1)
                try:
                    # 尝试不同的时间格式
                    formats = [
                        '%Y-%m-%dT%H:%M:%S%z',
                        '%Y-%m-%d %H:%M:%S',
                        '%d/%b/%Y:%H:%M:%S %z',
                        '%Y-%m-%dT%H:%M:%S.%f%z'
                    ]
                    
                    for fmt in formats:
                        try:
                            return datetime.strptime(time_str, fmt)
                        except ValueError:
                            continue
                            
                except Exception as e:
                    self.logger.warning(f"时间解析失败: {time_str}, 错误: {e}")
                    continue
        
        return None
    
    def extract_field(self, log_line: str, field_name: str) -> Optional[str]:
        """提取指定字段值"""
        if field_name not in self.field_patterns:
            return None
        
        patterns = self.field_patterns[field_name]
        for pattern in patterns:
            match = re.search(pattern, log_line)
            if match:
                value = match.group(1).strip()
                # 处理空值
                if value in ['-', '""', "''", 'null', 'NULL']:
                    return None
                return value
        
        return None
    
    def parse_request(self, request_str: str) -> Dict[str, str]:
        """解析请求字符串"""
        if not request_str:
            return {'method': '', 'uri': '', 'protocol': ''}
        
        # 标准格式: "GET /path HTTP/1.1"
        parts = request_str.split(' ', 2)
        
        result = {
            'method': parts[0] if len(parts) > 0 else '',
            'uri': parts[1] if len(parts) > 1 else '',
            'protocol': parts[2] if len(parts) > 2 else ''
        }
        
        # 解析URI的查询参数
        if '?' in result['uri']:
            uri_parts = result['uri'].split('?', 1)
            result['uri_path'] = uri_parts[0]
            result['query_string'] = uri_parts[1]
        else:
            result['uri_path'] = result['uri']
            result['query_string'] = ''
        
        return result
    
    def parse_log_line(self, log_line: str, line_number: int = 0) -> Optional[Dict[str, Any]]:
        """解析单行日志"""
        if not log_line or log_line.strip() == '':
            return None
        
        try:
            # 基础字段提取
            parsed_data = {
                'raw_line': log_line.strip(),
                'line_number': line_number,
                'parsing_errors': []
            }
            
            # 解析时间
            log_time = self.parse_time(log_line)
            if log_time:
                parsed_data['log_time'] = log_time
                parsed_data['date_str'] = log_time.strftime('%Y%m%d')
                parsed_data['hour'] = log_time.hour
                parsed_data['minute'] = log_time.minute
            else:
                parsed_data['parsing_errors'].append('时间字段解析失败')
            
            # 提取所有标准字段
            for field_name in self.field_patterns.keys():
                value = self.extract_field(log_line, field_name)
                parsed_data[field_name] = value
            
            # 解析请求字符串
            if parsed_data.get('request'):
                request_info = self.parse_request(parsed_data['request'])
                parsed_data.update(request_info)
            
            # 数值字段转换
            numeric_fields = ['code', 'body', 'ar_time', 'upstream_response_time', 
                            'upstream_connect_time', 'upstream_header_time']
            
            for field in numeric_fields:
                if parsed_data.get(field):
                    try:
                        if field in ['ar_time', 'upstream_response_time', 
                                   'upstream_connect_time', 'upstream_header_time']:
                            # 时间字段转为浮点数
                            value = parsed_data[field]
                            if value != '-':
                                parsed_data[f'{field}_float'] = float(value)
                        elif field in ['code', 'body']:
                            # 状态码和字节数转为整数
                            parsed_data[f'{field}_int'] = int(parsed_data[field])
                    except (ValueError, TypeError) as e:
                        parsed_data['parsing_errors'].append(f'{field}数值转换失败: {e}')
            
            # 记录解析质量
            parsed_data['data_quality_score'] = self.calculate_quality_score(parsed_data)
            
            return parsed_data
            
        except Exception as e:
            self.logger.error(f"解析日志行失败 (行 {line_number}): {e}")
            return {
                'raw_line': log_line.strip(),
                'line_number': line_number,
                'parsing_errors': [f'严重解析错误: {str(e)}'],
                'data_quality_score': 0.0
            }
    
    def calculate_quality_score(self, parsed_data: Dict[str, Any]) -> float:
        """计算数据质量评分 (0-1)"""
        total_fields = len(self.field_patterns) + 1  # +1 for time
        valid_fields = 0
        
        # 检查时间字段
        if parsed_data.get('log_time'):
            valid_fields += 1
        
        # 检查其他字段
        for field in self.field_patterns.keys():
            if parsed_data.get(field):
                valid_fields += 1
        
        # 基础质量评分
        base_score = valid_fields / total_fields
        
        # 扣分项
        penalty = len(parsed_data.get('parsing_errors', [])) * 0.1
        
        return max(0.0, min(1.0, base_score - penalty))
    
    def parse_log_file(self, file_path: Path, encoding: str = 'utf-8') -> Iterator[Dict[str, Any]]:
        """解析日志文件，返回迭代器"""
        if not file_path.exists():
            self.logger.error(f"日志文件不存在: {file_path}")
            return
        
        self.logger.info(f"开始解析日志文件: {file_path}")
        
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                for line_num, line in enumerate(f, 1):
                    parsed_line = self.parse_log_line(line, line_num)
                    if parsed_line:
                        # 添加文件信息
                        parsed_line['source_file'] = str(file_path.name)
                        parsed_line['source_path'] = str(file_path)
                        yield parsed_line
                        
        except UnicodeDecodeError:
            # 尝试其他编码
            self.logger.warning(f"UTF-8解码失败，尝试GBK编码: {file_path}")
            try:
                with open(file_path, 'r', encoding='gbk') as f:
                    for line_num, line in enumerate(f, 1):
                        parsed_line = self.parse_log_line(line, line_num)
                        if parsed_line:
                            parsed_line['source_file'] = str(file_path.name)
                            parsed_line['source_path'] = str(file_path)
                            yield parsed_line
            except Exception as e:
                self.logger.error(f"文件读取失败 ({file_path}): {e}")
        
        except Exception as e:
            self.logger.error(f"解析文件时发生错误 ({file_path}): {e}")
    
    def parse_directory(self, directory_path: Path, file_pattern: str = "*.log") -> Iterator[Dict[str, Any]]:
        """解析目录中的所有日志文件"""
        if not directory_path.exists():
            self.logger.error(f"目录不存在: {directory_path}")
            return
        
        log_files = list(directory_path.glob(file_pattern))
        if not log_files:
            self.logger.warning(f"目录中没有找到匹配的日志文件: {directory_path}")
            return
        
        self.logger.info(f"找到 {len(log_files)} 个日志文件")
        
        for log_file in sorted(log_files):
            self.logger.info(f"处理文件: {log_file.name}")
            yield from self.parse_log_file(log_file)
    
    def get_parsing_stats(self, parsed_data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """获取解析统计信息"""
        if not parsed_data_list:
            return {'total_lines': 0, 'valid_lines': 0, 'error_lines': 0, 'avg_quality': 0.0}
        
        total_lines = len(parsed_data_list)
        error_lines = sum(1 for d in parsed_data_list if d.get('parsing_errors'))
        valid_lines = total_lines - error_lines
        
        quality_scores = [d.get('data_quality_score', 0.0) for d in parsed_data_list]
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0.0
        
        # 错误统计
        error_types = {}
        for data in parsed_data_list:
            for error in data.get('parsing_errors', []):
                error_types[error] = error_types.get(error, 0) + 1
        
        return {
            'total_lines': total_lines,
            'valid_lines': valid_lines,
            'error_lines': error_lines,
            'success_rate': valid_lines / total_lines if total_lines > 0 else 0.0,
            'avg_quality_score': avg_quality,
            'error_types': error_types,
            'quality_distribution': {
                'high_quality': sum(1 for s in quality_scores if s >= 0.8),
                'medium_quality': sum(1 for s in quality_scores if 0.5 <= s < 0.8),
                'low_quality': sum(1 for s in quality_scores if s < 0.5)
            }
        }

def test_parser():
    """测试解析器功能"""
    parser = NginxLogParser()
    
    # 测试日志样例
    test_lines = [
        'http_host:zgtapp.zwfw.gxzf.gov.cn remote_addr:"100.100.8.44" time:"2025-04-23T00:00:02+08:00" request:"GET /group1/M00/06/B3/rBAWN2f-ZIKAJI2vAAIkLKrgt-I560.png HTTP/1.1" code:200 body:140076 ar_time:0.012 agent:"okhttp/4.9.3" referer:"-"',
        'http_host:zgtapp.zwfw.gxzf.gov.cn remote_addr:"100.100.8.65" time:"2025-04-23T00:00:03+08:00" request:"POST /api/v1/user/login HTTP/1.1" code:200 body:234 ar_time:0.156 agent:"WST-SDK-iOS/2.1.0" referer:"-"'
    ]
    
    print("=" * 60)
    print("Nginx日志解析器测试")
    print("=" * 60)
    
    parsed_results = []
    for i, line in enumerate(test_lines, 1):
        print(f"\n测试样例 {i}:")
        print(f"原始: {line[:100]}...")
        
        result = parser.parse_log_line(line, i)
        if result:
            parsed_results.append(result)
            print(f"时间: {result.get('log_time')}")
            print(f"主机: {result.get('http_host')}")
            print(f"IP: {result.get('remote_addr')}")
            print(f"方法: {result.get('method')}")
            print(f"路径: {result.get('uri_path')}")
            print(f"状态: {result.get('code')}")
            print(f"响应时间: {result.get('ar_time')}")
            print(f"质量评分: {result.get('data_quality_score'):.2f}")
            if result.get('parsing_errors'):
                print(f"解析错误: {result['parsing_errors']}")
    
    # 统计信息
    stats = parser.get_parsing_stats(parsed_results)
    print(f"\n解析统计:")
    print(f"总行数: {stats['total_lines']}")
    print(f"成功率: {stats['success_rate']:.1%}")
    print(f"平均质量: {stats['avg_quality_score']:.2f}")

if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    test_parser()