#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
底座格式日志解析器 - 适配新表结构
Base Log Parser - Adapted for new table structure

专门解析底座格式的nginx日志，格式如：
http_host:domain remote_addr:"ip" remote_port:"port" time:"timestamp" request:"method uri protocol" code:"status" body:"size" http_referer:"referer" ar_time:"duration" RealIp:"real_ip" agent:"user_agent"
"""

import re
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Iterator
from pathlib import Path

class BaseLogParser:
    """底座格式日志解析器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # 字段提取模式
        self.field_patterns = {
            # 基础字段
            'http_host': [r'http_host:([^\s]+)', r'http_host:"([^"]*)"'],
            'remote_addr': [r'remote_addr:"([^"]*)"'],
            'remote_port': [r'remote_port:"([^"]*)"'],
            'remote_user': [r'remote_user:"([^"]*)"'],
            'time': [r'time:"([^"]*)"'],
            'request': [r'request:"([^"]*)"'],
            'code': [r'code:"([^"]*)"'],
            'body': [r'body:"([^"]*)"'],
            'http_referer': [r'http_referer:"([^"]*)"'],
            'ar_time': [r'ar_time:"([^"]*)"'],
            'RealIp': [r'RealIp:"([^"]*)"'],
            'agent': [r'agent:"([^"]*)"']
        }
        
        # 统计信息
        self.stats = {
            'total_lines': 0,
            'parsed_lines': 0,
            'error_lines': 0,
            'empty_lines': 0
        }
    
    def can_parse(self, sample_line: str) -> bool:
        """检查是否能解析指定格式"""
        if not sample_line:
            return False
        
        # 底座格式的特征：包含http_host和remote_addr
        return ('http_host:' in sample_line and 
                'remote_addr:' in sample_line and
                'time:' in sample_line)
    
    def parse_line(self, line: str, line_number: int = 0, source_file: str = '') -> Optional[Dict[str, Any]]:
        """
        解析单行日志
        
        Args:
            line: 日志行内容
            line_number: 行号
            source_file: 源文件名
            
        Returns:
            解析后的字典，如果解析失败返回None
        """
        self.stats['total_lines'] += 1
        
        if not line or line.strip() == '':
            self.stats['empty_lines'] += 1
            return None
        
        line = line.strip()
        
        try:
            # 基础解析结果
            parsed_data = {
                'raw_line': line,
                'line_number': line_number,
                'source_file': source_file,
                'parsing_errors': []
            }
            
            # 提取所有字段
            for field_name, patterns in self.field_patterns.items():
                value = self._extract_field_value(line, patterns)
                parsed_data[field_name] = value
                
                # 记录缺失的关键字段
                if value is None and field_name in ['time', 'remote_addr', 'code']:
                    parsed_data['parsing_errors'].append(f'缺失关键字段: {field_name}')
            
            # 基本验证
            if not self._validate_parsed_data(parsed_data):
                self.stats['error_lines'] += 1
                return None
            
            self.stats['parsed_lines'] += 1
            return parsed_data
            
        except Exception as e:
            self.logger.error(f"解析第{line_number}行失败: {e}")
            self.stats['error_lines'] += 1
            return None
    
    def parse_file(self, file_path: str) -> Iterator[Dict[str, Any]]:
        """
        解析整个日志文件
        
        Args:
            file_path: 日志文件路径
            
        Yields:
            解析后的记录字典
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            self.logger.error(f"文件不存在: {file_path}")
            return
        
        self.logger.info(f"开始解析文件: {file_path.name}")
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                for line_number, line in enumerate(f, 1):
                    parsed_data = self.parse_line(line, line_number, file_path.name)
                    if parsed_data:
                        yield parsed_data
                    
                    # 每处理1000行输出一次进度
                    if line_number % 1000 == 0:
                        self.logger.debug(f"已处理 {line_number} 行")
        
        except Exception as e:
            self.logger.error(f"读取文件失败 {file_path}: {e}")
        
        # 输出统计信息
        self.logger.info(f"文件解析完成: {file_path.name}")
        self.logger.info(f"统计信息: 总行数={self.stats['total_lines']}, "
                        f"成功解析={self.stats['parsed_lines']}, "
                        f"解析失败={self.stats['error_lines']}, "
                        f"空行={self.stats['empty_lines']}")
    
    def batch_parse_files(self, file_paths: list, batch_size: int = 500) -> Iterator[list]:
        """
        批量解析多个文件
        
        Args:
            file_paths: 文件路径列表
            batch_size: 批次大小
            
        Yields:
            批量解析结果列表
        """
        batch = []
        
        for file_path in file_paths:
            for parsed_data in self.parse_file(file_path):
                batch.append(parsed_data)
                
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
        
        # 处理最后一批
        if batch:
            yield batch
    
    def get_stats(self) -> Dict[str, Any]:
        """获取解析统计信息"""
        stats = self.stats.copy()
        if stats['total_lines'] > 0:
            stats['success_rate'] = (stats['parsed_lines'] / stats['total_lines']) * 100
        else:
            stats['success_rate'] = 0.0
        return stats
    
    def reset_stats(self):
        """重置统计信息"""
        self.stats = {
            'total_lines': 0,
            'parsed_lines': 0,
            'error_lines': 0,
            'empty_lines': 0
        }
    
    # === 私有方法 ===
    
    def _extract_field_value(self, line: str, patterns: list) -> Optional[str]:
        """从日志行中提取字段值"""
        for pattern in patterns:
            match = re.search(pattern, line)
            if match:
                value = match.group(1).strip()
                
                # 处理空值和占位符
                if value in ['-', '""', "''", 'null', 'NULL', '']:
                    return None
                    
                return value
        
        return None
    
    def _validate_parsed_data(self, parsed_data: Dict[str, Any]) -> bool:
        """验证解析后的数据"""
        # 检查关键字段是否存在
        required_fields = ['time', 'remote_addr', 'code']
        
        for field in required_fields:
            if parsed_data.get(field) is None:
                return False
        
        # 检查时间格式
        time_str = parsed_data.get('time')
        if time_str and not self._is_valid_time_format(time_str):
            parsed_data['parsing_errors'].append(f'时间格式无效: {time_str}')
        
        # 检查状态码格式
        code_str = parsed_data.get('code')
        if code_str and not self._is_valid_status_code(code_str):
            parsed_data['parsing_errors'].append(f'状态码格式无效: {code_str}')
        
        return True
    
    def _is_valid_time_format(self, time_str: str) -> bool:
        """验证时间格式"""
        if not time_str:
            return False
        
        # 支持的时间格式
        time_formats = [
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}',  # ISO格式
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',                    # 标准格式
        ]
        
        return any(re.match(pattern, time_str) for pattern in time_formats)
    
    def _is_valid_status_code(self, code_str: str) -> bool:
        """验证HTTP状态码"""
        if not code_str:
            return False
        
        try:
            code = int(code_str)
            return 100 <= code <= 599
        except ValueError:
            return False


# 测试代码
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    parser = BaseLogParser()
    
    # 测试单行解析
    test_line = '''http_host:xxx.xxxx.xxxx.gov.cn remote_addr:"100.100.8.44" remote_port:"10305"  remote_user:"-"  time:"2025-04-23T00:00:02+08:00"  request:"GET /abc/M00/06/B3/rBAWN2f-87181789179-I560.png HTTP/1.1"  code:"200"  body:"140332"  http_referer:"-"  ar_time:"0.325"  RealIp:"100.1.8.44"  agent:"zzz-ios/1.4.1 (iPhone; iOS 15.4.1; Scale/3.00)"'''
    
    result = parser.parse_line(test_line, 1, 'test.log')
    if result:
        print("✅ 解析成功:")
        for key, value in result.items():
            if value is not None and key != 'raw_line':
                print(f"  {key}: {value}")
    else:
        print("❌ 解析失败")
    
    print(f"\n📊 统计信息: {parser.get_stats()}")