#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Debug DWD Writer - 检查字段数据类型问题
"""

import sys
from pathlib import Path

# 添加路径
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

from parsers.base_log_parser import BaseLogParser
from processors.field_mapper import FieldMapper

def main():
    # 测试日志文件
    test_file = Path("D:/project/nginx-log-analyzer/nginx-analytics-warehouse/nginx_logs/20250422/access186.log")
    
    parser = BaseLogParser()
    mapper = FieldMapper()
    
    # 解析第一行数据
    with open(test_file, 'r', encoding='utf-8', errors='replace') as f:
        first_line = f.readline().strip()
    
    parsed_data = parser.parse_line(first_line, 1, test_file.name)
    if parsed_data:
        mapped_data = mapper.map_to_dwd(parsed_data, test_file.name)
        
        print("=== 检查 'date' 字段数据 ===")
        date_value = mapped_data.get('date')
        print(f"date字段值: {date_value}")
        print(f"date字段类型: {type(date_value)}")
        
        # 检查所有时间相关字段
        time_fields = ['log_time', 'date_partition', 'date', 'created_at', 'updated_at']
        for field in time_fields:
            value = mapped_data.get(field)
            print(f"{field}: {value} (类型: {type(value)})")
        
        # 检查字段总数
        print(f"\n映射字段总数: {len(mapped_data)}")
        
        # 输出所有字段名（检查是否有异常字符）
        print("\n所有字段名:")
        for i, field_name in enumerate(mapped_data.keys(), 1):
            print(f"{i:3d}. '{field_name}' = {repr(mapped_data[field_name])}")

if __name__ == "__main__":
    main()