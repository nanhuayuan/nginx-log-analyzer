#!/usr/bin/env python3
"""
诊断连接数字段的工具脚本
用于检查CSV文件中的字段名和数据格式
"""

import csv
import sys
from datetime import datetime

def analyze_csv_fields(csv_path, max_rows=100):
    """分析CSV文件的字段和数据格式"""
    print(f"=== 分析文件: {csv_path} ===")
    
    if not csv_path.endswith('.csv'):
        print("❌ 文件不是CSV格式")
        return False
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            
            print(f"字段数量: {len(fieldnames)}")
            print(f"字段列表: {fieldnames}")
            
            # 检查时间相关字段
            time_fields = []
            for field in fieldnames:
                if any(keyword in field.lower() for keyword in 
                      ['time', 'timestamp', 'arrival', 'completion', 'start', 'end']):
                    time_fields.append(field)
            
            print(f"\n时间相关字段: {time_fields}")
            
            # 检查IP相关字段
            ip_fields = []
            for field in fieldnames:
                if any(keyword in field.lower() for keyword in 
                      ['ip', 'addr', 'client', 'remote']):
                    ip_fields.append(field)
            
            print(f"IP相关字段: {ip_fields}")
            
            # 检查状态码字段
            status_fields = []
            for field in fieldnames:
                if any(keyword in field.lower() for keyword in 
                      ['status', 'code', 'response']):
                    status_fields.append(field)
            
            print(f"状态码相关字段: {status_fields}")
            
            # 分析前几行数据
            print(f"\n=== 数据样本分析 (前{max_rows}行) ===")
            
            row_count = 0
            time_samples = {}
            
            for row in reader:
                row_count += 1
                if row_count > max_rows:
                    break
                
                # 收集时间字段样本
                for field in time_fields:
                    if field in row and row[field]:
                        if field not in time_samples:
                            time_samples[field] = []
                        if len(time_samples[field]) < 3:
                            time_samples[field].append(row[field])
            
            print(f"实际数据行数: {row_count}")
            
            # 显示时间字段样本
            print("\n时间字段样本:")
            for field, samples in time_samples.items():
                print(f"  {field}: {samples}")
                
                # 尝试解析时间格式
                for sample in samples[:1]:  # 只测试第一个样本
                    try_parse_time_formats(field, sample)
            
            # 生成修复建议
            generate_fix_recommendations(time_fields, ip_fields, status_fields)
            
            return True
            
    except Exception as e:
        print(f"❌ 文件分析失败: {e}")
        return False

def try_parse_time_formats(field_name, time_str):
    """尝试解析时间格式"""
    formats_to_try = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%d/%b/%Y:%H:%M:%S %z',
        '%Y-%m-%d',
        '%H:%M:%S'
    ]
    
    for fmt in formats_to_try:
        try:
            parsed = datetime.strptime(time_str, fmt)
            print(f"    ✓ {field_name} 格式: {fmt} -> {parsed}")
            return fmt
        except ValueError:
            continue
    
    print(f"    ❌ {field_name} 无法解析格式: {time_str}")
    return None

def generate_fix_recommendations(time_fields, ip_fields, status_fields):
    """生成修复建议"""
    print(f"\n=== 修复建议 ===")
    
    # 时间字段映射建议
    print("1. 时间字段映射建议:")
    if not time_fields:
        print("   ❌ 没有发现时间相关字段！请检查数据源。")
    else:
        print("   在 _cache_connection_info() 方法中，建议的字段映射:")
        print("   arrival_time = record.get('{}') or \\".format(time_fields[0] if time_fields else 'timestamp'))
        for field in time_fields[1:4]:  # 最多显示4个字段
            print(f"                  record.get('{field}') or \\")
        print("                  record.get('time_local')")
    
    # IP字段映射建议  
    print("\n2. IP字段映射建议:")
    if not ip_fields:
        print("   ❌ 没有发现IP相关字段！")
    else:
        print("   建议的字段映射:")
        print("   client_ip = record.get('{}') or \\".format(ip_fields[0] if ip_fields else 'client_ip'))
        for field in ip_fields[1:3]:  # 最多显示3个字段
            print(f"               record.get('{field}') or \\")
        print("               record.get('remote_addr')")
    
    # 状态码字段映射建议
    print("\n3. 状态码字段映射建议:")
    if not status_fields:
        print("   ❌ 没有发现状态码相关字段！")
    else:
        print("   建议的字段映射:")
        print("   status = record.get('{}') or \\".format(status_fields[0] if status_fields else 'status'))
        for field in status_fields[1:3]:  # 最多显示3个字段
            print(f"            record.get('{field}') or \\")
        print("            record.get('response_status_code')")

def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("用法: python3 diagnose_connection_fields.py <csv_file_path>")
        print("示例: python3 diagnose_connection_fields.py /path/to/nginx_logs.csv")
        return
    
    csv_path = sys.argv[1]
    success = analyze_csv_fields(csv_path)
    
    if success:
        print("\n✅ 诊断完成！请根据上述建议修改 _cache_connection_info() 方法中的字段映射。")
    else:
        print("\n❌ 诊断失败！请检查文件路径和格式。")

if __name__ == "__main__":
    main()