# -*- coding: utf-8 -*-
"""
自定义nginx日志格式解析器
支持键值对格式的nginx日志
"""

import re
from datetime import datetime
from urllib.parse import unquote

def parse_custom_nginx_log(line):
    """
    解析自定义键值对格式的nginx日志
    格式示例: http_host:example.com remote_addr:"1.2.3.4" time:"2025-04-23T00:00:02+08:00" request:"GET /path HTTP/1.1"
    """
    if not line or not line.strip():
        return None
    
    try:
        # 解析键值对
        fields = {}
        
        # 正则表达式匹配 key:"value" 或 key:value 格式
        pattern = r'(\w+):"([^"]*)"|\b(\w+):([^\s]+)'
        matches = re.findall(pattern, line)
        
        for match in matches:
            if match[0]:  # 带引号的值
                key = match[0]
                value = match[1]
            else:  # 不带引号的值
                key = match[2]  
                value = match[3]
            fields[key] = value
        
        # 解析request字段
        request_parts = []
        if 'request' in fields:
            request_str = fields['request']
            # 解析 "METHOD /path HTTP/version"
            request_match = re.match(r'([A-Z]+)\s+([^\s]+)\s+(HTTP/[\d.]+)', request_str)
            if request_match:
                method = request_match.group(1)
                uri = request_match.group(2)
                protocol = request_match.group(3)
                request_parts = [method, uri, protocol]
        
        # 解析时间
        timestamp_str = None
        if 'time' in fields:
            time_str = fields['time']
            try:
                # 解析 ISO 8601 格式: 2025-04-23T00:00:02+08:00
                # 直接使用本地时间，不进行时区转换（nginx日志已经是东八区）
                if '+08:00' in time_str:
                    # 去掉时区信息，保持原始时间值
                    time_str = time_str.replace('+08:00', '')
                dt = datetime.fromisoformat(time_str)
                timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 构建标准化记录
        record = {
            'timestamp': timestamp_str or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'client_ip': fields.get('remote_addr', '').strip('"') or fields.get('RealIp', '').strip('"'),
            'request_method': request_parts[0] if len(request_parts) > 0 else '',
            'request_full_uri': request_parts[1] if len(request_parts) > 1 else '',
            'request_protocol': request_parts[2] if len(request_parts) > 2 else '',
            'response_status_code': fields.get('code', ''),
            'response_body_size_kb': safe_float(fields.get('body', '0')) / 1024,
            'total_bytes_sent_kb': safe_float(fields.get('body', '0')) / 1024,  # 使用body作为近似值
            'referer': fields.get('http_referer', '').strip('"'),
            'user_agent': fields.get('agent', '').strip('"'),
            'total_request_duration': safe_float(fields.get('ar_time', '0')),
            'upstream_response_time': safe_float(fields.get('ar_time', '0')),  # 使用ar_time作为近似值
            'upstream_connect_time': 0.0,
            'upstream_header_time': 0.0,
            'application_name': extract_app_from_host(fields.get('http_host', '')),
            'service_name': extract_service_from_path(request_parts[1] if len(request_parts) > 1 else ''),
            'host': fields.get('http_host', ''),
            'remote_port': fields.get('remote_port', '').strip('"'),
        }
        
        return record
        
    except Exception as e:
        print(f"解析自定义日志行失败: {e}, line: {line[:100]}...")
        return None

def safe_float(value):
    """安全转换为浮点数"""
    try:
        if not value or value == '-':
            return 0.0
        return float(value)
    except:
        return 0.0

def extract_app_from_host(host):
    """从host提取应用名"""
    if not host:
        return 'unknown'
    
    # 从域名提取应用名
    if 'zgtapp' in host:
        return 'zgtapp'
    elif 'api' in host:
        return 'api'
    elif 'gateway' in host:
        return 'gateway'
    else:
        # 取域名第一部分作为应用名
        parts = host.split('.')
        return parts[0] if parts else 'unknown'

def extract_service_from_path(uri):
    """从URI提取服务名"""
    if not uri:
        return 'unknown'
    
    # 去除查询参数
    path = uri.split('?')[0]
    
    # 提取路径的主要部分
    if '/scmp-gateway/' in path:
        # 提取gateway后的服务名
        parts = path.split('/scmp-gateway/')
        if len(parts) > 1:
            service_parts = parts[1].split('/')
            return service_parts[0] if service_parts else 'gateway'
        return 'gateway'
    elif '/group1/' in path:
        return 'file-storage'
    elif '/zgt-h5/' in path:
        return 'h5-static'
    elif path.startswith('/api/'):
        # REST API服务
        parts = path.strip('/').split('/')
        return parts[1] if len(parts) > 1 else 'api'
    else:
        # 其他路径，取第一段作为服务名
        parts = path.strip('/').split('/')
        return parts[0] if parts and parts[0] else 'root'

def test_parser():
    """测试解析器"""
    test_line = 'http_host:zgtapp.zwfw.gxzf.gov.cn remote_addr:"100.100.8.44" remote_port:"10305"  remote_user:"-"  time:"2025-04-23T00:00:02+08:00"  request:"GET /group1/M00/06/B3/rBAWN2f-ZIKAJI2vAAIkLKrgt-I560.png HTTP/1.1"  code:"200"  body:"140332"  http_referer:"-"  ar_time:"0.325"  RealIp:"100.100.8.44"  agent:"zgt-ios/1.4.1 (iPhone; iOS 15.4.1; Scale/3.00)"'
    
    result = parse_custom_nginx_log(test_line)
    if result:
        print("解析成功:")
        for key, value in result.items():
            print(f"  {key}: {value}")
    else:
        print("解析失败")

if __name__ == "__main__":
    test_parser()