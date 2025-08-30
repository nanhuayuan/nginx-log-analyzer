# -*- coding: utf-8 -*-
"""
ClickHouse管道初始化脚本
创建必要的表结构、用户权限和初始配置
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))
sys.path.append(str(Path(__file__).parent))

from self.self_00_02_utils import log_info

def setup_clickhouse_tables():
    """设置ClickHouse表结构"""
    try:
        import clickhouse_connect
        
        # 连接ClickHouse
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='analytics_user',
            password='analytics_password',
            database='nginx_analytics'
        )
        
        log_info("✅ 连接ClickHouse成功")
        
        # 读取并执行基础表创建脚本
        sql_files = [
            'docker/clickhouse_init/002_create_tables.sql',
            'docker/clickhouse_init/003_create_advanced_tables.sql'
        ]
        
        for sql_file in sql_files:
            sql_path = Path(__file__).parent / sql_file
            if sql_path.exists():
                log_info(f"📄 执行SQL脚本: {sql_file}")
                
                with open(sql_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                
                # 分割并执行每个语句
                statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
                
                for i, stmt in enumerate(statements):
                    if stmt.upper().startswith(('CREATE', 'ALTER', 'INSERT')):
                        try:
                            client.command(stmt)
                            log_info(f"  ✅ 语句 {i+1}/{len(statements)} 执行成功")
                        except Exception as e:
                            if 'already exists' in str(e).lower() or 'duplicate' in str(e).lower():
                                log_info(f"  ⚠️  语句 {i+1} 已存在，跳过")
                            else:
                                log_info(f"  ❌ 语句 {i+1} 执行失败: {e}", level="WARN")
            else:
                log_info(f"⚠️  SQL脚本不存在: {sql_file}", level="WARN")
        
        # 验证表创建
        tables = client.query("SHOW TABLES").result_rows
        table_names = [row[0] for row in tables]
        
        expected_tables = [
            'ods_nginx_log', 'dwd_nginx_enriched', 'processing_status',
            'dws_nginx_hourly', 'dws_nginx_daily', 'dws_api_hourly',
            'ads_performance_metrics', 'ads_anomaly_log'
        ]
        
        log_info(f"📊 数据库表验证:")
        for table in expected_tables:
            if table in table_names:
                log_info(f"  ✅ {table}")
            else:
                log_info(f"  ❌ {table} (缺失)")
        
        # 检查物化视图
        views = client.query("SHOW TABLES WHERE engine = 'MaterializedView'").result_rows
        view_names = [row[0] for row in views]
        
        expected_views = ['mv_realtime_metrics', 'mv_api_performance', 'mv_platform_analysis']
        
        log_info(f"🔍 物化视图验证:")
        for view in expected_views:
            if view in view_names:
                log_info(f"  ✅ {view}")
            else:
                log_info(f"  ⚠️  {view} (可能未创建)")
        
        client.close()
        return True
        
    except ImportError:
        log_info("❌ clickhouse_connect未安装，请先安装: pip install clickhouse-connect", level="ERROR")
        return False
    except Exception as e:
        log_info(f"❌ 设置ClickHouse表结构失败: {e}", level="ERROR")
        return False

def verify_clickhouse_connection():
    """验证ClickHouse连接"""
    try:
        import clickhouse_connect
        
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='analytics_user', 
            password='analytics_password',
            database='nginx_analytics'
        )
        
        # 测试查询
        version = client.query("SELECT version()").first_row[0]
        log_info(f"ClickHouse连接正常，版本: {version}")
        
        # 测试权限
        databases = client.query("SHOW DATABASES").result_rows
        db_names = [row[0] for row in databases]
        
        if 'nginx_analytics' in db_names:
            log_info("✅ nginx_analytics数据库存在")
        else:
            log_info("❌ nginx_analytics数据库不存在", level="ERROR")
            return False
        
        client.close()
        return True
        
    except Exception as e:
        log_info(f"❌ ClickHouse连接失败: {e}", level="ERROR")
        return False

def create_sample_directories():
    """创建示例目录结构"""
    try:
        # 创建示例日志目录结构
        base_dir = Path.cwd() / 'sample_nginx_logs'
        
        sample_dates = ['2025-08-29', '2025-08-30', '2025-08-31']
        sample_files = ['nginx1.log', 'nginx2.log', 'api-gateway.log']
        
        for date_str in sample_dates:
            date_dir = base_dir / date_str
            date_dir.mkdir(parents=True, exist_ok=True)
            
            for filename in sample_files:
                sample_file = date_dir / filename
                if not sample_file.exists():
                    # 创建示例日志文件
                    with open(sample_file, 'w', encoding='utf-8') as f:
                        f.write(f"# Sample nginx log file for {date_str}\n")
                        f.write(f"# File: {filename}\n")
                        f.write("# Format: Combined Log Format\n")
                        f.write("# 请替换为实际的nginx日志内容\n")
        
        log_info(f"✅ 示例目录结构创建完成: {base_dir}")
        return True
        
    except Exception as e:
        log_info(f"❌ 创建示例目录失败: {e}", level="ERROR")
        return False

def generate_usage_instructions():
    """生成使用说明"""
    instructions = """
🎉 ClickHouse Nginx日志分析管道初始化完成！

📋 系统架构:
  ├── ODS层: 原始日志数据 (ods_nginx_log, processing_status)  
  ├── DWD层: 清洗富化数据 (dwd_nginx_enriched)
  ├── DWS层: 聚合统计数据 (dws_nginx_hourly, dws_nginx_daily, dws_api_hourly)
  ├── ADS层: 应用分析数据 (ads_performance_metrics, ads_anomaly_log)
  └── 物化视图: 实时指标计算 (mv_realtime_metrics, mv_api_performance, mv_platform_analysis)

🚀 快速开始:

1️⃣ 准备日志目录结构:
   mkdir -p /path/to/nginx-logs/2025-08-29
   cp your-nginx-logs/*.log /path/to/nginx-logs/2025-08-29/

2️⃣ 处理nginx日志:
   # 增量处理 (推荐)
   python process_nginx_logs.py --log-dir /path/to/nginx-logs
   
   # 全量处理指定日期
   python process_nginx_logs.py --log-dir /path/to/nginx-logs --date 2025-08-29 --mode full

3️⃣ 查看处理状态:
   python process_nginx_logs.py --status
   python process_nginx_logs.py --status --date 2025-08-29

4️⃣ 启动Web界面:
   python web_app/clickhouse_app.py
   访问: http://localhost:5001

📁 日志目录结构建议:
   /nginx-logs/
   ├── 2025-08-29/
   │   ├── nginx1.log
   │   ├── nginx2.log  
   │   └── api-gateway.log
   ├── 2025-08-30/
   │   └── ...
   └── processing-status.json (自动生成)

🔧 命令行工具:

   增量处理管理器:
   python scripts/incremental_manager.py --status
   python scripts/incremental_manager.py --reset-failed
   
   ClickHouse管道:
   python scripts/clickhouse_pipeline.py --log-dir /path --status
   
   nginx日志解析器:
   python scripts/nginx_log_processor.py --log-dir /path --date 2025-08-29

🏪 ClickHouse访问:
   HTTP接口: http://localhost:8123
   Web界面: http://localhost:8123/play
   用户名: analytics_user
   密码: analytics_password
   数据库: nginx_analytics

📊 数据查询示例:
   -- 查看总体统计
   SELECT * FROM ads_performance_metrics ORDER BY metric_time DESC LIMIT 10;
   
   -- 平台分布统计  
   SELECT platform, count(*) FROM dwd_nginx_enriched GROUP BY platform;
   
   -- 慢请求分析
   SELECT * FROM dwd_nginx_enriched WHERE is_slow = true ORDER BY response_time DESC LIMIT 10;

💡 提示:
   - 首次运行时使用全量模式 (--mode full)
   - 日常使用增量模式自动检测新文件
   - 定期查看处理状态确保数据完整性
   - 使用Web界面进行可视化分析

❓ 常见问题:
   - 如果ClickHouse连接失败，检查docker容器状态
   - 如果解析失败，确认nginx日志格式为标准Combined格式
   - 如果处理卡住，使用 --reset-failed 重置失败文件
"""
    
    print(instructions)
    
    # 保存使用说明到文件
    try:
        with open('USAGE_INSTRUCTIONS.md', 'w', encoding='utf-8') as f:
            f.write(instructions)
        log_info("📝 使用说明已保存到 USAGE_INSTRUCTIONS.md")
    except:
        pass

def main():
    """主初始化流程"""
    log_info("="*60)
    log_info("ClickHouse Nginx日志分析管道初始化")
    log_info("="*60)
    
    success_steps = 0
    total_steps = 4
    
    # 步骤1: 验证ClickHouse连接
    log_info("1. 验证ClickHouse连接...")
    if verify_clickhouse_connection():
        success_steps += 1
    else:
        log_info("请确保ClickHouse容器正在运行:")
        log_info("   docker ps | grep clickhouse")
        log_info("   如果未运行，请启动: docker-compose up -d")
    
    # 步骤2: 创建表结构
    log_info("\n2. 设置ClickHouse表结构...")
    if setup_clickhouse_tables():
        success_steps += 1
    
    # 步骤3: 创建示例目录
    log_info("\n3. 创建示例目录结构...")
    if create_sample_directories():
        success_steps += 1
    
    # 步骤4: 生成使用说明
    log_info("\n4. 生成使用说明...")
    try:
        generate_usage_instructions()
        success_steps += 1
    except Exception as e:
        log_info(f"生成使用说明失败: {e}", level="ERROR")
    
    # 总结
    log_info("="*60)
    if success_steps == total_steps:
        log_info("ClickHouse管道初始化完成！")
        log_info("所有组件都已就绪，可以开始处理nginx日志")
    else:
        log_info(f"初始化部分完成 ({success_steps}/{total_steps})")
        log_info("请检查上述错误信息并重新运行")
    
    log_info("="*60)
    
    return success_steps == total_steps

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)