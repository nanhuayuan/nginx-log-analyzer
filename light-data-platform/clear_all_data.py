# -*- coding: utf-8 -*-
"""
一键清空所有数据脚本
用于测试时重置整个系统
"""

import os
import sys
from pathlib import Path

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent))
sys.path.append(str(Path(__file__).parent))

from self.self_00_02_utils import log_info

def clear_clickhouse_data():
    """清空ClickHouse所有数据"""
    try:
        import clickhouse_connect
        
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='web_user',
            password='web_password',
            database='nginx_analytics'
        )
        
        log_info("连接ClickHouse成功")
        
        # 获取所有表
        tables = client.query("SHOW TABLES").result_rows
        table_names = [row[0] for row in tables]
        
        log_info(f"发现 {len(table_names)} 个表")
        
        # 清空每个表的数据 (保留表结构)
        cleared_tables = []
        for table in table_names:
            try:
                # 跳过系统表和物化视图
                if table.startswith('system') or table.startswith('.'):
                    continue
                    
                # 检查是否是物化视图
                engine_result = client.query(f"SELECT engine FROM system.tables WHERE name = '{table}'").result_rows
                if engine_result and 'MaterializedView' in engine_result[0][0]:
                    log_info(f"跳过物化视图: {table}")
                    continue
                
                client.command(f"TRUNCATE TABLE {table}")
                cleared_tables.append(table)
                log_info(f"已清空表: {table}")
                
            except Exception as e:
                log_info(f"清空表 {table} 失败: {e}", level="WARN")
        
        client.close()
        log_info(f"成功清空 {len(cleared_tables)} 个表")
        return True
        
    except Exception as e:
        log_info(f"清空ClickHouse数据失败: {e}", level="ERROR")
        return False

def clear_processing_status():
    """清空处理状态文件"""
    try:
        status_file = "processing-status.json"
        if os.path.exists(status_file):
            os.remove(status_file)
            log_info(f"已删除状态文件: {status_file}")
        else:
            log_info("状态文件不存在，跳过")
        return True
    except Exception as e:
        log_info(f"清空状态文件失败: {e}", level="ERROR")
        return False

def clear_sample_logs():
    """清空示例日志文件"""
    try:
        sample_dir = Path("sample_nginx_logs")
        if sample_dir.exists():
            import shutil
            shutil.rmtree(sample_dir)
            log_info(f"已删除示例日志目录: {sample_dir}")
        else:
            log_info("示例日志目录不存在，跳过")
        return True
    except Exception as e:
        log_info(f"清空示例日志失败: {e}", level="ERROR")
        return False

def recreate_sample_directory():
    """重新创建示例目录结构"""
    try:
        base_dir = Path('sample_nginx_logs')
        sample_dates = ['2025-08-29', '2025-08-30', '2025-08-31']
        
        for date_str in sample_dates:
            date_dir = base_dir / date_str
            date_dir.mkdir(parents=True, exist_ok=True)
            
            # 创建示例文件占位符
            readme_file = date_dir / 'README.txt'
            with open(readme_file, 'w', encoding='utf-8') as f:
                f.write(f"示例nginx日志目录: {date_str}\n")
                f.write("请将您的nginx日志文件(.log)放在这里\n")
                f.write("支持的格式:\n")
                f.write("- 标准nginx Combined Log格式\n")
                f.write("- JSON格式(未来支持)\n")
        
        log_info(f"重新创建示例目录结构: {base_dir}")
        return True
        
    except Exception as e:
        log_info(f"创建示例目录失败: {e}", level="ERROR")
        return False

def main():
    """主清理流程"""
    log_info("="*60)
    log_info("开始清空所有数据，重置系统")
    log_info("="*60)
    
    success_steps = 0
    total_steps = 4
    
    # 确认操作
    print("\n警告: 此操作将清空所有数据!")
    print("包括:")
    print("- ClickHouse中所有nginx日志数据")  
    print("- 处理状态记录")
    print("- 示例日志文件")
    print()
    confirm = input("确认要继续吗? (输入 'yes' 确认): ")
    
    if confirm.lower() != 'yes':
        log_info("操作已取消")
        return False
    
    # 步骤1: 清空ClickHouse数据
    log_info("\n1. 清空ClickHouse数据...")
    if clear_clickhouse_data():
        success_steps += 1
    
    # 步骤2: 清空处理状态
    log_info("\n2. 清空处理状态文件...")
    if clear_processing_status():
        success_steps += 1
    
    # 步骤3: 清空示例日志
    log_info("\n3. 清空示例日志文件...")
    if clear_sample_logs():
        success_steps += 1
    
    # 步骤4: 重新创建目录结构
    log_info("\n4. 重新创建示例目录...")
    if recreate_sample_directory():
        success_steps += 1
    
    # 总结
    log_info("="*60)
    if success_steps == total_steps:
        log_info("系统重置完成！")
        log_info("现在可以重新开始处理nginx日志")
        log_info("\n下一步:")
        log_info("1. 将nginx日志文件放到 sample_nginx_logs/YYYY-MM-DD/ 目录")
        log_info("2. 运行: python process_nginx_logs.py --log-dir sample_nginx_logs --date YYYY-MM-DD --mode full")
    else:
        log_info(f"重置部分完成 ({success_steps}/{total_steps})")
        log_info("请检查上述错误信息")
    
    log_info("="*60)
    
    return success_steps == total_steps

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)