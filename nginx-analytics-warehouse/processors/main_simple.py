#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nginx日志分析数据仓库 - 主启动脚本 (简化版)
Nginx Analytics Data Warehouse - Main Entry Point
"""

import argparse
import sys
import os
from pathlib import Path
from datetime import datetime
import subprocess

# 添加当前目录到Python路径
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

def print_banner():
    """打印系统banner"""
    print("=" * 70)
    print("   Nginx日志分析数据仓库 v1.0")
    print("   Nginx Analytics Data Warehouse")
    print("=" * 70)
    print()

def check_prerequisites():
    """检查系统前置条件"""
    print("检查系统环境...")
    
    # 检查Docker服务
    try:
        result = subprocess.run(['docker', 'ps'], capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            print("[ERROR] Docker服务未运行，请先启动Docker")
            return False
        print("[OK] Docker服务正常")
    except Exception as e:
        print(f"[ERROR] 无法检查Docker状态: {e}")
        return False
    
    # 检查ClickHouse容器
    try:
        result = subprocess.run(['docker', 'ps', '--filter', 'name=clickhouse'], capture_output=True, text=True)
        if 'clickhouse' not in result.stdout:
            print("[WARNING] ClickHouse容器未运行")
            print("   请运行: python main_simple.py start-services")
            return False
        print("[OK] ClickHouse容器正常运行")
    except Exception as e:
        print(f"[ERROR] 无法检查ClickHouse状态: {e}")
        return False
    
    return True

def show_usage():
    """显示使用帮助"""
    print("快速开始:")
    print()
    print("1. 日常日志处理:")
    print("   python main_simple.py process-all   # 处理所有未处理的日志(推荐)")
    print("   python main_simple.py process --date 20250422  # 处理指定日期")
    print("   python main_simple.py process --date 20250422 --force  # 强制重新处理")
    print()
    print("2. 查看系统状态:")
    print("   python main_simple.py status")
    print()
    print("3. 数据管理:")
    print("   python main_simple.py clear-all     # 清空所有数据")
    print("   python main_simple.py demo         # 运行演示")
    print()
    print("4. 启动服务:")
    print("   python main_simple.py start-services")
    print("   python main_simple.py stop-services")
    print()
    print("日志目录结构:")
    print("   D:/project/nginx-log-analyzer/nginx-analytics-warehouse/nginx_logs/YYYYMMDD/*.log")
    print("   例如: D:/project/.../nginx_logs/20250422/access186.log")
    print()

def process_logs(date_str, force=False):
    """处理nginx日志"""
    print(f"开始处理 {date_str} 的nginx日志")
    
    # 使用新的模块化处理器
    try:
        from nginx_processor_modular import NginxProcessorModular
        processor = NginxProcessorModular()
        
        # 处理日志
        result = processor.process_specific_date(date_str, force_reprocess=force)
        
        if result['success']:
            print("[SUCCESS] 日志处理完成")
            print(f"   处理文件: {result.get('processed_files', 0)}")
            print(f"   总记录数: {result.get('total_records', 0):,}")
            print(f"   处理时间: {result.get('duration', 0):.2f}秒")
        else:
            print(f"[ERROR] 日志处理失败: {result.get('error', '未知错误')}")
            if result.get('errors'):
                for error in result['errors']:
                    print(f"   详细错误: {error}")
            return False
            
    except ImportError as e:
        print(f"[ERROR] 找不到模块化处理器: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] 处理过程中发生错误: {e}")
        return False
    
    return True

def show_status():
    """显示系统状态"""
    print("系统状态检查")
    print("-" * 50)
    
    try:
        from nginx_processor_modular import NginxProcessorModular
        processor = NginxProcessorModular()
        processor.show_status()
    except ImportError as e:
        print(f"[ERROR] 找不到模块化处理器: {e}")
    except Exception as e:
        print(f"[ERROR] 状态检查失败: {e}")

def clear_all_data():
    """清空所有数据"""
    print("清空所有数据 (仅开发环境使用)")
    
    confirm = input("确认清空所有数据？这将删除所有已处理的日志数据 (y/N): ")
    if confirm.lower() != 'y':
        print("操作已取消")
        return
    
    try:
        from nginx_processor_modular import NginxProcessorModular
        processor = NginxProcessorModular()
        processor.clear_all_data()
        print("[SUCCESS] 所有数据已清空")
    except Exception as e:
        print(f"[ERROR] 清空数据失败: {e}")

def run_demo():
    """运行演示数据流"""
    print("运行数据流演示")
    
    try:
        from final_working_demo import main as demo_main
        demo_main()
    except ImportError:
        print("[ERROR] 找不到演示模块")
    except Exception as e:
        print(f"[ERROR] 演示运行失败: {e}")

def validate_data():
    """验证数据处理质量"""
    print("验证数据处理质量")
    
    try:
        from validate_processing import main as validate_main
        validate_main()
    except ImportError:
        print("[ERROR] 找不到验证模块")
    except Exception as e:
        print(f"[ERROR] 验证失败: {e}")

def process_all_unprocessed():
    """处理所有未处理的日志（默认推荐模式）"""
    print("处理所有未处理的日志")
    
    try:
        from nginx_processor_modular import NginxProcessorModular
        processor = NginxProcessorModular()
        result = processor.process_all_unprocessed_logs()
        
        if result['success']:
            print("[SUCCESS] 日志处理完成")
            print(f"   处理日期: {result.get('processed_dates', 0)} 个")
            print(f"   总记录数: {result.get('total_records', 0):,} 条")
            print(f"   处理时间: {result.get('duration', 0):.2f}秒")
        else:
            print(f"[ERROR] 处理失败: {result.get('error', '未知错误')}")
            if result.get('errors'):
                for error in result['errors'][:5]:  # 只显示前5个错误
                    print(f"   详细错误: {error}")
    except ImportError as e:
        print(f"[ERROR] 找不到模块化处理器: {e}")
    except Exception as e:
        print(f"[ERROR] 处理失败: {e}")

def start_services():
    """启动服务"""
    print("启动ClickHouse等服务")
    
    # 查找docker-compose文件，优先使用新的目录结构
    docker_dir = current_dir.parent / "docker"
    docker_compose_file = docker_dir / "docker-compose.yml"
    
    # 兼容旧的文件位置
    if not docker_compose_file.exists():
        docker_compose_file = current_dir / "docker-compose-simple-fixed.yml"
    
    if not docker_compose_file.exists():
        print("[ERROR] 找不到docker-compose配置文件")
        print("期望位置:")
        print(f"  1. {docker_dir / 'docker-compose.yml'}")
        print(f"  2. {current_dir / 'docker-compose-simple-fixed.yml'}")
        return
    
    try:
        # 切换到docker-compose文件所在目录
        work_dir = docker_compose_file.parent
        
        result = subprocess.run([
            'docker-compose', 
            '-f', str(docker_compose_file), 
            'up', '-d'
        ], cwd=work_dir, check=True, capture_output=True, text=True)
        
        print("[SUCCESS] 服务启动完成")
        print("访问地址:")
        print("   ClickHouse: http://localhost:8123")
        print("   Grafana: http://localhost:3000 (admin/admin123)")
        print("   Superset: http://localhost:8088 (admin/admin123)")
        
        # 显示启动的容器
        print("\n启动的容器:")
        containers_result = subprocess.run(['docker', 'ps', '--filter', 'name=nginx-analytics'], 
                                         capture_output=True, text=True)
        print(containers_result.stdout)
        
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] 服务启动失败: {e}")
        if e.stderr:
            print(f"错误详情: {e.stderr}")

def stop_services():
    """停止服务"""
    print("停止服务")
    
    # 查找docker-compose文件
    docker_dir = current_dir.parent / "docker"
    docker_compose_file = docker_dir / "docker-compose.yml"
    
    # 兼容旧的文件位置
    if not docker_compose_file.exists():
        docker_compose_file = current_dir / "docker-compose-simple-fixed.yml"
    
    if not docker_compose_file.exists():
        print("[ERROR] 找不到docker-compose配置文件")
        return
    
    try:
        # 切换到docker-compose文件所在目录
        work_dir = docker_compose_file.parent
        
        subprocess.run([
            'docker-compose', 
            '-f', str(docker_compose_file), 
            'down'
        ], cwd=work_dir, check=True)
        print("[SUCCESS] 服务已停止")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] 停止服务失败: {e}")

def interactive_menu():
    """交互式菜单"""
    while True:
        print("\n" + "=" * 60)
        print("🏛️   Nginx日志分析数据仓库 - 统一管理入口")
        print("=" * 60)
        print("1. 处理所有未处理的日志 (推荐)")
        print("2. 处理指定日期的日志")
        print("3. 查看系统状态")
        print("4. 清空所有数据 (仅开发环境)")
        print("5. 运行演示数据流")
        print("6. 验证数据处理质量") 
        print("7. 启动ClickHouse等服务")
        print("8. 停止服务")
        print("0. 退出")
        print("-" * 60)
        
        try:
            choice = input("请选择操作 [0-8]: ").strip()
            
            if choice == '0':
                print("👋 再见！")
                break
            
            elif choice == '1':
                print("\n🔄 开始处理所有未处理的日志...")
                process_all_unprocessed()
                input("\n按回车键继续...")
            
            elif choice == '2':
                date_str = input("\n请输入日期 (YYYYMMDD格式，如: 20250422): ").strip()
                if not date_str or len(date_str) != 8 or not date_str.isdigit():
                    print("❌ 日期格式错误，请使用YYYYMMDD格式")
                    input("按回车键继续...")
                    continue
                
                try:
                    datetime.strptime(date_str, '%Y%m%d')
                except ValueError:
                    print("❌ 无效的日期")
                    input("按回车键继续...")
                    continue
                
                force = input("是否强制重新处理？(y/N): ").strip().lower() == 'y'
                
                print(f"\n🔄 开始处理 {date_str} 的日志...")
                success = process_logs(date_str, force)
                if success:
                    print(f"\n日志处理完成！现在可以访问BI工具进行分析:")
                    print(f"   Grafana: http://localhost:3000")
                    print(f"   Superset: http://localhost:8088")
                input("\n按回车键继续...")
            
            elif choice == '3':
                print()
                show_status()
                input("\n按回车键继续...")
            
            elif choice == '4':
                print("\n⚠️  清空所有数据 (仅开发环境使用)")
                clear_all_data()
                input("\n按回车键继续...")
            
            elif choice == '5':
                print("\n🔄 运行演示数据流...")
                run_demo()
                input("\n按回车键继续...")
            
            elif choice == '6':
                print("\n🔄 验证数据处理质量...")
                validate_data()
                input("\n按回车键继续...")
            
            elif choice == '7':
                print("\n🔄 启动ClickHouse等服务...")
                start_services()
                input("\n按回车键继续...")
            
            elif choice == '8':
                print("\n🔄 停止服务...")
                stop_services()
                input("\n按回车键继续...")
            
            else:
                print("❌ 无效选择，请输入 0-8")
                input("按回车键继续...")
                
        except KeyboardInterrupt:
            print("\n\n👋 用户中断，再见！")
            break
        except Exception as e:
            print(f"\n❌ 操作过程中发生错误: {e}")
            input("按回车键继续...")

def main():
    """主函数"""
    print_banner()
    
    parser = argparse.ArgumentParser(
        description='Nginx日志分析数据仓库 - 统一管理入口',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python main_simple.py                             # 交互式菜单 (推荐)
  python main_simple.py process --date 20250901     # 处理指定日期日志
  python main_simple.py process --date 20250901 --force  # 强制重新处理
  python main_simple.py process-all                 # 处理所有未处理日志 
  python main_simple.py status                      # 查看系统状态
  python main_simple.py clear-all                   # 清空所有数据
  python main_simple.py demo                        # 运行演示
  python main_simple.py start-services              # 启动服务
  python main_simple.py stop-services               # 停止服务
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # process命令
    process_parser = subparsers.add_parser('process', help='处理nginx日志')
    process_parser.add_argument('--date', required=True, help='日期 (YYYYMMDD格式)')
    process_parser.add_argument('--force', action='store_true', help='强制重新处理')
    
    # 其他命令
    subparsers.add_parser('process-all', help='处理所有未处理的日志 (推荐)')
    subparsers.add_parser('status', help='查看系统状态')
    subparsers.add_parser('clear-all', help='清空所有数据 (仅开发环境)')
    subparsers.add_parser('demo', help='运行演示数据流')
    subparsers.add_parser('validate', help='验证数据处理质量')
    subparsers.add_parser('start-services', help='启动ClickHouse等服务')
    subparsers.add_parser('stop-services', help='停止服务')
    
    args = parser.parse_args()
    
    # 如果没有参数，显示交互式菜单
    if not args.command:
        interactive_menu()
        return
    
    # 对于需要服务的命令，检查前置条件
    if args.command in ['process', 'process-all', 'status', 'demo', 'clear-all', 'validate']:
        if not check_prerequisites():
            print("\n建议先运行: python main_simple.py start-services")
            return
    
    # 执行对应命令
    if args.command == 'process':
        # 验证日期格式
        try:
            datetime.strptime(args.date, '%Y%m%d')
        except ValueError:
            print("[ERROR] 日期格式错误，请使用YYYYMMDD格式，例如: 20250901")
            return
        
        success = process_logs(args.date, args.force)
        if success:
            print(f"\n日志处理完成！现在可以访问BI工具进行分析:")
            print(f"   Grafana: http://localhost:3000")
            print(f"   Superset: http://localhost:8088")
    
    elif args.command == 'process-all':
        process_all_unprocessed()
    
    elif args.command == 'status':
        show_status()
    
    elif args.command == 'clear-all':
        clear_all_data()
    
    elif args.command == 'demo':
        run_demo()
    
    elif args.command == 'validate':
        validate_data()
    
    elif args.command == 'start-services':
        start_services()
    
    elif args.command == 'stop-services':
        stop_services()
    
    print("\n" + "=" * 70)

if __name__ == "__main__":
    main()