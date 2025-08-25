import os
import pandas as pd
from datetime import datetime

#from self_01_api_analyzer import analyze_api_performance
from self_01_api_analyzer_optimized import analyze_api_performance
from self_00_01_constants import DEFAULT_LOG_DIR, DEFAULT_SUCCESS_CODES, DEFAULT_SLOW_THRESHOLD, DEFAULT_COLUMN_API, \
    DEFAULT_START_DATE, DEFAULT_END_DATE
#from self_06_performance_stability_analyzer import analyze_service_stability
from self_06_performance_stability_analyzer_advanced import analyze_service_stability
#from self_07_generate_summary_report_analyzer import generate_summary_report
from self_07_generate_summary_report_analyzer_advanced import generate_summary_report
#from self_08_ip_analyzer import analyze_ip_sources
from self_08_ip_analyzer_advanced import analyze_ip_sources
#from self_10_request_header_analyzer import analyze_request_headers
from self_10_request_header_analyzer_advanced import analyze_request_headers
from self_11_header_performance_analyzer import analyze_header_performance_correlation
from self_13_interface_error_analyzer import analyze_interface_errors
from self_00_03_log_parser import collect_log_files, process_log_files
#from self_02_service_analyzer import analyze_service_performance
from self_02_service_analyzer_advanced import analyze_service_performance_advanced
#from self_03_slow_requests_analyzer import analyze_slow_requests
from self_03_slow_requests_analyzer_advanced import analyze_slow_requests_advanced
from self_04_status_analyzer_advanced import analyze_status_codes
#from self_04_status_analyzer import analyze_status_codes
#from self_05_time_dimension_analyzer import analyze_time_dimension
from self_05_time_dimension_analyzer_advanced import analyze_time_dimension
from self_00_02_utils import log_info


def main():
    try:
        script_start_time = datetime.now()
        log_info(f"开始执行Nginx日志分析任务 (版本: 1.0.0)", show_memory=True)

        # 设置输入和输出路径
        log_dir = DEFAULT_LOG_DIR
        output_dir = f"{log_dir}_分析结果_{script_start_time.strftime('%Y%m%d_%H%M%S')}"
        temp_dir = f"{output_dir}_temp"

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

        log_info(f"找到 {len(log_files)} 个日志文件")

        # 处理日志文件
        temp_csv = os.path.join(temp_dir, "processed_logs.csv")
        total_records = process_log_files(log_files, temp_csv, start_date=DEFAULT_START_DATE, end_date=DEFAULT_END_DATE)

        # 初始化输出数据
        outputs = {
            'log_dir': log_dir,
            'log_files': log_files,
            'total_requests': total_records,
            'analysis_time': script_start_time.strftime('%Y-%m-%d %H:%M:%S')
        }

        # 定义输出文件路径
        api_output = os.path.join(output_dir, "01.接口性能分析.xlsx")
        service_output = os.path.join(output_dir, "02.服务层级分析.xlsx")
        slow_output = os.path.join(output_dir, "03_慢请求分析.xlsx")

        status_output = os.path.join(output_dir, "04.状态码统计.xlsx")
        time_output = os.path.join(output_dir, "05.时间维度分析-全部接口.xlsx")
        specific_uri_time_output = os.path.join(output_dir, "05_01.时间维度分析-指定接口.xlsx")
        #slow_requests_time_output = os.path.join(output_dir, "时间维度分析-指定接口.xlsx")
        stability_output = os.path.join(output_dir, "06_服务稳定性.xlsx")
        ip_analysis_output = os.path.join(output_dir, "08_IP来源分析.xlsx")
        header_analysis_output = os.path.join(output_dir, "10_请求头分析.xlsx")
        header_performance_output = os.path.join(output_dir, "11_请求头性能关联分析.xlsx")
        interface_error_output = os.path.join(output_dir, "13_接口错误分析.xlsx")
        summary_output = os.path.join(output_dir, "12_综合报告.xlsx")

        # 定义基本分析任务
        analysis_tasks = [
            {"name": "API性能分析", "func": analyze_api_performance, "args": {
                "csv_path": temp_csv, "output_path": api_output,
                "success_codes": DEFAULT_SUCCESS_CODES, "slow_threshold": DEFAULT_SLOW_THRESHOLD
            }},
            #{"name": "服务层级分析", "func": analyze_service_performance, "args": {
            {"name": "服务层级分析", "func": analyze_service_performance_advanced, "args": {
                "csv_path": temp_csv, "output_path": service_output,
                "success_codes": DEFAULT_SUCCESS_CODES
            }},
            #{"name": "慢请求分析", "func": analyze_slow_requests, "args": {
            {"name": "慢请求分析", "func": analyze_slow_requests_advanced, "args": {
                "csv_path": temp_csv, "output_path": slow_output,
                "slow_threshold": DEFAULT_SLOW_THRESHOLD
            }},
            #{"name": "状态码统计", "func": analyze_status_codes, "args": {
            {"name": "状态码统计", "func": analyze_status_codes, "args": {
                "csv_path": temp_csv, "output_path": status_output,
                "slow_request_threshold": DEFAULT_SLOW_THRESHOLD
            }},
            {"name": "时间维度分析-全部接口", "func": analyze_time_dimension, "args": {
                "csv_path": temp_csv, "output_path": time_output
            }},
            {"name": "时间维度分析-特定接口", "func": analyze_time_dimension, "args": {
                "csv_path": temp_csv, "output_path": specific_uri_time_output,
                "specific_uri_list": DEFAULT_COLUMN_API
            }},

            {"name": "服务稳定性分析", "func": analyze_service_stability, "args": {
                "csv_path": temp_csv, "output_path": stability_output
            }},
            {"name": "IP来源分析", "func": analyze_ip_sources, "args": {
                "csv_path": temp_csv, "output_path": ip_analysis_output
            }},
            {"name": "请求头分析", "func": analyze_request_headers, "args": {
                "csv_path": temp_csv, "output_path": header_analysis_output
            }},
            {"name": "请求头性能关联分析", "func": analyze_header_performance_correlation, "args": {
                "csv_path": temp_csv, "output_path": header_performance_output,
                "slow_threshold": DEFAULT_SLOW_THRESHOLD
            }},
            {"name": "接口错误分析", "func": analyze_interface_errors, "args": {
                "csv_path": temp_csv, "output_path": interface_error_output,
                "slow_request_threshold": DEFAULT_SLOW_THRESHOLD
            }}
        ]

        # 优先执行API性能分析以获取最慢的接口
        log_info("预先执行API性能分析以识别最慢的接口...")
        top_apis_task = next((task for task in analysis_tasks if task["name"] == "API性能分析"), None)
        top_5_slowest = None

        if top_apis_task:
            top_5_slowest = top_apis_task["func"](**top_apis_task["args"])

            # 添加针对最慢接口的时间维度分析任务
            if top_5_slowest is not None and not top_5_slowest.empty:
                for i, row in top_5_slowest.iterrows():
                    slow_api = row['请求URI']
                    # 截取URI，避免文件名过长
                    safe_api_name = slow_api.replace("/", "_").replace("?", "_")[:30]
                    specific_api_output = os.path.join(output_dir, f"05_02.时间维度分析-慢接口-{safe_api_name}.xlsx")

                    log_info(f"添加慢接口时间维度分析任务: {slow_api}")
                    analysis_tasks.append({
                        "name": f"特定接口时间维度分析 ({safe_api_name})",
                        "func": analyze_time_dimension,
                        "args": {
                            "csv_path": temp_csv,
                            "output_path": specific_api_output,
                            "specific_uri_list": slow_api
                        }
                    })

        # 执行所有分析任务
        total_tasks = len(analysis_tasks)
        for i, task in enumerate(analysis_tasks, 1):
            task_name = task["name"]
            log_info(f"[{i}/{total_tasks}] 开始执行: {task_name}")

            start_time = datetime.now()

            # 跳过已执行的API性能分析
            if task_name == "API性能分析" and top_5_slowest is not None:
                result = top_5_slowest
            else:
                result = task["func"](**task["args"])

            elapsed = (datetime.now() - start_time).total_seconds()

            log_info(f"完成分析: {task_name} (耗时: {elapsed:.2f} 秒)", show_memory=True)

            # 保存结果到outputs
            if task_name == "API性能分析":
                outputs['slow_apis'] = result
                if result is not None and not result.empty:
                    log_info(f"最慢的接口前5名:")
                    for idx, row in result.iterrows():
                        log_info(f"  {idx + 1}. {row['请求URI']} (平均响应时间: {row['平均请求时长(秒)']:.3f}秒)")
            elif task_name == "服务层级分析":
                outputs['service_stats'] = result
            elif task_name == "状态码统计":
                outputs['status_stats'] = result
            elif task_name == "慢请求分析":
                if result is not None and not result.empty:
                    outputs['slowest_requests'] = result
                    log_info(f"发现 {len(result)} 条慢请求记录")
            elif task_name == "服务稳定性分析":
                outputs['service_stability'] = result
            elif task_name == "IP来源分析":
                outputs['ip_analysis'] = result
                if result is not None and not result.empty:
                    log_info(f"分析了 {len(result)} 个IP地址")
                    # 显示前5个请求量最大的IP
                    log_info("IP请求量前5名：")
                    for idx, row in result.iterrows():
                        log_info(f"  {idx + 1}. {row['IP地址']} (请求数: {row['总请求数']}, 风险评分: {row['风险评分']})")
            elif task_name == "请求头分析":
                outputs['header_analysis'] = result
                if result is not None and isinstance(result, dict):
                    log_info(f"请求头分析完成：")
                    log_info(f"  唯一User-Agent: {result.get('unique_user_agents', 0)} 个")
                    log_info(f"  唯一Referer: {result.get('unique_referers', 0)} 个")
                    if result.get('top_browsers'):
                        log_info("  TOP浏览器：")
                        for browser, count in list(result['top_browsers'].items())[:3]:
                            log_info(f"    - {browser}: {count}")
                    if result.get('top_domains'):
                        log_info("  TOP来源域名：")
                        for domain, count in list(result['top_domains'].items())[:3]:
                            log_info(f"    - {domain}: {count}")
            elif task_name == "请求头性能关联分析":
                outputs['header_performance_analysis'] = result
                if result is not None and isinstance(result, dict):
                    log_info(f"请求头性能关联分析完成：")
                    log_info(f"  整体慢请求率: {result.get('slow_rate_overall', 0)}%")
                    
                    # 显示性能最差的浏览器
                    if result.get('worst_browsers'):
                        log_info("  性能最差浏览器：")
                        for browser_info in result['worst_browsers'][:3]:
                            log_info(f"    - {browser_info['browser']}: 慢请求率 {browser_info['slow_rate']}%")
                    
                    # 显示优化建议
                    if result.get('performance_recommendations'):
                        log_info("  优化建议：")
                        for recommendation in result['performance_recommendations'][:3]:
                            log_info(f"    - {recommendation}")
            elif task_name == "接口错误分析":
                outputs['interface_error_analysis'] = result
                if result is not None and isinstance(result, dict):
                    log_info(f"接口错误分析完成：")
                    log_info(f"  全局错误率: {result.get('error_rate', 0):.2f}%")
                    log_info(f"  错误接口数: {result.get('error_interfaces', 0)}")
                    log_info(f"  受影响客户端: {result.get('affected_clients', 0)} 个")
                    
                    # 显示Top错误接口
                    if result.get('top_error_interfaces'):
                        log_info("  Top错误接口：")
                        for i, (interface, error_count) in enumerate(result['top_error_interfaces'][:3], 1):
                            log_info(f"    {i}. {interface}: {error_count} 次错误")

        # 生成综合报告
        log_info("生成综合报告...")
        generate_summary_report(outputs, summary_output)

        # 清理临时文件
        log_info("清理临时文件...")
        os.remove(temp_csv)
        log_info(f"已删除临时文件: {temp_csv}")

    except Exception as e:
        log_info(f"分析过程中发生错误: {str(e)}", level="ERROR")
        import traceback
        log_info(traceback.format_exc(), level="ERROR")
    finally:
        log_info("分析任务结束", level="INFO")


# 启动分析
if __name__ == "__main__":
    main()