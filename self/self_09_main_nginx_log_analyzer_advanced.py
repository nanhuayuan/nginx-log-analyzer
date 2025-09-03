import os
import gc
import pandas as pd
from datetime import datetime
import traceback

# 导入优化版本的分析模块
from self_01_api_analyzer_optimized import analyze_api_performance
from self_02_service_analyzer_advanced import analyze_service_performance_advanced
from self_03_slow_requests_analyzer_advanced import analyze_slow_requests_advanced
from self_04_status_analyzer_advanced import analyze_status_codes
from self_05_time_dimension_analyzer_advanced import analyze_time_dimension
from self_06_performance_stability_analyzer_advanced import analyze_service_stability
from self_07_generate_summary_report_analyzer_advanced import generate_advanced_summary_report
from self_08_ip_analyzer_advanced import analyze_ip_sources
from self_10_request_header_analyzer import analyze_request_headers
from self_11_header_performance_analyzer import analyze_header_performance_correlation

# 导入常量和工具函数
from self_00_01_constants import (
    DEFAULT_LOG_DIR, DEFAULT_SUCCESS_CODES, DEFAULT_SLOW_THRESHOLD, 
    DEFAULT_COLUMN_API, DEFAULT_START_DATE, DEFAULT_END_DATE
)
from self_00_03_log_parser import collect_log_files, process_log_files
from self_00_02_utils import log_info


class AdvancedNginxLogAnalyzer:
    """高级Nginx日志分析器 - 统一协调所有分析模块"""
    
    def __init__(self):
        self.script_start_time = datetime.now()
        self.outputs = {}
        self.temp_files = []
        
    def main(self):
        """主分析流程 - 优化版"""
        try:
            log_info(f"🚀 开始执行高级Nginx日志分析任务 (版本: 2.0.0-Advanced)", show_memory=True)
            
            # 初始化分析环境
            log_dir, output_dir, temp_dir, temp_csv = self._setup_analysis_environment()
            
            # 收集和处理日志文件
            total_records = self._collect_and_process_logs(log_dir, temp_csv)
            if total_records == 0:
                log_info("❌ 未找到有效日志数据，分析终止", level="ERROR")
                return
            
            # 初始化输出数据结构
            self._initialize_outputs(log_dir, total_records)
            
            # 定义分析任务配置
            analysis_tasks = self._define_analysis_tasks(temp_csv, output_dir)
            
            # 执行所有分析任务
            self._execute_analysis_tasks(analysis_tasks, temp_csv, output_dir)
            
            # 生成高级综合报告
            summary_output = os.path.join(output_dir, "12_高级综合报告.xlsx")
            self._generate_advanced_summary_report(summary_output)
            
            # 清理临时文件
            self._cleanup_temp_files()
            
            # 显示分析完成摘要
            self._show_completion_summary(output_dir)
            
        except Exception as e:
            log_info(f"❌ 分析过程中发生错误: {str(e)}", level="ERROR")
            log_info(traceback.format_exc(), level="ERROR")
        finally:
            self._final_cleanup()
            total_elapsed = (datetime.now() - self.script_start_time).total_seconds()
            log_info(f"🎉 高级分析任务完成，总耗时: {total_elapsed:.2f} 秒", level="INFO")
    
    def _setup_analysis_environment(self):
        """设置分析环境"""
        log_info("🔧 设置分析环境...")
        
        log_dir = DEFAULT_LOG_DIR
        output_dir = f"{log_dir}_高级分析结果_{self.script_start_time.strftime('%Y%m%d_%H%M%S')}"
        temp_dir = f"{output_dir}_temp"
        temp_csv = os.path.join(temp_dir, "processed_logs.csv")
        
        # 创建目录
        for directory in [output_dir, temp_dir]:
            if not os.path.exists(directory):
                os.makedirs(directory)
                log_info(f"📁 创建目录: {directory}")
        
        self.temp_files.append(temp_csv)
        return log_dir, output_dir, temp_dir, temp_csv
    
    def _collect_and_process_logs(self, log_dir, temp_csv):
        """收集和处理日志文件"""
        log_info("📂 收集日志文件...")
        
        log_files = collect_log_files(log_dir)
        if not log_files:
            log_info("⚠️ 未找到日志文件!", level="ERROR")
            return 0
        
        log_info(f"✅ 找到 {len(log_files)} 个日志文件")
        
        # 处理日志文件
        log_info("🔄 处理日志文件...")
        total_records = process_log_files(
            log_files, temp_csv, 
            start_date=DEFAULT_START_DATE, 
            end_date=DEFAULT_END_DATE
        )
        
        log_info(f"✅ 日志处理完成，共 {total_records:,} 条记录")
        return total_records
    
    def _initialize_outputs(self, log_dir, total_records):
        """初始化输出数据结构"""
        self.outputs = {
            'log_dir': log_dir,
            'total_requests': total_records,
            'analysis_time': self.script_start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'analyzer_version': '2.0.0-Advanced',
            'optimization_features': [
                'T-Digest分位数计算',
                'HyperLogLog唯一值计数',
                '蓄水池采样算法',
                '流式内存管理',
                '智能异常检测',
                '多维度性能分析'
            ]
        }
    
    def _define_analysis_tasks(self, temp_csv, output_dir):
        """定义分析任务配置"""
        return [
            {
                "name": "API性能分析", 
                "priority": 1,
                "func": analyze_api_performance, 
                "args": {
                    "csv_path": temp_csv, 
                    "output_path": os.path.join(output_dir, "01.接口性能分析.xlsx"),
                    "success_codes": DEFAULT_SUCCESS_CODES, 
                    "slow_threshold": DEFAULT_SLOW_THRESHOLD
                },
                "description": "分析API性能指标，识别性能瓶颈",
                "output_key": "slow_apis"
            },
            {
                "name": "高级服务层级分析", 
                "priority": 2,
                "func": analyze_service_performance_advanced, 
                "args": {
                    "csv_path": temp_csv, 
                    "output_path": os.path.join(output_dir, "02.服务层级分析.xlsx"),
                    "success_codes": DEFAULT_SUCCESS_CODES
                },
                "description": "深度分析服务层级性能，使用流式算法",
                "output_key": "service_stats"
            },
            {
                "name": "高级慢请求分析", 
                "priority": 3,
                "func": analyze_slow_requests_advanced, 
                "args": {
                    "csv_path": temp_csv, 
                    "output_path": os.path.join(output_dir, "03_慢请求分析.xlsx"),
                    "slow_threshold": DEFAULT_SLOW_THRESHOLD
                },
                "description": "智能识别和分析慢请求模式",
                "output_key": "slowest_requests"
            },
            {
                "name": "高级状态码分析", 
                "priority": 4,
                "func": analyze_status_codes, 
                "args": {
                    "csv_path": temp_csv, 
                    "output_path": os.path.join(output_dir, "04.状态码统计.xlsx"),
                    "slow_request_threshold": DEFAULT_SLOW_THRESHOLD
                },
                "description": "全面分析HTTP状态码分布",
                "output_key": "status_stats"
            },
            {
                "name": "高级时间维度分析-全部接口", 
                "priority": 5,
                "func": analyze_time_dimension, 
                "args": {
                    "csv_path": temp_csv, 
                    "output_path": os.path.join(output_dir, "05.时间维度分析-全部接口.xlsx")
                },
                "description": "基于T-Digest的时间维度深度分析",
                "output_key": "time_analysis_all"
            },
            {
                "name": "时间维度分析-特定接口", 
                "priority": 6,
                "func": analyze_time_dimension, 
                "args": {
                    "csv_path": temp_csv, 
                    "output_path": os.path.join(output_dir, "05_01.时间维度分析-指定接口.xlsx"),
                    "specific_uri_list": DEFAULT_COLUMN_API
                },
                "description": "针对关键接口的时间维度分析",
                "output_key": "time_analysis_specific"
            },
            {
                "name": "高级服务稳定性分析", 
                "priority": 7,
                "func": analyze_service_stability, 
                "args": {
                    "csv_path": temp_csv, 
                    "output_path": os.path.join(output_dir, "06_服务稳定性.xlsx")
                },
                "description": "多维度服务稳定性评估，含异常检测",
                "output_key": "service_stability"
            },
            {
                "name": "高级IP来源分析", 
                "priority": 8,
                "func": analyze_ip_sources, 
                "args": {
                    "csv_path": temp_csv, 
                    "output_path": os.path.join(output_dir, "08_IP来源分析.xlsx")
                },
                "description": "智能IP行为分析，含风险评估",
                "output_key": "ip_analysis"
            },
            {
                "name": "请求头分析", 
                "priority": 9,
                "func": analyze_request_headers, 
                "args": {
                    "csv_path": temp_csv, 
                    "output_path": os.path.join(output_dir, "10_请求头分析.xlsx")
                },
                "description": "User-Agent和Referer深度分析",
                "output_key": "header_analysis"
            },
            {
                "name": "请求头性能关联分析", 
                "priority": 10,
                "func": analyze_header_performance_correlation, 
                "args": {
                    "csv_path": temp_csv, 
                    "output_path": os.path.join(output_dir, "11_请求头性能关联分析.xlsx"),
                    "slow_threshold": DEFAULT_SLOW_THRESHOLD
                },
                "description": "请求头与性能指标的关联性分析",
                "output_key": "header_performance_analysis"
            }
        ]
    
    def _execute_analysis_tasks(self, analysis_tasks, temp_csv, output_dir):
        """执行所有分析任务"""
        # 优先执行API性能分析以获取最慢的接口
        api_task = next((task for task in analysis_tasks if task["name"] == "API性能分析"), None)
        top_5_slowest = None
        
        if api_task:
            log_info("🔍 预先执行API性能分析以识别最慢的接口...")
            top_5_slowest = self._execute_single_task(api_task)
            
            # 为最慢接口添加专门的时间维度分析
            if top_5_slowest is not None and not top_5_slowest.empty:
                self._add_slow_api_analysis_tasks(analysis_tasks, top_5_slowest, temp_csv, output_dir)
        
        # 执行所有分析任务
        total_tasks = len(analysis_tasks)
        log_info(f"📊 开始执行 {total_tasks} 个分析任务...")
        
        for i, task in enumerate(analysis_tasks, 1):
            task_name = task["name"]
            
            # 跳过已执行的API性能分析
            if task_name == "API性能分析" and top_5_slowest is not None:
                log_info(f"[{i}/{total_tasks}] ✅ {task_name} (已预先执行)")
                self.outputs[task.get('output_key', task_name.lower())] = top_5_slowest
                continue
            
            log_info(f"[{i}/{total_tasks}] 🔄 开始执行: {task_name}")
            log_info(f"    📝 {task.get('description', '执行分析任务')}")
            
            result = self._execute_single_task(task)
            
            # 处理任务结果
            self._process_task_result(task, result)
            
            # 定期执行垃圾回收
            if i % 3 == 0:
                gc.collect()
                log_info(f"🧹 执行垃圾回收 ({i}/{total_tasks} 任务已完成)")
    
    def _execute_single_task(self, task):
        """执行单个分析任务"""
        start_time = datetime.now()
        result = None
        
        try:
            result = task["func"](**task["args"])
            elapsed = (datetime.now() - start_time).total_seconds()
            log_info(f"    ✅ 完成分析: {task['name']} (耗时: {elapsed:.2f} 秒)", show_memory=True)
            
        except Exception as e:
            elapsed = (datetime.now() - start_time).total_seconds()
            log_info(f"    ❌ 任务失败: {task['name']} (耗时: {elapsed:.2f} 秒)", level="ERROR")
            log_info(f"    错误详情: {str(e)}", level="ERROR")
            result = None
            
        return result
    
    def _add_slow_api_analysis_tasks(self, analysis_tasks, top_5_slowest, temp_csv, output_dir):
        """为最慢接口添加专门的时间维度分析任务"""
        log_info("🐌 为最慢接口添加专门分析任务...")
        
        for i, row in top_5_slowest.iterrows():
            slow_api = row['请求URI']
            # 截取URI，避免文件名过长
            safe_api_name = slow_api.replace("/", "_").replace("?", "_").replace("&", "_")[:30]
            specific_api_output = os.path.join(output_dir, f"05_02.时间维度分析-慢接口-{safe_api_name}.xlsx")
            
            log_info(f"    📌 添加慢接口分析: {slow_api}")
            analysis_tasks.append({
                "name": f"慢接口时间维度分析 ({safe_api_name})",
                "priority": 5.5 + i * 0.1,  # 插入到时间分析任务之后
                "func": analyze_time_dimension,
                "args": {
                    "csv_path": temp_csv,
                    "output_path": specific_api_output,
                    "specific_uri_list": slow_api
                },
                "description": f"针对慢接口 {slow_api[:50]} 的深度时间分析",
                "output_key": f"slow_api_analysis_{i}"
            })
        
        # 按优先级重新排序任务
        analysis_tasks.sort(key=lambda x: x.get('priority', 999))
    
    def _process_task_result(self, task, result):
        """处理任务结果"""
        task_name = task["name"]
        output_key = task.get('output_key', task_name.lower())
        
        # 保存结果到outputs
        self.outputs[output_key] = result
        
        # 根据任务类型显示特定的结果摘要
        if "API性能分析" in task_name and result is not None and not result.empty:
            self._show_api_analysis_summary(result)
            
        elif "服务层级分析" in task_name and result is not None:
            log_info(f"    📋 服务层级分析完成")
            
        elif "慢请求分析" in task_name and result is not None and not result.empty:
            log_info(f"    🐌 发现 {len(result)} 条慢请求记录")
            
        elif "IP来源分析" in task_name and result is not None and not result.empty:
            self._show_ip_analysis_summary(result)
            
        elif "请求头分析" in task_name and result is not None and isinstance(result, dict):
            self._show_header_analysis_summary(result)
            
        elif "请求头性能关联分析" in task_name and result is not None and isinstance(result, dict):
            self._show_header_performance_summary(result)
    
    def _show_api_analysis_summary(self, result):
        """显示API分析摘要"""
        log_info(f"    📊 最慢的接口前5名:")
        for idx, row in result.iterrows():
            log_info(f"      {idx + 1}. {row['请求URI']} (平均响应时间: {row['平均请求时长(秒)']:.3f}秒)")
    
    def _show_ip_analysis_summary(self, result):
        """显示IP分析摘要"""
        log_info(f"    📊 分析了 {len(result)} 个IP地址")
        log_info(f"    📊 IP请求量前5名：")
        for idx, row in result.iterrows():
            if idx >= 5:
                break
            risk_level = "🔴高风险" if row.get('风险评分', 0) > 70 else ("🟡中风险" if row.get('风险评分', 0) > 50 else "🟢低风险")
            log_info(f"      {idx + 1}. {row['IP地址']} (请求数: {row['总请求数']}, {risk_level}: {row.get('风险评分', 0)})")
    
    def _show_header_analysis_summary(self, result):
        """显示请求头分析摘要"""
        log_info(f"    📊 请求头分析完成：")
        log_info(f"      唯一User-Agent: {result.get('unique_user_agents', 0)} 个")
        log_info(f"      唯一Referer: {result.get('unique_referers', 0)} 个")
        
        if result.get('top_browsers'):
            log_info(f"      TOP浏览器：")
            for browser, count in list(result['top_browsers'].items())[:3]:
                log_info(f"        - {browser}: {count}")
    
    def _show_header_performance_summary(self, result):
        """显示请求头性能分析摘要"""
        log_info(f"    📊 请求头性能关联分析完成：")
        log_info(f"      整体慢请求率: {result.get('slow_rate_overall', 0)}%")
        
        if result.get('worst_browsers'):
            log_info(f"      性能最差浏览器：")
            for browser_info in result['worst_browsers'][:3]:
                log_info(f"        - {browser_info['browser']}: 慢请求率 {browser_info['slow_rate']}%")
    
    def _generate_advanced_summary_report(self, summary_output):
        """生成高级综合报告"""
        log_info("📋 生成高级综合报告...")
        try:
            generate_advanced_summary_report(self.outputs, summary_output)
            log_info(f"✅ 高级综合报告已生成：{summary_output}")
        except Exception as e:
            log_info(f"❌ 生成综合报告失败: {str(e)}", level="ERROR")
            log_info(traceback.format_exc(), level="ERROR")
    
    def _cleanup_temp_files(self):
        """清理临时文件"""
        log_info("🧹 清理临时文件...")
        for temp_file in self.temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    log_info(f"    🗑️ 已删除临时文件: {temp_file}")
            except Exception as e:
                log_info(f"    ⚠️ 删除临时文件失败 {temp_file}: {e}", level="WARNING")
    
    def _show_completion_summary(self, output_dir):
        """显示分析完成摘要"""
        log_info("🎯 === 高级分析完成摘要 ===")
        log_info(f"📁 输出目录: {output_dir}")
        log_info(f"📊 总处理记录: {self.outputs.get('total_requests', 0):,}")
        log_info(f"⏱️ 分析开始时间: {self.outputs.get('analysis_time', 'Unknown')}")
        log_info(f"🚀 分析器版本: {self.outputs.get('analyzer_version', 'Unknown')}")
        
        log_info("🔧 优化特性:")
        for feature in self.outputs.get('optimization_features', []):
            log_info(f"  ✅ {feature}")
        
        # 统计生成的文件
        if os.path.exists(output_dir):
            output_files = [f for f in os.listdir(output_dir) if f.endswith('.xlsx')]
            log_info(f"📄 生成了 {len(output_files)} 个分析报告")
    
    def _final_cleanup(self):
        """最终清理"""
        gc.collect()
        log_info("🧹 执行最终垃圾回收", level="INFO")


# 向后兼容的函数接口
def main():
    """主函数 - 兼容接口"""
    analyzer = AdvancedNginxLogAnalyzer()
    analyzer.main()


# 启动高级分析
if __name__ == "__main__":
    main()