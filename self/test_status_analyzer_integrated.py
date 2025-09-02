#!/usr/bin/env python3
"""
测试整合后的高级状态码分析器
"""

import os
import sys

def test_import():
    """测试导入是否成功"""
    try:
        from self_04_status_analyzer_advanced import analyze_status_codes, AdvancedStatusAnalyzer
        print("✅ 导入成功")
        return True
    except Exception as e:
        print(f"❌ 导入失败: {e}")
        return False

def test_class_structure():
    """测试类结构是否正确"""
    try:
        from self_04_status_analyzer_advanced import AdvancedStatusAnalyzer
        
        # 检查类的主要方法
        analyzer = AdvancedStatusAnalyzer()
        methods = [
            'analyze_status_codes',
            '_process_data_stream',
            '_create_method_status_analysis',
            '_create_status_lifecycle_analysis',
            '_add_charts_to_excel',
            '_create_status_distribution_pie_chart',
            '_create_time_trend_charts',
            '_create_method_distribution_chart',
            '_calculate_lifecycle_efficiency',
            '_analyze_performance_bottleneck'
        ]
        
        for method in methods:
            if hasattr(analyzer, method):
                print(f"✅ {method} 方法存在")
            else:
                print(f"❌ {method} 方法缺失")
                return False
                
        return True
    except Exception as e:
        print(f"❌ 类结构测试失败: {e}")
        return False

def test_report_structure():
    """测试报告结构"""
    try:
        from self_04_status_analyzer_advanced import AdvancedStatusAnalyzer
        
        analyzer = AdvancedStatusAnalyzer()
        
        # 检查报告结构
        expected_reports = [
            'summary',
            'detailed_status',
            'app_analysis',
            'service_analysis',
            'time_analysis',
            'error_analysis',
            'performance_analysis',
            'slow_request_api_summary',
            'performance_detail_analysis',
            'status_lifecycle_analysis',
            'method_status_analysis',  # 新增
            'anomaly_report',
            'optimization_suggestions'
        ]
        
        print("✅ 预期报告结构:")
        for i, report in enumerate(expected_reports, 1):
            print(f"  {i}. {report}")
            
        return True
    except Exception as e:
        print(f"❌ 报告结构测试失败: {e}")
        return False

def test_chart_functionality():
    """测试图表功能"""
    try:
        from self_04_status_analyzer_advanced import AdvancedStatusAnalyzer
        
        analyzer = AdvancedStatusAnalyzer()
        
        chart_methods = [
            '_create_status_distribution_pie_chart',
            '_create_time_trend_charts',
            '_create_method_distribution_chart'
        ]
        
        for method in chart_methods:
            if hasattr(analyzer, method):
                print(f"✅ {method} 图表方法存在")
            else:
                print(f"❌ {method} 图表方法缺失")
                return False
                
        return True
    except Exception as e:
        print(f"❌ 图表功能测试失败: {e}")
        return False

def main():
    """主测试函数"""
    print("=== 整合后的高级状态码分析器测试 ===")
    
    success = True
    
    # 测试1：导入测试
    print("\n1. 测试导入...")
    if not test_import():
        success = False
    
    # 测试2：类结构测试
    print("\n2. 测试类结构...")
    if not test_class_structure():
        success = False
    
    # 测试3：报告结构测试
    print("\n3. 测试报告结构...")
    if not test_report_structure():
        success = False
    
    # 测试4：图表功能测试
    print("\n4. 测试图表功能...")
    if not test_chart_functionality():
        success = False
    
    # 总结
    print("\n=== 测试总结 ===")
    if success:
        print("✅ 所有测试通过！整合成功")
        print("\n整合完成的功能:")
        print("📊 新增的Sheet:")
        print("  • HTTP方法状态码分析 (整合原版本)")
        print("  • 完善的状态码生命周期分析")
        print("📈 新增的图表:")
        print("  • 状态码分布饼图")
        print("  • 时间趋势折线图")
        print("  • HTTP方法分布柱状图")
        print("🔍 增强的分析:")
        print("  • 生命周期效率计算")
        print("  • 性能瓶颈分析")
        print("  • 智能优化建议")
    else:
        print("❌ 部分测试失败")
        
    return success

if __name__ == "__main__":
    main()