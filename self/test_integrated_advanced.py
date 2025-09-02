#!/usr/bin/env python3
"""
测试整合后的高级慢请求分析器
"""

import os
import sys

# 简单的语法和导入测试
def test_import():
    """测试导入是否成功"""
    try:
        from self_03_slow_requests_analyzer_advanced import analyze_slow_requests_advanced, AdvancedSlowRequestAnalyzer
        print("✅ 导入成功")
        return True
    except Exception as e:
        print(f"❌ 导入失败: {e}")
        return False

def test_class_structure():
    """测试类结构是否正确"""
    try:
        from self_03_slow_requests_analyzer_advanced import AdvancedSlowRequestAnalyzer
        
        # 检查类的主要方法
        analyzer = AdvancedSlowRequestAnalyzer()
        methods = [
            'analyze_slow_requests',
            '_process_data_stream',
            '_update_api_stats',
            '_generate_api_summary_stats',
            '_create_api_summary_sheet',
            '_create_performance_analysis_sheet',
            '_create_transfer_efficiency_sheet',
            '_create_performance_insights_sheet',
            '_create_optimization_recommendations_sheet'
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

def main():
    """主测试函数"""
    print("=== 整合后的高级慢请求分析器测试 ===")
    
    success = True
    
    # 测试1：导入测试
    print("\n1. 测试导入...")
    if not test_import():
        success = False
    
    # 测试2：类结构测试
    print("\n2. 测试类结构...")
    if not test_class_structure():
        success = False
    
    # 测试3：基础功能测试
    print("\n3. 测试基础功能...")
    if not test_basic_functionality():
        success = False
    
    # 总结
    print("\n=== 测试总结 ===")
    if success:
        print("✅ 所有测试通过！整合成功")
        print("\n新的Sheet结构:")
        print("1. 慢请求详细列表")
        print("2. 智能分析汇总")
        print("3. 慢请求API汇总 ⭐ (整合原版本)")
        print("4. 性能分析 ⭐ (整合原版本)")
        print("5. 根因分析")
        print("6. 传输效率分析 ⭐ (整合原版本)")
        print("7. 智能性能洞察")
        print("8. 优化建议")
    else:
        print("❌ 部分测试失败")

def test_basic_functionality():
    """测试基础功能"""
    try:
        from self_03_slow_requests_analyzer_advanced import AdvancedSlowRequestAnalyzer
        
        # 创建分析器实例
        analyzer = AdvancedSlowRequestAnalyzer(slow_threshold=3.0)
        
        # 检查初始化
        if hasattr(analyzer, 'slow_threshold') and analyzer.slow_threshold == 3.0:
            print("✅ 分析器初始化成功")
        else:
            print("❌ 分析器初始化失败")
            return False
            
        # 检查重要属性
        required_attrs = ['api_stats', 'global_stats', 'analysis_results']
        for attr in required_attrs:
            if hasattr(analyzer, attr):
                print(f"✅ {attr} 属性存在")
            else:
                print(f"❌ {attr} 属性缺失")
                return False
        
        return True
    except Exception as e:
        print(f"❌ 基础功能测试失败: {e}")
        return False

if __name__ == "__main__":
    main()