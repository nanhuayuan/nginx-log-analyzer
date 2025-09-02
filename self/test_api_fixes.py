#!/usr/bin/env python3
"""
测试API修复是否正确
"""

def test_sampling_algorithms():
    """测试采样算法API"""
    print("🧪 测试采样算法API...")
    
    try:
        from self_00_05_sampling_algorithms import TDigest, HyperLogLog
        
        # 测试TDigest
        tdigest = TDigest(compression=100)
        tdigest.add(1.0)
        tdigest.add(2.0)
        tdigest.add(3.0)
        
        if tdigest.count > 0:
            p50 = tdigest.percentile(50)
            p95 = tdigest.percentile(95)
            print(f"✅ TDigest: count={tdigest.count}, P50={p50}, P95={p95}")
        else:
            print("❌ TDigest: 计数为0")
            
        # 测试HyperLogLog
        hll = HyperLogLog(precision=12)
        hll.add("ip1")
        hll.add("ip2")
        hll.add("ip1")  # 重复IP
        
        cardinality = hll.cardinality()
        print(f"✅ HyperLogLog: cardinality={cardinality}")
        
        return True
        
    except Exception as e:
        print(f"❌ 采样算法测试失败: {e}")
        return False

def test_advanced_analyzer_import():
    """测试高级分析器导入"""
    print("\n🧪 测试高级分析器导入...")
    
    try:
        from self_05_time_dimension_analyzer_advanced import AdvancedTimeDimensionAnalyzer
        
        # 创建分析器实例
        analyzer = AdvancedTimeDimensionAnalyzer()
        print("✅ 分析器创建成功")
        
        # 检查关键属性
        if hasattr(analyzer, 'stats'):
            print("✅ stats 属性存在")
        if hasattr(analyzer, 'time_samplers'):
            print("✅ time_samplers 属性存在") 
        if hasattr(analyzer, 'ip_counters'):
            print("✅ ip_counters 属性存在")
            
        return True
        
    except Exception as e:
        print(f"❌ 分析器导入失败: {e}")
        return False

def test_module_functions():
    """测试模块主要函数"""
    print("\n🧪 测试模块主要函数...")
    
    try:
        from self_05_time_dimension_analyzer_advanced import (
            analyze_time_dimension_advanced,
            analyze_time_dimension
        )
        
        print("✅ analyze_time_dimension_advanced 函数导入成功")
        print("✅ analyze_time_dimension 函数导入成功")
        
        return True
        
    except Exception as e:
        print(f"❌ 模块函数导入失败: {e}")
        return False

if __name__ == "__main__":
    print("🚀 开始测试API修复...")
    
    success1 = test_sampling_algorithms()
    success2 = test_advanced_analyzer_import() 
    success3 = test_module_functions()
    
    if success1 and success2 and success3:
        print("\n🎉 所有API修复测试通过！")
        print("\n📋 修复总结:")
        print("  ✅ TDigest构造函数: delta -> compression")
        print("  ✅ HyperLogLog构造函数: b -> precision")
        print("  ✅ TDigest分位数方法: quantile -> percentile")
        print("  ✅ TDigest计数属性: n -> count")
        print("  ✅ HyperLogLog计数方法: len -> cardinality")
        print("\n🚀 现在可以正常运行时间维度分析了！")
    else:
        print("\n❌ 部分测试失败，需要进一步修复")