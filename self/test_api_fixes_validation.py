#!/usr/bin/env python3
"""
验证算法API修复的测试脚本
"""

def test_algorithm_apis():
    """测试算法API调用是否正确"""
    print("🧪 验证算法API修复...")
    
    try:
        from self_00_05_sampling_algorithms import TDigest, HyperLogLog, ReservoirSampler
        
        # 测试 TDigest
        print("✅ 测试 TDigest API:")
        tdigest = TDigest(compression=100)
        tdigest.add(1.0)
        tdigest.add(2.0)
        tdigest.add(3.0)
        
        p50 = tdigest.percentile(50)  # 正确的方法
        print(f"  ✅ TDigest.percentile(50) = {p50}")
        print(f"  ✅ TDigest.count = {tdigest.count}")
        
        # 测试 HyperLogLog
        print("✅ 测试 HyperLogLog API:")
        hll = HyperLogLog(precision=12)
        hll.add("item1")
        hll.add("item2")
        hll.add("item3")
        
        cardinality = hll.cardinality()  # 正确的方法，不是count()
        print(f"  ✅ HyperLogLog.cardinality() = {cardinality}")
        
        # 测试 ReservoirSampler
        print("✅ 测试 ReservoirSampler API:")
        sampler = ReservoirSampler(max_size=1000)
        sampler.add({'value': 1})
        sampler.add({'value': 2})
        sampler.add({'value': 3})
        
        samples = sampler.get_samples()  # 正确的方法，不是get_sample()
        print(f"  ✅ ReservoirSampler.get_samples() = {len(samples)} samples")
        
        print("🎉 所有算法API调用验证通过!")
        return True
        
    except Exception as e:
        print(f"❌ API测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_import_fixes():
    """测试导入修复"""
    print("\n🧪 测试优化模块导入...")
    
    try:
        # 测试关键模块导入
        from self_08_ip_analyzer_advanced import AdvancedIPAnalyzer
        print("  ✅ self_08_ip_analyzer_advanced 导入成功")
        
        from self_10_request_header_analyzer_advanced import analyze_request_headers
        print("  ✅ self_10_request_header_analyzer_advanced 导入成功") 
        
        from self_11_header_performance_analyzer import analyze_header_performance_correlation
        print("  ✅ self_11_header_performance_analyzer 导入成功")
        
        print("🎉 所有模块导入验证通过!")
        return True
        
    except Exception as e:
        print(f"❌ 导入测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_algorithm_creation():
    """测试算法对象创建"""
    print("\n🧪 测试算法对象创建...")
    
    try:
        from self_00_05_sampling_algorithms import TDigest, HyperLogLog, ReservoirSampler
        
        # 模拟IP分析器中的用法
        ip_stats = {
            'response_time_digest': TDigest(compression=100),
            'data_size_digest': TDigest(compression=100),
            'unique_apis_hll': HyperLogLog(precision=12),
            'user_agents_sampler': ReservoirSampler(1000)
        }
        
        print("  ✅ IP分析器算法对象创建成功")
        
        # 模拟请求头分析器中的用法
        header_stats = {
            'unique_ips_hll': HyperLogLog(precision=12),
        }
        
        print("  ✅ 请求头分析器算法对象创建成功")
        
        # 模拟性能分析器中的用法
        perf_stats = {
            'response_times_sampler': ReservoirSampler(1000),
        }
        
        print("  ✅ 性能分析器算法对象创建成功")
        print("🎉 所有算法对象创建验证通过!")
        
        return True
        
    except Exception as e:
        print(f"❌ 算法对象创建测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 开始API修复验证测试...")
    
    test1 = test_algorithm_apis()
    test2 = test_import_fixes() 
    test3 = test_algorithm_creation()
    
    if test1 and test2 and test3:
        print("\n🎉 所有API修复验证测试通过!")
        print("✅ 优化版本可以正常使用了!")
    else:
        print("\n❌ 部分测试失败，需要进一步检查")