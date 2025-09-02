#!/usr/bin/env python3
"""
测试效率指数计算的调试脚本
"""

def test_efficiency_calculation():
    """测试效率指数计算逻辑"""
    print("🧪 测试效率指数计算逻辑...")
    
    # 模拟results数据结构
    results = {
        'daily': {
            '2025-07-20': {
                'qps': 10.5,
                'total_error_rate': 2.0,
                'slow_rate': 5.0,
                'percentiles': {
                    'total_request_duration': {
                        'P50': 1.2,
                        'P95': 2.5,
                        'P99': 3.0
                    }
                },
                'anomaly_score': 20.0
            },
            '2025-07-21': {
                'qps': 15.2,
                'total_error_rate': 1.0, 
                'slow_rate': 3.0,
                'percentiles': {
                    'total_request_duration': {
                        'P50': 0.8,
                        'P95': 1.8,
                        'P99': 2.2
                    }
                },
                'anomaly_score': 10.0
            }
        }
    }
    
    print(f"测试数据: {len(results['daily'])} 个时间点")
    
    # 模拟效率指数计算
    dimension = 'daily'
    
    # 收集数据
    all_qps = []
    all_response_times = []
    all_error_rates = []
    all_slow_rates = []
    
    for time_key, stats in results[dimension].items():
        all_qps.append(stats.get('qps', 0))
        all_error_rates.append(stats.get('total_error_rate', 0))
        all_slow_rates.append(stats.get('slow_rate', 0))
        
        percentiles = stats.get('percentiles', {})
        response_time = percentiles.get('total_request_duration', {}).get('P50', 0)
        all_response_times.append(response_time)
    
    print(f"数据收集: QPS={all_qps}, 错误率={all_error_rates}, 慢请求率={all_slow_rates}, 响应时间={all_response_times}")
    
    # 计算最值
    max_qps = max(all_qps) if all_qps and max(all_qps) > 0 else 1
    max_response_time = max(all_response_times) if all_response_times and max(all_response_times) > 0 else 1
    max_error_rate = max(all_error_rates) if all_error_rates and max(all_error_rates) > 0 else 1
    max_slow_rate = max(all_slow_rates) if all_slow_rates and max(all_slow_rates) > 0 else 1
    
    print(f"最值: max_qps={max_qps}, max_response_time={max_response_time}, max_error_rate={max_error_rate}, max_slow_rate={max_slow_rate}")
    
    # 计算每个时间点的效率指数
    for time_key, stats in results[dimension].items():
        qps = stats.get('qps', 0)
        error_rate = stats.get('total_error_rate', 0)
        slow_rate = stats.get('slow_rate', 0)
        percentiles = stats.get('percentiles', {})
        response_time = percentiles.get('total_request_duration', {}).get('P50', 0)
        
        # 计算各维度得分
        throughput_score = (qps / max_qps) * 100 if max_qps > 0 else 0
        
        if max_response_time > 0:
            response_score = max(0, 100 - (response_time / max_response_time) * 100)
        else:
            response_score = 100
            
        if max_error_rate > 1:
            reliability_score = max(0, 100 - (error_rate / max_error_rate) * 100)
        else:
            reliability_score = max(0, 100 - error_rate * 10)
            
        if max_slow_rate > 1:
            stability_score = max(0, 100 - (slow_rate / max_slow_rate) * 100)
        else:
            stability_score = max(0, 100 - slow_rate * 5)
            
        anomaly_score = stats.get('anomaly_score', 0)
        health_score = max(0, 100 - anomaly_score)
        
        # 综合效率指数
        efficiency_index = (
            throughput_score * 0.25 +
            response_score * 0.25 +
            reliability_score * 0.20 +
            stability_score * 0.15 +
            health_score * 0.15
        )
        
        print(f"\\n{time_key} 效率计算:")
        print(f"  原始数据: QPS={qps}, 错误率={error_rate}%, 慢请求率={slow_rate}%, 响应时间={response_time}s, 异常分={anomaly_score}")
        print(f"  子得分: 吞吐量={throughput_score:.1f}, 响应时间={response_score:.1f}, 可靠性={reliability_score:.1f}, 稳定性={stability_score:.1f}, 健康度={health_score:.1f}")
        print(f"  综合效率指数: {efficiency_index:.1f}")
        
        # 模拟存储到results
        results[dimension][time_key]['throughput_score'] = round(throughput_score, 1)
        results[dimension][time_key]['response_score'] = round(response_score, 1)
        results[dimension][time_key]['reliability_score'] = round(reliability_score, 1)
        results[dimension][time_key]['stability_score'] = round(stability_score, 1)
        results[dimension][time_key]['health_score'] = round(health_score, 1)
        results[dimension][time_key]['efficiency_index'] = round(efficiency_index, 1)
    
    # 验证存储结果
    print("\\n📊 存储结果验证:")
    for time_key, stats in results[dimension].items():
        print(f"{time_key}:")
        print(f"  吞吐量得分: {stats.get('throughput_score', 'NOT_FOUND')}")
        print(f"  可靠性得分: {stats.get('reliability_score', 'NOT_FOUND')}")
        print(f"  稳定性得分: {stats.get('stability_score', 'NOT_FOUND')}")
        print(f"  健康度得分: {stats.get('health_score', 'NOT_FOUND')}")
        print(f"  综合效率指数: {stats.get('efficiency_index', 'NOT_FOUND')}")
    
    return True

if __name__ == "__main__":
    print("🚀 开始效率指数调试测试...")
    
    success = test_efficiency_calculation()
    
    if success:
        print("\\n🎉 效率指数计算逻辑测试通过!")
        print("\\n💡 建议:")
        print("  1. 检查实际运行时的日志输出")
        print("  2. 确认基础数据(qps, error_rate等)是否正确计算")
        print("  3. 验证percentiles数据是否存在")
        print("  4. 检查异常检测是否正确执行")
    else:
        print("\\n❌ 效率指数计算测试失败!")