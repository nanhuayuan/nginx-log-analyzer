#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据质量验证和保证机制
验证7个物化视图的健康状态和数据完整性
"""

import clickhouse_connect
from datetime import datetime, timedelta

class DataQualityValidator:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host='localhost', 
            port=8123,
            username='analytics_user',
            password='analytics_password'
        )
        
    def validate_all(self):
        """运行完整的数据质量验证"""
        print("🔍 启动数据质量验证...")
        print("="*80)
        
        results = {}
        
        # 1. 基础连接验证
        results['connection'] = self._validate_connection()
        
        # 2. 架构完整性验证
        results['architecture'] = self._validate_architecture()
        
        # 3. 物化视图状态验证
        results['materialized_views'] = self._validate_materialized_views()
        
        # 4. 数据流验证
        results['data_flow'] = self._validate_data_flow()
        
        # 5. 性能验证
        results['performance'] = self._validate_performance()
        
        # 6. 生成质量报告
        self._generate_quality_report(results)
        
        return results
        
    def _validate_connection(self):
        """验证数据库连接"""
        print("\n1️⃣ 验证数据库连接...")
        try:
            result = self.client.query("SELECT 1")
            print("   ✅ ClickHouse连接正常")
            return {"status": "healthy", "message": "连接正常"}
        except Exception as e:
            print(f"   ❌ 连接失败: {e}")
            return {"status": "failed", "message": str(e)}
    
    def _validate_architecture(self):
        """验证架构完整性"""
        print("\n2️⃣ 验证架构完整性...")
        
        expected_tables = {
            "ODS层": ["ods_nginx_raw"],
            "DWD层": ["dwd_nginx_enriched_v2"],
            "ADS层": [
                "ads_api_performance_analysis",
                "ads_service_level_analysis", 
                "ads_slow_request_analysis",
                "ads_status_code_analysis",
                "ads_time_dimension_analysis",
                "ads_error_analysis_detailed",
                "ads_request_header_analysis"
            ]
        }
        
        result = {"status": "healthy", "missing_tables": [], "table_counts": {}}
        
        for layer, tables in expected_tables.items():
            print(f"\n   📋 检查{layer}:")
            for table in tables:
                try:
                    count_result = self.client.query(f"SELECT count() FROM nginx_analytics.{table}")
                    count = count_result.result_rows[0][0]
                    result["table_counts"][table] = count
                    print(f"      ✅ {table}: {count:,} 条")
                except Exception as e:
                    result["missing_tables"].append(table)
                    print(f"      ❌ {table}: 不存在或无法访问")
        
        if result["missing_tables"]:
            result["status"] = "degraded"
            
        return result
    
    def _validate_materialized_views(self):
        """验证物化视图状态"""
        print("\n3️⃣ 验证物化视图状态...")
        
        expected_views = [
            "mv_api_performance_hourly",
            "mv_service_level_hourly",
            "mv_slow_request_hourly", 
            "mv_status_code_hourly",
            "mv_time_dimension_hourly",
            "mv_error_analysis_hourly",
            "mv_request_header_hourly"
        ]
        
        result = {"status": "healthy", "active_views": 0, "total_views": len(expected_views)}
        
        for view in expected_views:
            try:
                # 检查视图是否存在
                view_check = self.client.query(f"""
                    SELECT count() FROM system.tables 
                    WHERE database = 'nginx_analytics' 
                    AND name = '{view}' 
                    AND engine = 'MaterializedView'
                """)
                
                if view_check.result_rows[0][0] > 0:
                    result["active_views"] += 1
                    print(f"   ✅ {view}: 运行中")
                else:
                    print(f"   ❌ {view}: 不存在")
                    
            except Exception as e:
                print(f"   ❌ {view}: 检查失败 - {str(e)[:50]}")
        
        success_rate = (result["active_views"] / result["total_views"]) * 100
        print(f"\n   📊 物化视图成功率: {success_rate:.1f}% ({result['active_views']}/{result['total_views']})")
        
        if success_rate < 100:
            result["status"] = "degraded"
        
        return result
    
    def _validate_data_flow(self):
        """验证数据流"""
        print("\n4️⃣ 验证数据流...")
        
        result = {"status": "healthy", "flow_health": {}}
        
        try:
            # ODS → DWD 流验证
            ods_count = self.client.query("SELECT count() FROM nginx_analytics.ods_nginx_raw").result_rows[0][0]
            dwd_count = self.client.query("SELECT count() FROM nginx_analytics.dwd_nginx_enriched_v2").result_rows[0][0]
            
            ods_dwd_ratio = (dwd_count / ods_count) * 100 if ods_count > 0 else 0
            result["flow_health"]["ods_to_dwd"] = ods_dwd_ratio
            
            print(f"   📊 ODS → DWD 数据流: {ods_dwd_ratio:.1f}% ({dwd_count:,}/{ods_count:,})")
            
            if ods_dwd_ratio < 95:
                print(f"   ⚠️  数据流健康度较低: {ods_dwd_ratio:.1f}%")
                result["status"] = "degraded"
            else:
                print(f"   ✅ 数据流健康: {ods_dwd_ratio:.1f}%")
            
            # 检查数据时间分布
            time_dist = self.client.query("""
                SELECT 
                    toDate(min(log_time)) as earliest_date,
                    toDate(max(log_time)) as latest_date,
                    dateDiff('day', min(log_time), max(log_time)) as time_span_days
                FROM nginx_analytics.dwd_nginx_enriched_v2
            """)
            
            if time_dist.result_rows:
                earliest, latest, span = time_dist.result_rows[0]
                print(f"   📅 数据时间范围: {earliest} 至 {latest} (共{span}天)")
                result["flow_health"]["time_span_days"] = span
            
        except Exception as e:
            print(f"   ❌ 数据流验证失败: {e}")
            result["status"] = "failed"
        
        return result
    
    def _validate_performance(self):
        """验证性能指标"""
        print("\n5️⃣ 验证性能指标...")
        
        result = {"status": "healthy", "query_times": {}}
        
        # 简单查询性能测试
        test_queries = {
            "basic_count": "SELECT count() FROM nginx_analytics.dwd_nginx_enriched_v2",
            "aggregation": "SELECT platform, count() FROM nginx_analytics.dwd_nginx_enriched_v2 GROUP BY platform",
            "time_query": "SELECT toHour(log_time), count() FROM nginx_analytics.dwd_nginx_enriched_v2 GROUP BY toHour(log_time)"
        }
        
        for query_name, query in test_queries.items():
            try:
                start_time = datetime.now()
                self.client.query(query)
                end_time = datetime.now()
                
                query_time = (end_time - start_time).total_seconds()
                result["query_times"][query_name] = query_time
                
                if query_time < 1.0:
                    print(f"   ✅ {query_name}: {query_time:.3f}s")
                elif query_time < 5.0:
                    print(f"   ⚠️  {query_name}: {query_time:.3f}s (较慢)")
                else:
                    print(f"   ❌ {query_name}: {query_time:.3f}s (过慢)")
                    result["status"] = "degraded"
                    
            except Exception as e:
                print(f"   ❌ {query_name}: 查询失败")
                result["status"] = "degraded"
        
        return result
    
    def _generate_quality_report(self, results):
        """生成质量报告"""
        print("\n6️⃣ 生成数据质量报告...")
        print("="*80)
        
        # 计算总体健康分数
        health_scores = []
        for component, result in results.items():
            if result.get("status") == "healthy":
                health_scores.append(100)
            elif result.get("status") == "degraded":
                health_scores.append(70)
            else:
                health_scores.append(0)
        
        overall_health = sum(health_scores) / len(health_scores) if health_scores else 0
        
        print(f"📊 整体数据质量评分: {overall_health:.1f}/100")
        
        if overall_health >= 90:
            print("🎉 数据质量: 优秀")
            health_status = "excellent"
        elif overall_health >= 75:
            print("✅ 数据质量: 良好")
            health_status = "good"
        elif overall_health >= 60:
            print("⚠️  数据质量: 一般")
            health_status = "fair"
        else:
            print("❌ 数据质量: 需要改进")
            health_status = "poor"
        
        # 保存报告
        report = {
            "timestamp": datetime.now().isoformat(),
            "overall_health_score": overall_health,
            "health_status": health_status,
            "component_results": results
        }
        
        return report
    
    def close(self):
        """关闭连接"""
        if self.client:
            self.client.close()

def main():
    validator = DataQualityValidator()
    
    try:
        report = validator.validate_all()
        print("\n" + "="*80)
        print("✅ 数据质量验证完成!")
        
    except Exception as e:
        print(f"❌ 验证过程中发生错误: {e}")
    finally:
        validator.close()

if __name__ == "__main__":
    main()