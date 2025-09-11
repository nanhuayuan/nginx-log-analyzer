# Grafana权限控制方案设计

## 方案选择：字段逻辑隔离 ✅

### 推荐理由

1. **Grafana原生支持好**
   - Dashboard变量系统完美配合字段过滤
   - 组织(Organization)权限天然隔离
   - 文件夹(Folder)权限精确控制仪表板访问

2. **ClickHouse性能优化**
   - 利用分区和索引优化基于字段的查询
   - 避免多表JOIN的性能开销
   - 统一的存储格式便于压缩

3. **运维管理简单** 
   - 单表结构，DDL变更简单
   - 备份恢复一致性好
   - 跨业务线统计分析方便

## 数据模型设计

### 表结构设计（基于现有nginx_logs扩展）

```sql
-- 增强版nginx日志表：支持多维度权限控制
CREATE TABLE nginx_analytics.nginx_logs_with_permissions
(
    -- ========== 原有字段保持不变 ==========
    timestamp DateTime,
    remote_addr String,
    request_method String,
    request_uri String,
    status_code UInt16,
    response_time Float32,
    request_size UInt32,
    response_size UInt32,
    user_agent String,
    referer String,
    
    -- ========== 权限控制字段（新增）==========
    
    -- 业务维度（核心隔离字段）
    tenant_code String,              -- 租户代码：company_a, company_b
    business_domain String,          -- 业务域：finance, marketing, ops
    project_name String,             -- 项目名称：project_alpha, project_beta
    
    -- 环境维度  
    environment Enum8(               -- 环境：开发/测试/生产
        'dev' = 1, 
        'test' = 2, 
        'staging' = 3, 
        'prod' = 4
    ),
    region String,                   -- 区域：beijing, shanghai, guangzhou
    
    -- 服务维度
    service_name String,             -- 服务名称：user-service, order-service
    service_version String,          -- 服务版本：v1.2.3
    cluster_name String,             -- 集群名称：k8s-prod-01
    
    -- 团队和责任人
    team_name String,                -- 团队名称：backend-team, frontend-team
    cost_center String,              -- 成本中心：CC001, CC002
    
    -- 数据分类（用于细粒度权限控制）
    data_sensitivity Enum8(          -- 数据敏感级别
        'public' = 1,                -- 公开：可跨团队查看
        'internal' = 2,              -- 内部：仅本业务域查看  
        'confidential' = 3,          -- 机密：仅指定角色查看
        'restricted' = 4             -- 受限：仅管理员查看
    ),
    
    -- ========== 性能优化索引 ==========
    -- 关键字段建立跳跃索引，优化过滤查询性能
    INDEX idx_tenant_business (tenant_code, business_domain) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_env_team (environment, team_name) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_service_cluster (service_name, cluster_name) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
-- 按租户+时间分区，确保数据物理隔离和查询性能
PARTITION BY (tenant_code, toYYYYMM(timestamp))
-- 排序键优化权限过滤查询
ORDER BY (tenant_code, business_domain, environment, timestamp, service_name)
-- 采样表达式用于近似查询
SAMPLE BY xxHash32(remote_addr)
-- 数据TTL管理
TTL timestamp + INTERVAL 180 DAY DELETE;
```

## Grafana权限集成方案

### 1. 组织架构映射

```yaml
grafana_organizations:
  # 租户级隔离
  company_a:
    org_id: 2
    datasource: "ClickHouse_CompanyA"  
    folders:
      - name: "财务业务线"
        permissions:
          - user: "finance_analyst@company-a.com"
            role: "Viewer"
          - user: "finance_manager@company-a.com"  
            role: "Editor"
      - name: "营销业务线"
        permissions:
          - user: "marketing_analyst@company-a.com"
            role: "Viewer"
            
  company_b:
    org_id: 3
    datasource: "ClickHouse_CompanyB"
    folders:
      - name: "运维监控"
        permissions:
          - user: "ops_engineer@company-b.com"
            role: "Editor"
```

### 2. Dashboard变量配置

```json
{
  "templating": {
    "list": [
      {
        "name": "tenant_code",
        "type": "constant",
        "current": {
          "value": "company_a"
        },
        "hide": 2
      },
      {
        "name": "business_domain", 
        "type": "query",
        "query": "SELECT DISTINCT business_domain FROM nginx_analytics.nginx_logs_with_permissions WHERE tenant_code = '$tenant_code' ORDER BY business_domain",
        "current": {
          "text": "All",
          "value": "$__all"
        },
        "includeAll": true
      },
      {
        "name": "environment",
        "type": "query", 
        "query": "SELECT DISTINCT environment FROM nginx_analytics.nginx_logs_with_permissions WHERE tenant_code = '$tenant_code' AND business_domain IN ($business_domain) ORDER BY environment",
        "current": {
          "text": "prod",
          "value": "prod"  
        }
      },
      {
        "name": "team_name",
        "type": "query",
        "query": "SELECT DISTINCT team_name FROM nginx_analytics.nginx_logs_with_permissions WHERE tenant_code = '$tenant_code' AND business_domain IN ($business_domain) ORDER BY team_name"
      }
    ]
  }
}
```

### 3. 查询模板示例

```sql
-- Dashboard查询自动注入权限过滤条件
SELECT 
    toUnixTimestamp(toStartOfMinute(timestamp)) * 1000 as t,
    count() as requests,
    avg(response_time) as avg_response_time
FROM nginx_analytics.nginx_logs_with_permissions 
WHERE 1=1
    -- 租户隔离（必须）
    AND tenant_code = '$tenant_code'
    -- 业务域过滤（基于用户权限）
    AND business_domain IN ($business_domain)
    -- 环境过滤（基于角色权限）
    AND environment IN ($environment)
    -- 团队过滤（可选）
    AND ($team_name = 'All' OR team_name IN ($team_name))
    -- 数据敏感性过滤（基于用户角色）
    AND (
        data_sensitivity = 'public' 
        OR (data_sensitivity = 'internal' AND hasRole('internal_reader'))
        OR (data_sensitivity = 'confidential' AND hasRole('confidential_reader'))
        OR hasRole('admin')
    )
    -- 时间范围
    AND timestamp >= $__timeFrom() AND timestamp <= $__timeTo()
GROUP BY t
ORDER BY t
```

## 权限控制实现

### 1. Grafana数据源配置

```json
{
  "name": "ClickHouse_CompanyA",
  "type": "clickhouse", 
  "url": "http://clickhouse:8123",
  "database": "nginx_analytics",
  "user": "company_a_user",
  "jsonData": {
    "defaultTable": "nginx_logs_with_permissions",
    "timeout": 30,
    "addCorsHeader": true
  },
  "secureJsonData": {
    "password": "company_a_password"
  }
}
```

### 2. ClickHouse用户权限配置

```xml
<!-- ClickHouse用户配置 -->
<clickhouse>
    <users>
        <company_a_user>
            <password_sha256_hex>...</password_sha256_hex>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>readonly_profile</profile>
            <quota>default</quota>
            <!-- 行级安全策略 -->
            <access_management>0</access_management>
            <databases>
                <nginx_analytics>
                    <nginx_logs_with_permissions>
                        <!-- 强制行级过滤条件 -->
                        <filter>tenant_code = 'company_a'</filter>
                    </nginx_logs_with_permissions>
                </nginx_analytics>
            </databases>
        </company_a_user>
    </users>
</clickhouse>
```

### 3. 动态权限注入中间件

```python
class GrafanaPermissionMiddleware:
    def inject_tenant_filter(self, query, user_context):
        """为查询自动注入租户过滤条件"""
        
        # 提取租户信息
        tenant_code = user_context.get('tenant_code')
        allowed_domains = user_context.get('allowed_business_domains', [])
        user_role = user_context.get('role', 'viewer')
        
        # 构建过滤条件
        filters = [
            f"tenant_code = '{tenant_code}'"
        ]
        
        # 业务域权限过滤
        if allowed_domains and 'admin' not in user_role:
            domain_filter = "', '".join(allowed_domains)
            filters.append(f"business_domain IN ('{domain_filter}')")
            
        # 环境权限过滤
        if 'prod_access' not in user_context.get('permissions', []):
            filters.append("environment != 'prod'")
            
        # 数据敏感性过滤  
        if 'confidential_reader' not in user_context.get('permissions', []):
            filters.append("data_sensitivity IN ('public', 'internal')")
            
        # 注入WHERE条件
        where_clause = " AND ".join(filters)
        
        if "WHERE" in query.upper():
            return query.replace("WHERE", f"WHERE {where_clause} AND ")
        else:
            return f"{query} WHERE {where_clause}"
```

## 数据填充策略

### 1. ETL处理时字段赋值

```python
class PermissionFieldMapper:
    def __init__(self):
        self.tenant_mapping = {
            # 根据来源IP/域名映射租户
            '192.168.1.0/24': 'company_a',
            '192.168.2.0/24': 'company_b',
            'api-prod.company-a.com': 'company_a'
        }
        
        self.service_mapping = {
            # 根据请求URI映射服务
            '/api/user/': 'user-service',
            '/api/order/': 'order-service',
            '/api/payment/': 'payment-service'
        }
    
    def enrich_log_record(self, log_record):
        """为日志记录补充权限字段"""
        
        # 1. 租户识别
        remote_addr = log_record['remote_addr'] 
        log_record['tenant_code'] = self.identify_tenant(remote_addr)
        
        # 2. 业务域识别  
        request_uri = log_record['request_uri']
        log_record['business_domain'] = self.identify_business_domain(request_uri)
        
        # 3. 环境识别
        host = log_record.get('host', '')
        if 'prod' in host:
            log_record['environment'] = 'prod'
        elif 'test' in host: 
            log_record['environment'] = 'test'
        else:
            log_record['environment'] = 'dev'
            
        # 4. 服务识别
        log_record['service_name'] = self.identify_service(request_uri)
        
        # 5. 数据敏感性分类
        if '/admin/' in request_uri or '/internal/' in request_uri:
            log_record['data_sensitivity'] = 'confidential'
        elif '/api/' in request_uri:
            log_record['data_sensitivity'] = 'internal'  
        else:
            log_record['data_sensitivity'] = 'public'
            
        return log_record
```

## 实施建议

### 阶段1：表结构扩展（1周）
1. 创建新表结构
2. 修改ETL逻辑补充权限字段
3. 数据迁移和验证

### 阶段2：Grafana集成（1-2周）  
1. 配置多组织架构
2. 创建权限感知的Dashboard模板
3. 配置数据源和变量

### 阶段3：权限细化（1-2周）
1. 实现动态权限注入中间件
2. 配置ClickHouse行级安全
3. 权限测试和调优

### 性能优化建议
1. **合理分区**：按租户+时间分区，避免跨分区查询
2. **索引优化**：为常用过滤字段建立跳跃索引
3. **缓存策略**：Dashboard查询结果适度缓存
4. **资源配额**：限制单个租户的查询资源使用

---

这个方案在保证数据安全隔离的同时，充分利用了Grafana和ClickHouse的原生能力，实现简单且性能优秀。