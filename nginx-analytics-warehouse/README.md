# Nginx Analytics Data Warehouse

现代化的Nginx日志分析数据仓库，基于ClickHouse + Grafana + Superset构建。

## ⚡ 快速开始

```bash
# 1. 初始化目录结构
python manage.py init

# 2. 启动所有服务  
python manage.py start

# 3. 初始化数据库
python manage.py init-db

# 4. 处理nginx日志
python manage.py process
```

## 🏗️ 架构设计

### 数据流向
```
Nginx日志 → ODS层(原始数据) → DWD层(清洗数据) → ADS层(聚合数据) → BI工具
```

### 技术栈
- **存储**: ClickHouse (列存储数据库)
- **可视化**: Grafana + Superset  
- **处理**: Python + Docker
- **数据持久化**: Docker Volumes + 本地目录映射

## 📁 目录结构

```
nginx-analytics-warehouse/
├── docker/                    # Docker配置
│   ├── docker-compose.yml    # 服务编排文件
│   └── .env                  # 环境变量配置
├── data/                     # 数据持久化目录
│   ├── clickhouse/          # ClickHouse数据
│   ├── grafana/             # Grafana配置
│   └── postgres/            # PostgreSQL数据
├── logs/                     # 日志文件目录
├── processors/               # 数据处理器代码
├── nginx_logs/              # Nginx日志文件目录
│   └── YYYYMMDD/           # 按日期组织
└── backup/                  # 备份目录
```

## 🚀 核心功能

### 数据处理
- ✅ **智能去重**: 基于文件hash避免重复处理
- ✅ **平台识别**: 自动识别iOS/Android/Web平台
- ✅ **API分类**: 智能分类API接口类型
- ✅ **性能分析**: 响应时间、成功率等指标

### 数据存储
- ✅ **4层架构**: ODS → DWD → DWS → ADS
- ✅ **数据持久化**: 本地目录映射存储
- ✅ **自动备份**: 支持完整备份和恢复
- ✅ **监控告警**: 健康检查和状态监控

### 可视化分析
- ✅ **Grafana仪表板**: 实时监控和趋势分析
- ✅ **Superset报表**: 复杂查询和数据探索
- ✅ **多维分析**: 时间、平台、API等维度

## 📊 支持的日志格式

### 底座格式 (当前支持)
```log
http_host:"domain.com" remote_addr:"192.168.1.1" time:"2025-04-23T00:00:02+08:00" request:"GET /api/user HTTP/1.1" status:"200" response_time:"0.123"
```

### JSON格式 (推荐)
```json
{
  "time": "2025-04-23T00:00:02+08:00",
  "remote_addr": "192.168.1.1", 
  "method": "GET",
  "uri": "/api/user",
  "status": "200",
  "response_time": "0.123"
}
```

## 🔧 管理命令

```bash
# 服务管理
python manage.py start         # 启动所有服务
python manage.py stop          # 停止所有服务  
python manage.py restart       # 重启所有服务
python manage.py status        # 查看服务状态

# 数据处理
python manage.py process                    # 处理所有未处理日志
python manage.py process --date 20250422    # 处理指定日期
python manage.py process --date 20250422 --force  # 强制重新处理

# 数据管理
python manage.py backup ./backup    # 备份数据
python manage.py restore ./backup   # 恢复数据
python manage.py clean             # 清理所有数据

# 系统管理
python manage.py init              # 初始化目录结构
python manage.py init-db          # 初始化数据库表结构
```

## 🌐 访问地址

| 服务 | 地址 | 用户名/密码 | 说明 |
|------|------|------------|------|
| ClickHouse | http://localhost:8123 | analytics_user/analytics_password | 数据库访问 |
| Grafana | http://localhost:3000 | admin/admin123 | 监控仪表板 |
| Superset | http://localhost:8088 | admin/admin123 | 数据分析平台 |

## 📈 数据指标

### 核心指标
- **请求量**: 总请求数、成功请求数、失败请求数
- **性能**: 平均响应时间、P95响应时间、慢请求数
- **稳定性**: 成功率、错误率、可用性
- **业务**: API调用分布、平台使用情况

### 维度分析
- **时间维度**: 按小时、天、周、月统计
- **平台维度**: iOS、Android、Web分别统计
- **API维度**: 接口级别的性能和调用分析
- **地理维度**: IP来源和地域分布

## 🛠️ 开发调试

### PyCharm配置
参考 `docs/PYCHARM_SETUP.md` 进行IDE配置

### 调试建议
```python
# 设置断点在关键位置
processors/nginx_processor_complete.py:80   # 主处理流程
processors/nginx_processor_complete.py:200  # 日志解析
processors/nginx_processor_complete.py:300  # 数据入库
```

### 数据验证
```bash
# 验证数据处理质量
cd processors && python validate_processing.py

# 查看数据流状态
cd processors && python show_data_flow.py
```

## 📋 最佳实践

### 生产环境部署
1. **安全配置**: 修改默认密码和密钥
2. **资源配置**: 根据数据量调整内存和CPU
3. **监控告警**: 配置Grafana告警规则
4. **数据备份**: 建立定期备份策略
5. **日志轮转**: 配置nginx日志轮转

### 性能优化
1. **ClickHouse优化**: 合理设置分区和索引
2. **数据清理**: 定期清理过期数据
3. **查询优化**: 使用合适的查询条件
4. **硬件配置**: SSD存储用于数据目录

### 运维建议
1. **监控告警**: 监控服务状态和资源使用
2. **日志审计**: 定期检查处理日志
3. **数据校验**: 验证数据完整性和一致性
4. **容量规划**: 根据增长预测扩容

## 🆘 故障排查

### 常见问题
1. **Docker服务未启动** → 启动Docker Desktop
2. **ClickHouse连接失败** → 检查容器状态和网络
3. **数据不一致** → 使用清理和重新处理
4. **日志解析错误** → 检查日志格式和编码

### 获取帮助
```bash
# 查看详细状态
python manage.py status

# 查看处理日志
tail -f logs/nginx-processor/processing.log

# 手动调试
cd processors && python -i main_simple.py
```

## 📚 详细文档

- [部署指南](docs/DEPLOYMENT_GUIDE.md) - 详细的部署说明
- [PyCharm配置](docs/PYCHARM_SETUP.md) - IDE配置指南
- [目录结构](DIRECTORY_STRUCTURE.md) - 完整的目录说明
- [故障排查](docs/TROUBLESHOOTING.md) - 问题解决方案

## 🤝 贡献

欢迎提交Issues和Pull Requests来改进这个项目！

## 📄 许可证

本项目采用MIT许可证 - 查看[LICENSE](LICENSE)文件了解详情

---

**快速体验**: `python manage.py init && python manage.py start && python manage.py process`