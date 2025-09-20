# 🚀 快速部署指南

## 新环境部署步骤

```bash
# 1. 克隆仓库
git clone <repository-url>
cd nginx-analytics-warehouse/docker

# 2. 检查配置完整性
./check-config-files.sh

# 3. 自动初始化环境
./init-fresh-environment.sh

# 4. 验证部署结果
./validate-deployment.sh
```

## 🌐 服务访问地址

| 服务 | 地址 | 用户名/密码 |
|------|------|-------------|
| **Grafana** | http://localhost:3000 | admin/admin123 |
| **Superset** | http://localhost:8088 | - |
| **DataEase** | http://localhost:8810 | - |
| **Nightingale** | http://localhost:17000 | root/root.2020 |
| **DolphinScheduler** | http://localhost:12345 | - |

## 🔧 故障排除

```bash
# 查看服务状态
docker-compose ps

# 查看服务日志
docker-compose logs nginx-analytics-nightingale

# 重新初始化数据库
./force-init-databases.sh

# 重启服务
docker-compose restart
```