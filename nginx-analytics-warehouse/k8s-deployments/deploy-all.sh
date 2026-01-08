#!/bin/bash
# ==========================================
# Nginx Analytics - 一键部署脚本
# 方案B: Grafana快速验证方案
# ==========================================

set -e  # 遇到错误立即退出

NAMESPACE="nginx-analytics"

echo "=========================================="
echo "Nginx日志分析数据仓库 - K8s部署"
echo "=========================================="
echo ""

# 检查kubectl可用性
if ! command -v kubectl &> /dev/null; then
    echo "错误: kubectl未安装或不在PATH中"
    exit 1
fi

# 检查集群连接
if ! kubectl cluster-info &> /dev/null; then
    echo "错误: 无法连接到K8s集群"
    exit 1
fi

echo "✓ K8s集群连接正常"
echo ""

# Step 1: 创建命名空间
echo "[1/8] 创建命名空间..."
kubectl apply -f 00-namespace.yaml
sleep 2

# Step 2: 创建ConfigMap
echo "[2/8] 创建配置文件..."
kubectl apply -f 01-configmap.yaml
sleep 2

# Step 3: 创建Secrets
echo "[3/8] 创建密钥配置..."
kubectl apply -f 02-secrets.yaml
sleep 2

# Step 4: 创建持久化卷
echo "[4/8] 创建持久化存储卷..."
kubectl apply -f 03-persistent-volumes.yaml
sleep 5

# 等待PVC绑定
echo "    等待PVC绑定..."
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/clickhouse-data-pvc -n $NAMESPACE --timeout=60s
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/redis-data-pvc -n $NAMESPACE --timeout=60s
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/dataease-mysql-data-pvc -n $NAMESPACE --timeout=60s
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/dataease-data-pvc -n $NAMESPACE --timeout=60s
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/grafana-data-pvc -n $NAMESPACE --timeout=60s
echo "    ✓ 所有PVC已绑定"

# Step 5: 部署ClickHouse
echo "[5/8] 部署ClickHouse数据库..."
kubectl apply -f 04-clickhouse.yaml
echo "    等待ClickHouse就绪..."
kubectl wait --for=condition=ready pod -l app=clickhouse -n $NAMESPACE --timeout=180s
echo "    ✓ ClickHouse已就绪"

# Step 6: 部署Redis
echo "[6/8] 部署Redis缓存..."
kubectl apply -f 05-redis.yaml
echo "    等待Redis就绪..."
kubectl wait --for=condition=ready pod -l app=redis -n $NAMESPACE --timeout=120s
echo "    ✓ Redis已就绪"

# Step 7: 部署DataEase MySQL
echo "[7/8] 部署DataEase MySQL..."
kubectl apply -f 06-dataease-mysql.yaml
echo "    等待MySQL就绪..."
kubectl wait --for=condition=ready pod -l app=dataease-mysql -n $NAMESPACE --timeout=180s
echo "    ✓ MySQL已就绪"

# 部署DataEase
echo "    部署DataEase应用..."
kubectl apply -f 07-dataease.yaml
echo "    等待DataEase就绪(可能需要2-3分钟)..."
sleep 30  # DataEase启动较慢,先等待30秒
kubectl wait --for=condition=ready pod -l app=dataease -n $NAMESPACE --timeout=300s
echo "    ✓ DataEase已就绪"

# Step 8: 部署Grafana
echo "[8/8] 部署Grafana..."
kubectl apply -f 08-grafana.yaml
echo "    等待Grafana就绪..."
kubectl wait --for=condition=ready pod -l app=grafana -n $NAMESPACE --timeout=180s
echo "    ✓ Grafana已就绪"

echo ""
echo "=========================================="
echo "✓ 所有服务部署完成!"
echo "=========================================="
echo ""

# 显示服务状态
echo "服务状态："
kubectl get pods -n $NAMESPACE -o wide

echo ""
echo "服务访问地址："
kubectl get svc -n $NAMESPACE

echo ""
echo "=========================================="
echo "访问信息："
echo "=========================================="
echo "Grafana:     http://192.168.0.140:3000"
echo "  账号:      admin / admin123"
echo ""
echo "DataEase:    http://192.168.0.140:8810"
echo "  账号:      admin / DataEase123@"
echo ""
echo "ClickHouse:  http://192.168.0.140:8123"
echo "  账号:      analytics_user / analytics_password_change_in_prod"
echo ""
echo "=========================================="
echo ""
echo "下一步："
echo "1. 初始化ClickHouse数据库(执行DDL)"
echo "2. 配置ETL连接到K8s ClickHouse"
echo "3. 运行ETL测试处理数据"
echo "4. 在Grafana中配置Dashboard"
echo ""
echo "详细步骤请查看: README.md"
echo "=========================================="
