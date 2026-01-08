#!/bin/bash
# ==========================================
# ClickHouse数据库初始化脚本
# 执行DDL创建表和物化视图
# ==========================================

set -e

NAMESPACE="nginx-analytics"
POD_NAME="clickhouse-0"
DDL_PATH="/tmp/ddl"

echo "=========================================="
echo "ClickHouse数据库初始化"
echo "=========================================="
echo ""

# 检查ClickHouse Pod是否运行
echo "[1/4] 检查ClickHouse状态..."
if ! kubectl get pod $POD_NAME -n $NAMESPACE &> /dev/null; then
    echo "错误: ClickHouse Pod未运行"
    exit 1
fi

POD_STATUS=$(kubectl get pod $POD_NAME -n $NAMESPACE -o jsonpath='{.status.phase}')
if [ "$POD_STATUS" != "Running" ]; then
    echo "错误: ClickHouse Pod状态异常: $POD_STATUS"
    exit 1
fi

echo "✓ ClickHouse Pod运行正常"
echo ""

# 复制DDL文件到Pod
echo "[2/4] 复制DDL文件到ClickHouse Pod..."

# 检查本地DDL文件路径
if [ -d "/mnt/d/project/nginx-log-analyzer/nginx-analytics-warehouse/ddl" ]; then
    DDL_SOURCE="/mnt/d/project/nginx-log-analyzer/nginx-analytics-warehouse/ddl"
elif [ -d "D:/project/nginx-log-analyzer/nginx-analytics-warehouse/ddl" ]; then
    DDL_SOURCE="D:/project/nginx-log-analyzer/nginx-analytics-warehouse/ddl"
else
    echo "错误: 找不到DDL文件目录"
    echo "请确认路径: D:/project/nginx-log-analyzer/nginx-analytics-warehouse/ddl"
    exit 1
fi

# 创建临时目录
kubectl exec -n $NAMESPACE $POD_NAME -- mkdir -p $DDL_PATH

# 复制DDL文件
kubectl cp "$DDL_SOURCE/01_ods_layer_real.sql" $NAMESPACE/$POD_NAME:$DDL_PATH/01_ods_layer_real.sql
kubectl cp "$DDL_SOURCE/02_dwd_layer_real.sql" $NAMESPACE/$POD_NAME:$DDL_PATH/02_dwd_layer_real.sql
kubectl cp "$DDL_SOURCE/03_ads_layer_real.sql" $NAMESPACE/$POD_NAME:$DDL_PATH/03_ads_layer_real.sql
kubectl cp "$DDL_SOURCE/04_materialized_views_corrected.sql" $NAMESPACE/$POD_NAME:$DDL_PATH/04_materialized_views_corrected.sql

echo "✓ DDL文件复制完成"
echo ""

# 执行DDL
echo "[3/4] 执行数据库初始化..."

# 获取ClickHouse凭据
CH_USER=$(kubectl get secret nginx-analytics-secrets -n $NAMESPACE -o jsonpath='{.data.clickhouse-user}' | base64 -d)
CH_PASS=$(kubectl get secret nginx-analytics-secrets -n $NAMESPACE -o jsonpath='{.data.clickhouse-password}' | base64 -d)

echo "  执行 01_ods_layer_real.sql (ODS层)..."
kubectl exec -n $NAMESPACE $POD_NAME -- clickhouse-client \
    --user=$CH_USER \
    --password=$CH_PASS \
    --queries-file=$DDL_PATH/01_ods_layer_real.sql

echo "  执行 02_dwd_layer_real.sql (DWD层)..."
kubectl exec -n $NAMESPACE $POD_NAME -- clickhouse-client \
    --user=$CH_USER \
    --password=$CH_PASS \
    --queries-file=$DDL_PATH/02_dwd_layer_real.sql

echo "  执行 03_ads_layer_real.sql (ADS层)..."
kubectl exec -n $NAMESPACE $POD_NAME -- clickhouse-client \
    --user=$CH_USER \
    --password=$CH_PASS \
    --queries-file=$DDL_PATH/03_ads_layer_real.sql

echo "  执行 04_materialized_views_corrected.sql (物化视图)..."
kubectl exec -n $NAMESPACE $POD_NAME -- clickhouse-client \
    --user=$CH_USER \
    --password=$CH_PASS \
    --queries-file=$DDL_PATH/04_materialized_views_corrected.sql

echo "✓ 数据库初始化完成"
echo ""

# 验证表创建
echo "[4/4] 验证数据库表..."
kubectl exec -n $NAMESPACE $POD_NAME -- clickhouse-client \
    --user=$CH_USER \
    --password=$CH_PASS \
    --query="SELECT database, name, engine, total_rows FROM system.tables WHERE database = 'nginx_analytics' ORDER BY name"

echo ""
echo "=========================================="
echo "✓ ClickHouse数据库初始化成功!"
echo "=========================================="
echo ""
echo "核心表已创建:"
echo "  - ods_nginx_raw (ODS原始层)"
echo "  - dwd_nginx_enriched_v3 (DWD明细层)"
echo "  - ads_* (18个ADS主题表)"
echo "  - mv_* (17个物化视图)"
echo ""
echo "下一步:"
echo "1. 配置ETL连接到ClickHouse"
echo "2. 运行ETL测试处理日志数据"
echo "=========================================="
