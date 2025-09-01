#!/bin/bash
set -e

echo "=== Nginx Analytics Log Processor Starting ==="
echo "ClickHouse Host: ${CLICKHOUSE_HOST:-localhost}"
echo "ClickHouse Port: ${CLICKHOUSE_PORT:-8123}"
echo "ClickHouse User: ${CLICKHOUSE_USER:-analytics_user}"
echo "ClickHouse Database: ${CLICKHOUSE_DATABASE:-nginx_analytics}"

# 等待ClickHouse启动
echo "Waiting for ClickHouse to be ready..."
until curl -f "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/ping" >/dev/null 2>&1; do
    echo "ClickHouse is not ready yet. Waiting..."
    sleep 5
done
echo "ClickHouse is ready!"

# 初始化数据库表结构
echo "Initializing database schema..."
python -c "
import sys
sys.path.append('/app/etl')
from database_initializer import DatabaseInitializer
init = DatabaseInitializer()
init.initialize_all_tables()
print('Database schema initialized successfully!')
"

# 启动日志处理服务
echo "Starting log processing service..."
if [ "${PROCESSING_MODE:-daemon}" = "daemon" ]; then
    # 守护进程模式 - 持续监控日志目录
    python /app/etl/log_processor_daemon.py
elif [ "${PROCESSING_MODE}" = "once" ]; then
    # 单次处理模式 - 处理一次后退出
    python /app/etl/batch_processor.py --data-dir /app/data
else
    # 交互式模式 - 等待手动触发
    echo "Interactive mode. Use docker exec to run processing commands."
    tail -f /dev/null
fi