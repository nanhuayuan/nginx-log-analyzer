#!/bin/sh
# 初始化ClickHouse数据库和表结构

echo "开始初始化ClickHouse数据库和表结构..."

# 等待ClickHouse服务就绪
echo "等待ClickHouse服务启动..."
while ! clickhouse-client --host nginx-analytics-clickhouse-simple --port 9000 --query "SELECT 1" 2>/dev/null; do
    echo "等待ClickHouse..."
    sleep 2
done

echo "ClickHouse服务已就绪"

# 创建数据库
echo "创建数据库nginx_analytics..."
clickhouse-client --host nginx-analytics-clickhouse-simple --port 9000 --query "CREATE DATABASE IF NOT EXISTS nginx_analytics"

# 设置时区
echo "设置时区..."
clickhouse-client --host nginx-analytics-clickhouse-simple --port 9000 -d nginx_analytics --query "SET session_timezone = 'Asia/Shanghai'"

# 执行DDL文件
echo "创建ODS层表..."
clickhouse-client --host nginx-analytics-clickhouse-simple --port 9000 -d nginx_analytics --multiquery < /ddl/01_ods_layer_real.sql

echo "创建DWD层表..."
clickhouse-client --host nginx-analytics-clickhouse-simple --port 9000 -d nginx_analytics --multiquery < /ddl/02_dwd_layer_real.sql

echo "创建ADS层表..."
clickhouse-client --host nginx-analytics-clickhouse-simple --port 9000 -d nginx_analytics --multiquery < /ddl/03_ads_layer_real.sql

echo "创建物化视图..."
clickhouse-client --host nginx-analytics-clickhouse-simple --port 9000 -d nginx_analytics --multiquery < /ddl/04_materialized_views.sql

# 验证表创建
echo "验证表创建..."
table_count=$(clickhouse-client --host nginx-analytics-clickhouse-simple --port 9000 -d nginx_analytics --query "SELECT count() FROM system.tables WHERE database = 'nginx_analytics'" 2>/dev/null)
echo "成功创建 $table_count 个表"

echo "数据库初始化完成!"