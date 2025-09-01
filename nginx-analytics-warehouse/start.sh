#!/bin/bash

echo "🚀 启动Nginx日志分析平台..."

# 检查Docker和Docker Compose
if ! command -v docker &> /dev/null; then
    echo "❌ Docker 未安装，请先安装Docker"
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose 未安装，请先安装Docker Compose"
    exit 1
fi

# 创建必要目录
echo "📁 创建数据目录..."
mkdir -p volumes/{clickhouse/{data,logs},grafana,postgres,redis,superset}
mkdir -p data logs

# 设置目录权限
chmod -R 755 volumes/
chmod -R 777 volumes/grafana  # Grafana需要写权限

echo "🐳 启动服务..."

# 启动核心服务 (ClickHouse + Grafana + Superset)
if command -v docker-compose &> /dev/null; then
    docker-compose up -d clickhouse grafana superset-redis superset-postgres superset
else
    docker compose up -d clickhouse grafana superset-redis superset-postgres superset
fi

echo "⏳ 等待服务启动..."
sleep 30

# 检查服务状态
echo "🔍 检查服务状态:"

# ClickHouse
if curl -s http://localhost:8123/ping > /dev/null; then
    echo "  ✅ ClickHouse: http://localhost:8123"
else
    echo "  ❌ ClickHouse: 启动失败"
fi

# Grafana
if curl -s http://localhost:3000 > /dev/null; then
    echo "  ✅ Grafana: http://localhost:3000 (admin/admin123)"
else
    echo "  ⏳ Grafana: 仍在启动中..."
fi

# Superset
if curl -s http://localhost:8088 > /dev/null; then
    echo "  ✅ Superset: http://localhost:8088 (admin/admin123)"
else
    echo "  ⏳ Superset: 仍在启动中..."
fi

echo ""
echo "🎉 平台启动完成!"
echo ""
echo "📊 访问地址:"
echo "  • ClickHouse: http://localhost:8123"
echo "  • Grafana: http://localhost:3000 (admin/admin123)" 
echo "  • Superset: http://localhost:8088 (admin/admin123)"
echo ""
echo "📝 下一步:"
echo "  1. 等待所有服务完全启动 (约2-3分钟)"
echo "  2. 访问Grafana和Superset配置ClickHouse数据源"
echo "  3. 运行数据处理脚本导入nginx日志"
echo ""
echo "🔧 管理命令:"
echo "  • 查看日志: docker-compose logs -f [service_name]"
echo "  • 停止服务: docker-compose down"
echo "  • 重启服务: docker-compose restart [service_name]"