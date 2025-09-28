#!/bin/bash

# Nginx Analytics Platform - 分组启动脚本
# 按组启动服务，支持灵活的服务管理

set -e

echo "🚀 Nginx Analytics Platform - 分组启动脚本"
echo "=" * 60

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 等待服务健康
wait_for_health() {
    local service=$1
    local timeout=${2:-120}
    local interval=5
    local elapsed=0

    log_info "等待 $service 服务健康检查..."

    while [ $elapsed -lt $timeout ]; do
        if docker-compose ps $service | grep -q "healthy"; then
            log_success "$service 服务已健康"
            return 0
        fi

        if docker-compose ps $service | grep -q "unhealthy"; then
            log_error "$service 服务不健康，请检查日志"
            return 1
        fi

        sleep $interval
        elapsed=$((elapsed + interval))
        echo -n "."
    done

    log_error "$service 服务健康检查超时"
    return 1
}

# 启动服务组
start_group() {
    local group_name=$1
    shift
    local services=("$@")

    log_info "启动服务组: $group_name"
    echo "服务列表: ${services[*]}"

    for service in "${services[@]}"; do
        log_info "启动服务: $service"
        if docker-compose up -d $service; then
            log_success "$service 启动完成"
        else
            log_error "$service 启动失败"
            return 1
        fi
    done
}

# 检查服务状态
check_services() {
    log_info "检查所有服务状态..."
    docker-compose ps
}

# 主启动流程
main() {
    case "${1:-all}" in
        "databases"|"db")
            log_info "启动数据存储层..."
            start_group "数据存储层" clickhouse redis postgres n9e-mysql dataease-mysql
            ;;

        "timeseries"|"ts")
            log_info "启动时序数据库..."
            start_group "时序数据库" victoriametrics
            ;;

        "compute"|"engine")
            log_info "启动计算引擎..."
            start_group "计算引擎" spark-master spark-worker flink-jobmanager flink-taskmanager
            ;;

        "bi"|"visualization")
            log_info "启动BI可视化工具..."
            start_group "BI工具" grafana superset dataease
            ;;

        "monitoring"|"monitor")
            log_info "启动监控系统..."
            start_group "监控系统" nightingale categraf node-exporter
            ;;

        "etl"|"workflow")
            log_info "启动ETL和工作流..."
            start_group "ETL工作流" seatunnel-master dolphinscheduler-standalone
            ;;

        "all")
            log_info "启动所有服务 (推荐顺序)..."

            # L0: 基础数据存储
            start_group "L0-数据存储层" clickhouse redis postgres n9e-mysql dataease-mysql
            sleep 10

            # L1: 时序数据库和计算引擎
            start_group "L1-时序数据库" victoriametrics
            start_group "L1-计算引擎" spark-master spark-worker flink-jobmanager flink-taskmanager
            sleep 15

            # L2: BI和监控应用
            start_group "L2-BI工具" grafana superset dataease
            start_group "L2-监控系统" nightingale
            sleep 10

            # L3: ETL和工作流
            start_group "L3-ETL工作流" seatunnel-master dolphinscheduler-standalone
            sleep 5

            # L4: 监控代理
            start_group "L4-监控代理" categraf node-exporter
            ;;

        "help"|"-h"|"--help")
            echo "用法: $0 [组名]"
            echo ""
            echo "可用的服务组:"
            echo "  databases, db       - 数据存储层 (ClickHouse, Redis, PostgreSQL, MySQL)"
            echo "  timeseries, ts      - 时序数据库 (VictoriaMetrics)"
            echo "  compute, engine     - 计算引擎 (Spark, Flink)"
            echo "  bi, visualization   - BI可视化工具 (Grafana, Superset, DataEase)"
            echo "  monitoring, monitor - 监控系统 (Nightingale, Categraf, Node-Exporter)"
            echo "  etl, workflow       - ETL和工作流 (SeaTunnel, DolphinScheduler)"
            echo "  all                 - 所有服务 (推荐，按依赖顺序启动)"
            echo ""
            echo "示例:"
            echo "  $0 all              # 启动所有服务"
            echo "  $0 databases        # 只启动数据库"
            echo "  $0 bi               # 只启动BI工具"
            exit 0
            ;;

        *)
            log_error "未知的服务组: $1"
            log_info "使用 '$0 help' 查看可用选项"
            exit 1
            ;;
    esac

    # 最终状态检查
    sleep 5
    check_services

    log_success "启动流程完成！"
    log_info "访问地址:"
    echo "  Grafana:        http://localhost:3000  (admin/admin123)"
    echo "  Superset:       http://localhost:8088"
    echo "  DataEase:       http://localhost:8810"
    echo "  Nightingale:    http://localhost:17000"
    echo "  Spark Master:   http://localhost:8080"
    echo "  Flink:          http://localhost:8082"
    echo "  DolphinScheduler: http://localhost:12345"
}

# 执行主函数
main "$@"