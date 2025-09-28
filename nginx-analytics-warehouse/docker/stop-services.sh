#!/bin/bash

# Nginx Analytics Platform - 分组停止脚本
# 按组停止服务，支持灵活的服务管理

set -e

echo "🛑 Nginx Analytics Platform - 分组停止脚本"
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

# 停止服务组
stop_group() {
    local group_name=$1
    shift
    local services=("$@")

    log_info "停止服务组: $group_name"
    echo "服务列表: ${services[*]}"

    for service in "${services[@]}"; do
        log_info "停止服务: $service"
        if docker-compose stop $service; then
            log_success "$service 停止完成"
        else
            log_warning "$service 停止失败或已停止"
        fi
    done
}

# 强制停止并删除容器
force_stop() {
    log_warning "强制停止所有服务并删除容器..."
    docker-compose down --remove-orphans
    log_success "所有服务已强制停止"
}

# 检查服务状态
check_services() {
    log_info "检查服务状态..."
    docker-compose ps
}

# 主停止流程
main() {
    case "${1:-graceful}" in
        "databases"|"db")
            log_info "停止数据存储层..."
            stop_group "数据存储层" clickhouse redis postgres n9e-mysql dataease-mysql
            ;;

        "timeseries"|"ts")
            log_info "停止时序数据库..."
            stop_group "时序数据库" victoriametrics
            ;;

        "compute"|"engine")
            log_info "停止计算引擎..."
            stop_group "计算引擎" spark-master spark-worker flink-jobmanager flink-taskmanager
            ;;

        "bi"|"visualization")
            log_info "停止BI可视化工具..."
            stop_group "BI工具" grafana superset dataease
            ;;

        "monitoring"|"monitor")
            log_info "停止监控系统..."
            stop_group "监控系统" nightingale categraf node-exporter
            ;;

        "etl"|"workflow")
            log_info "停止ETL和工作流..."
            stop_group "ETL工作流" seatunnel-master dolphinscheduler-standalone
            ;;

        "graceful"|"all")
            log_info "优雅停止所有服务 (推荐顺序)..."

            # L4: 先停止监控代理
            stop_group "L4-监控代理" categraf node-exporter
            sleep 2

            # L3: 停止ETL和工作流
            stop_group "L3-ETL工作流" seatunnel-master dolphinscheduler-standalone
            sleep 3

            # L2: 停止BI和监控应用
            stop_group "L2-BI工具" grafana superset dataease
            stop_group "L2-监控系统" nightingale
            sleep 5

            # L1: 停止计算引擎和时序数据库
            stop_group "L1-计算引擎" spark-worker flink-taskmanager spark-master flink-jobmanager
            stop_group "L1-时序数据库" victoriametrics
            sleep 5

            # L0: 最后停止基础数据存储
            stop_group "L0-数据存储层" dataease-mysql n9e-mysql postgres redis clickhouse
            ;;

        "force"|"down")
            force_stop
            ;;

        "help"|"-h"|"--help")
            echo "用法: $0 [组名]"
            echo ""
            echo "可用的停止选项:"
            echo "  databases, db       - 数据存储层 (ClickHouse, Redis, PostgreSQL, MySQL)"
            echo "  timeseries, ts      - 时序数据库 (VictoriaMetrics)"
            echo "  compute, engine     - 计算引擎 (Spark, Flink)"
            echo "  bi, visualization   - BI可视化工具 (Grafana, Superset, DataEase)"
            echo "  monitoring, monitor - 监控系统 (Nightingale, Categraf, Node-Exporter)"
            echo "  etl, workflow       - ETL和工作流 (SeaTunnel, DolphinScheduler)"
            echo "  graceful, all       - 所有服务 (优雅停止，按反向依赖顺序)"
            echo "  force, down         - 强制停止并删除所有容器"
            echo ""
            echo "示例:"
            echo "  $0 graceful         # 优雅停止所有服务"
            echo "  $0 force            # 强制停止所有服务"
            echo "  $0 bi               # 只停止BI工具"
            exit 0
            ;;

        *)
            log_error "未知的服务组: $1"
            log_info "使用 '$0 help' 查看可用选项"
            exit 1
            ;;
    esac

    # 最终状态检查
    sleep 2
    check_services

    log_success "停止流程完成！"
}

# 执行主函数
main "$@"