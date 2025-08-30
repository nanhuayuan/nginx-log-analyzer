#!/bin/bash

# ClickHouse Docker Compose 管理脚本
# 使用方法: ./clickhouse-manager.sh [command]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

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

# 检查Docker和Docker Compose
check_dependencies() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker未安装，请先安装Docker"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        log_error "Docker Compose未安装，请先安装Docker Compose"
        exit 1
    fi
}

# 启动ClickHouse
start_clickhouse() {
    log_info "启动ClickHouse服务..."
    cd "$SCRIPT_DIR"
    
    if docker compose version &> /dev/null; then
        docker compose up -d clickhouse
    else
        docker-compose up -d clickhouse
    fi
    
    log_success "ClickHouse服务启动中..."
    log_info "等待服务就绪..."
    
    # 等待健康检查通过
    local retry_count=0
    local max_retries=30
    
    while [ $retry_count -lt $max_retries ]; do
        if docker exec nginx-analytics-clickhouse wget --quiet --tries=1 --spider http://localhost:8123/ping 2>/dev/null; then
            log_success "ClickHouse服务已就绪!"
            log_info "HTTP接口: http://localhost:8123"
            log_info "Native接口: localhost:9000"
            log_info "默认用户: analytics_user / analytics_password"
            return 0
        fi
        
        retry_count=$((retry_count + 1))
        echo -n "."
        sleep 2
    done
    
    log_error "ClickHouse启动超时，请检查日志"
    return 1
}

# 启动完整环境（包括监控）
start_full() {
    log_info "启动完整环境 (ClickHouse + Grafana)..."
    cd "$SCRIPT_DIR"
    
    if docker compose version &> /dev/null; then
        docker compose --profile monitoring up -d
    else
        docker-compose --profile monitoring up -d
    fi
    
    log_success "完整环境启动中..."
    log_info "ClickHouse: http://localhost:8123"
    log_info "Grafana: http://localhost:3000 (admin/admin)"
}

# 停止服务
stop_services() {
    log_info "停止所有服务..."
    cd "$SCRIPT_DIR"
    
    if docker compose version &> /dev/null; then
        docker compose down
    else
        docker-compose down
    fi
    
    log_success "所有服务已停止"
}

# 重启服务
restart_services() {
    log_info "重启服务..."
    stop_services
    sleep 2
    start_clickhouse
}

# 查看服务状态
status_services() {
    log_info "服务状态:"
    cd "$SCRIPT_DIR"
    
    if docker compose version &> /dev/null; then
        docker compose ps
    else
        docker-compose ps
    fi
}

# 查看日志
show_logs() {
    local service=${1:-clickhouse}
    log_info "显示 $service 服务日志..."
    cd "$SCRIPT_DIR"
    
    if docker compose version &> /dev/null; then
        docker compose logs -f --tail=100 "$service"
    else
        docker-compose logs -f --tail=100 "$service"
    fi
}

# 进入ClickHouse客户端
connect_client() {
    log_info "连接到ClickHouse客户端..."
    
    if ! docker ps | grep -q nginx-analytics-clickhouse; then
        log_error "ClickHouse服务未运行，请先启动服务"
        exit 1
    fi
    
    docker exec -it nginx-analytics-clickhouse clickhouse-client \
        --user analytics_user \
        --password analytics_password \
        --database nginx_analytics
}

# 执行SQL文件
execute_sql() {
    local sql_file="$1"
    
    if [ ! -f "$sql_file" ]; then
        log_error "SQL文件不存在: $sql_file"
        exit 1
    fi
    
    log_info "执行SQL文件: $sql_file"
    
    docker exec -i nginx-analytics-clickhouse clickhouse-client \
        --user analytics_user \
        --password analytics_password \
        --database nginx_analytics \
        --multiquery < "$sql_file"
    
    log_success "SQL执行完成"
}

# 备份数据
backup_data() {
    local backup_dir="${1:-./backups}"
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local backup_file="$backup_dir/clickhouse_backup_$timestamp.sql"
    
    mkdir -p "$backup_dir"
    
    log_info "备份数据到: $backup_file"
    
    docker exec nginx-analytics-clickhouse clickhouse-client \
        --user analytics_user \
        --password analytics_password \
        --database nginx_analytics \
        --query "SHOW TABLES" \
        --format TabSeparated | while read table; do
            echo "-- Table: $table" >> "$backup_file"
            docker exec nginx-analytics-clickhouse clickhouse-client \
                --user analytics_user \
                --password analytics_password \
                --database nginx_analytics \
                --query "SHOW CREATE TABLE $table" >> "$backup_file"
            echo "" >> "$backup_file"
            
            docker exec nginx-analytics-clickhouse clickhouse-client \
                --user analytics_user \
                --password analytics_password \
                --database nginx_analytics \
                --query "SELECT * FROM $table FORMAT JSONEachRow" >> "$backup_file"
            echo "" >> "$backup_file"
        done
    
    log_success "备份完成: $backup_file"
}

# 清理数据
cleanup_data() {
    read -p "确认要清理所有数据吗? [y/N]: " confirm
    if [[ $confirm =~ ^[Yy]$ ]]; then
        log_info "清理所有数据..."
        cd "$SCRIPT_DIR"
        
        if docker compose version &> /dev/null; then
            docker compose down -v
        else
            docker-compose down -v
        fi
        
        log_success "数据已清理"
    else
        log_info "取消清理操作"
    fi
}

# 显示帮助
show_help() {
    echo "ClickHouse Docker Compose 管理工具"
    echo ""
    echo "使用方法: $0 [command]"
    echo ""
    echo "命令:"
    echo "  start          启动ClickHouse服务"
    echo "  start-full     启动完整环境 (ClickHouse + Grafana)"
    echo "  stop           停止所有服务"
    echo "  restart        重启服务"
    echo "  status         查看服务状态"
    echo "  logs [service] 查看服务日志 (默认: clickhouse)"
    echo "  client         连接ClickHouse客户端"
    echo "  sql <file>     执行SQL文件"
    echo "  backup [dir]   备份数据 (默认: ./backups)"
    echo "  cleanup        清理所有数据和容器"
    echo "  help           显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 start                    # 启动ClickHouse"
    echo "  $0 logs clickhouse          # 查看ClickHouse日志"
    echo "  $0 sql ./init.sql          # 执行SQL文件"
    echo "  $0 backup ./my_backups     # 备份到指定目录"
}

# 主函数
main() {
    check_dependencies
    
    case "${1:-start}" in
        start)
            start_clickhouse
            ;;
        start-full)
            start_full
            ;;
        stop)
            stop_services
            ;;
        restart)
            restart_services
            ;;
        status)
            status_services
            ;;
        logs)
            show_logs "$2"
            ;;
        client)
            connect_client
            ;;
        sql)
            execute_sql "$2"
            ;;
        backup)
            backup_data "$2"
            ;;
        cleanup)
            cleanup_data
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "未知命令: $1"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

# 执行主函数
main "$@"