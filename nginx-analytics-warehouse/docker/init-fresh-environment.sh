#!/bin/bash

# 新环境自动初始化脚本
# 适用于git克隆后的首次环境搭建

set -e

echo "🚀 Nginx Analytics Platform - 新环境初始化脚本"
echo "=" * 80

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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

# 检查Docker环境
check_docker() {
    log_info "检查Docker环境..."

    if ! command -v docker &> /dev/null; then
        log_error "Docker未安装，请先安装Docker"
        exit 1
    fi

    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose未安装，请先安装Docker Compose"
        exit 1
    fi

    if ! docker info &> /dev/null; then
        log_error "Docker服务未运行，请启动Docker"
        exit 1
    fi

    log_success "Docker环境检查通过"
}

# 检查端口占用
check_ports() {
    log_info "检查端口占用..."

    local ports=(3000 3307 3308 5433 6380 7077 8080 8081 8082 8088 8100 8123 8428 8810 9000 9100 12345 17000 20090)
    local occupied_ports=()

    for port in "${ports[@]}"; do
        if netstat -tuln 2>/dev/null | grep -q ":$port "; then
            occupied_ports+=($port)
        fi
    done

    if [ ${#occupied_ports[@]} -gt 0 ]; then
        log_warning "以下端口被占用: ${occupied_ports[*]}"
        log_warning "这可能导致服务启动失败，建议释放这些端口"
        read -p "是否继续? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_info "用户取消操作"
            exit 0
        fi
    else
        log_success "端口检查通过"
    fi
}

# 清理旧环境
cleanup_old() {
    log_info "清理旧环境..."

    # 停止现有服务
    if docker-compose ps -q 2>/dev/null | grep -q .; then
        log_info "停止现有服务..."
        docker-compose down -v --remove-orphans
    fi

    # 清理数据目录（谨慎操作）
    if [ -d "./data" ]; then
        log_warning "发现现有数据目录"
        read -p "是否删除现有数据? 这将清除所有历史数据 (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            rm -rf ./data/*
            log_success "数据目录已清理"
        else
            log_info "保留现有数据目录"
        fi
    fi

    log_success "旧环境清理完成"
}

# 创建必要目录
create_directories() {
    log_info "创建必要目录..."

    local dirs=(
        "data/grafana"
        "data/n9e"
        "data/n9e-mysql"
        "data/victoriametrics"
    )

    for dir in "${dirs[@]}"; do
        mkdir -p "$dir"
    done

    log_success "目录创建完成"
}

# 验证配置文件
verify_configs() {
    log_info "验证配置文件..."

    local required_files=(
        "services/n9e/init-scripts/00-init-database.sql"
        "services/grafana/datasources/clickhouse.yml"
        "services/n9e/config/nightingale/config.toml"
        ".env"
        "docker-compose.yml"
    )

    local missing_files=()

    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            missing_files+=("$file")
        fi
    done

    if [ ${#missing_files[@]} -gt 0 ]; then
        log_error "缺少关键配置文件:"
        for file in "${missing_files[@]}"; do
            echo "  - $file"
        done
        log_error "请确保所有配置文件都已提交到git仓库"
        exit 1
    fi

    log_success "配置文件验证通过"
}

# 启动服务
start_services() {
    log_info "启动服务..."

    # 拉取最新镜像
    log_info "拉取Docker镜像..."
    docker-compose pull

    # 启动所有服务
    log_info "启动所有服务..."
    docker-compose up -d

    log_success "服务启动命令已执行"
}

# 等待服务就绪
wait_for_services() {
    log_info "等待服务就绪..."

    local services=("clickhouse" "redis" "postgres" "n9e-mysql")
    local timeout=120
    local elapsed=0

    while [ $elapsed -lt $timeout ]; do
        local healthy_count=0

        for service in "${services[@]}"; do
            if docker-compose ps "$service" | grep -q "healthy\|Up"; then
                ((healthy_count++))
            fi
        done

        if [ $healthy_count -eq ${#services[@]} ]; then
            log_success "核心服务已就绪"
            break
        fi

        echo -n "."
        sleep 5
        elapsed=$((elapsed + 5))
    done

    if [ $elapsed -ge $timeout ]; then
        log_warning "等待服务超时，请手动检查服务状态"
    fi
}

# 验证服务
verify_services() {
    log_info "验证服务状态..."

    echo ""
    docker-compose ps
    echo ""

    # 显示访问地址
    log_success "服务验证完成！"
    echo ""
    echo "🌐 服务访问地址:"
    echo "  Grafana:        http://localhost:3000  (admin/admin123)"
    echo "  Superset:       http://localhost:8088"
    echo "  DataEase:       http://localhost:8810"
    echo "  Nightingale:    http://localhost:17000  (root/root.2020)"
    echo "  Spark Master:   http://localhost:8080"
    echo "  Flink:          http://localhost:8082"
    echo "  DolphinScheduler: http://localhost:12345"
    echo ""
}

# 主函数
main() {
    log_info "开始新环境初始化..."

    check_docker
    check_ports
    cleanup_old
    create_directories
    verify_configs
    start_services
    wait_for_services
    verify_services

    log_success "🎉 新环境初始化完成！"
    log_info "💡 如有问题，请查看 INITIALIZE_GUIDE.md 获取详细说明"
}

# 执行主函数
main "$@"