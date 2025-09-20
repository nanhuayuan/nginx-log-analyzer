#!/bin/bash

# 环境完全重置脚本
# 用于清理所有数据并重新部署到新环境

set -e

echo "🔄 环境完全重置脚本"
echo "=" * 60

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

# 停止所有服务
stop_all_services() {
    log_info "停止所有服务..."

    if docker-compose ps -q 2>/dev/null | grep -q .; then
        log_info "发现运行中的服务，正在停止..."
        docker-compose down
        log_success "服务已停止"
    else
        log_info "没有运行中的服务"
    fi
}

# 清理所有数据卷和网络
cleanup_volumes_networks() {
    log_warning "⚠️  即将删除所有数据卷，这将清除所有数据！"
    read -p "确认删除所有数据卷? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "清理数据卷和网络..."

        # 停止并删除容器、网络和卷
        docker-compose down -v --remove-orphans

        # 清理可能残留的卷
        docker volume ls -q | grep "nginx-analytics" | xargs -r docker volume rm

        # 清理本地数据目录
        if [ -d "./data" ]; then
            log_info "清理本地数据目录..."
            rm -rf ./data/*
            log_success "本地数据目录已清理"
        fi

        log_success "数据卷和网络清理完成"
    else
        log_info "用户取消清理操作"
        exit 0
    fi
}

# 清理Docker系统（可选）
cleanup_docker_system() {
    log_info "是否清理Docker系统缓存?"
    read -p "清理Docker系统缓存 (镜像、构建缓存等)? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "清理Docker系统缓存..."
        docker system prune -f
        log_success "Docker系统缓存清理完成"
    else
        log_info "跳过Docker系统清理"
    fi
}

# 验证配置文件
verify_config_files() {
    log_info "验证配置文件..."

    if [ -f "./check-config-files.sh" ]; then
        if ./check-config-files.sh; then
            log_success "配置文件验证通过"
        else
            log_error "配置文件验证失败"
            exit 1
        fi
    else
        log_warning "配置文件检查脚本不存在，跳过验证"
    fi
}

# 重新启动所有服务
restart_all_services() {
    log_info "重新启动所有服务..."

    # 分阶段启动以确保依赖关系
    log_info "第1阶段: 启动数据库服务..."
    docker-compose up -d n9e-mysql clickhouse postgres redis victoriametrics

    log_info "等待数据库服务就绪..."
    sleep 30

    log_info "第2阶段: 启动应用服务..."
    docker-compose up -d

    log_success "所有服务启动命令已执行"
}

# 等待服务就绪
wait_for_services() {
    log_info "等待关键服务就绪..."

    local timeout=180
    local elapsed=0

    while [ $elapsed -lt $timeout ]; do
        # 检查N9E MySQL
        if docker exec n9e-mysql mysqladmin ping -h localhost -uroot -p1234 --silent 2>/dev/null; then
            log_success "N9E MySQL已就绪"
            break
        fi

        echo -n "."
        sleep 5
        elapsed=$((elapsed + 5))
    done

    if [ $elapsed -ge $timeout ]; then
        log_warning "等待服务超时，请手动检查"
    fi
}

# 验证部署结果
verify_deployment() {
    log_info "验证部署结果..."

    # 运行N9E测试
    if [ -f "./test-n9e-init.sh" ]; then
        ./test-n9e-init.sh
    fi

    # 运行部署验证
    if [ -f "./validate-deployment.sh" ]; then
        ./validate-deployment.sh
    fi

    # 显示服务状态
    echo ""
    log_info "当前服务状态:"
    docker-compose ps
}

# 显示访问信息
show_access_info() {
    echo ""
    log_success "🎉 环境重置完成！"
    echo ""
    echo "🌐 服务访问地址:"
    echo "  Grafana:         http://localhost:3000  (admin/admin123)"
    echo "  Superset:        http://localhost:8088"
    echo "  DataEase:        http://localhost:8810"
    echo "  Nightingale:     http://localhost:17000  (root/root.2020)"
    echo "  DolphinScheduler: http://localhost:12345"
    echo ""
    echo "🔧 如有问题:"
    echo "  - 查看日志: docker-compose logs [service-name]"
    echo "  - 重启服务: docker-compose restart [service-name]"
    echo "  - 强制重置数据库: ./force-init-databases.sh"
}

# 主函数
main() {
    log_info "开始环境完全重置..."

    # 检查Docker是否运行
    if ! docker info &> /dev/null; then
        log_error "Docker服务未运行，请启动Docker"
        exit 1
    fi

    # 执行重置步骤
    stop_all_services
    echo ""

    cleanup_volumes_networks
    echo ""

    cleanup_docker_system
    echo ""

    verify_config_files
    echo ""

    restart_all_services
    echo ""

    wait_for_services
    echo ""

    verify_deployment
    echo ""

    show_access_info
}

# 显示警告
echo ""
log_warning "⚠️  此脚本将完全重置环境，包括："
log_warning "   - 停止所有服务"
log_warning "   - 删除所有数据卷和数据"
log_warning "   - 清理Docker缓存(可选)"
log_warning "   - 重新部署所有服务"
echo ""
read -p "确认继续? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log_info "用户取消操作"
    exit 0
fi

# 执行主函数
main "$@"