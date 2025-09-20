#!/bin/bash

# 部署验证脚本
# 验证新环境部署是否成功

set -e

echo "🔍 Nginx Analytics Platform - 部署验证脚本"
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

# 检查服务状态
check_services() {
    log_info "检查Docker服务状态..."

    local services=("nginx-analytics-clickhouse" "nginx-analytics-redis" "nginx-analytics-postgres" "n9e-mysql" "nginx-analytics-grafana" "nginx-analytics-nightingale")
    local healthy_count=0

    for service in "${services[@]}"; do
        if docker ps --filter "name=$service" --filter "status=running" | grep -q "$service"; then
            log_success "✓ $service 正在运行"
            ((healthy_count++))
        else
            log_error "✗ $service 未运行或不健康"
        fi
    done

    echo ""
    log_info "核心服务状态: $healthy_count/${#services[@]} 健康"

    if [ $healthy_count -eq ${#services[@]} ]; then
        log_success "所有核心服务运行正常"
        return 0
    else
        log_warning "部分服务可能需要更多时间启动"
        return 1
    fi
}

# 检查数据库连接
check_databases() {
    log_info "检查数据库连接..."

    # 检查ClickHouse
    if docker exec nginx-analytics-clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
        log_success "✓ ClickHouse连接正常"
    else
        log_error "✗ ClickHouse连接失败"
    fi

    # 检查N9E MySQL
    if docker exec n9e-mysql mysql -uroot -p1234 -e "SELECT 1;" >/dev/null 2>&1; then
        log_success "✓ N9E MySQL连接正常"
    else
        log_error "✗ N9E MySQL连接失败"
    fi

    # 检查PostgreSQL
    if docker exec nginx-analytics-postgres pg_isready -U superset -d superset >/dev/null 2>&1; then
        log_success "✓ PostgreSQL连接正常"
    else
        log_error "✗ PostgreSQL连接失败"
    fi

    # 检查Redis
    if docker exec nginx-analytics-redis redis-cli ping >/dev/null 2>&1; then
        log_success "✓ Redis连接正常"
    else
        log_error "✗ Redis连接失败"
    fi
}

# 检查Web服务
check_web_services() {
    log_info "检查Web服务可访问性..."

    # 检查Grafana
    if curl -f http://localhost:3000/api/health >/dev/null 2>&1; then
        log_success "✓ Grafana (http://localhost:3000) 可访问"
    else
        log_warning "! Grafana可能还在启动中"
    fi

    # 检查Superset
    if curl -f http://localhost:8088/health >/dev/null 2>&1; then
        log_success "✓ Superset (http://localhost:8088) 可访问"
    else
        log_warning "! Superset可能还在启动中"
    fi

    # 检查DataEase
    if curl -f http://localhost:8810/ >/dev/null 2>&1; then
        log_success "✓ DataEase (http://localhost:8810) 可访问"
    else
        log_warning "! DataEase可能还在启动中"
    fi

    # 检查Nightingale
    if curl -f http://localhost:17000 >/dev/null 2>&1; then
        log_success "✓ Nightingale (http://localhost:17000) 可访问"
    else
        log_warning "! Nightingale可能还在启动中"
    fi

    # 检查DolphinScheduler
    if curl -f http://localhost:12345 >/dev/null 2>&1; then
        log_success "✓ DolphinScheduler (http://localhost:12345) 可访问"
    else
        log_warning "! DolphinScheduler可能还在启动中"
    fi
}

# 检查数据库表结构
check_database_schema() {
    log_info "检查数据库表结构..."

    # 检查N9E数据库表
    if docker exec n9e-mysql mysql -uroot -p1234 -e "USE n9e_v6; SHOW TABLES;" | grep -q "users"; then
        log_success "✓ N9E数据库表结构正常"
    else
        log_error "✗ N9E数据库表结构缺失"
        log_info "建议运行: ./force-init-databases.sh"
    fi

    # 检查ClickHouse数据库
    if docker exec nginx-analytics-clickhouse clickhouse-client --query "SHOW DATABASES" | grep -q "nginx_analytics"; then
        log_success "✓ ClickHouse数据库存在"
    else
        log_error "✗ ClickHouse数据库不存在"
    fi
}

# 生成验证报告
generate_report() {
    log_info "生成验证报告..."

    local report_file="deployment-validation-report.txt"

    {
        echo "部署验证报告"
        echo "生成时间: $(date)"
        echo "=" * 50
        echo ""

        echo "Docker服务状态:"
        docker-compose ps
        echo ""

        echo "端口占用情况:"
        netstat -tuln 2>/dev/null | grep -E ":(3000|8088|8810|17000|12345|8123|3308|5433|6380)" || echo "无相关端口占用信息"
        echo ""

        echo "容器资源使用:"
        docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}" 2>/dev/null || echo "无法获取资源使用信息"
        echo ""

        echo "下一步操作建议:"
        echo "1. 如果有服务未启动，等待5-10分钟后重新检查"
        echo "2. 如果数据库连接失败，运行 ./force-init-databases.sh"
        echo "3. 查看特定服务日志: docker-compose logs [service-name]"
        echo "4. 重启特定服务: docker-compose restart [service-name]"

    } > "$report_file"

    log_success "验证报告已生成: $report_file"
}

# 主函数
main() {
    log_info "开始部署验证..."

    # 等待一些时间让服务完全启动
    log_info "等待服务启动完成..."
    sleep 10

    # 执行各项检查
    check_services
    echo ""

    check_databases
    echo ""

    check_database_schema
    echo ""

    check_web_services
    echo ""

    # 生成报告
    generate_report

    # 显示访问信息
    echo ""
    log_success "🎉 部署验证完成！"
    echo ""
    echo "🌐 服务访问地址:"
    echo "  Grafana:         http://localhost:3000  (admin/admin123)"
    echo "  Superset:        http://localhost:8088"
    echo "  DataEase:        http://localhost:8810"
    echo "  Nightingale:     http://localhost:17000  (root/root.2020)"
    echo "  DolphinScheduler: http://localhost:12345"
    echo ""
    echo "🔧 如有问题，请查看:"
    echo "  - 验证报告: deployment-validation-report.txt"
    echo "  - 初始化指南: INITIALIZE_GUIDE.md"
    echo "  - 服务日志: docker-compose logs [service-name]"
}

# 执行主函数
main "$@"