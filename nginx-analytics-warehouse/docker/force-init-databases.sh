#!/bin/bash

# 强制初始化所有数据库脚本
# 当数据库表结构有问题时使用

set -e

echo "🔧 强制初始化数据库脚本"
echo "=" * 50

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

# 初始化N9E数据库
init_n9e_database() {
    log_info "初始化N9E数据库..."

    # 等待MySQL服务就绪
    log_info "等待N9E MySQL服务就绪..."
    for i in {1..30}; do
        if docker exec n9e-mysql mysqladmin ping -h localhost -uroot -p1234 --silent; then
            log_success "N9E MySQL服务已就绪"
            break
        fi
        if [ $i -eq 30 ]; then
            log_error "N9E MySQL服务未就绪，请检查服务状态"
            return 1
        fi
        sleep 2
    done

    # 执行初始化脚本
    log_info "执行N9E数据库初始化脚本..."

    # 先删除现有数据库（如果存在）
    docker exec n9e-mysql mysql -uroot -p1234 -e "DROP DATABASE IF EXISTS n9e_v6;" || true

    # 执行完整的N9E schema文件
    if [ -f "services/n9e/init-scripts/a-n9e.sql" ]; then
        log_info "执行完整的N9E schema..."
        if docker exec n9e-mysql mysql -uroot -p1234 < services/n9e/init-scripts/a-n9e.sql; then
            log_success "N9E数据库初始化完成"
        else
            log_error "N9E数据库初始化失败"
            return 1
        fi
    else
        log_error "N9E初始化脚本不存在: services/n9e/init-scripts/a-n9e.sql"
        return 1
    fi
}

# 初始化DataEase数据库
init_dataease_database() {
    log_info "初始化DataEase数据库..."

    # 等待DataEase MySQL服务就绪
    log_info "等待DataEase MySQL服务就绪..."
    for i in {1..30}; do
        if docker exec nginx-analytics-dataease-mysql mysqladmin ping -h localhost -uroot -pPassword123@mysql --silent; then
            log_success "DataEase MySQL服务已就绪"
            break
        fi
        if [ $i -eq 30 ]; then
            log_error "DataEase MySQL服务未就绪，请检查服务状态"
            return 1
        fi
        sleep 2
    done

    # 检查是否有初始化脚本
    if [ -f "services/dataease/init-scripts/init-db.sql" ]; then
        log_info "执行DataEase数据库初始化..."
        docker exec nginx-analytics-dataease-mysql mysql -uroot -pPassword123@mysql < services/dataease/init-scripts/init-db.sql
        log_success "DataEase数据库初始化完成"
    else
        log_info "DataEase使用默认初始化"
    fi
}

# 初始化ClickHouse数据库
init_clickhouse_database() {
    log_info "初始化ClickHouse数据库..."

    # 等待ClickHouse服务就绪
    log_info "等待ClickHouse服务就绪..."
    for i in {1..30}; do
        if docker exec nginx-analytics-clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
            log_success "ClickHouse服务已就绪"
            break
        fi
        if [ $i -eq 30 ]; then
            log_error "ClickHouse服务未就绪，请检查服务状态"
            return 1
        fi
        sleep 2
    done

    # 创建数据库和用户
    log_info "创建ClickHouse数据库和用户..."
    docker exec nginx-analytics-clickhouse clickhouse-client --query "
        CREATE DATABASE IF NOT EXISTS nginx_analytics;
        CREATE USER IF NOT EXISTS analytics_user IDENTIFIED BY 'analytics_password_change_in_prod';
        GRANT ALL ON nginx_analytics.* TO analytics_user;
    "
    log_success "ClickHouse数据库初始化完成"
}

# 初始化PostgreSQL数据库
init_postgres_database() {
    log_info "初始化PostgreSQL数据库..."

    # 等待PostgreSQL服务就绪
    log_info "等待PostgreSQL服务就绪..."
    for i in {1..30}; do
        if docker exec nginx-analytics-postgres pg_isready -U superset -d superset >/dev/null 2>&1; then
            log_success "PostgreSQL服务已就绪"
            break
        fi
        if [ $i -eq 30 ]; then
            log_error "PostgreSQL服务未就绪，请检查服务状态"
            return 1
        fi
        sleep 2
    done

    log_success "PostgreSQL数据库已就绪"
}

# 验证所有数据库
verify_databases() {
    log_info "验证数据库状态..."

    # 验证N9E数据库
    if docker exec n9e-mysql mysql -uroot -p1234 -e "USE n9e_v6; SHOW TABLES;" >/dev/null 2>&1; then
        log_success "N9E数据库验证通过"
    else
        log_error "N9E数据库验证失败"
    fi

    # 验证ClickHouse数据库
    if docker exec nginx-analytics-clickhouse clickhouse-client --query "SHOW DATABASES" | grep -q "nginx_analytics"; then
        log_success "ClickHouse数据库验证通过"
    else
        log_error "ClickHouse数据库验证失败"
    fi

    # 验证PostgreSQL数据库
    if docker exec nginx-analytics-postgres psql -U superset -d superset -c "\\l" >/dev/null 2>&1; then
        log_success "PostgreSQL数据库验证通过"
    else
        log_error "PostgreSQL数据库验证失败"
    fi
}

# 主函数
main() {
    log_warning "⚠️  此脚本将重新初始化所有数据库！"
    log_warning "这可能会删除现有数据，请确认是否继续。"

    read -p "是否继续初始化数据库? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "用户取消操作"
        exit 0
    fi

    # 检查服务是否运行
    if ! docker-compose ps | grep -q "Up"; then
        log_error "服务未启动，请先运行: docker-compose up -d"
        exit 1
    fi

    # 初始化各个数据库
    init_clickhouse_database
    sleep 2
    init_postgres_database
    sleep 2
    init_dataease_database
    sleep 2
    init_n9e_database
    sleep 2

    # 验证数据库
    verify_databases

    log_success "🎉 数据库强制初始化完成！"
    log_info "💡 现在可以重启服务: docker-compose restart"
}

# 执行主函数
main "$@"