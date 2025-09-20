#!/bin/bash

# N9E数据库初始化测试脚本
# 用于验证数据库初始化是否正确

set -e

echo "🧪 N9E数据库初始化测试"
echo "=" * 40

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

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查脚本文件
check_scripts() {
    log_info "检查N9E初始化脚本..."

    if [ -f "services/n9e/init-scripts/a-n9e.sql" ]; then
        log_success "✓ a-n9e.sql 存在"

        # 检查关键表定义
        if grep -q "CREATE TABLE.*users" services/n9e/init-scripts/a-n9e.sql; then
            log_success "✓ users表定义存在"
        else
            log_error "✗ users表定义缺失"
        fi

        if grep -q "CREATE TABLE.*role_operation" services/n9e/init-scripts/a-n9e.sql; then
            log_success "✓ role_operation表定义存在"
        else
            log_error "✗ role_operation表定义缺失"
        fi

        # 检查默认数据
        if grep -q "insert into.*users.*root" services/n9e/init-scripts/a-n9e.sql; then
            log_success "✓ root用户默认数据存在"
        else
            log_error "✗ root用户默认数据缺失"
        fi

    else
        log_error "✗ a-n9e.sql 不存在"
    fi

    if [ -f "services/n9e/init-scripts/c-init.sql" ]; then
        log_success "✓ c-init.sql 权限脚本存在"
    else
        log_error "✗ c-init.sql 权限脚本缺失"
    fi
}

# 检查Docker配置
check_docker_config() {
    log_info "检查Docker配置..."

    if grep -q "init-scripts:/docker-entrypoint-initdb.d" docker-compose.yml; then
        log_success "✓ init-scripts挂载配置正确"
    else
        log_error "✗ init-scripts挂载配置错误"
    fi

    if grep -q "MYSQL_DATABASE: n9e_v6" docker-compose.yml; then
        log_success "✓ 默认数据库配置正确"
    else
        log_error "✗ 默认数据库配置错误"
    fi
}

# 测试数据库连接和表结构（需要容器运行）
test_database() {
    log_info "测试数据库连接和表结构..."

    if docker ps | grep -q "n9e-mysql"; then
        log_info "N9E MySQL容器正在运行，执行表结构检查..."

        # 检查数据库是否存在
        if docker exec n9e-mysql mysql -uroot -p1234 -e "USE n9e_v6; SELECT 1;" >/dev/null 2>&1; then
            log_success "✓ n9e_v6数据库可访问"

            # 检查关键表
            if docker exec n9e-mysql mysql -uroot -p1234 -e "USE n9e_v6; SHOW TABLES;" | grep -q "users"; then
                log_success "✓ users表存在"
            else
                log_error "✗ users表不存在"
            fi

            if docker exec n9e-mysql mysql -uroot -p1234 -e "USE n9e_v6; SHOW TABLES;" | grep -q "role_operation"; then
                log_success "✓ role_operation表存在"
            else
                log_error "✗ role_operation表不存在"
            fi

            # 检查root用户
            if docker exec n9e-mysql mysql -uroot -p1234 -e "USE n9e_v6; SELECT username FROM users WHERE username='root';" | grep -q "root"; then
                log_success "✓ root用户存在"
            else
                log_error "✗ root用户不存在"
            fi

        else
            log_error "✗ 无法访问n9e_v6数据库"
        fi
    else
        log_info "N9E MySQL容器未运行，跳过数据库测试"
        log_info "启动容器后可运行此脚本进行完整测试"
    fi
}

# 生成测试报告
generate_report() {
    log_info "生成测试报告..."

    {
        echo "N9E数据库初始化测试报告"
        echo "生成时间: $(date)"
        echo "=" * 40
        echo ""

        echo "初始化脚本检查:"
        ls -la services/n9e/init-scripts/ || echo "目录不存在"
        echo ""

        echo "表定义统计:"
        if [ -f "services/n9e/init-scripts/a-n9e.sql" ]; then
            echo "总表数: $(grep -c "CREATE TABLE" services/n9e/init-scripts/a-n9e.sql)"
            echo "INSERT语句数: $(grep -c "insert into" services/n9e/init-scripts/a-n9e.sql)"
        fi
        echo ""

        echo "修复建议:"
        echo "1. 确保a-n9e.sql包含完整的表结构"
        echo "2. 确保Docker配置正确挂载init-scripts目录"
        echo "3. 重新启动容器以应用数据库初始化"
        echo "4. 检查容器日志: docker-compose logs n9e-mysql"

    } > "n9e-init-test-report.txt"

    log_success "测试报告已生成: n9e-init-test-report.txt"
}

# 主函数
main() {
    log_info "开始N9E数据库初始化测试..."

    check_scripts
    echo ""

    check_docker_config
    echo ""

    test_database
    echo ""

    generate_report

    log_success "🎉 N9E数据库初始化测试完成！"
    log_info "如需启动测试，请运行: docker-compose up -d n9e-mysql"
}

# 执行主函数
main "$@"