#!/bin/bash

# 配置文件完整性检查脚本
# 用于验证新环境是否包含所有必要的配置文件

set -e

echo "🔍 配置文件完整性检查"
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

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 必需的配置文件列表
declare -A required_files=(
    ["docker-compose.yml"]="Docker Compose配置文件"
    [".env"]="环境变量配置文件"
    ["services/n9e/init-scripts/00-init-database.sql"]="N9E数据库初始化脚本"
    ["services/n9e/init-scripts/a-n9e.sql"]="N9E完整数据库结构"
    ["services/n9e/config/nightingale/config.toml"]="Nightingale配置文件"
    ["services/grafana/datasources/clickhouse.yml"]="Grafana ClickHouse数据源配置"
    ["services/dataease/config/application.yml"]="DataEase应用配置"
    ["services/dataease/config/mysql.env"]="DataEase MySQL环境变量"
    ["INITIALIZE_GUIDE.md"]="初始化指南文档"
    ["init-fresh-environment.sh"]="新环境初始化脚本"
    ["force-init-databases.sh"]="数据库强制初始化脚本"
)

# 推荐的配置文件列表
declare -A recommended_files=(
    ["start-services.sh"]="分组启动脚本"
    ["stop-services.sh"]="分组停止脚本"
    ["services/superset/config/superset_config.py"]="Superset配置文件"
    ["services/n9e/config/categraf/conf.toml"]="Categraf配置文件"
)

# 检查必需文件
check_required_files() {
    log_info "检查必需配置文件..."

    local missing_count=0

    for file in "${!required_files[@]}"; do
        if [ -f "$file" ]; then
            log_success "✓ $file - ${required_files[$file]}"
        else
            log_error "✗ $file - ${required_files[$file]} (缺失)"
            ((missing_count++))
        fi
    done

    if [ $missing_count -eq 0 ]; then
        log_success "所有必需配置文件都存在"
        return 0
    else
        log_error "缺失 $missing_count 个必需配置文件"
        return 1
    fi
}

# 检查推荐文件
check_recommended_files() {
    log_info "检查推荐配置文件..."

    local missing_count=0

    for file in "${!recommended_files[@]}"; do
        if [ -f "$file" ]; then
            log_success "✓ $file - ${recommended_files[$file]}"
        else
            log_warning "! $file - ${recommended_files[$file]} (推荐但缺失)"
            ((missing_count++))
        fi
    done

    if [ $missing_count -eq 0 ]; then
        log_success "所有推荐配置文件都存在"
    else
        log_warning "缺失 $missing_count 个推荐配置文件"
    fi
}

# 检查文件内容
check_file_contents() {
    log_info "检查关键配置文件内容..."

    # 检查docker-compose.yml
    if [ -f "docker-compose.yml" ]; then
        if grep -q "nginx-analytics-clickhouse" docker-compose.yml && \
           grep -q "nginx-analytics-grafana" docker-compose.yml && \
           grep -q "n9e-mysql" docker-compose.yml; then
            log_success "docker-compose.yml 内容验证通过"
        else
            log_error "docker-compose.yml 内容验证失败"
        fi
    fi

    # 检查.env文件
    if [ -f ".env" ]; then
        if grep -q "ST_DOCKER_MEMBER_LIST" .env && \
           grep -q "CLICKHOUSE_" .env; then
            log_success ".env 文件内容验证通过"
        else
            log_error ".env 文件内容验证失败"
        fi
    fi

    # 检查N9E初始化脚本
    if [ -f "services/n9e/init-scripts/00-init-database.sql" ]; then
        if grep -q "CREATE DATABASE IF NOT EXISTS n9e_v6" services/n9e/init-scripts/00-init-database.sql && \
           grep -q "CREATE TABLE IF NOT EXISTS.*users" services/n9e/init-scripts/00-init-database.sql; then
            log_success "N9E初始化脚本内容验证通过"
        else
            log_error "N9E初始化脚本内容验证失败"
        fi
    fi
}

# 检查目录结构
check_directory_structure() {
    log_info "检查目录结构..."

    local required_dirs=(
        "services/n9e/init-scripts"
        "services/n9e/config/nightingale"
        "services/grafana/datasources"
        "services/dataease/config"
    )

    local missing_dirs=0

    for dir in "${required_dirs[@]}"; do
        if [ -d "$dir" ]; then
            log_success "✓ 目录 $dir 存在"
        else
            log_error "✗ 目录 $dir 不存在"
            ((missing_dirs++))
        fi
    done

    if [ $missing_dirs -eq 0 ]; then
        log_success "目录结构验证通过"
        return 0
    else
        log_error "目录结构验证失败"
        return 1
    fi
}

# 生成配置报告
generate_report() {
    log_info "生成配置检查报告..."

    local report_file="config-check-report.txt"

    {
        echo "配置文件检查报告"
        echo "生成时间: $(date)"
        echo "=" * 50
        echo ""

        echo "必需配置文件状态:"
        for file in "${!required_files[@]}"; do
            if [ -f "$file" ]; then
                echo "✓ $file"
            else
                echo "✗ $file (缺失)"
            fi
        done

        echo ""
        echo "推荐配置文件状态:"
        for file in "${!recommended_files[@]}"; do
            if [ -f "$file" ]; then
                echo "✓ $file"
            else
                echo "! $file (推荐)"
            fi
        done

        echo ""
        echo "修复建议:"
        echo "1. 确保所有标记为'缺失'的文件都已创建"
        echo "2. 运行 ./init-fresh-environment.sh 进行完整初始化"
        echo "3. 如需强制重新初始化数据库，运行 ./force-init-databases.sh"

    } > "$report_file"

    log_success "报告已生成: $report_file"
}

# 主函数
main() {
    log_info "开始配置文件完整性检查..."

    local overall_status=0

    # 执行各项检查
    check_directory_structure || overall_status=1
    echo ""

    check_required_files || overall_status=1
    echo ""

    check_recommended_files
    echo ""

    check_file_contents
    echo ""

    # 生成报告
    generate_report

    # 总结
    if [ $overall_status -eq 0 ]; then
        log_success "🎉 配置文件检查通过！"
        log_info "环境已准备就绪，可以运行: docker-compose up -d"
    else
        log_error "❌ 配置文件检查失败！"
        log_info "请修复上述问题后再次运行检查"
        exit 1
    fi
}

# 执行主函数
main "$@"