#Requires -RunAsAdministrator
# ==========================================
# Nginx Analytics - 一键部署脚本 (PowerShell版本)
# 方案B: Grafana快速验证方案
# 适用于Windows环境
# ==========================================

$ErrorActionPreference = "Stop"

$NAMESPACE = "nginx-analytics"
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Nginx日志分析数据仓库 - K8s部署" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# 检查kubectl可用性
Write-Host "检查kubectl..." -ForegroundColor Yellow
try {
    $null = kubectl version --client 2>&1
    Write-Host "✓ kubectl可用" -ForegroundColor Green
} catch {
    Write-Host "✗ 错误: kubectl未安装或不在PATH中" -ForegroundColor Red
    Read-Host "按Enter键退出"
    exit 1
}

# 检查集群连接
Write-Host "检查K8s集群连接..." -ForegroundColor Yellow
try {
    $null = kubectl cluster-info 2>&1
    Write-Host "✓ K8s集群连接正常" -ForegroundColor Green
} catch {
    Write-Host "✗ 错误: 无法连接到K8s集群" -ForegroundColor Red
    Read-Host "按Enter键退出"
    exit 1
}
Write-Host ""

# 切换到脚本目录
Set-Location $SCRIPT_DIR

# Step 1: 创建命名空间
Write-Host "[1/8] 创建命名空间..." -ForegroundColor Yellow
kubectl apply -f 00-namespace.yaml
Start-Sleep -Seconds 2

# Step 2: 创建ConfigMap
Write-Host "[2/8] 创建配置文件..." -ForegroundColor Yellow
kubectl apply -f 01-configmap.yaml
Start-Sleep -Seconds 2

# Step 3: 创建Secrets
Write-Host "[3/8] 创建密钥配置..." -ForegroundColor Yellow
kubectl apply -f 02-secrets.yaml
Start-Sleep -Seconds 2

# Step 4: 创建持久化卷
Write-Host "[4/8] 创建持久化存储卷..." -ForegroundColor Yellow
kubectl apply -f 03-persistent-volumes.yaml
Start-Sleep -Seconds 5

# 等待PVC绑定
Write-Host "    等待PVC绑定..." -ForegroundColor Gray
$pvcs = @("clickhouse-data-pvc", "redis-data-pvc", "dataease-mysql-data-pvc", "dataease-data-pvc", "grafana-data-pvc")
foreach ($pvc in $pvcs) {
    Write-Host "    检查 $pvc..." -ForegroundColor Gray
    $timeout = 60
    $elapsed = 0
    while ($elapsed -lt $timeout) {
        $result = kubectl get pvc $pvc -n $NAMESPACE 2>&1 | Out-String
        if ($result -match "Bound") {
            Write-Host "    ✓ $pvc 已绑定" -ForegroundColor Green
            break
        }
        Start-Sleep -Seconds 2
        $elapsed += 2
    }
    if ($elapsed -ge $timeout) {
        Write-Host "    ✗ $pvc 绑定超时" -ForegroundColor Red
    }
}

# Step 5: 部署ClickHouse
Write-Host "[5/8] 部署ClickHouse数据库..." -ForegroundColor Yellow
kubectl apply -f 04-clickhouse.yaml
Write-Host "    等待ClickHouse就绪..." -ForegroundColor Gray
Start-Sleep -Seconds 10
$timeout = 180
$elapsed = 0
while ($elapsed -lt $timeout) {
    $result = kubectl get pods -l app=clickhouse -n $NAMESPACE 2>&1 | Out-String
    if ($result -match "Running") {
        Write-Host "    ✓ ClickHouse已就绪" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 5
    $elapsed += 5
}
if ($elapsed -ge $timeout) {
    Write-Host "    ⚠ ClickHouse启动超时，请稍后检查" -ForegroundColor Yellow
}

# Step 6: 部署Redis
Write-Host "[6/8] 部署Redis缓存..." -ForegroundColor Yellow
kubectl apply -f 05-redis.yaml
Write-Host "    等待Redis就绪..." -ForegroundColor Gray
Start-Sleep -Seconds 5
$timeout = 120
$elapsed = 0
while ($elapsed -lt $timeout) {
    $result = kubectl get pods -l app=redis -n $NAMESPACE 2>&1 | Out-String
    if ($result -match "Running") {
        Write-Host "    ✓ Redis已就绪" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 5
    $elapsed += 5
}
if ($elapsed -ge $timeout) {
    Write-Host "    ⚠ Redis启动超时，请稍后检查" -ForegroundColor Yellow
}

# Step 7: 部署DataEase MySQL
Write-Host "[7/8] 部署DataEase MySQL..." -ForegroundColor Yellow
kubectl apply -f 06-dataease-mysql.yaml
Write-Host "    等待MySQL就绪..." -ForegroundColor Gray
Start-Sleep -Seconds 10
$timeout = 180
$elapsed = 0
while ($elapsed -lt $timeout) {
    $result = kubectl get pods -l app=dataease-mysql -n $NAMESPACE 2>&1 | Out-String
    if ($result -match "Running") {
        Write-Host "    ✓ MySQL已就绪" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 5
    $elapsed += 5
}
if ($elapsed -ge $timeout) {
    Write-Host "    ⚠ MySQL启动超时，请稍后检查" -ForegroundColor Yellow
}

# 部署DataEase
Write-Host "    部署DataEase应用..." -ForegroundColor Gray
kubectl apply -f 07-dataease.yaml
Write-Host "    等待DataEase就绪(可能需要2-3分钟)..." -ForegroundColor Gray
Start-Sleep -Seconds 30
$timeout = 300
$elapsed = 0
while ($elapsed -lt $timeout) {
    $result = kubectl get pods -l app=dataease -n $NAMESPACE 2>&1 | Out-String
    if ($result -match "Running") {
        Write-Host "    ✓ DataEase已就绪" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 10
    $elapsed += 10
}
if ($elapsed -ge $timeout) {
    Write-Host "    ⚠ DataEase启动超时，请稍后检查" -ForegroundColor Yellow
}

# Step 8: 部署Grafana
Write-Host "[8/8] 部署Grafana..." -ForegroundColor Yellow
kubectl apply -f 08-grafana.yaml
Write-Host "    等待Grafana就绪..." -ForegroundColor Gray
Start-Sleep -Seconds 10
$timeout = 180
$elapsed = 0
while ($elapsed -lt $timeout) {
    $result = kubectl get pods -l app=grafana -n $NAMESPACE 2>&1 | Out-String
    if ($result -match "Running") {
        Write-Host "    ✓ Grafana已就绪" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 5
    $elapsed += 5
}
if ($elapsed -ge $timeout) {
    Write-Host "    ⚠ Grafana启动超时，请稍后检查" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "==========================================" -ForegroundColor Green
Write-Host "✓ 所有服务部署完成!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""

# 显示服务状态
Write-Host "服务状态：" -ForegroundColor Cyan
kubectl get pods -n $NAMESPACE -o wide

Write-Host ""
Write-Host "服务访问地址：" -ForegroundColor Cyan
kubectl get svc -n $NAMESPACE

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "访问信息：" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Grafana:     http://192.168.0.140:3000" -ForegroundColor White
Write-Host "  账号:      admin / admin123" -ForegroundColor Gray
Write-Host ""
Write-Host "DataEase:    http://192.168.0.140:8810" -ForegroundColor White
Write-Host "  账号:      admin / DataEase123@" -ForegroundColor Gray
Write-Host ""
Write-Host "ClickHouse:  http://192.168.0.140:8123" -ForegroundColor White
Write-Host "  账号:      analytics_user / analytics_password_change_in_prod" -ForegroundColor Gray
Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "下一步：" -ForegroundColor Yellow
Write-Host "1. 初始化ClickHouse数据库: .\init-clickhouse-db.ps1" -ForegroundColor White
Write-Host "2. 配置ETL连接: .\config-etl-auto.ps1" -ForegroundColor White
Write-Host "3. 运行ETL测试处理数据" -ForegroundColor White
Write-Host "4. 在Grafana中配置Dashboard" -ForegroundColor White
Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan

Read-Host "按Enter键退出"
