#Requires -RunAsAdministrator

$ErrorActionPreference = "Stop"
$NAMESPACE = "nginx-analytics"
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Nginx Analytics - K8s Deployment" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# Check kubectl
Write-Host "Checking kubectl..." -ForegroundColor Yellow
try {
    $null = kubectl version --client 2>&1
    Write-Host "OK: kubectl available" -ForegroundColor Green
} catch {
    Write-Host "ERROR: kubectl not found" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

# Check cluster
Write-Host "Checking K8s cluster..." -ForegroundColor Yellow
try {
    $null = kubectl cluster-info 2>&1
    Write-Host "OK: K8s cluster connected" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Cannot connect to K8s cluster" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}
Write-Host ""

Set-Location $SCRIPT_DIR

# Step 1
Write-Host "[1/8] Creating namespace..." -ForegroundColor Yellow
kubectl apply -f 00-namespace.yaml
Start-Sleep -Seconds 2

# Step 2
Write-Host "[2/8] Creating ConfigMap..." -ForegroundColor Yellow
kubectl apply -f 01-configmap.yaml
Start-Sleep -Seconds 2

# Step 3
Write-Host "[3/8] Creating Secrets..." -ForegroundColor Yellow
kubectl apply -f 02-secrets.yaml
Start-Sleep -Seconds 2

# Step 4
Write-Host "[4/8] Creating PersistentVolumes..." -ForegroundColor Yellow
kubectl apply -f 03-persistent-volumes.yaml
Start-Sleep -Seconds 5

Write-Host "    Waiting for PVC binding..." -ForegroundColor Gray
$pvcs = @("clickhouse-data-pvc", "redis-data-pvc", "dataease-mysql-data-pvc", "dataease-data-pvc", "grafana-data-pvc")
foreach ($pvc in $pvcs) {
    Write-Host "    Checking $pvc..." -ForegroundColor Gray
    $timeout = 60
    $elapsed = 0
    while ($elapsed -lt $timeout) {
        $result = kubectl get pvc $pvc -n $NAMESPACE 2>&1 | Out-String
        if ($result -match "Bound") {
            Write-Host "    OK: $pvc bound" -ForegroundColor Green
            break
        }
        Start-Sleep -Seconds 2
        $elapsed += 2
    }
    if ($elapsed -ge $timeout) {
        Write-Host "    WARN: $pvc binding timeout" -ForegroundColor Red
    }
}

# Step 5
Write-Host "[5/8] Deploying ClickHouse..." -ForegroundColor Yellow
kubectl apply -f 04-clickhouse.yaml
Write-Host "    Waiting for ClickHouse..." -ForegroundColor Gray
Start-Sleep -Seconds 10
$timeout = 180
$elapsed = 0
while ($elapsed -lt $timeout) {
    $result = kubectl get pods -l app=clickhouse -n $NAMESPACE 2>&1 | Out-String
    if ($result -match "Running") {
        Write-Host "    OK: ClickHouse ready" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 5
    $elapsed += 5
}

# Step 6
Write-Host "[6/8] Deploying Redis..." -ForegroundColor Yellow
kubectl apply -f 05-redis.yaml
Write-Host "    Waiting for Redis..." -ForegroundColor Gray
Start-Sleep -Seconds 5
$timeout = 120
$elapsed = 0
while ($elapsed -lt $timeout) {
    $result = kubectl get pods -l app=redis -n $NAMESPACE 2>&1 | Out-String
    if ($result -match "Running") {
        Write-Host "    OK: Redis ready" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 5
    $elapsed += 5
}

# Step 7
Write-Host "[7/8] Deploying DataEase MySQL..." -ForegroundColor Yellow
kubectl apply -f 06-dataease-mysql.yaml
Write-Host "    Waiting for MySQL..." -ForegroundColor Gray
Start-Sleep -Seconds 10
$timeout = 180
$elapsed = 0
while ($elapsed -lt $timeout) {
    $result = kubectl get pods -l app=dataease-mysql -n $NAMESPACE 2>&1 | Out-String
    if ($result -match "Running") {
        Write-Host "    OK: MySQL ready" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 5
    $elapsed += 5
}

Write-Host "    Deploying DataEase app..." -ForegroundColor Gray
kubectl apply -f 07-dataease.yaml
Write-Host "    Waiting for DataEase (2-3 min)..." -ForegroundColor Gray
Start-Sleep -Seconds 30
$timeout = 300
$elapsed = 0
while ($elapsed -lt $timeout) {
    $result = kubectl get pods -l app=dataease -n $NAMESPACE 2>&1 | Out-String
    if ($result -match "Running") {
        Write-Host "    OK: DataEase ready" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 10
    $elapsed += 10
}

# Step 8
Write-Host "[8/8] Deploying Grafana..." -ForegroundColor Yellow
kubectl apply -f 08-grafana.yaml
Write-Host "    Waiting for Grafana..." -ForegroundColor Gray
Start-Sleep -Seconds 10
$timeout = 180
$elapsed = 0
while ($elapsed -lt $timeout) {
    $result = kubectl get pods -l app=grafana -n $NAMESPACE 2>&1 | Out-String
    if ($result -match "Running") {
        Write-Host "    OK: Grafana ready" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 5
    $elapsed += 5
}

Write-Host ""
Write-Host "==========================================" -ForegroundColor Green
Write-Host "DEPLOYMENT COMPLETE!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""

Write-Host "Service Status:" -ForegroundColor Cyan
kubectl get pods -n $NAMESPACE -o wide

Write-Host ""
Write-Host "Service Addresses:" -ForegroundColor Cyan
kubectl get svc -n $NAMESPACE

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Access Information:" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Grafana:     http://192.168.0.140:3000" -ForegroundColor White
Write-Host "  Login:     admin / admin123" -ForegroundColor Gray
Write-Host ""
Write-Host "DataEase:    http://192.168.0.140:8810" -ForegroundColor White
Write-Host "  Login:     admin / DataEase123@" -ForegroundColor Gray
Write-Host ""
Write-Host "ClickHouse:  http://192.168.0.140:8123" -ForegroundColor White
Write-Host "  Login:     analytics_user / analytics_password_change_in_prod" -ForegroundColor Gray
Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "1. Initialize ClickHouse: .\init-clickhouse-db.ps1" -ForegroundColor White
Write-Host "2. Configure ETL: .\config-etl-auto.ps1" -ForegroundColor White
Write-Host "3. Run ETL test" -ForegroundColor White
Write-Host "4. Configure Grafana Dashboard" -ForegroundColor White
Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan

Read-Host "Press Enter to exit"
