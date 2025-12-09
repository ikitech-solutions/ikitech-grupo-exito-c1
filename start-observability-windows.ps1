# ============================================
# Script de Inicio y Validación - Observabilidad
# Para Windows con Docker Desktop
# ============================================

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Inicio de Stack con Observabilidad" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Función para verificar si un puerto está en uso
function Test-Port {
    param([int]$Port)
    $connection = Test-NetConnection -ComputerName localhost -Port $Port -WarningAction SilentlyContinue -InformationLevel Quiet
    return $connection
}

# Función para esperar que un servicio esté disponible
function Wait-ForService {
    param(
        [string]$ServiceName,
        [string]$Url,
        [int]$MaxAttempts = 30
    )
    
    Write-Host "Esperando a que $ServiceName esté disponible..." -ForegroundColor Yellow
    $attempt = 0
    
    while ($attempt -lt $MaxAttempts) {
        try {
            $response = Invoke-WebRequest -Uri $Url -Method Get -TimeoutSec 2 -UseBasicParsing -ErrorAction SilentlyContinue
            if ($response.StatusCode -eq 200) {
                Write-Host "✓ $ServiceName está disponible" -ForegroundColor Green
                return $true
            }
        } catch {
            # Servicio aún no disponible
        }
        
        $attempt++
        Start-Sleep -Seconds 2
        Write-Host "." -NoNewline
    }
    
    Write-Host ""
    Write-Host "✗ $ServiceName no respondió después de $MaxAttempts intentos" -ForegroundColor Red
    return $false
}

# ============================================
# PASO 1: Validaciones Previas
# ============================================

Write-Host "[1/6] Validaciones previas..." -ForegroundColor Cyan

# Verificar que Docker Desktop está corriendo
try {
    docker ps | Out-Null
    Write-Host "✓ Docker Desktop está corriendo" -ForegroundColor Green
} catch {
    Write-Host "✗ Docker Desktop no está corriendo o no responde" -ForegroundColor Red
    Write-Host "Por favor, inicia Docker Desktop y vuelve a intentar" -ForegroundColor Yellow
    exit 1
}

# Verificar que los archivos necesarios existen
$requiredFiles = @(
    "docker-compose.yml",
    "docker-compose.observability-windows.yml",
    "observability/prometheus/prometheus.yml",
    "observability/grafana/provisioning/datasources/datasources.yml",
    "observability/loki/loki-config.yml"
)

foreach ($file in $requiredFiles) {
    if (Test-Path $file) {
        Write-Host "✓ Archivo encontrado: $file" -ForegroundColor Green
    } else {
        Write-Host "✗ Archivo faltante: $file" -ForegroundColor Red
        exit 1
    }
}

# Verificar puertos disponibles
$ports = @(
    @{Port=5432; Service="PostgreSQL"},
    @{Port=8080; Service="Airflow"},
    @{Port=9090; Service="Prometheus"},
    @{Port=3000; Service="Grafana"},
    @{Port=3100; Service="Loki"}
)

Write-Host ""
Write-Host "Verificando puertos..." -ForegroundColor Cyan
foreach ($portInfo in $ports) {
    if (Test-Port -Port $portInfo.Port) {
        Write-Host "⚠ Puerto $($portInfo.Port) ($($portInfo.Service)) ya está en uso" -ForegroundColor Yellow
        $response = Read-Host "¿Deseas detener los contenedores existentes? (S/N)"
        if ($response -eq "S" -or $response -eq "s") {
            Write-Host "Deteniendo contenedores existentes..." -ForegroundColor Yellow
            docker compose down
            docker compose -f docker-compose.observability-windows.yml down
            Start-Sleep -Seconds 5
            break
        } else {
            Write-Host "Abortando. Por favor, libera los puertos manualmente." -ForegroundColor Red
            exit 1
        }
    }
}

Write-Host ""

# ============================================
# PASO 2: Limpiar Estado Previo (Opcional)
# ============================================

Write-Host "[2/6] Limpieza de estado previo..." -ForegroundColor Cyan
$cleanup = Read-Host "¿Deseas limpiar contenedores y volúmenes previos? (S/N)"

if ($cleanup -eq "S" -or $cleanup -eq "s") {
    Write-Host "Limpiando contenedores y volúmenes..." -ForegroundColor Yellow
    docker compose down -v
    docker compose -f docker-compose.observability-windows.yml down -v
    Start-Sleep -Seconds 5
    Write-Host "✓ Limpieza completada" -ForegroundColor Green
} else {
    Write-Host "Saltando limpieza" -ForegroundColor Yellow
}

Write-Host ""

# ============================================
# PASO 3: Iniciar Aplicación Principal
# ============================================

Write-Host "[3/6] Iniciando aplicación principal..." -ForegroundColor Cyan
docker compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Error al iniciar la aplicación principal" -ForegroundColor Red
    exit 1
}

Write-Host "✓ Contenedores de aplicación iniciados" -ForegroundColor Green
Write-Host ""

# ============================================
# PASO 4: Esperar a que los Servicios Estén Listos
# ============================================

Write-Host "[4/6] Esperando a que los servicios estén listos..." -ForegroundColor Cyan

# Esperar PostgreSQL
Write-Host "Esperando PostgreSQL (60 segundos)..." -ForegroundColor Yellow
Start-Sleep -Seconds 60

# Verificar Airflow
$airflowReady = Wait-ForService -ServiceName "Airflow" -Url "http://localhost:8080/health" -MaxAttempts 30

if (-not $airflowReady) {
    Write-Host "⚠ Airflow no respondió, pero continuaremos..." -ForegroundColor Yellow
}

Write-Host ""

# ============================================
# PASO 5: Iniciar Stack de Observabilidad
# ============================================

Write-Host "[5/6] Iniciando stack de observabilidad..." -ForegroundColor Cyan
docker compose -f docker-compose.observability-windows.yml up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Error al iniciar el stack de observabilidad" -ForegroundColor Red
    exit 1
}

Write-Host "✓ Contenedores de observabilidad iniciados" -ForegroundColor Green
Write-Host ""

# Esperar a que Prometheus y Grafana estén listos
Write-Host "Esperando servicios de observabilidad..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

$prometheusReady = Wait-ForService -ServiceName "Prometheus" -Url "http://localhost:9090/-/ready" -MaxAttempts 20
$grafanaReady = Wait-ForService -ServiceName "Grafana" -Url "http://localhost:3000/api/health" -MaxAttempts 20

Write-Host ""

# ============================================
# PASO 6: Verificación Final
# ============================================

Write-Host "[6/6] Verificación final..." -ForegroundColor Cyan
Write-Host ""

# Mostrar estado de contenedores
Write-Host "Estado de contenedores:" -ForegroundColor Cyan
docker compose ps
Write-Host ""

# Resumen de servicios
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Resumen de Servicios" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$services = @(
    @{Name="Airflow"; Url="http://localhost:8080"; Status=$airflowReady},
    @{Name="Prometheus"; Url="http://localhost:9090"; Status=$prometheusReady},
    @{Name="Grafana"; Url="http://localhost:3000"; Status=$grafanaReady}
)

foreach ($service in $services) {
    $statusIcon = if ($service.Status) { "✓" } else { "✗" }
    $statusColor = if ($service.Status) { "Green" } else { "Red" }
    Write-Host "$statusIcon $($service.Name): $($service.Url)" -ForegroundColor $statusColor
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Credenciales:" -ForegroundColor Cyan
Write-Host "  Grafana: admin / admin" -ForegroundColor White
Write-Host "  Airflow: admin / admin" -ForegroundColor White
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Verificar si hay errores en los logs
Write-Host "Verificando logs de errores..." -ForegroundColor Cyan
$errors = docker compose logs --tail=50 2>&1 | Select-String -Pattern "error|Error|ERROR|failed|Failed|FAILED" -CaseSensitive

if ($errors) {
    Write-Host "⚠ Se encontraron posibles errores en los logs:" -ForegroundColor Yellow
    $errors | ForEach-Object { Write-Host $_.Line -ForegroundColor Yellow }
    Write-Host ""
    Write-Host "Para ver logs completos, ejecuta:" -ForegroundColor Cyan
    Write-Host "  docker compose logs <nombre_servicio>" -ForegroundColor White
} else {
    Write-Host "✓ No se encontraron errores críticos en los logs" -ForegroundColor Green
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Inicio Completado" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Preguntar si desea abrir los navegadores
$openBrowser = Read-Host "¿Deseas abrir las interfaces en el navegador? (S/N)"
if ($openBrowser -eq "S" -or $openBrowser -eq "s") {
    Start-Process "http://localhost:8080"
    Start-Process "http://localhost:9090"
    Start-Process "http://localhost:3000"
}
