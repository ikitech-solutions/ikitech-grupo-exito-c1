#!/bin/bash

# ============================================
# Script de Inicio y Validación - Observabilidad
# Para Linux/Mac
# ============================================

set -e

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}========================================"
echo -e "  Inicio de Stack con Observabilidad"
echo -e "========================================${NC}"
echo ""

# Función para esperar que un servicio esté disponible
wait_for_service() {
    local service_name=$1
    local url=$2
    local max_attempts=${3:-30}
    local attempt=0
    
    echo -e "${YELLOW}Esperando a que $service_name esté disponible...${NC}"
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -s -f "$url" > /dev/null 2>&1; then
            echo -e "${GREEN}✓ $service_name está disponible${NC}"
            return 0
        fi
        
        attempt=$((attempt + 1))
        echo -n "."
        sleep 2
    done
    
    echo ""
    echo -e "${RED}✗ $service_name no respondió después de $max_attempts intentos${NC}"
    return 1
}

# ============================================
# PASO 1: Validaciones Previas
# ============================================

echo -e "${CYAN}[1/6] Validaciones previas...${NC}"

# Verificar que Docker está corriendo
if ! docker ps > /dev/null 2>&1; then
    echo -e "${RED}✗ Docker no está corriendo o no tienes permisos${NC}"
    echo -e "${YELLOW}Por favor, inicia Docker y vuelve a intentar${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker está corriendo${NC}"

# Verificar que los archivos necesarios existen
required_files=(
    "docker-compose.yml"
    "docker-compose.observability-windows.yml"
    "observability/prometheus/prometheus.yml"
    "observability/grafana/provisioning/datasources/datasources.yml"
    "observability/loki/loki-config.yml"
)

for file in "${required_files[@]}"; do
    if [ -f "$file" ]; then
        echo -e "${GREEN}✓ Archivo encontrado: $file${NC}"
    else
        echo -e "${RED}✗ Archivo faltante: $file${NC}"
        exit 1
    fi
done

echo ""

# ============================================
# PASO 2: Limpiar Estado Previo (Opcional)
# ============================================

echo -e "${CYAN}[2/6] Limpieza de estado previo...${NC}"
read -p "¿Deseas limpiar contenedores y volúmenes previos? (s/N): " cleanup

if [[ $cleanup =~ ^[Ss]$ ]]; then
    echo -e "${YELLOW}Limpiando contenedores y volúmenes...${NC}"
    docker compose down -v
    docker compose -f docker-compose.observability-windows.yml down -v
    sleep 5
    echo -e "${GREEN}✓ Limpieza completada${NC}"
else
    echo -e "${YELLOW}Saltando limpieza${NC}"
fi

echo ""

# ============================================
# PASO 3: Iniciar Aplicación Principal
# ============================================

echo -e "${CYAN}[3/6] Iniciando aplicación principal...${NC}"
docker compose up -d

if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Error al iniciar la aplicación principal${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Contenedores de aplicación iniciados${NC}"
echo ""

# ============================================
# PASO 4: Esperar a que los Servicios Estén Listos
# ============================================

echo -e "${CYAN}[4/6] Esperando a que los servicios estén listos...${NC}"

# Esperar PostgreSQL
echo -e "${YELLOW}Esperando PostgreSQL (60 segundos)...${NC}"
sleep 60

# Verificar Airflow
if wait_for_service "Airflow" "http://localhost:8080/health" 30; then
    airflow_ready=true
else
    echo -e "${YELLOW}⚠ Airflow no respondió, pero continuaremos...${NC}"
    airflow_ready=false
fi

echo ""

# ============================================
# PASO 5: Iniciar Stack de Observabilidad
# ============================================

echo -e "${CYAN}[5/6] Iniciando stack de observabilidad...${NC}"
docker compose -f docker-compose.observability-windows.yml up -d

if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Error al iniciar el stack de observabilidad${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Contenedores de observabilidad iniciados${NC}"
echo ""

# Esperar a que Prometheus y Grafana estén listos
echo -e "${YELLOW}Esperando servicios de observabilidad...${NC}"
sleep 30

wait_for_service "Prometheus" "http://localhost:9090/-/ready" 20 && prometheus_ready=true || prometheus_ready=false
wait_for_service "Grafana" "http://localhost:3000/api/health" 20 && grafana_ready=true || grafana_ready=false

echo ""

# ============================================
# PASO 6: Verificación Final
# ============================================

echo -e "${CYAN}[6/6] Verificación final...${NC}"
echo ""

# Mostrar estado de contenedores
echo -e "${CYAN}Estado de contenedores:${NC}"
docker compose ps
echo ""

# Resumen de servicios
echo -e "${CYAN}========================================"
echo -e "  Resumen de Servicios"
echo -e "========================================${NC}"
echo ""

if [ "$airflow_ready" = true ]; then
    echo -e "${GREEN}✓ Airflow: http://localhost:8080${NC}"
else
    echo -e "${RED}✗ Airflow: http://localhost:8080${NC}"
fi

if [ "$prometheus_ready" = true ]; then
    echo -e "${GREEN}✓ Prometheus: http://localhost:9090${NC}"
else
    echo -e "${RED}✗ Prometheus: http://localhost:9090${NC}"
fi

if [ "$grafana_ready" = true ]; then
    echo -e "${GREEN}✓ Grafana: http://localhost:3000${NC}"
else
    echo -e "${RED}✗ Grafana: http://localhost:3000${NC}"
fi

echo ""
echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN}Credenciales:${NC}"
echo -e "  Grafana: admin / admin"
echo -e "  Airflow: admin / admin"
echo -e "${CYAN}========================================${NC}"
echo ""

# Verificar si hay errores en los logs
echo -e "${CYAN}Verificando logs de errores...${NC}"
errors=$(docker compose logs --tail=50 2>&1 | grep -iE "error|failed" || true)

if [ -n "$errors" ]; then
    echo -e "${YELLOW}⚠ Se encontraron posibles errores en los logs:${NC}"
    echo "$errors"
    echo ""
    echo -e "${CYAN}Para ver logs completos, ejecuta:${NC}"
    echo "  docker compose logs <nombre_servicio>"
else
    echo -e "${GREEN}✓ No se encontraron errores críticos en los logs${NC}"
fi

echo ""
echo -e "${CYAN}========================================"
echo -e "${GREEN}  Inicio Completado"
echo -e "${CYAN}========================================${NC}"
echo ""
