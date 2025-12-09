#!/bin/bash

# Test and Validation Script for Observability Stack
# Grupo Éxito - Caso de Uso 1

set -e

echo "=========================================="
echo "Observability Stack - Test & Validation"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print success
print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

# Function to print error
print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Function to print warning
print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

echo "1. Validating YAML configuration files..."
echo ""

# Check if required files exist
FILES=(
    "observability/prometheus/prometheus.yml"
    "observability/prometheus/alerts/airflow_alerts.yml"
    "observability/prometheus/alerts/kafka_alerts.yml"
    "observability/prometheus/statsd-mapping.yml"
    "observability/loki/loki-config.yml"
    "observability/loki/promtail-config.yml"
    "observability/marquez/marquez.yml"
    "observability/grafana/provisioning/datasources/datasources.yml"
    "observability/grafana/provisioning/dashboards/dashboards.yml"
    "docker-compose.observability.yml"
)

for file in "${FILES[@]}"; do
    if [ -f "$file" ]; then
        print_success "Found: $file"
    else
        print_error "Missing: $file"
        exit 1
    fi
done

echo ""
echo "2. Validating YAML syntax..."
echo ""

# Validate YAML files using Python
python3 << 'EOF'
import yaml
import sys

files_to_check = [
    "observability/prometheus/prometheus.yml",
    "observability/prometheus/alerts/airflow_alerts.yml",
    "observability/prometheus/alerts/kafka_alerts.yml",
    "observability/prometheus/statsd-mapping.yml",
    "observability/loki/loki-config.yml",
    "observability/loki/promtail-config.yml",
    "observability/marquez/marquez.yml",
    "observability/grafana/provisioning/datasources/datasources.yml",
    "observability/grafana/provisioning/dashboards/dashboards.yml",
    "docker-compose.observability.yml",
]

all_valid = True
for file_path in files_to_check:
    try:
        with open(file_path, 'r') as f:
            yaml.safe_load(f)
        print(f"✓ Valid YAML: {file_path}")
    except yaml.YAMLError as e:
        print(f"✗ Invalid YAML: {file_path}")
        print(f"  Error: {e}")
        all_valid = False
    except FileNotFoundError:
        print(f"✗ File not found: {file_path}")
        all_valid = False

if not all_valid:
    sys.exit(1)
EOF

if [ $? -eq 0 ]; then
    print_success "All YAML files are valid"
else
    print_error "Some YAML files have syntax errors"
    exit 1
fi

echo ""
echo "3. Checking Docker Compose configuration..."
echo ""

# Check if docker-compose is available
if command -v docker-compose &> /dev/null; then
    docker-compose -f docker-compose.observability.yml config > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        print_success "Docker Compose configuration is valid"
    else
        print_error "Docker Compose configuration has errors"
        exit 1
    fi
else
    print_warning "docker-compose not found, skipping validation"
fi

echo ""
echo "4. Checking required directories..."
echo ""

DIRS=(
    "observability/prometheus/alerts"
    "observability/prometheus/jmx_exporter"
    "observability/grafana/dashboards"
    "observability/grafana/provisioning/datasources"
    "observability/grafana/provisioning/dashboards"
    "observability/loki"
    "observability/marquez"
)

for dir in "${DIRS[@]}"; do
    if [ -d "$dir" ]; then
        print_success "Directory exists: $dir"
    else
        print_error "Directory missing: $dir"
        exit 1
    fi
done

echo ""
echo "5. Checking JMX Exporter..."
echo ""

if [ -f "observability/prometheus/jmx_exporter/jmx_prometheus_javaagent.jar" ]; then
    print_success "JMX Prometheus Java Agent found"
    SIZE=$(du -h observability/prometheus/jmx_exporter/jmx_prometheus_javaagent.jar | cut -f1)
    echo "  Size: $SIZE"
else
    print_error "JMX Prometheus Java Agent not found"
    echo "  Run: cd observability/prometheus/jmx_exporter && wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar -O jmx_prometheus_javaagent.jar"
    exit 1
fi

echo ""
echo "6. Checking Grafana dashboards..."
echo ""

DASHBOARDS=$(find observability/grafana/dashboards -name "*.json" 2>/dev/null | wc -l)
if [ "$DASHBOARDS" -gt 0 ]; then
    print_success "Found $DASHBOARDS Grafana dashboard(s)"
    find observability/grafana/dashboards -name "*.json" -exec basename {} \; | sed 's/^/  - /'
else
    print_warning "No Grafana dashboards found"
fi

echo ""
echo "=========================================="
echo "Validation Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Start the observability stack:"
echo "   docker-compose -f docker-compose.observability.yml up -d"
echo ""
echo "2. Access the services:"
echo "   - Prometheus: http://localhost:9090"
echo "   - Grafana: http://localhost:3000 (admin/admin)"
echo "   - Loki: http://localhost:3100"
echo "   - Marquez: http://localhost:3001"
echo ""
echo "3. Start the main application:"
echo "   docker-compose up -d"
echo ""
print_success "All checks passed!"
