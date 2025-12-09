# Observability Stack

This directory contains the complete observability stack for the Grupo Éxito data ingestion project.

## Quick Start

### 1. Start the Observability Stack

```bash
docker-compose -f docker-compose.observability.yml up -d
```

### 2. Start the Main Application

```bash
docker-compose up -d
```

### 3. Access the Services

-   **Prometheus:** http://localhost:9090
-   **Grafana:** http://localhost:3000 (admin/admin)
-   **Loki:** http://localhost:3100
-   **Marquez Web UI:** http://localhost:3001

## Directory Structure

```
observability/
├── prometheus/
│   ├── prometheus.yml              # Prometheus configuration
│   ├── alerts/                     # Alert rules
│   │   ├── airflow_alerts.yml
│   │   └── kafka_alerts.yml
│   ├── statsd-mapping.yml          # StatsD to Prometheus mapping
│   └── jmx_exporter/               # Kafka JMX exporter
│       ├── jmx_prometheus_javaagent.jar
│       └── kafka-broker.yml
├── grafana/
│   ├── dashboards/                 # Grafana dashboards (JSON)
│   │   └── airflow-overview.json
│   └── provisioning/               # Auto-provisioning configs
│       ├── datasources/
│       │   └── datasources.yml
│       └── dashboards/
│           └── dashboards.yml
├── loki/
│   ├── loki-config.yml             # Loki configuration
│   └── promtail-config.yml         # Promtail configuration
├── marquez/
│   ├── marquez.yml                 # Marquez configuration
│   └── init-marquez-db.sql         # Database initialization
└── test-observability.sh           # Validation script
```

## Components

### Prometheus
-   **Purpose:** Metrics collection and storage
-   **Port:** 9090
-   **Scrape Interval:** 15s
-   **Retention:** 30 days

### Grafana
-   **Purpose:** Visualization and dashboards
-   **Port:** 3000
-   **Default Credentials:** admin/admin
-   **Datasources:** Prometheus, Loki, PostgreSQL

### Loki
-   **Purpose:** Log aggregation
-   **Port:** 3100
-   **Storage:** Filesystem (local)

### Promtail
-   **Purpose:** Log collector for Loki
-   **Monitored Paths:**
    -   Airflow logs: `/opt/airflow/logs/**/*.log`
    -   Docker container logs

### Marquez
-   **Purpose:** Data lineage and metadata management
-   **API Port:** 5000
-   **Admin Port:** 5001
-   **Web UI Port:** 3001

### Exporters
-   **StatsD Exporter:** Converts Airflow StatsD metrics to Prometheus format (port 9102)
-   **Postgres Exporter:** Exports PostgreSQL metrics (port 9187)
-   **Node Exporter:** Exports system metrics (port 9100)
-   **cAdvisor:** Exports container metrics (port 8081)

## Validation

Run the validation script to check the configuration:

```bash
./observability/test-observability.sh
```

This script will:
-   Validate all YAML configuration files
-   Check that all required files and directories exist
-   Verify the JMX exporter is downloaded
-   List available Grafana dashboards

## Metrics Collected

### Airflow Metrics
-   DAG run status (success, failed, running)
-   Task execution duration
-   Scheduler heartbeat
-   Executor queue size
-   Pool utilization

### Kafka Metrics
-   Broker status
-   Under-replicated partitions
-   Consumer lag
-   Request rates
-   Disk usage

### PostgreSQL Metrics
-   Connection count
-   Query performance
-   Database size
-   Transaction rates

### System Metrics
-   CPU usage
-   Memory usage
-   Disk I/O
-   Network I/O
-   Container resource usage

## Alerts

Alert rules are defined in `prometheus/alerts/`:

### Airflow Alerts
-   DAG failures
-   Scheduler downtime
-   High task queue

### Kafka Alerts
-   Broker downtime
-   Under-replicated partitions
-   High consumer lag

## Data Lineage with Marquez

Marquez provides end-to-end data lineage tracking for the entire pipeline.

### Features:
-   **Visual Lineage Graph:** See how data flows through the system
-   **Impact Analysis:** Understand what will be affected by changes
-   **Data Governance:** Track data ownership and classification
-   **Debugging:** Trace the origin of data issues

### Configuration:

Airflow is configured to send lineage metadata to Marquez via OpenLineage:

```
AIRFLOW__LINEAGE__BACKEND=openlineage.lineage_backend.OpenLineageBackend
OPENLINEAGE_URL=http://marquez:5000
OPENLINEAGE_NAMESPACE=grupo_exito_c1
```

## Troubleshooting

### Prometheus not scraping targets

Check the Prometheus targets page: http://localhost:9090/targets

If a target is down, verify:
-   The service is running
-   The port is correct in `prometheus.yml`
-   Network connectivity between containers

### Grafana dashboards not showing data

-   Verify the datasource is configured correctly in Grafana
-   Check that Prometheus is collecting metrics
-   Ensure the time range in Grafana matches the data availability

### Loki not receiving logs

-   Check that Promtail is running
-   Verify the log paths in `promtail-config.yml`
-   Check Promtail logs for errors

### Marquez not showing lineage

-   Verify OpenLineage is configured in Airflow
-   Check that the Marquez database is initialized
-   Run a DAG and check the Marquez API: http://localhost:5000/api/v1/namespaces

## Additional Resources

-   [Prometheus Documentation](https://prometheus.io/docs/)
-   [Grafana Documentation](https://grafana.com/docs/)
-   [Loki Documentation](https://grafana.com/docs/loki/latest/)
-   [Marquez Documentation](https://marquezproject.ai/docs/)
-   [OpenLineage Documentation](https://openlineage.io/docs/)

## Support

For issues or questions, contact the IkiTech Solutions team.
