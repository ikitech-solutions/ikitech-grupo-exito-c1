# Observability Stack

Este directorio contiene la configuración para el stack de observabilidad optimizado para Windows, que incluye:

- **Prometheus:** Para recolección de métricas.
- **Grafana:** Para visualización y dashboards.
- **Loki:** Para agregación de logs.
- **StatsD Exporter:** Para convertir métricas de Airflow (StatsD) a formato Prometheus.
- **Postgres Exporter:** Para exportar métricas de PostgreSQL a Prometheus.

## Uso

Para levantar el stack de observabilidad en Windows, utiliza el script de PowerShell en la raíz del proyecto:

```powershell
.\start-observability-windows.ps1
```

Este script se encarga de:
1. Verificar que Docker esté en ejecución
2. Verificar que la red `mdh_network` exista
3. Iniciar los servicios de observabilidad
4. Validar que todos los servicios estén funcionando correctamente
5. Mostrar las URLs de acceso

### Inicio manual

Si prefieres iniciar los servicios manualmente:

```powershell
docker compose -f docker-compose.observability-windows.yml up -d
```

## Estructura

```
observability/
├── prometheus/
│   ├── prometheus-windows.yml      # Configuración de Prometheus para Windows
│   ├── statsd-mapping.yml          # Mapeo de métricas StatsD a Prometheus
│   └── alerts/                     # Reglas de alerta (futuro)
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/            # Configuración de datasources (Prometheus, Loki)
│   │   └── dashboards/             # Configuración de provisioning de dashboards
│   └── dashboards/
│       ├── airflow-overview.json   # Dashboard de Airflow
│       └── system-overview.json    # Dashboard de sistema (PostgreSQL)
└── loki/
    └── loki-config.yml             # Configuración de Loki
```

## Servicios

| Servicio | Puerto | Descripción | URL |
|----------|--------|-------------|-----|
| Prometheus | 9090 | Recolección de métricas | http://localhost:9090 |
| Grafana | 3000 | Visualización de dashboards | http://localhost:3000 |
| Loki | 3100 | Agregación de logs | http://localhost:3100 |
| StatsD Exporter | 9102, 9125/udp | Exportador de métricas de Airflow | http://localhost:9102/metrics |
| Postgres Exporter | 9187 | Exportador de métricas de PostgreSQL | http://localhost:9187/metrics |

## Acceso a Grafana

- **URL:** http://localhost:3000
- **Usuario:** `admin`
- **Contraseña:** `admin`

### Dashboards disponibles:

1. **Airflow Overview:** Métricas de DAGs, tareas, scheduler y executor
2. **System Overview:** Métricas de PostgreSQL, conexiones y rendimiento

## Verificación de Métricas

Para verificar que Prometheus está recolectando métricas correctamente:

1. Accede a http://localhost:9090
2. Ve a `Status` → `Targets`
3. Verifica que todos los targets estén en estado `UP`

## Troubleshooting

### Los dashboards no aparecen en Grafana

1. Verifica que los volúmenes estén montados correctamente
2. Reinicia Grafana: `docker compose -f docker-compose.observability-windows.yml restart grafana`
3. Revisa los logs: `docker compose -f docker-compose.observability-windows.yml logs grafana`

### StatsD Exporter en restart loop

1. Verifica la sintaxis del archivo `prometheus/statsd-mapping.yml`
2. Revisa los logs: `docker compose -f docker-compose.observability-windows.yml logs statsd-exporter`

### No aparecen métricas de Airflow

1. Verifica que Airflow esté configurado con las variables de entorno correctas:
   - `AIRFLOW__METRICS__STATSD_ON=True`
   - `AIRFLOW__METRICS__STATSD_HOST=statsd-exporter`
   - `AIRFLOW__METRICS__STATSD_PORT=9125`
2. Ejecuta un DAG en Airflow para generar métricas
3. Verifica que el target `statsd-exporter` esté `UP` en Prometheus

## Notas sobre compatibilidad con Windows

Esta configuración está optimizada para Windows y **no incluye** los siguientes componentes que son comunes en implementaciones Linux:

- **Node Exporter:** No compatible con Windows
- **cAdvisor:** Problemas de compatibilidad con Docker Desktop en Windows
- **Promtail:** Dificultades para acceder a logs del sistema en Windows

## Contribuciones

Cualquier cambio en la configuración de observabilidad debe ser probado localmente antes de hacer commit.
