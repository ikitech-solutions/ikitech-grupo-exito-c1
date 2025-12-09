# Observability Stack

Este directorio contiene la configuración para el stack de observabilidad, que incluye:

- **Prometheus:** Para recolección de métricas.
- **Grafana:** Para visualización y dashboards.
- **Loki:** Para agregación de logs.

## Uso

Para levantar el stack de observabilidad, utiliza los scripts de inicio en la raíz del proyecto:

- **Windows:** `start-observability-windows.ps1`
- **Linux/Mac:** `start-observability.sh`

Estos scripts se encargan de iniciar los servicios en el orden correcto y validar que todo funcione.

## Estructura

- `prometheus/`: Configuración de Prometheus, reglas de alerta y exportadores.
- `grafana/`: Configuración de provisioning para datasources y dashboards.
- `loki/`: Configuración de Loki.

## Servicios

| Servicio | Puerto | Descripción |
|----------|--------|-------------|
| Prometheus | 9090 | Recolección de métricas |
| Grafana | 3000 | Visualización de dashboards |
| Loki | 3100 | Agregación de logs |

## Contribuciones

Cualquier cambio en la configuración de observabilidad debe ser probado localmente antes de hacer commit.
