# Configuración de la Solución

## Variables de Airflow

Las siguientes variables deben configurarse en la interfaz de Airflow (`Admin` → `Variables`):

| Variable | Descripción | Ejemplo | Requerida |
|----------|-------------|---------|-----------|
| `gcp_input_path` | Ruta de la carpeta de input | `/input/` | ✅ |
| `gcp_processed_path` | Ruta de la carpeta de procesados | `/processed/` | ✅ |
| `gcp_error_path` | Ruta de la carpeta de errores | `/error/` | ✅ |
| `kafka_bootstrap_servers` | Servidores de Kafka | `kafka:29092` | ✅ |
| `kafka_topic_prefix` | Prefijo para los topics de Kafka | `maestras.` | ✅ |
| `postgres_conn_id` | ID de la conexión a PostgreSQL | `postgres_masterdata` | ✅ |
| `maestras_layouts_toon` | Configuración de layouts en formato TOON | (ver sección Layouts) | ✅ |

## Configuración de Layouts (Formato TOON)

El archivo `dags/masterdata_ikitech/config/layouts.toon` contiene la configuración de parseo para cada archivo de maestras.

### Estructura del Archivo TOON

```toon
<NOMBRE_ARCHIVO>:
  tabla_destino: <esquema>.<tabla>
  encoding: <encoding>
  tipo: <fixed|delimited>
  campos[N]{nombre,start,end,required}:
    <CAMPO1>,<start>,<end>,<required>
    <CAMPO2>,<start>,<end>,<required>
    ...
  campos_hardcoded:
    <CAMPO>: "<valor>"
  kafka_topic: <topic>
```

### Ejemplo Completo

```toon
XWECIA.txt:
  tabla_destino: db_masterdata_hub.erp_COMPANIA
  encoding: utf-8
  tipo: fixed
  campos[2]{nombre,start,end,required}:
    CODCIA,0,2,true
    NOMBRE,2,32,true
  kafka_topic: masterdata.erp.compania

XWEPAI.txt:
  tabla_destino: db_masterdata_hub.erp_PAIS
  encoding: utf-8
  tipo: fixed
  campos[3]{nombre,start,end,required}:
    COD_PAIS,0,4,true
    NOMBRE,4,34,true
    CONTINENTE,34,36,false
  campos_hardcoded:
    ACTIVO: "S"
  kafka_topic: masterdata.erp.pais
```

### Campos del Layout

#### `tabla_destino`
Tabla de destino en PostgreSQL donde se almacenarán los datos procesados.

**Formato:** `esquema.tabla`

**Ejemplo:** `db_masterdata_hub.erp_COMPANIA`

#### `encoding`
Codificación del archivo de entrada.

**Valores comunes:** `utf-8`, `latin-1`, `cp1252`

#### `tipo`
Tipo de archivo a procesar.

**Valores:**
- `fixed`: Archivo de ancho fijo (fixed-width)
- `delimited`: Archivo delimitado (CSV, TSV, etc.)

#### `campos`
Lista de campos del archivo con sus posiciones y propiedades.

**Formato:** `campos[N]{nombre,start,end,required}:`

**Parámetros:**
- `N`: Número total de campos
- `nombre`: Nombre del campo
- `start`: Posición inicial (0-indexed)
- `end`: Posición final (exclusiva)
- `required`: `true` si el campo es obligatorio, `false` si es opcional

#### `campos_hardcoded`
Campos que se agregan con valores fijos a todos los registros.

**Uso:** Para agregar valores por defecto o constantes.

**Ejemplo:**
```toon
campos_hardcoded:
  CODCIA: "01"
  ACTIVO: "S"
  FECHA_CARGA: "CURRENT_TIMESTAMP"
```

#### `kafka_topic`
Nombre del topic de Kafka donde se publicarán los eventos.

**Convención:** `<prefijo>.<dominio>.<entidad>`

**Ejemplo:** `masterdata.erp.compania`

## Configuración de Mapeo Archivo-Tabla-Topic

El archivo `dags/masterdata_ikitech/config/file_table_mapping.json` contiene el mapeo entre archivos, tablas y topics.

### Estructura del Archivo

```json
{
  "XWECIA.txt": {
    "tabla": "erp_COMPANIA",
    "topic": "masterdata.erp.compania",
    "primary_key": ["CODCIA"]
  },
  "XWEPAI.txt": {
    "tabla": "erp_PAIS",
    "topic": "masterdata.erp.pais",
    "primary_key": ["COD_PAIS"]
  }
}
```

### Campos del Mapeo

#### `tabla`
Nombre de la tabla destino (sin esquema).

#### `topic`
Nombre del topic de Kafka.

#### `primary_key`
Lista de campos que conforman la llave primaria.

**Uso:** Para identificar registros únicos y realizar upserts.

## Conexiones de Airflow

### PostgreSQL Master Data Hub

**Connection ID:** `postgres_masterdata`

**Configuración:**
- **Host:** `postgres`
- **Schema:** `airflow`
- **Login:** `airflow`
- **Password:** `airflow`
- **Port:** `5432`

### Oracle Database

**Connection ID:** `oracle_masterdata`

**Configuración:**
- **Host:** `oracle`
- **Schema:** `FREE`
- **Login:** `system`
- **Password:** `oracle_password`
- **Port:** `1521`

## Variables de Entorno (Docker Compose)

### Airflow

```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__METRICS__STATSD_ON: "True"
AIRFLOW__METRICS__STATSD_HOST: statsd-exporter
AIRFLOW__METRICS__STATSD_PORT: 9125
AIRFLOW__METRICS__STATSD_PREFIX: airflow
```

### Kafka

```yaml
KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

### PostgreSQL

```yaml
POSTGRES_USER: airflow
POSTGRES_PASSWORD: airflow
POSTGRES_DB: airflow
```

## Configuración de Observabilidad

### Prometheus

**Archivo:** `observability/prometheus/prometheus-windows.yml`

**Targets configurados:**
- `statsd-exporter:9102` - Métricas de Airflow
- `postgres-exporter:9187` - Métricas de PostgreSQL
- `prometheus:9090` - Métricas de Prometheus

### Grafana

**Archivo:** `observability/grafana/provisioning/datasources/datasources.yml`

**Datasources:**
- Prometheus (http://prometheus:9090)
- Loki (http://loki:3100)

**Dashboards:**
- Airflow Overview: `observability/grafana/dashboards/airflow-overview.json`
- System Overview: `observability/grafana/dashboards/system-overview.json`

### StatsD Exporter

**Archivo:** `observability/prometheus/statsd-mapping.yml`

**Métricas mapeadas:**
- `airflow.dag.*.*.duration` → `airflow_dag_run_duration`
- `airflow.task.*.*.*.duration` → `airflow_task_duration`
- `airflow.scheduler.heartbeat` → `airflow_scheduler_heartbeat`
- Y más...

## Configuración de Validadores

Los validadores se configuran en el código Python en `dags/masterdata_ikitech/validators/validators.py`.

### Tipos de Validación

#### Schema Validation
Valida tipos de datos, campos requeridos y longitudes.

```python
validator.validate_schema(record, schema)
```

#### Business Rules
Aplica reglas de negocio específicas por maestra.

```python
validator.validate_business_rules(record, table_name)
```

#### Data Quality
Verifica completitud, unicidad e integridad referencial.

```python
validator.validate_data_quality(records)
```

## Configuración de Logs

### Airflow Logs

**Ubicación:** `./logs/`

**Estructura:**
```
logs/
├── dag_id=<dag_id>/
│   └── run_id=<run_id>/
│       └── task_id=<task_id>/
│           └── attempt=<attempt>.log
```

### Loki

**Archivo:** `observability/loki/loki-config.yml`

**Configuración de retención:**
- Retention period: 30 días
- Compaction: Habilitado

## Troubleshooting de Configuración

### Error: Variable 'maestras_layouts_toon' no está definida

**Solución:**
1. Ir a Airflow UI → Admin → Variables
2. Crear variable `maestras_layouts_toon`
3. Copiar contenido de `dags/masterdata_ikitech/config/layouts.toon`
4. Guardar

### Error: Connection 'postgres_masterdata' doesn't exist

**Solución:**
1. Ir a Airflow UI → Admin → Connections
2. Crear conexión con ID `postgres_masterdata`
3. Configurar parámetros según sección "Conexiones de Airflow"

### Error: Kafka producer cannot connect

**Solución:**
1. Verificar que Kafka esté corriendo: `docker compose ps kafka`
2. Verificar variable `kafka_bootstrap_servers` en Airflow
3. Probar conectividad: `docker compose exec airflow-webserver nc -zv kafka 29092`

### Error: StatsD Exporter en restart loop

**Solución:**
1. Verificar sintaxis de `observability/prometheus/statsd-mapping.yml`
2. Ver logs: `docker compose -f docker-compose.observability-windows.yml logs statsd-exporter`
3. Verificar que no haya patrones inválidos (usar `.` en lugar de `-`)

## Mejores Prácticas

### Configuración de Variables

1. **Usar Variables de Airflow** para configuración dinámica
2. **No hardcodear valores** en el código
3. **Documentar todas las variables** requeridas
4. **Usar valores por defecto** cuando sea apropiado

### Configuración de Layouts

1. **Validar sintaxis TOON** antes de desplegar
2. **Mantener layouts.json como backup** (legacy)
3. **Versionar cambios** en layouts
4. **Probar con archivos de ejemplo** antes de producción

### Configuración de Seguridad

1. **Cambiar contraseñas por defecto** en producción
2. **Usar secretos de Airflow** para credenciales sensibles
3. **Habilitar autenticación** en Kafka y PostgreSQL
4. **Configurar SSL/TLS** para comunicaciones

### Configuración de Monitoreo

1. **Configurar alertas** en Prometheus
2. **Definir SLOs** para métricas críticas
3. **Revisar dashboards** regularmente
4. **Ajustar retención** de métricas según necesidades
