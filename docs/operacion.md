# Operación de la Solución

## Ejecución Diaria

### Ejecución Automática

El DAG `ingesta_diaria_maestras_erp` se ejecuta automáticamente todos los días a las **2:00 AM** según el schedule configurado.

**No se requiere intervención manual** para la ejecución diaria.

### Ejecución Manual

Para ejecutar el DAG manualmente:

1. Acceder a Airflow UI: http://localhost:8080
2. Buscar el DAG `ingesta_diaria_maestras_erp`
3. Hacer clic en el botón "Trigger DAG" (▶️)
4. Opcionalmente, configurar parámetros de ejecución
5. Confirmar la ejecución

### Monitoreo de Ejecución

#### En Airflow UI

**Vista de Graph:**
- Visualiza el flujo completo del DAG
- Muestra el estado de cada tarea (success, failed, running)
- Permite navegar a logs de tareas específicas

**Vista de Gantt:**
- Muestra la duración de cada tarea
- Identifica cuellos de botella
- Útil para optimización de performance

**Vista de Logs:**
- Logs detallados de cada tarea
- Mensajes de error y warnings
- Información de debug

#### En Grafana

**Dashboard "Airflow Overview":**
- Número de DAG runs por estado
- Duración de ejecuciones
- Estado del scheduler
- Métricas del executor
- Pool slots disponibles

**Dashboard "System Overview":**
- Conexiones a PostgreSQL
- Tamaño de base de datos
- Performance de queries
- Tasa de transacciones

**Acceso:** http://localhost:3000
**Credenciales:** admin / admin

## Manejo de Errores

### Archivos Inválidos

**Síntoma:** Archivo no cumple con el formato esperado

**Acción Automática:**
1. Archivo movido a `/error/`
2. Entrada creada en tabla `error_log`
3. Task marcada como `failed`

**Acción Manual:**
1. Revisar archivo en `/error/`
2. Identificar problema (formato, encoding, etc.)
3. Corregir archivo
4. Mover a `/input/` para reprocesar
5. Ejecutar DAG manualmente

### Registros Inválidos

**Síntoma:** Algunos registros del archivo fallan validación

**Acción Automática:**
1. Registros válidos continúan procesamiento
2. Registros inválidos guardados en `error_log`
3. Si tasa de error > 5%, se genera alerta

**Acción Manual:**
1. Consultar tabla `error_log`:
   ```sql
   SELECT * FROM error_log 
   WHERE fecha_proceso = CURRENT_DATE 
   ORDER BY timestamp DESC;
   ```
2. Analizar errores comunes
3. Corregir datos en origen si es necesario
4. Notificar al equipo responsable del ERP

### Fallos de Kafka

**Síntoma:** No se pueden publicar eventos en Kafka

**Acción Automática:**
1. Reintentos automáticos (3 intentos)
2. Datos guardados en PostgreSQL como fallback
3. Task marcada como `failed` si todos los reintentos fallan

**Acción Manual:**
1. Verificar estado de Kafka:
   ```bash
   docker compose ps kafka
   docker compose logs kafka --tail=100
   ```
2. Reiniciar Kafka si es necesario:
   ```bash
   docker compose restart kafka
   ```
3. Re-ejecutar DAG fallido
4. Verificar que eventos se publiquen correctamente

### Fallos de PostgreSQL

**Síntoma:** No se puede conectar a la base de datos

**Acción Automática:**
1. Reintentos con backoff exponencial (3 intentos)
2. Task marcada como `failed` si todos los reintentos fallan
3. Alerta enviada al equipo

**Acción Manual:**
1. Verificar estado de PostgreSQL:
   ```bash
   docker compose ps postgres
   docker compose logs postgres --tail=100
   ```
2. Verificar conexiones:
   ```sql
   SELECT count(*) FROM pg_stat_activity;
   ```
3. Reiniciar PostgreSQL si es necesario:
   ```bash
   docker compose restart postgres
   ```
4. Re-ejecutar DAG fallido

### Fallos de Oracle

**Síntoma:** No se puede cargar datos en Oracle

**Acción Automática:**
1. Reintentos automáticos (3 intentos)
2. Task marcada como `failed` si todos los reintentos fallan

**Acción Manual:**
1. Verificar estado de Oracle:
   ```bash
   docker compose ps oracle
   docker compose logs oracle --tail=100
   ```
2. Verificar conexión desde Airflow:
   ```bash
   docker compose exec airflow-webserver airflow connections test oracle_masterdata
   ```
3. Reiniciar Oracle si es necesario:
   ```bash
   docker compose restart oracle
   ```
4. Re-ejecutar DAG fallido

## Monitoreo Continuo

### Métricas Clave

#### Métricas de Airflow

**DAG Run Duration:**
- **Objetivo:** < 30 minutos
- **Alerta:** > 45 minutos

**Task Success Rate:**
- **Objetivo:** > 95%
- **Alerta:** < 90%

**Scheduler Heartbeat:**
- **Objetivo:** Activo (< 10 segundos desde último heartbeat)
- **Alerta:** > 30 segundos

#### Métricas de PostgreSQL

**Conexiones Activas:**
- **Objetivo:** < 80% del máximo
- **Alerta:** > 90% del máximo

**Query Duration:**
- **Objetivo:** < 1 segundo (promedio)
- **Alerta:** > 5 segundos

**Database Size:**
- **Objetivo:** Crecimiento lineal esperado
- **Alerta:** Crecimiento anómalo (> 20% en un día)

#### Métricas de Kafka

**Consumer Lag:**
- **Objetivo:** < 1000 mensajes
- **Alerta:** > 5000 mensajes

**Broker Status:**
- **Objetivo:** Todos los brokers UP
- **Alerta:** Cualquier broker DOWN

### Alertas Configuradas

#### Alertas de Airflow

**AirflowDAGFailed:**
- **Condición:** Un DAG ha fallado
- **Severidad:** Critical
- **Acción:** Revisar logs y re-ejecutar

**AirflowSchedulerDown:**
- **Condición:** Scheduler no responde
- **Severidad:** Critical
- **Acción:** Reiniciar scheduler inmediatamente

**AirflowTaskQueueHigh:**
- **Condición:** Cola de tareas > 50
- **Severidad:** Warning
- **Acción:** Revisar recursos del executor

#### Alertas de PostgreSQL

**PostgreSQLConnectionsHigh:**
- **Condición:** Conexiones activas > 90%
- **Severidad:** Warning
- **Acción:** Revisar queries lentas

**PostgreSQLDiskSpaceLow:**
- **Condición:** Espacio en disco < 20%
- **Severidad:** Critical
- **Acción:** Limpiar datos antiguos o expandir disco

### Verificación de Salud del Sistema

**Script de verificación (PowerShell):**

```powershell
# Verificar estado de todos los servicios
docker compose ps

# Verificar logs recientes
docker compose logs --tail=50

# Verificar Prometheus targets
curl http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health}'

# Verificar DAGs en Airflow
curl -u admin:admin http://localhost:8080/api/v1/dags | jq '.dags[] | {dag_id: .dag_id, is_paused: .is_paused}'
```

## Mantenimiento

### Mantenimiento Diario

**Tareas automáticas:**
- ✅ Limpieza de logs antiguos (> 30 días)
- ✅ Compactación de topics de Kafka
- ✅ Vacuum de PostgreSQL (automático)

**Tareas manuales:**
- Revisar dashboard de Grafana
- Verificar alertas en Prometheus
- Revisar tabla `error_log` para errores recurrentes

### Mantenimiento Semanal

1. **Revisar métricas de performance:**
   - Duración promedio de DAG runs
   - Tasa de éxito de tareas
   - Uso de recursos (CPU, memoria, disco)

2. **Analizar logs de errores:**
   ```sql
   SELECT error_type, COUNT(*) as count
   FROM error_log
   WHERE fecha_proceso >= CURRENT_DATE - INTERVAL '7 days'
   GROUP BY error_type
   ORDER BY count DESC;
   ```

3. **Verificar espacio en disco:**
   ```bash
   docker compose exec postgres df -h
   docker compose exec airflow-webserver df -h
   ```

4. **Revisar topics de Kafka:**
   ```bash
   docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

### Mantenimiento Mensual

1. **Backup de PostgreSQL:**
   ```bash
   docker compose exec postgres pg_dump -U airflow airflow > backup_$(date +%Y%m%d).sql
   ```

2. **Limpieza de archivos procesados:**
   - Mover archivos de `/processed/` a almacenamiento de largo plazo
   - Comprimir archivos antiguos

3. **Actualización de dependencias:**
   - Revisar nuevas versiones de Airflow
   - Actualizar providers de Airflow
   - Actualizar librerías Python

4. **Revisión de configuración:**
   - Verificar que variables de Airflow estén actualizadas
   - Revisar layouts TOON para nuevas maestras
   - Actualizar documentación si hay cambios

### Mantenimiento Trimestral

1. **Optimización de base de datos:**
   ```sql
   VACUUM FULL;
   REINDEX DATABASE airflow;
   ```

2. **Revisión de seguridad:**
   - Rotar contraseñas
   - Revisar permisos de usuarios
   - Actualizar certificados SSL (si aplica)

3. **Pruebas de disaster recovery:**
   - Restaurar backup de PostgreSQL
   - Verificar replay de eventos en Kafka
   - Probar failover de servicios

## Procedimientos de Emergencia

### Scheduler de Airflow Caído

**Síntomas:**
- DAGs no se ejecutan
- UI de Airflow muestra "Scheduler is not running"

**Procedimiento:**
1. Verificar estado:
   ```bash
   docker compose ps airflow-scheduler
   ```
2. Ver logs:
   ```bash
   docker compose logs airflow-scheduler --tail=100
   ```
3. Reiniciar scheduler:
   ```bash
   docker compose restart airflow-scheduler
   ```
4. Verificar que vuelva a funcionar en UI

### Base de Datos Corrupta

**Síntomas:**
- Errores de integridad en logs
- Queries fallan con errores de datos

**Procedimiento:**
1. Detener todos los servicios:
   ```bash
   docker compose down
   ```
2. Restaurar último backup:
   ```bash
   docker compose up -d postgres
   docker compose exec -T postgres psql -U airflow airflow < backup_YYYYMMDD.sql
   ```
3. Reiniciar servicios:
   ```bash
   docker compose up -d
   ```
4. Verificar integridad de datos

### Kafka Lleno (Disco)

**Síntomas:**
- Errores al publicar mensajes
- Logs muestran "No space left on device"

**Procedimiento:**
1. Verificar espacio:
   ```bash
   docker compose exec kafka df -h
   ```
2. Limpiar logs antiguos:
   ```bash
   docker compose exec kafka kafka-configs --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name <topic> --add-config retention.ms=86400000
   ```
3. Reiniciar Kafka:
   ```bash
   docker compose restart kafka
   ```

### Pérdida de Datos

**Síntomas:**
- Archivos desaparecidos
- Datos faltantes en tablas

**Procedimiento:**
1. Verificar tabla `audit_log` para rastrear procesamiento
2. Buscar archivos en `/processed/` o `/error/`
3. Restaurar desde backup si es necesario
4. Re-ejecutar DAGs para fechas afectadas
5. Documentar incidente y causa raíz

## Contactos de Soporte

**Equipo IkiTech:**
- Manuela Larrea: manuela.larrea@ikitech.com.co
- Jeferson Mesa: jeferson.mesa@ikitech.com.co

**Escalamiento:**
- Nivel 1: Operador de turno
- Nivel 2: Equipo de desarrollo IkiTech
- Nivel 3: Arquitecto de soluciones

**Horarios de soporte:**
- Lunes a Viernes: 8:00 AM - 6:00 PM
- Emergencias: 24/7 (vía teléfono)
