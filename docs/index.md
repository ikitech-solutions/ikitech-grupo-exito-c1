# Documentación Técnica: Ingesta de Maestras ERP

## 1. Introducción

Esta documentación describe la arquitectura, diseño, y operación de la solución de ingesta de maestras ERP para Grupo Éxito.

## 2. Arquitectura

Ver la sección de arquitectura en el [README principal](../README.md).

## 3. Componentes

### 3.1. DAG Maestro

- **ID:** `ingesta_diaria_maestras_erp`
- **Schedule:** Diario a las 2 AM
- **Responsabilidades:**
  - Descubrir archivos en `/input/`
  - Disparar DAGs hijos en paralelo
  - Consolidar resultados
  - Enviar notificaciones

### 3.2. DAG Hijo

- **ID:** `procesamiento_archivo_maestra`
- **Schedule:** Disparado por el DAG maestro
- **Responsabilidades:**
  - Parsear archivo fixed-width
  - Validar registros
  - Publicar en Kafka
  - Mover archivo
  - Registrar en audit_log

### 3.3. Parser de Fixed-Width

- **Módulo:** `parsers.fixed_width_parser`
- **Clase:** `FixedWidthParser`
- **Configuración:** `config/layouts.json`

### 3.4. Validadores

- **Módulo:** `validators.validators`
- **Clase:** `DataValidator`
- **Reglas:** Definidas por archivo en el módulo

## 4. Operación

### 4.1. Ejecución Diaria

El proceso se ejecuta automáticamente a las 2 AM. No se requiere intervención manual.

### 4.2. Monitoreo

- **Dashboards de Grafana:**
  - Ingesta Diaria
  - Calidad de Datos
  - Performance
- **Logs en Loki:** Logs detallados de cada ejecución
- **Alertas en Prometheus:** Notificaciones para fallos críticos

### 4.3. Manejo de Errores

- **Archivos inválidos:** Movidos a `/error/`
- **Registros inválidos:** Registrados en tabla de errores
- **Alertas:** Enviadas para tasas de error > 5%

## 5. Despliegue

El despliegue se realiza automáticamente a través del pipeline de CI/CD en Azure DevOps.

## 6. Contacto

- **Equipo:** IkiTech
- **Email:** data-team@ikitech.com
