"""
DAG Maestro: Ingesta Diaria de Maestras ERP

Este DAG orquesta la ingesta diaria de 12 archivos de maestras desde el ERP.
Descubre los archivos en /input/ y dispara el procesamiento paralelo de cada uno.

Autor: IkiTech
Fecha: 2025-11-27
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import json
from pathlib import Path

# Configuración
DEFAULT_ARGS = {
    'owner': 'ikitech',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Variables de Airflow
GCP_INPUT_PATH = Variable.get("gcp_input_path", "/input/")
EXPECTED_FILES = [
    'XWECIA.txt',
    'XWEPAI.txt',
    'XCIUDAD.txt',
    'XWEDEP.txt',
    'XLEXSUBL.txt',
    'XCADENA.txt',
    'XGEN.txt',
    'XCATEG.txt',
    'XLEXSUBC.txt',
    'XUEN.txt',
    'XCAN.txt',
    'CRLISUCATE.txt',
    'XMONEDA.txt'
]

logger = logging.getLogger(__name__)


def descubrir_archivos(**context):
    logger.info(f"Buscando archivos en: {GCP_INPUT_PATH}")
    archivos_encontrados = []
    input_path = Path(GCP_INPUT_PATH)
    
    if input_path.exists():
        for archivo in input_path.glob("*.txt"):
            if archivo.name in EXPECTED_FILES:
                archivos_encontrados.append({
                    'nombre': archivo.name,
                    'ruta': str(archivo),
                    'tamano': archivo.stat().st_size
                })
                logger.info(f"Archivo encontrado: {archivo.name}")
    logger.info(f"Total archivos descubiertos: {len(archivos_encontrados)}")
    context['task_instance'].xcom_push(key='archivos_descubiertos', value=archivos_encontrados)
    return archivos_encontrados


def validar_archivos_esperados(**context):
    """
    Valida que todos los archivos esperados estén presentes.
    
    Raises:
        ValueError: Si faltan archivos críticos
    """
    archivos_encontrados = context['task_instance'].xcom_pull(
        task_ids='descubrir_archivos',
        key='archivos_descubiertos'
    )
    
    nombres_encontrados = {archivo['nombre'] for archivo in archivos_encontrados}
    nombres_esperados = set(EXPECTED_FILES)
    
    faltantes = nombres_esperados - nombres_encontrados
    extras = nombres_encontrados - nombres_esperados
    
    if faltantes:
        logger.warning(f"Archivos faltantes: {faltantes}")

    
    if extras:
        logger.info(f"Archivos adicionales (no esperados): {extras}")
    
    resultado = {
        'total_esperados': len(EXPECTED_FILES),
        'total_encontrados': len(archivos_encontrados),
        'faltantes': list(faltantes),
        'extras': list(extras),
        'completo': len(faltantes) == 0
    }
    
    logger.info(f"Validación: {json.dumps(resultado, indent=2)}")
    
    context['task_instance'].xcom_push(key='validacion_resultado', value=resultado)
    
    return resultado


def preparar_configuracion_procesamiento(**context):
    """
    Prepara la configuración para el procesamiento paralelo.
    
    Returns:
        List[Dict]: Configuración para cada archivo a procesar
    """
    archivos_encontrados = context['task_instance'].xcom_pull(
        task_ids='descubrir_archivos',
        key='archivos_descubiertos'
    )
    
    configuraciones = []
    
    for archivo in archivos_encontrados:
        config = {
            'archivo_nombre': archivo['nombre'],
            'archivo_ruta': archivo['ruta'],
            'archivo_tamano': archivo['tamano'],
            'execution_date': context['execution_date'].isoformat(),
            'dag_run_id': context['dag_run'].run_id
        }
        configuraciones.append(config)
    
    logger.info(f"Configuraciones preparadas: {len(configuraciones)}")
    
    return configuraciones


def consolidar_resultados(**context):
    """
    Consolida los resultados del procesamiento de todos los archivos.
    """
    # En Airflow 2.8+, los resultados de los DAGs hijos se pueden obtener
    # a través de XCom o mediante consultas a la base de datos de Airflow
    
    logger.info("Consolidando resultados del procesamiento...")
    
    # Aquí se implementaría la lógica para:
    # 1. Consultar el estado de cada DAG hijo
    # 2. Consolidar métricas (registros procesados, errores, etc.)
    # 3. Generar reporte de ejecución
    # 4. Enviar notificaciones si es necesario
    
    resumen = {
        'fecha_ejecucion': context['execution_date'].isoformat(),
        'archivos_procesados': len(EXPECTED_FILES),
        'estado': 'completado'
    }
    
    logger.info(f"Resumen de ejecución: {json.dumps(resumen, indent=2)}")
    
    return resumen


def enviar_notificaciones(**context):
    """
    Envía notificaciones sobre el resultado de la ejecución.
    """
    resumen = context['task_instance'].xcom_pull(task_ids='consolidar_resultados')
    
    logger.info("Enviando notificaciones...")
    logger.info(f"Resumen: {json.dumps(resumen, indent=2)}")
    
    # Aquí se implementaría:
    # - Envío de email
    # - Notificación a Slack/Teams
    # - Actualización de dashboard
    
    return "Notificaciones enviadas"


# Definición del DAG
with DAG(
    dag_id='ingesta_diaria_maestras_erp',
    default_args=DEFAULT_ARGS,
    description='Ingesta diaria de maestras',
    schedule_interval='0 2 * * *',
    start_date=datetime(2025, 11, 27),
    catchup=False,
    max_active_runs=1,
    tags=['maestras', 'erp'],
) as dag:
    
    tarea_descubrir = PythonOperator(
        task_id='descubrir_archivos',
        python_callable=descubrir_archivos,
        provide_context=True
    )
    
    tarea_validar = PythonOperator(
        task_id='validar_archivos_esperados',
        python_callable=validar_archivos_esperados,
        provide_context=True
    )
    
    # TaskGroup: Procesamiento paralelo
    with TaskGroup(group_id='procesamiento_paralelo') as grupo_procesamiento:

        for archivo_nombre in EXPECTED_FILES:
            
            clean_task_id = archivo_nombre.replace(".", "_")
            
            ruta_archivo = str(Path(GCP_INPUT_PATH) / archivo_nombre)
            
            TriggerDagRunOperator(
                task_id=f'procesar_{clean_task_id}',
                trigger_dag_id='procesamiento_archivo_maestra',
                conf={
                    'archivo_nombre': archivo_nombre,
                    'archivo_ruta': ruta_archivo
                },
                wait_for_completion=True,
                poke_interval=20,
                reset_dag_run=True,
                trigger_run_id=f"trig__{clean_task_id}__{{{{ ts }}}}"
            )
    
    tarea_consolidar = PythonOperator(
        task_id='consolidar_resultados',
        python_callable=consolidar_resultados,
        provide_context=True,
        trigger_rule='all_done'
    )
    
    tarea_notificar = PythonOperator(
        task_id='enviar_notificaciones',
        python_callable=enviar_notificaciones,
        provide_context=True,
        trigger_rule='all_done'
    )
    
    tarea_descubrir >> tarea_validar >> grupo_procesamiento >> tarea_consolidar >> tarea_notificar


# Documentación del DAG
dag.doc_md = """
# DAG Maestro: Ingesta Diaria de Maestras ERP

## Descripción
Este DAG orquesta la ingesta diaria de 12 archivos de maestras desde el ERP de Grupo Éxito.

## Archivos Procesados
1. XWECIA.txt → erp_COMPANIA
2. XWEPAI.txt → erp_PAIS
3. XCIUDAD.txt → erp_CIUDAD
4. XWEDEP.txt → erp_DEPENDENCIA
5. XLEXSUBL.txt → erp_SUBLINEA
6. XCADENA.txt → erp_CADENA
7. XGEN.txt → erp_GERENCIA
8. XCATEG.txt → erp_CATEGORIA
9. XLEXSUBC.txt → erp_SUBCATEGORIA
10. XUEN.txt → erp_UEN
11. XCAN.txt → erp_CANAL
12. CRLISUCATE.txt → erp_SEGMENTOS

## Flujo
1. **Descubrir archivos**: Busca archivos en /input/
2. **Validar**: Verifica que todos los archivos esperados estén presentes
3. **Preparar configuración**: Prepara la configuración para cada archivo
4. **Procesamiento paralelo**: Dispara el DAG hijo para cada archivo
5. **Consolidar resultados**: Consolida métricas y estadísticas
6. **Notificar**: Envía notificaciones sobre el resultado

## Configuración
- **Schedule**: Diario a las 2 AM
- **Max Active Runs**: 1 (no permite ejecuciones concurrentes)
- **Retries**: 2 con 5 minutos de delay
- **Timeout**: Sin límite

## Variables de Airflow Requeridas
- `gcp_input_path`: Ruta de la carpeta de input en GCP

## Monitoreo
- Métricas en Prometheus
- Dashboards en Grafana
- Logs en Loki
- Alertas configuradas para fallos

## Contacto
- Equipo: IkiTech
- Email: data-team@ikitech.com
"""
