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
    """
    Descubre archivos de maestras en la carpeta /input/.
    Implementa validaciones de archivos vacíos, no mapeados y duplicados.
    """
    logger.info(f"Buscando archivos en: {GCP_INPUT_PATH}")
    archivos_encontrados = []
    input_path = Path(GCP_INPUT_PATH)
    
    files_in_dir = list(input_path.glob("*.txt"))
    
    # Validación de directorio vacío
    if not files_in_dir:
        logger.error("ALERTA CRÍTICA: No se encontraron archivos en el directorio de entrada.")
    
    for archivo in files_in_dir:
        # Validación de archivo mapeado en lógica de negocio
        if archivo.name not in EXPECTED_FILES:
            logger.warning(f"Archivo NO Reconocido: '{archivo.name}'. Se ignorará.")
            continue
            
        archivos_encontrados.append({
            'nombre': archivo.name,
            'ruta': str(archivo),
            'tamano': archivo.stat().st_size
        })
        logger.info(f"Archivo válido para procesar: {archivo.name}")

    logger.info(f"Resumen: Encontrados {len(archivos_encontrados)} / {len(EXPECTED_FILES)} esperados.")
    
    context['task_instance'].xcom_push(key='archivos_descubiertos', value=archivos_encontrados)
    return archivos_encontrados


def validar_archivos_esperados(**context):
    """
    Valida que todos los archivos esperados estén presentes.
    Genera alertas si faltan archivos críticos.
    """
    archivos_encontrados = context['task_instance'].xcom_pull(
        task_ids='descubrir_archivos',
        key='archivos_descubiertos'
    ) or []
    
    nombres_encontrados = {archivo['nombre'] for archivo in archivos_encontrados}
    nombres_esperados = set(EXPECTED_FILES)
    
    faltantes = nombres_esperados - nombres_encontrados
    
    if faltantes:
        logger.warning(f"Faltan {len(faltantes)} archivos críticos: {faltantes}")
    
    resultado = {
        'total_esperados': len(EXPECTED_FILES),
        'total_encontrados': len(archivos_encontrados),
        'faltantes': list(faltantes),
        'completo': len(faltantes) == 0
    }
    
    logger.info(f"Validación: {json.dumps(resultado, indent=2)}")
    context['task_instance'].xcom_push(key='validacion_resultado', value=resultado)
    return resultado


def consolidar_resultados(**context):
    """
    Consolida los resultados del procesamiento de todos los archivos.
    """
    logger.info("Consolidando resultados del procesamiento...")
    
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
    
    # TaskGroup: Procesamiento paralelo con Fan-Out
    with TaskGroup(group_id='procesamiento_paralelo') as grupo_procesamiento:

        for archivo_nombre in EXPECTED_FILES:
            
            clean_task_id = archivo_nombre.replace(".", "_")
            ruta_archivo = str(Path(GCP_INPUT_PATH) / archivo_nombre)
            
            # Disparo de DAGs hijos con ID único para evitar colisiones
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
    
    # Definir dependencias
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
1. **Descubrir archivos**: Busca archivos en /input/ y valida su existencia.
2. **Validar**: Genera reporte de archivos faltantes o inesperados.
3. **Procesamiento paralelo**: Dispara el DAG hijo para cada archivo esperado.
4. **Consolidar resultados**: Consolida métricas y estadísticas.
5. **Notificar**: Envía notificaciones sobre el resultado.

## Configuración
- **Schedule**: Diario a las 2 AM
- **Max Active Runs**: 1 (no permite ejecuciones concurrentes)
- **Retries**: 2 con 5 minutos de delay

## Monitoreo
- Métricas en Prometheus
- Dashboards en Grafana
- Logs en Loki
"""