"""
DAG Maestro: Ingesta Diaria de Maestras ERP

Este DAG orquesta la ingesta diaria de archivos de maestras desde el ERP.
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

try:
    # Usamos toon_format para leer la definición de layouts desde Variables de Airflow
    from toon_format import decode as toon_decode
except ImportError:
    toon_decode = None

# Logger de módulo
logger = logging.getLogger(__name__)

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
LAYOUTS_DICT = {}
# Lista por defecto solo como fallback; en producción se sobrescribe con lo que venga del .toon
EXPECTED_FILES = [
    'XWECIA.txt', 'XWEPAI.txt', 'XCIUDAD.txt', 'XWEDEP.txt',
    'XLEXSUBL.txt', 'XCADENA.txt', 'XGEN.txt', 'XCATEG.txt',
    'XLEXSUBC.txt', 'XUEN.txt', 'XCAN.txt', 'CRLISUCATE.txt', 'XMONEDA.txt'
]

if toon_decode is not None:
    try:
        layouts_toon = Variable.get("maestras_layouts_toon", default_var=None)
        if layouts_toon:
            # Decodificamos TOON -> dict Python
            LAYOUTS_DICT = toon_decode(layouts_toon)
            # Cada key del .toon (XWECIA.txt, XUEN.txt, etc.) se considera una maestra esperada
            EXPECTED_FILES = sorted(str(k) for k in LAYOUTS_DICT.keys())
            logger.info(
                f"[DAG MAESTRO] Layouts TOON cargados desde Variable 'maestras_layouts_toon'. "
                f"Maestras configuradas: {EXPECTED_FILES}"
            )
        else:
            logger.warning(
                "[DAG MAESTRO] Variable 'maestras_layouts_toon' no está definida. "
                "Se utilizará EXPECTED_FILES por defecto."
            )
    except Exception as e:
        logger.exception(
            f"[DAG MAESTRO] Error decodificando 'maestras_layouts_toon': {e}. "
            "Se usará EXPECTED_FILES por defecto."
        )

logger.info(f"[DAG MAESTRO] EXPECTED_FILES finales: {EXPECTED_FILES}")


def descubrir_archivos(**context):
    """
    Descubre archivos en /input/.
    Valida extensión .txt y lista de esperados.
    """
    logger.info(f"Buscando archivos en: {GCP_INPUT_PATH}")
    archivos_encontrados = []
    input_path = Path(GCP_INPUT_PATH)

    if not input_path.exists():
        logger.error(f"El directorio de entrada no existe: {GCP_INPUT_PATH}")
        return []

    files_in_dir = list(input_path.glob("*"))

    if not files_in_dir:
        logger.error("ALERTA CRÍTICA: No se encontraron archivos en el directorio de entrada.")

    for archivo in files_in_dir:
        nombre_real = archivo.name

        # --- VALIDACIÓN DE EXTENSIÓN ESTRICTA ---
        if archivo.suffix.lower() != '.txt':
            logger.warning(f"[RECHAZADO] El archivo '{nombre_real}' no tiene extensión .txt. Se ignorará.")
            continue

        # --- VALIDACIÓN DE NOMBRE ESPERADO ---
        es_esperado = False
        for expected in EXPECTED_FILES:
            nombre_base_esperado = expected.replace('.txt', '')

            if nombre_real.startswith(nombre_base_esperado):
                es_esperado = True
                break

        if not es_esperado:
            logger.warning(f"[OMITIDO] Archivo desconocido: '{nombre_real}'. No está en la lista de configuración.")
            continue

        archivos_encontrados.append({
            'nombre': nombre_real,
            'ruta': str(archivo),
            'tamano': archivo.stat().st_size
        })
        logger.info(f"[ACEPTADO] Archivo válido: {nombre_real}")

    logger.info(f"Resumen: {len(archivos_encontrados)} archivos válidos enviados a procesar.")

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

    # Normalizamos nombres para comparar (quitando timestamps si los hubiera para la validación)
    nombres_encontrados = {f['nombre'] for f in archivos_encontrados}
    # Simple heurística: verificar si el nombre base está presente
    nombres_base_encontrados = set()
    for f in nombres_encontrados:
        for expected in EXPECTED_FILES:
            if f.startswith(expected.replace('.txt', '')):
                nombres_base_encontrados.add(expected)

    nombres_esperados = set(EXPECTED_FILES)
    faltantes = nombres_esperados - nombres_base_encontrados

    if faltantes:
        logger.warning(f"Faltan {len(faltantes)} archivos críticos: {faltantes}")
    else:
        logger.info("Todos los archivos esperados están presentes.")

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
            # Construimos la ruta base, pero el hijo buscará inteligentemente
            ruta_archivo = str(Path(GCP_INPUT_PATH) / archivo_nombre)

            # Disparo de DAGs hijos con ID único para evitar colisiones (trigger_run_id)
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
Este DAG orquesta la ingesta diaria de archivos de maestras desde el ERP de Grupo Éxito.

## Archivos Procesados
Se toman dinámicamente de la Variable `maestras_layouts_toon`. Cada key del .toon
(XWECIA.txt, XUEN.txt, etc.) se considera una maestra esperada.

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
"""
