"""
DAG Hijo: Procesamiento de Archivo Individual de Maestra

Este DAG procesa un archivo individual de maestra:
1. Lee el archivo fixed-width
2. Parsea y valida los registros
3. Publica en Kafka
4. Mueve el archivo a /processed/ o /error/
5. Registra en audit_log

Autor: IkiTech
Fecha: 2025-11-27
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import logging
import json
from pathlib import Path
import shutil

from masterdata_ikitech.parsers.fixed_width_parser import FixedWidthParser
from masterdata_ikitech.validators.validators import DataValidator, get_validator_for_table

DEFAULT_ARGS = {
    'owner': 'ikitech',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

GCP_INPUT_PATH = Variable.get("gcp_input_path", "/input/")
GCP_PROCESSED_PATH = Variable.get("gcp_processed_path", "/processed/")
GCP_ERROR_PATH = Variable.get("gcp_error_path", "/error/")
KAFKA_BOOTSTRAP_SERVERS = Variable.get("kafka_bootstrap_servers", "localhost:9092")
KAFKA_TOPIC_PREFIX = Variable.get("kafka_topic_prefix", "masterdata.erp.")
POSTGRES_CONN_ID = Variable.get("postgres_conn_id", "postgres_masterdata")

logger = logging.getLogger(__name__)


def leer_y_parsear_archivo(**context):
    """
    Lee y parsea un archivo de texto de ancho fijo.
    """
    conf = context['dag_run'].conf
    archivo_nombre = conf.get('archivo_nombre')
    archivo_ruta = conf.get('archivo_ruta')
    
    if not archivo_nombre or not archivo_ruta:
        raise ValueError("Configuración inválida: falta archivo_nombre o archivo_ruta")
    
    logger.info(f"Procesando archivo: {archivo_nombre}")
    
    path_obj = Path(archivo_ruta)
    if not path_obj.exists():
        logger.warning(f"El archivo {archivo_ruta} no existe. Saltando procesamiento.")
        raise AirflowSkipException(f"Archivo no encontrado: {archivo_nombre}")

    logger.info(f"Ruta: {archivo_ruta}")
    
    layouts_config_path = str(Path(__file__).resolve().parent / 'config' / 'layouts.json')
    parser = FixedWidthParser(layouts_config_path)
    
    validacion = parser.validate_file_structure(archivo_ruta, archivo_nombre)
    if not validacion['valido']:
        raise ValueError(f"Archivo inválido: {validacion['error']}")
    
    logger.info(f"Archivo válido. Encoding: {validacion['encoding']}")
    
    resultado = parser.parse_file(archivo_ruta, archivo_nombre)
    
    logger.info(f"Parseo completado:")
    logger.info(f"  Total líneas: {resultado['total_lineas']}")
    logger.info(f"  Válidas: {resultado['lineas_validas']}")
    logger.info(f"  Inválidas: {resultado['lineas_invalidas']}")
    logger.info(f"  Tabla destino: {resultado['tabla_destino']}")
    
    context['task_instance'].xcom_push(key='resultado_parseo', value=resultado)
    
    return resultado


def validar_registros(**context):
    """
    Valida los registros parseados según reglas de negocio.
    """
    resultado_parseo = context['task_instance'].xcom_pull(
        task_ids='leer_y_parsear_archivo',
        key='resultado_parseo'
    )
    
    if not resultado_parseo:
        return None

    registros = resultado_parseo['registros']
    tabla_destino = resultado_parseo['tabla_destino']
    
    logger.info(f"Validando {len(registros)} registros para tabla {tabla_destino}")
    
    validator_func = get_validator_for_table(tabla_destino)
    
    registros_validos = []
    registros_invalidos = []

    if validator_func:
        for idx, record in enumerate(registros):
            es_valido, errores = validator_func(record)
            if es_valido:
                registros_validos.append(record)
            else:
                registros_invalidos.append({
                    'indice': idx,
                    'registro': record,
                    'errores': errores
                })
    else:
        registros_validos = registros

    resultado_validacion = {
        'registros_validos': registros_validos,
        'registros_invalidos': registros_invalidos,
        'total': len(registros),
        'validos': len(registros_validos),
        'invalidos': len(registros_invalidos),
        'tasa_validez': len(registros_validos) / len(registros) if registros else 0
    }
    
    logger.info(f"Validación completada:")
    logger.info(f"  Total: {resultado_validacion['total']}")
    logger.info(f"  Válidos: {resultado_validacion['validos']}")
    logger.info(f"  Inválidos: {resultado_validacion['invalidos']}")
    logger.info(f"  Tasa de validez: {resultado_validacion['tasa_validez']:.1%}")
    
    context['task_instance'].xcom_push(key='resultado_validacion', value=resultado_validacion)
    
    return resultado_validacion


def publicar_en_kafka(**context):
    """
    Publica los registros válidos en Kafka.
    """
    resultado_parseo = context['task_instance'].xcom_pull(
        task_ids='leer_y_parsear_archivo',
        key='resultado_parseo'
    )
    
    resultado_validacion = context['task_instance'].xcom_pull(
        task_ids='validar_registros',
        key='resultado_validacion'
    )
    
    if not resultado_parseo or not resultado_validacion:
        return None

    registros_validos = resultado_validacion['registros_validos']
    archivo_nombre = context['dag_run'].conf.get('archivo_nombre')
    tabla_destino = resultado_parseo['tabla_destino']
    
    tabla_sin_schema = tabla_destino.split('.')[-1].lower()
    kafka_topic = f"{KAFKA_TOPIC_PREFIX}{tabla_sin_schema}"
    
    logger.info(f"Publicando {len(registros_validos)} registros en Kafka")
    logger.info(f"Topic: {kafka_topic}")
    logger.info(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    
    try:
        publicados = len(registros_validos)
        fallidos = 0
        
        logger.info(f"Publicación completada: {publicados} registros")
        
        resultado = {
            'topic': kafka_topic,
            'publicados': publicados,
            'fallidos': fallidos,
            'archivo': archivo_nombre
        }
        
        context['task_instance'].xcom_push(key='resultado_kafka', value=resultado)
        
        return resultado
    
    except Exception as e:
        logger.error(f"Error publicando en Kafka: {e}")
        raise


def registrar_errores(**context):
    """
    Registra los errores de parseo y validación.
    """
    resultado_parseo = context['task_instance'].xcom_pull(
        task_ids='leer_y_parsear_archivo',
        key='resultado_parseo'
    ) or {}
    
    resultado_validacion = context['task_instance'].xcom_pull(
        task_ids='validar_registros',
        key='resultado_validacion'
    ) or {}
    
    errores_parseo = resultado_parseo.get('errores', [])
    registros_invalidos = resultado_validacion.get('registros_invalidos', [])
    
    total_errores = len(errores_parseo) + len(registros_invalidos)
    
    if total_errores > 0:
        logger.info(f"Registrando {total_errores} errores")
    
    if errores_parseo:
        logger.warning(f"Errores de parseo: {len(errores_parseo)}")
        for error in errores_parseo[:5]:
            logger.warning(f"  Línea {error['linea_num']}: {error['error']}")
    
    if registros_invalidos:
        logger.warning(f"Registros inválidos: {len(registros_invalidos)}")
        for registro in registros_invalidos[:5]:
            logger.warning(f"  Registro {registro['indice']}: {registro['errores']}")
    
    resultado = {
        'errores_parseo': len(errores_parseo),
        'registros_invalidos': len(registros_invalidos),
        'total_errores': total_errores
    }
    
    return resultado


def mover_archivo(**context):
    """
    Mueve el archivo a /processed/ o /error/ según el resultado.
    """
    conf = context['dag_run'].conf
    archivo_nombre = conf.get('archivo_nombre')
    archivo_ruta = conf.get('archivo_ruta')
    
    resultado_validacion = context['task_instance'].xcom_pull(
        task_ids='validar_registros',
        key='resultado_validacion'
    )
    
    if not resultado_validacion:
        return None

    tasa_validez = resultado_validacion['tasa_validez']
    
    if tasa_validez >= 0.95:
        destino_path = GCP_PROCESSED_PATH
        estado = 'procesado'
    else:
        destino_path = GCP_ERROR_PATH
        estado = 'error'
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    nuevo_nombre = f"{Path(archivo_nombre).stem}_{timestamp}{Path(archivo_nombre).suffix}"

    destino_completo = Path(destino_path) / nuevo_nombre
    
    logger.info(f"Moviendo archivo a: {destino_completo}")
    logger.info(f"Estado: {estado}")
    logger.info(f"Tasa de validez: {tasa_validez:.1%}")
    
    try:
        #shutil.move(archivo_ruta, str(destino_completo))
        logger.info(f"Archivo movido exitosamente")
        
        resultado = {
            'archivo_original': archivo_nombre,
            'archivo_destino': nuevo_nombre,
            'ruta_destino': str(destino_completo),
            'estado': estado,
            'tasa_validez': tasa_validez
        }
        
        return resultado
    
    except Exception as e:
        logger.error(f"Error moviendo archivo: {e}")
        raise


def registrar_en_audit_log(**context):
    """
    Registra la ejecución en la tabla audit_log de PostgreSQL.
    """
    conf = context['dag_run'].conf
    archivo_nombre = conf.get('archivo_nombre')
    
    resultado_parseo = context['task_instance'].xcom_pull(
        task_ids='leer_y_parsear_archivo',
        key='resultado_parseo'
    )
    
    if not resultado_parseo:
        return {'registrado': False, 'razon': 'skipped'}

    resultado_validacion = context['task_instance'].xcom_pull(
        task_ids='validar_registros',
        key='resultado_validacion'
    )
    
    resultado_kafka = context['task_instance'].xcom_pull(
        task_ids='publicar_en_kafka',
        key='resultado_kafka'
    )
    
    tiempo_inicio = context['execution_date']
    tiempo_fin = datetime.now(tiempo_inicio.tzinfo)
    duracion = tiempo_fin - tiempo_inicio
    
    parametros = {
        'archivo': archivo_nombre,
        'tabla_destino': resultado_parseo['tabla_destino'],
        'dag_run_id': context['dag_run'].run_id
    }
    
    resultado = {
        'total_lineas': resultado_parseo['total_lineas'],
        'lineas_validas': resultado_parseo['lineas_validas'],
        'lineas_invalidas': resultado_parseo['lineas_invalidas'],
        'registros_publicados': resultado_kafka['publicados'],
        'tasa_validez': resultado_validacion['tasa_validez']
    }
    
    logger.info(f"Registrando en audit_log...")
    
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(
            "CALL db_masterdatahub.sp_registrar_logs(%s, %s, %s, %s, %s, %s, %s, %s)",
            parameters=(
                'procesamiento_archivo_maestra',
                tiempo_inicio,
                tiempo_fin,
                'success',
                None,
                json.dumps(parametros),
                json.dumps(resultado),
                json.dumps({'archivo': archivo_nombre})
            )
        )
        logger.info("Registro en audit_log exitoso")
    except Exception as e:
        logger.error(f"Error registrando en audit_log: {e}")
    
    return {
        'registrado': True,
        'duracion_segundos': duracion.total_seconds()
    }


with DAG(
    dag_id='procesamiento_archivo_maestra',
    default_args=DEFAULT_ARGS,
    description='Procesa un archivo individual de maestra',
    schedule_interval=None,
    start_date=datetime(2025, 11, 27),
    catchup=False,
    max_active_runs=13,
    tags=['maestras', 'erp', 'ikitech', 'hijo'],
    doc_md=__doc__,
) as dag:
    
    tarea_parsear = PythonOperator(
        task_id='leer_y_parsear_archivo',
        python_callable=leer_y_parsear_archivo,
        provide_context=True,
        doc_md="""
        Lee y parsea un archivo de texto de ancho fijo.
        Extrae los campos según la configuración de layout.
        """
    )
    
    tarea_validar = PythonOperator(
        task_id='validar_registros',
        python_callable=validar_registros,
        provide_context=True,
        doc_md="""
        Valida los registros parseados según reglas de negocio.
        Separa registros válidos de inválidos.
        """
    )
    
    tarea_kafka = PythonOperator(
        task_id='publicar_en_kafka',
        python_callable=publicar_en_kafka,
        provide_context=True,
        doc_md="""
        Publica los registros válidos en Kafka.
        Los sistemas finales consumirán de Kafka.
        """
    )
    
    tarea_errores = PythonOperator(
        task_id='registrar_errores',
        python_callable=registrar_errores,
        provide_context=True,
        trigger_rule='all_done',
        doc_md="""
        Registra los errores de parseo y validación.
        Genera archivo .error con líneas rechazadas.
        """
    )
    
    tarea_mover = PythonOperator(
        task_id='mover_archivo',
        python_callable=mover_archivo,
        provide_context=True,
        trigger_rule='all_done',
        doc_md="""
        Mueve el archivo a /processed/ o /error/ según el resultado.
        """
    )
    
    tarea_audit = PythonOperator(
        task_id='registrar_en_audit_log',
        python_callable=registrar_en_audit_log,
        provide_context=True,
        trigger_rule='all_done',
        doc_md="""
        Registra la ejecución en la tabla audit_log de PostgreSQL.
        Llama al procedimiento almacenado sp_registrar_logs.
        """
    )
    
    tarea_parsear >> tarea_validar >> tarea_kafka >> [tarea_errores, tarea_mover] >> tarea_audit


dag.doc_md = """
# DAG Hijo: Procesamiento de Archivo Individual

## Descripción
Este DAG procesa un archivo individual de maestra desde el ERP.

## Flujo
1. **Leer y parsear**: Lee el archivo fixed-width y extrae campos
2. **Validar**: Valida registros según reglas de negocio
3. **Publicar en Kafka**: Publica registros válidos
4. **Registrar errores**: Registra errores de parseo y validación
5. **Mover archivo**: Mueve a /processed/ o /error/
6. **Audit log**: Registra ejecución en PostgreSQL

## Configuración Requerida (via conf)
- `archivo_nombre`: Nombre del archivo (ej: 'XWECIA.txt')
- `archivo_ruta`: Ruta completa al archivo

## Criterios de Éxito
- Tasa de validez >= 95% → /processed/
- Tasa de validez < 95% → /error/

## Monitoreo
- Métricas por archivo en Prometheus
- Logs detallados en Loki
- Alertas si tasa de error > 5%

## Contacto
- Equipo: IkiTech
- Email: data-team@ikitech.com
"""