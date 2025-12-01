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
from kafka import KafkaProducer

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
KAFKA_BOOTSTRAP_SERVERS = Variable.get("kafka_bootstrap_servers", "kafka:9092")
KAFKA_TOPIC_PREFIX = Variable.get("kafka_topic_prefix", "masterdata.erp.")
POSTGRES_CONN_ID = Variable.get("postgres_conn_id", "postgres_masterdata")

logger = logging.getLogger(__name__)


def leer_y_parsear_archivo(**context):
    """
    Lee y parsea un archivo de texto de ancho fijo o delimitado.
    Implementa búsqueda por coincidencia parcial de nombre.
    """
    conf = context['dag_run'].conf
    archivo_nombre_base = conf.get('archivo_nombre') # Ej: XWECIA.txt
    
    if not archivo_nombre_base:
        raise ValueError("Configuración inválida: falta archivo_nombre")
    
    # Limpiamos la extensión para buscar cualquier variación del nombre base
    nombre_busqueda = Path(archivo_nombre_base).stem # Ej: XWECIA
    input_dir = Path(GCP_INPUT_PATH)
    
    logger.info(f"Buscando archivo que contenga: {nombre_busqueda} en {input_dir}")
    
    # Archivo que empiece con el nombre base
    archivos_encontrados = list(input_dir.glob(f"{nombre_busqueda}*"))
    
    if not archivos_encontrados:
        logger.warning(f"No se encontró ningún archivo que coincida con {nombre_busqueda}. Saltando.")
        raise AirflowSkipException(f"Archivo no encontrado: {nombre_busqueda}")
    
    # Tomamos el primer archivo encontrado
    archivo_real = archivos_encontrados[0]
    archivo_ruta = str(archivo_real)
    
    logger.info(f"Archivo encontrado: {archivo_real.name}")
    logger.info(f"Ruta completa: {archivo_ruta}")
    
    # Guardamos el nombre real encontrado para usarlo en las siguientes tareas
    context['task_instance'].xcom_push(key='archivo_real_nombre', value=archivo_real.name)
    context['task_instance'].xcom_push(key='archivo_real_ruta', value=archivo_ruta)
    
    layouts_config_path = str(Path(__file__).resolve().parent / 'config' / 'layouts.json')
    parser = FixedWidthParser(layouts_config_path)
    
    # Usamos el nombre base original para buscar el layout, no el nombre con fecha
    validacion = parser.validate_file_structure(archivo_ruta, archivo_nombre_base)
    if not validacion['valido']:
        raise ValueError(f"Archivo inválido: {validacion['error']}")
    
    logger.info(f"Archivo válido. Encoding: {validacion['encoding']}")
    
    resultado = parser.parse_file(archivo_ruta, archivo_nombre_base)
    
    tabla = resultado.get('tabla_destino')
    if tabla and '.' not in tabla:
        resultado['tabla_destino'] = f"db_masterdatahub.erp_{tabla}"

    # Logs de Casos de Negocio
    if resultado.get('es_vacio'):
        logger.error(f"Archivo VACÍO detectado: {archivo_real.name}. Se enviará a ERROR.")
    else:
        logger.info(f"Parseo OK. {resultado['lineas_validas']} registros procesados.")
        
    if resultado['lineas_invalidas'] > 0:
        logger.warning(f"Se detectaron {resultado['lineas_invalidas']} registros inválidos o con errores.")
    
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

    # Si viene vacío, retornamos estructura mínima para que mover_archivo decida
    if resultado_parseo.get('es_vacio'):
        return {'tasa_validez': 0, 'es_vacio': True}

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

    tasa_validez = len(registros_validos) / len(registros) if registros else 0

    if len(registros_invalidos) > 0:
        logger.warning(f"{len(registros_invalidos)} registros rechazados por validación de negocio.")

    resultado_validacion = {
        'registros_validos': registros_validos,
        'registros_invalidos': registros_invalidos,
        'total': len(registros),
        'validos': len(registros_validos),
        'invalidos': len(registros_invalidos),
        'tasa_validez': tasa_validez,
        'es_vacio': False
    }
    
    logger.info(f"Validación completada. Tasa: {tasa_validez:.1%}")
    context['task_instance'].xcom_push(key='resultado_validacion', value=resultado_validacion)
    
    return resultado_validacion


def publicar_en_kafka(**context):
    """
    Publica los registros válidos en Kafka Real.
    """
    resultado_parseo = context['task_instance'].xcom_pull(
        task_ids='leer_y_parsear_archivo',
        key='resultado_parseo'
    )
    
    resultado_validacion = context['task_instance'].xcom_pull(
        task_ids='validar_registros',
        key='resultado_validacion'
    )
    
    archivo_real_nombre = context['task_instance'].xcom_pull(
        task_ids='leer_y_parsear_archivo',
        key='archivo_real_nombre'
    )
    
    if not resultado_parseo or not resultado_validacion:
        return None

    if resultado_validacion.get('es_vacio'):
        logger.warning("Archivo vacío. No se enviará nada a Kafka.")
        return None

    registros_validos = resultado_validacion.get('registros_validos', [])
    tabla_destino = resultado_parseo['tabla_destino']
    
    if not registros_validos:
        logger.warning("No hay registros válidos para Kafka.")
        return None
    
    # Construcción del Tópico
    tabla_sin_schema = tabla_destino.split('.')[-1].lower()
    kafka_topic = f"{KAFKA_TOPIC_PREFIX}{tabla_sin_schema}"
    
    logger.info(f"Iniciando conexión a Kafka Brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Topic destino: {kafka_topic}. Mensajes: {len(registros_validos)}")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        for reg in registros_validos:
            producer.send(kafka_topic, value=reg)
        
        producer.flush()
        producer.close()
        
        logger.info(f"Envío a Kafka exitoso. {len(registros_validos)} mensajes publicados.")
        
        resultado = {
            'topic': kafka_topic,
            'publicados': len(registros_validos),
            'archivo': archivo_real_nombre
        }
        context['task_instance'].xcom_push(key='resultado_kafka', value=resultado)
        return resultado
    
    except Exception as e:
        logger.error(f"Error CRÍTICO publicando en Kafka: {e}")
        raise


def registrar_errores(**context):
    """
    Registra los errores de parseo y validación.
    """
    resultado_parseo = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo') or {}
    resultado_validacion = context['task_instance'].xcom_pull(task_ids='validar_registros', key='resultado_validacion') or {}
    
    errores_parseo = resultado_parseo.get('errores', [])
    registros_invalidos = resultado_validacion.get('registros_invalidos', [])
    
    total_errores = len(errores_parseo) + len(registros_invalidos)
    
    if total_errores > 0:
        logger.info(f"Registrando {total_errores} errores en total.")
    
    return {'total_errores': total_errores}


def mover_archivo(**context):
    """
    Mueve el archivo a /processed/ o /error/ según el resultado.
    """
    archivo_real_nombre = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='archivo_real_nombre')
    archivo_real_ruta = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='archivo_real_ruta')
    
    resultado_validacion = context['task_instance'].xcom_pull(
        task_ids='validar_registros',
        key='resultado_validacion'
    )
    
    if not resultado_validacion:
        return None

    # Lógica de decisión inteligente
    es_vacio = resultado_validacion.get('es_vacio', False)
    tasa_validez = resultado_validacion.get('tasa_validez', 0)
    
    if es_vacio:
        destino_path = GCP_ERROR_PATH
        estado = 'error_vacio'
        logger.error("Moviendo archivo VACÍO a carpeta ERROR.")
    elif tasa_validez < 0.95:
        destino_path = GCP_ERROR_PATH
        estado = 'error_calidad'
        logger.warning(f"Moviendo a ERROR por baja calidad ({tasa_validez:.1%})")
    else:
        destino_path = GCP_PROCESSED_PATH
        estado = 'procesado'
        logger.info("Moviendo a PROCESSED (Éxito).")
    
    nombre_base = Path(archivo_real_nombre).stem
    extension = Path(archivo_real_nombre).suffix
    
    if len(nombre_base) < 20: 
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nuevo_nombre = f"{nombre_base}_{timestamp}{extension}"
    else:
        nuevo_nombre = f"PROC_{nombre_base}{extension}"
    
    destino_completo = Path(destino_path) / nuevo_nombre
    
    try:
        shutil.move(archivo_real_ruta, str(destino_completo))
        logger.info(f"Archivo movido exitosamente a: {destino_completo}")
        
        resultado = {
            'archivo_original': archivo_real_nombre,
            'archivo_destino': nuevo_nombre,
            'ruta_destino': str(destino_completo),
            'estado': estado
        }
        return resultado
    
    except Exception as e:
        logger.error(f"Error moviendo archivo: {e}")
        raise


def registrar_en_audit_log(**context):
    """
    Registra la ejecución en la tabla audit_log de PostgreSQL.
    """
    archivo_real_nombre = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='archivo_real_nombre')
    resultado_parseo = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')
    
    if not resultado_parseo:
        return {'registrado': False, 'razon': 'skipped'}

    resultado_validacion = context['task_instance'].xcom_pull(task_ids='validar_registros', key='resultado_validacion') or {}
    resultado_kafka = context['task_instance'].xcom_pull(task_ids='publicar_en_kafka', key='resultado_kafka') or {}
    
    tiempo_inicio = context['execution_date']
    tiempo_fin = datetime.now(tiempo_inicio.tzinfo)
    
    parametros = {
        'archivo': archivo_real_nombre,
        'tabla_destino': resultado_parseo.get('tabla_destino'),
        'dag_run_id': context['dag_run'].run_id
    }
    
    resultado = {
        'total_lineas': resultado_parseo.get('total_lineas', 0),
        'lineas_validas': resultado_parseo.get('lineas_validas', 0),
        'lineas_invalidas': resultado_parseo.get('lineas_invalidas', 0),
        'registros_publicados': resultado_kafka.get('publicados', 0),
        'tasa_validez': resultado_validacion.get('tasa_validez', 0)
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
                json.dumps({'archivo': archivo_real_nombre})
            )
        )
        logger.info("Registro en audit_log exitoso")
    except Exception as e:
        logger.error(f"Error registrando en audit_log: {e}")
    
    return {'registrado': True}


with DAG(
    dag_id='procesamiento_archivo_maestra',
    default_args=DEFAULT_ARGS,
    description='Procesa un archivo individual de maestra',
    schedule_interval=None,
    start_date=datetime(2025, 11, 27),
    catchup=False,
    max_active_runs=15,
    tags=['maestras', 'erp', 'ikitech', 'hijo'],
    doc_md=__doc__,
) as dag:
    
    tarea_parsear = PythonOperator(
        task_id='leer_y_parsear_archivo',
        python_callable=leer_y_parsear_archivo,
        provide_context=True
    )
    
    tarea_validar = PythonOperator(
        task_id='validar_registros',
        python_callable=validar_registros,
        provide_context=True
    )
    
    tarea_kafka = PythonOperator(
        task_id='publicar_en_kafka',
        python_callable=publicar_en_kafka,
        provide_context=True
    )
    
    tarea_errores = PythonOperator(
        task_id='registrar_errores',
        python_callable=registrar_errores,
        provide_context=True,
        trigger_rule='all_done'
    )
    
    tarea_mover = PythonOperator(
        task_id='mover_archivo',
        python_callable=mover_archivo,
        provide_context=True,
        trigger_rule='all_done'
    )
    
    tarea_audit = PythonOperator(
        task_id='registrar_en_audit_log',
        python_callable=registrar_en_audit_log,
        provide_context=True,
        trigger_rule='all_done'
    )
    
    # Flujo de dependencias
    tarea_parsear >> tarea_validar >> tarea_kafka >> tarea_errores >> tarea_mover >> tarea_audit


dag.doc_md = """
# DAG Hijo: Procesamiento de Archivo Individual

## Descripción
Este DAG procesa un archivo individual de maestra desde el ERP.

## Flujo
1. **Leer y parsear**: Lee el archivo fixed-width o delimitado.
2. **Validar**: Valida registros según reglas de negocio.
3. **Kafka**: Publica registros válidos al topic correspondiente.
4. **Mover**: Mueve a /processed/ o /error/ según calidad.
5. **Audit log**: Registra ejecución en PostgreSQL.
"""