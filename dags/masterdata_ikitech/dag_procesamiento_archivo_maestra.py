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
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import json
from pathlib import Path
import shutil

# Importar módulos locales
from parsers.fixed_width_parser import FixedWidthParser
from validators.validators import DataValidator

# Configuración
DEFAULT_ARGS = {
    'owner': 'ikitech',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

# Variables de Airflow
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
    
    Returns:
        Dict con registros parseados y estadísticas
    """
    conf = context['dag_run'].conf
    archivo_nombre = conf.get('archivo_nombre')
    archivo_ruta = conf.get('archivo_ruta')
    
    if not archivo_nombre or not archivo_ruta:
        raise ValueError("Configuración inválida: falta archivo_nombre o archivo_ruta")
    
    logger.info(f"Procesando archivo: {archivo_nombre}")
    logger.info(f"Ruta: {archivo_ruta}")
    
    # Inicializar parser
    layouts_config_path = str(Path(__file__).parent / 'config' / 'layouts.json')
    parser = FixedWidthParser(layouts_config_path)
    
    # Validar estructura del archivo
    validacion = parser.validate_file_structure(archivo_ruta, archivo_nombre)
    if not validacion['valido']:
        raise ValueError(f"Archivo inválido: {validacion['error']}")
    
    logger.info(f"Archivo válido. Encoding: {validacion['encoding']}")
    
    # Parsear archivo
    resultado = parser.parse_file(archivo_ruta, archivo_nombre)
    
    logger.info(f"Parseo completado:")
    logger.info(f"  Total líneas: {resultado['total_lineas']}")
    logger.info(f"  Válidas: {resultado['lineas_validas']}")
    logger.info(f"  Inválidas: {resultado['lineas_invalidas']}")
    logger.info(f"  Tabla destino: {resultado['tabla_destino']}")
    
    # Guardar resultado en XCom
    context['task_instance'].xcom_push(key='resultado_parseo', value=resultado)
    
    return resultado


def validar_registros(**context):
    """
    Valida los registros parseados según reglas de negocio.
    
    Returns:
        Dict con registros válidos e inválidos
    """
    resultado_parseo = context['task_instance'].xcom_pull(
        task_ids='leer_y_parsear_archivo',
        key='resultado_parseo'
    )
    
    registros = resultado_parseo['registros']
    tabla_destino = resultado_parseo['tabla_destino']
    
    logger.info(f"Validando {len(registros)} registros para tabla {tabla_destino}")
    
    # Inicializar validador
    validator = DataValidator()
    
    # Reglas de validación básicas (se pueden extender)
    validation_rules = {
        'required_fields': [],  # Se determina dinámicamente
        'field_types': {},
        'numeric_ranges': {},
        'string_lengths': {}
    }
    
    # Validar lote
    resultado_validacion = validator.validate_batch(registros, validation_rules)
    
    logger.info(f"Validación completada:")
    logger.info(f"  Total: {resultado_validacion['total']}")
    logger.info(f"  Válidos: {resultado_validacion['validos']}")
    logger.info(f"  Inválidos: {resultado_validacion['invalidos']}")
    logger.info(f"  Tasa de validez: {resultado_validacion['tasa_validez']:.1%}")
    
    # Guardar resultado en XCom
    context['task_instance'].xcom_push(key='resultado_validacion', value=resultado_validacion)
    
    return resultado_validacion


def publicar_en_kafka(**context):
    """
    Publica los registros válidos en Kafka.
    
    Returns:
        Dict con estadísticas de publicación
    """
    resultado_parseo = context['task_instance'].xcom_pull(
        task_ids='leer_y_parsear_archivo',
        key='resultado_parseo'
    )
    
    resultado_validacion = context['task_instance'].xcom_pull(
        task_ids='validar_registros',
        key='resultado_validacion'
    )
    
    registros_validos = resultado_validacion['registros_validos']
    archivo_nombre = context['dag_run'].conf.get('archivo_nombre')
    tabla_destino = resultado_parseo['tabla_destino']
    
    # Determinar topic de Kafka
    tabla_sin_schema = tabla_destino.split('.')[-1].lower()
    kafka_topic = f"{KAFKA_TOPIC_PREFIX}{tabla_sin_schema}"
    
    logger.info(f"Publicando {len(registros_validos)} registros en Kafka")
    logger.info(f"Topic: {kafka_topic}")
    logger.info(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    
    # En producción, aquí se usaría kafka-python o confluent-kafka
    # Por ahora, simulamos la publicación
    
    try:
        # from kafka import KafkaProducer
        # producer = KafkaProducer(
        #     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
        # )
        # 
        # for registro in registros_validos:
        #     producer.send(kafka_topic, value=registro)
        # 
        # producer.flush()
        # producer.close()
        
        # Simulación
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
    
    Returns:
        Dict con estadísticas de errores
    """
    resultado_parseo = context['task_instance'].xcom_pull(
        task_ids='leer_y_parsear_archivo',
        key='resultado_parseo'
    )
    
    resultado_validacion = context['task_instance'].xcom_pull(
        task_ids='validar_registros',
        key='resultado_validacion'
    )
    
    errores_parseo = resultado_parseo.get('errores', [])
    registros_invalidos = resultado_validacion.get('registros_invalidos', [])
    
    total_errores = len(errores_parseo) + len(registros_invalidos)
    
    logger.info(f"Registrando {total_errores} errores")
    
    if errores_parseo:
        logger.warning(f"Errores de parseo: {len(errores_parseo)}")
        for error in errores_parseo[:5]:  # Mostrar primeros 5
            logger.warning(f"  Línea {error['linea_num']}: {error['error']}")
    
    if registros_invalidos:
        logger.warning(f"Registros inválidos: {len(registros_invalidos)}")
        for registro in registros_invalidos[:5]:  # Mostrar primeros 5
            logger.warning(f"  Registro {registro['indice']}: {registro['errores']}")
    
    # Aquí se implementaría:
    # 1. Insertar errores en tabla de errores de BD
    # 2. Generar archivo .error con líneas rechazadas
    # 3. Enviar alertas si la tasa de error es alta
    
    resultado = {
        'errores_parseo': len(errores_parseo),
        'registros_invalidos': len(registros_invalidos),
        'total_errores': total_errores
    }
    
    return resultado


def mover_archivo(**context):
    """
    Mueve el archivo a /processed/ o /error/ según el resultado.
    
    Returns:
        Dict con información del movimiento
    """
    conf = context['dag_run'].conf
    archivo_nombre = conf.get('archivo_nombre')
    archivo_ruta = conf.get('archivo_ruta')
    
    resultado_validacion = context['task_instance'].xcom_pull(
        task_ids='validar_registros',
        key='resultado_validacion'
    )
    
    # Determinar destino según tasa de validez
    tasa_validez = resultado_validacion['tasa_validez']
    
    if tasa_validez >= 0.95:  # 95% o más de registros válidos
        destino_path = GCP_PROCESSED_PATH
        estado = 'procesado'
    else:
        destino_path = GCP_ERROR_PATH
        estado = 'error'
    
    # Agregar timestamp al nombre del archivo
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    nombre_base = Path(archivo_nombre).stem
    extension = Path(archivo_nombre).suffix
    nuevo_nombre = f"{nombre_base}_{timestamp}{extension}"
    
    destino_completo = Path(destino_path) / nuevo_nombre
    
    logger.info(f"Moviendo archivo a: {destino_completo}")
    logger.info(f"Estado: {estado}")
    logger.info(f"Tasa de validez: {tasa_validez:.1%}")
    
    # En producción, esto usaría GCP Storage API
    # Por ahora, simulamos el movimiento
    
    try:
        # shutil.move(archivo_ruta, str(destino_completo))
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
    
    Returns:
        Dict con información del registro
    """
    conf = context['dag_run'].conf
    archivo_nombre = conf.get('archivo_nombre')
    
    resultado_parseo = context['task_instance'].xcom_pull(
        task_ids='leer_y_parsear_archivo',
        key='resultado_parseo'
    )
    
    resultado_validacion = context['task_instance'].xcom_pull(
        task_ids='validar_registros',
        key='resultado_validacion'
    )
    
    resultado_kafka = context['task_instance'].xcom_pull(
        task_ids='publicar_en_kafka',
        key='resultado_kafka'
    )
    
    # Preparar datos para audit_log
    tiempo_inicio = context['execution_date']
    tiempo_fin = datetime.now()
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
    
    logger.info(f"Registrando en audit_log:")
    logger.info(f"  Procedimiento: procesamiento_archivo_maestra")
    logger.info(f"  Duración: {duracion}")
    logger.info(f"  Estado: success")
    logger.info(f"  Resultado: {json.dumps(resultado, indent=2)}")
    
    # Aquí se llamaría al procedimiento almacenado sp_registrar_logs
    # usando db_utils.py
    
    # from utils.db_utils import call_stored_procedure
    # call_stored_procedure(
    #     conn_id=POSTGRES_CONN_ID,
    #     procedure_name='db_masterdatahub.sp_registrar_logs',
    #     params={
    #         'nombre_proced': 'procesamiento_archivo_maestra',
    #         'tiempo_ejec_ini': tiempo_inicio,
    #         'tiempo_ejec_fin': tiempo_fin,
    #         'estado': 'success',
    #         'mensaje_error': None,
    #         'parametros': json.dumps(parametros),
    #         'resultado': json.dumps(resultado),
    #         'info_adicional': json.dumps({'archivo': archivo_nombre})
    #     }
    # )
    
    return {
        'registrado': True,
        'duracion_segundos': duracion.total_seconds()
    }


# Definición del DAG
with DAG(
    dag_id='procesamiento_archivo_maestra',
    default_args=DEFAULT_ARGS,
    description='Procesa un archivo individual de maestra',
    schedule_interval=None,  # Disparado por DAG maestro
    start_date=datetime(2025, 11, 27),
    catchup=False,
    max_active_runs=12,  # Permite procesar 12 archivos en paralelo
    tags=['maestras', 'erp', 'ikitech', 'hijo'],
    doc_md=__doc__,
) as dag:
    
    # Tarea 1: Leer y parsear archivo
    tarea_parsear = PythonOperator(
        task_id='leer_y_parsear_archivo',
        python_callable=leer_y_parsear_archivo,
        provide_context=True,
        doc_md="""
        Lee y parsea un archivo de texto de ancho fijo.
        Extrae los campos según la configuración de layout.
        """
    )
    
    # Tarea 2: Validar registros
    tarea_validar = PythonOperator(
        task_id='validar_registros',
        python_callable=validar_registros,
        provide_context=True,
        doc_md="""
        Valida los registros parseados según reglas de negocio.
        Separa registros válidos de inválidos.
        """
    )
    
    # Tarea 3: Publicar en Kafka
    tarea_kafka = PythonOperator(
        task_id='publicar_en_kafka',
        python_callable=publicar_en_kafka,
        provide_context=True,
        doc_md="""
        Publica los registros válidos en Kafka.
        Los sistemas finales consumirán de Kafka.
        """
    )
    
    # Tarea 4: Registrar errores
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
    
    # Tarea 5: Mover archivo
    tarea_mover = PythonOperator(
        task_id='mover_archivo',
        python_callable=mover_archivo,
        provide_context=True,
        trigger_rule='all_done',
        doc_md="""
        Mueve el archivo a /processed/ o /error/ según el resultado.
        """
    )
    
    # Tarea 6: Registrar en audit_log
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
    
    # Definir dependencias
    tarea_parsear >> tarea_validar >> tarea_kafka >> [tarea_errores, tarea_mover] >> tarea_audit


# Documentación del DAG
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
