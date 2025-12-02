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
import hashlib
import re

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

# MAPA DE LLAVES PRIMARIAS
PK_MAPPING = {
    'db_masterdatahub.erp_compania': ['codcia'],
    'db_masterdatahub.erp_moneda': ['cod_mon'],
    'db_masterdatahub.erp_pais': ['cod_pais'],
    'db_masterdatahub.erp_ciudad': ['cod_pais', 'ciudad'],
    'db_masterdatahub.erp_dependencia': ['depen'],
    'db_masterdatahub.erp_sublinea': ['codcia', 'sublin'],
    'db_masterdatahub.erp_cadena': ['codcade'],
    'db_masterdatahub.erp_gerencia': ['codcia', 'divis'],
    'db_masterdatahub.erp_categoria': ['codcia', 'sublin', 'catego'],
    'db_masterdatahub.erp_subcategoria': ['codcia'], 
    'db_masterdatahub.erp_uen': ['codcia', 'depto'],
    'db_masterdatahub.erp_canal': ['canal'],
    'db_masterdatahub.erp_segmentos': ['codcia']
}

logger = logging.getLogger(__name__)


def normalizar_tabla(tabla: str) -> str:
    if not tabla:
        return tabla
    tabla = tabla.replace('db_masterdata_hub', 'db_masterdatahub')
    return tabla.lower()


def obtener_pk(pg_hook: PostgresHook, tabla: str):
    try:
        sql = f"""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '{tabla}'::regclass AND i.indisprimary;
        """
        rows = pg_hook.get_records(sql)
        return [r[0] for r in rows]
    except Exception:
        return []


def asegurar_columna_hash_e_indice(pg_hook: PostgresHook, tabla: str):
    # Crear columna _record_hash si no existe y un índice único sobre ella
    add_col_sql = f"ALTER TABLE {tabla} ADD COLUMN IF NOT EXISTS _record_hash TEXT;"
    pg_hook.run(add_col_sql)
    safe_index_name = f"ux_{tabla.replace('.', '_')}_record_hash"
    create_index_sql = f"CREATE UNIQUE INDEX IF NOT EXISTS {safe_index_name} ON {tabla} (_record_hash);"
    pg_hook.run(create_index_sql)


def normalizar_valores(v):

    if v is None:
        return ''
    s = str(v)
    # eliminar \r y normalizar saltos de línea a espacio
    s = s.replace('\r', '').replace('\n', ' ')
    # trim
    s = s.strip()
    # colapsar cualquier whitespace (espacios, tabs) a un solo espacio
    s = re.sub(r'\s+', ' ', s)
    return s


def cadena_registro_canónico(record: dict) -> str:

    keys = sorted(record.keys(), key=lambda x: x.lower())
    parts = []
    for k in keys:
        # Soportar claves en mayúscula/minúscula
        v = None
        if k in record:
            v = record[k]
        elif k.upper() in record:
            v = record[k.upper()]
        elif k.lower() in record:
            v = record[k.lower()]
        val = normalizar_valores(v)
        parts.append(f"{str(k).upper()}={val}")
    return "|".join(parts)


def leer_y_parsear_archivo(**context):
    conf = context['dag_run'].conf
    archivo_nombre_base = conf.get('archivo_nombre')
    
    if not archivo_nombre_base:
        raise ValueError("Configuración inválida: falta archivo_nombre")
    
    nombre_busqueda = Path(archivo_nombre_base).stem
    input_dir = Path(GCP_INPUT_PATH)
    
    logger.info(f"Buscando archivo que contenga: {nombre_busqueda} en {input_dir}")
    
    archivos_encontrados = list(input_dir.glob(f"{nombre_busqueda}*"))
    
    if not archivos_encontrados:
        logger.warning(f"No se encontró ningún archivo que coincida con {nombre_busqueda}. Saltando.")
        raise AirflowSkipException(f"Archivo no encontrado: {nombre_busqueda}")
    
    archivo_real = archivos_encontrados[0]
    archivo_ruta = str(archivo_real)
    
    logger.info(f"Archivo encontrado: {archivo_real.name}")
    
    context['task_instance'].xcom_push(key='archivo_real_nombre', value=archivo_real.name)
    context['task_instance'].xcom_push(key='archivo_real_ruta', value=archivo_ruta)
    
    layouts_config_path = str(Path(__file__).resolve().parent / 'config' / 'layouts.json')
    parser = FixedWidthParser(layouts_config_path)
    
    validacion = parser.validate_file_structure(archivo_ruta, archivo_nombre_base)
    
    if not validacion['valido']:
        if validacion.get('error') == "Archivo vacío":
            logger.error(f"Archivo VACÍO detectado: {archivo_real.name}. Se enviará a ERROR.")
            resultado_vacio = {
                'registros': [], 'errores': [], 'total_lineas': 0, 'lineas_validas': 0,
                'lineas_invalidas': 0, 'tabla_destino': None, 'archivo': archivo_real.name, 'es_vacio': True
            }
            context['task_instance'].xcom_push(key='resultado_parseo', value=resultado_vacio)
            return resultado_vacio
        else:
            raise ValueError(f"Archivo inválido: {validacion['error']}")
    
    logger.info(f"Archivo válido. Encoding: {validacion['encoding']}")
    resultado = parser.parse_file(archivo_ruta, archivo_nombre_base)
    
    tabla = resultado.get('tabla_destino')
    if tabla and '.' not in tabla:
        resultado['tabla_destino'] = f"db_masterdatahub.erp_{tabla}"

    if resultado.get('es_vacio'):
        logger.error(f"Archivo VACÍO detectado: {archivo_real.name}. Se enviará a ERROR.")
    else:
        logger.info(f"Parseo OK. {resultado.get('lineas_validas', 0)} registros procesados.")
        
    if resultado.get('lineas_invalidas', 0) > 0:
        logger.warning(f"{resultado['lineas_invalidas']} registros con error de formato.")
    
    context['task_instance'].xcom_push(key='resultado_parseo', value=resultado)
    return resultado


def validar_registros(**context):
    resultado_parseo = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')
    if not resultado_parseo:
        return None
    if resultado_parseo.get('es_vacio'):
        return {'lineas_validas_count': 0, 'es_vacio': True}

    registros = resultado_parseo['registros']
    tabla_destino = resultado_parseo['tabla_destino']

    logger.info(f"Validando {len(registros)} registros para {tabla_destino}")
    validator_func = get_validator_for_table(tabla_destino)
    pks = PK_MAPPING.get(tabla_destino.lower(), [])

    registros_validos = []
    registros_invalidos = []

    seen_hashes = set()
    seen_pks = set()
    error_duplicidad_detectado = False

    for idx, record in enumerate(registros):
        # canonical string + stable hash
        canonical = cadena_registro_canónico(record)
        row_hash = hashlib.sha256(canonical.encode('utf-8')).hexdigest()

        if row_hash in seen_hashes:
            msg = f"Registro DUPLICADO EXACTO detectado en línea {idx+1}"
            logger.error(msg)
            registros_invalidos.append({'idx': idx, 'err': [msg]})
            error_duplicidad_detectado = True
            continue
        seen_hashes.add(row_hash)

        if pks:
            pk_values = []
            for pk in pks:
                # intentar buscar en diferentes formas
                v = None
                if pk.upper() in record:
                    v = record.get(pk.upper())
                elif pk.lower() in record:
                    v = record.get(pk.lower())
                else:
                    v = record.get(pk)
                pk_values.append(normalizar_valores(v))
            pk_tuple = tuple(pk_values)

            if any(v == '' for v in pk_tuple):
                pass

            if pk_tuple in seen_pks:
                msg = f"PK DUPLICADA {pk_tuple} detectada en línea {idx+1}"
                logger.error(msg)
                registros_invalidos.append({'idx': idx, 'err': [msg]})
                error_duplicidad_detectado = True
                continue
            seen_pks.add(pk_tuple)

        if validator_func:
            try:
                es_valido, errores = validator_func(record)
            except Exception as e:
                msg = f"ERROR VALIDANDO TIPO DE DATO en línea {idx+1}: {str(e)}"
                logger.error(msg)
                registros_invalidos.append({'idx': idx, 'err': [msg]})
                error_duplicidad_detectado = True
                continue

            if es_valido:
                registros_validos.append(record)
            else:
                registros_invalidos.append({'idx': idx, 'err': errores})
                logger.error(f"Registro rechazado en línea {idx+1}: {errores}")
        else:
            registros_validos.append(record)

    if len(registros_invalidos) > 0:
        logger.warning(f"{len(registros_invalidos)} registros rechazados en total.")

    total = len(registros)
    validos = len(registros_validos)
    porcentaje_validez = (validos / total) * 100 if total > 0 else 0

    VALID_PERCENTAGE = float(Variable.get("valid_percentage", "100"))

    error_tasa = porcentaje_validez < VALID_PERCENTAGE
    error_archivo_completo = error_duplicidad_detectado or error_tasa or len(registros_invalidos) > 0

    resultado_validacion = {
        'registros_validos': registros_validos,
        'registros_invalidos': registros_invalidos,
        'lineas_validas_count': validos,
        'es_vacio': False,
        'error_critico_integridad': error_archivo_completo
    }

    context['task_instance'].xcom_push(key='resultado_validacion', value=resultado_validacion)
    return resultado_validacion


def cargar_datos_postgres(**context):
    """
    Inserta registros usando UPSERT.
    """
    resultado_parseo = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')
    resultado_validacion = context['task_instance'].xcom_pull(task_ids='validar_registros', key='resultado_validacion')
    
    if not resultado_parseo or not resultado_validacion:
        return None
    if resultado_validacion.get('es_vacio') or resultado_validacion.get('error_critico_integridad'):
        logger.info("Inserción a Postgres cancelada por validación.")
        return None
    
    registros = resultado_validacion.get('registros_validos', [])
    
    tabla_destino = normalizar_tabla(resultado_parseo['tabla_destino'].lower()) 
    pks = PK_MAPPING.get(tabla_destino)
    
    if not registros:
        logger.info("No hay registros válidos para insertar.")
        return
    
    registros_min = []
    for reg in registros:
        registros_min.append({k.lower(): v for k, v in reg.items()})
        
    logger.info(f"Iniciando UPSERT de {len(registros)} filas en {tabla_destino}")
    
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    try:
        if not pks:
            detected_pks = obtener_pk(pg_hook, tabla_destino)
            if detected_pks:
                pks = [c.lower() for c in detected_pks]
            else:
                asegurar_columna_hash_e_indice(pg_hook, tabla_destino)
                pks = ['_record_hash']
                # Añadir hash a cada registro
                for r in registros_min:
                    s = json.dumps({k: r.get(k) for k in sorted(r.keys())}, ensure_ascii=False, sort_keys=True)
                    r['_record_hash'] = hashlib.sha256(s.encode('utf-8')).hexdigest()
        
        # Determinar target_fields de forma determinista
        target_fields = list(registros_min[0].keys())
        rows = [[r.get(f) for f in target_fields] for r in registros_min]
        
        pg_hook.insert_rows(
            table=tabla_destino,
            rows=rows,
            target_fields=target_fields,
            commit_every=1000,
            replace=True,        
            replace_index=pks    
        )
        logger.info(f"[BD SUCCESS] Carga/Actualización completada en {tabla_destino}.")
    except Exception as e:
        logger.error(f"Error crítico insertando en BD: {e}")
        raise


def publicar_en_kafka(**context):
    resultado_parseo = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')
    resultado_validacion = context['task_instance'].xcom_pull(task_ids='validar_registros', key='resultado_validacion')
    archivo_real_nombre = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='archivo_real_nombre')
    
    if not resultado_parseo or not resultado_validacion:
        return None
    if resultado_validacion.get('es_vacio'):
        return None
    if resultado_validacion.get('error_critico_integridad'):
        logger.info("Publicación a Kafka cancelada por validación.")
        return None

    registros_validos = resultado_validacion.get('registros_validos', [])
    tabla_destino = resultado_parseo['tabla_destino']
    
    if not registros_validos:
        return None
    
    tabla_sin_schema = tabla_destino.split('.')[-1].lower()
    kafka_topic = f"{KAFKA_TOPIC_PREFIX}{tabla_sin_schema}"
    
    logger.info(f"Enviando a {KAFKA_BOOTSTRAP_SERVERS}. Topic: {kafka_topic}")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10, 2),
            client_id='airflow-producer'
        )
        for reg in registros_validos:
            producer.send(kafka_topic, value=reg)
        producer.flush()
        producer.close()
        
        logger.info(f"Envío a Kafka exitoso. {len(registros_validos)} mensajes.")
        
        resultado = {'topic': kafka_topic, 'publicados': len(registros_validos), 'archivo': archivo_real_nombre}
        context['task_instance'].xcom_push(key='resultado_kafka', value=resultado)
        return resultado
    except Exception as e:
        logger.error(f"[ERROR KAFKA] {e}")
        raise


def registrar_errores(**context):
    resultado_parseo = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo') or {}
    resultado_validacion = context['task_instance'].xcom_pull(task_ids='validar_registros', key='resultado_validacion') or {}
    total = len(errores_parseo := resultado_parseo.get('errores', [])) + len(registros_invalidos := resultado_validacion.get('registros_invalidos', []))
    if total > 0: logger.info(f"Registrando {total} errores.")
    return {'total_errores': total}


def mover_archivo(**context):
    archivo_real_nombre = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='archivo_real_nombre')
    archivo_real_ruta = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='archivo_real_ruta')
    res_parseo = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')
    res_validacion = context['task_instance'].xcom_pull(task_ids='validar_registros', key='resultado_validacion')
    
    if not archivo_real_nombre or not archivo_real_ruta: return None

    es_vacio = res_parseo.get('es_vacio', False)
    cantidad_validos = res_validacion.get('lineas_validas_count', 0) if res_validacion else 0
    error_integridad = res_validacion.get('error_critico_integridad', False) if res_validacion else False
    
    if es_vacio:
        destino_path = GCP_ERROR_PATH
        estado = 'error_vacio'
        logger.error(f"Archivo VACÍO {archivo_real_nombre} -> ERROR")
    elif error_integridad:
        destino_path = GCP_ERROR_PATH
        estado = 'error_integridad_duplicados'
        logger.error(f"Archivo con DUPLICADOS -> ERROR")
    elif cantidad_validos == 0:
        destino_path = GCP_ERROR_PATH
        estado = 'error_calidad_total'
        logger.error(f"Moviendo a ERROR (0 registros válidos).")
    else:
        destino_path = GCP_PROCESSED_PATH
        estado = 'procesado'
        logger.info(f"Moviendo a PROCESSED ({cantidad_validos} registros útiles).")
    
    nombre_base = Path(archivo_real_nombre).stem
    extension = Path(archivo_real_nombre).suffix
    # Restauré la lógica original con timestamp si el nombre base es corto o no contiene año
    if len(nombre_base) < 20 or nombre_base.find('_202') == -1: 
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nuevo_nombre = f"{nombre_base}_{timestamp}{extension}"
    else:
        nuevo_nombre = f"PROC_{archivo_real_nombre}"

    destino_completo = Path(destino_path) / nuevo_nombre
    
    try:
        shutil.move(archivo_real_ruta, str(destino_completo))
        logger.info(f"Movido a: {destino_completo}")
        return {'estado': estado}
    except Exception as e:
        logger.error(f"Error moviendo: {e}")
        raise


def registrar_en_audit_log(**context):
    archivo_real_nombre = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='archivo_real_nombre')
    resultado_parseo = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')
    if not resultado_parseo: return {'registrado': False}
    
    resultado_validacion = context['task_instance'].xcom_pull(task_ids='validar_registros', key='resultado_validacion') or {}
    resultado_kafka = context['task_instance'].xcom_pull(task_ids='publicar_en_kafka', key='resultado_kafka') or {}
    
    tiempo_inicio = context.get('data_interval_start') or context.get('execution_date')
    if hasattr(tiempo_inicio, '_get_current_object'): tiempo_inicio = tiempo_inicio._get_current_object()
    tiempo_fin = datetime.now(tiempo_inicio.tzinfo)
    
    parametros = {'archivo': archivo_real_nombre, 'tabla': resultado_parseo.get('tabla_destino'), 'run': context['dag_run'].run_id}
    
    status_final = 'OK'
    if resultado_validacion.get('error_critico_integridad'): status_final = 'ERROR_DUPLICADOS'
    elif resultado_validacion.get('es_vacio'): status_final = 'ERROR_VACIO'
    
    resultado = {
        'total': resultado_parseo.get('total_lineas', 0),
        'validos': resultado_validacion.get('lineas_validas_count', 0),
        'publicados': resultado_kafka.get('publicados', 0),
        'status': status_final
    }
    
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.callproc(
            "db_masterdatahub.sp_registrar_logs",
            (
                'procesamiento_archivo_maestra',
                tiempo_inicio,
                tiempo_fin,
                status_final,
                None,
                json.dumps(parametros),
                json.dumps(resultado),
                json.dumps({'file': archivo_real_nombre})
            )
        )

        conn.commit()
        cursor.close()
        logger.info("Audit Log OK")

    except Exception as e:
        logger.error(f"Error Audit: {e}")
    
    return {'registrado': True}


with DAG(
    dag_id='procesamiento_archivo_maestra',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2025, 11, 27),
    catchup=False,
    max_active_runs=15,
) as dag:
    
    t1 = PythonOperator(task_id='leer_y_parsear_archivo', python_callable=leer_y_parsear_archivo, provide_context=True)
    t2 = PythonOperator(task_id='validar_registros', python_callable=validar_registros, provide_context=True)
    t3 = PythonOperator(task_id='cargar_datos_postgres', python_callable=cargar_datos_postgres, provide_context=True)
    t4 = PythonOperator(task_id='publicar_en_kafka', python_callable=publicar_en_kafka, provide_context=True)
    t5 = PythonOperator(task_id='registrar_errores', python_callable=registrar_errores, provide_context=True, trigger_rule='all_done')
    t6 = PythonOperator(task_id='mover_archivo', python_callable=mover_archivo, provide_context=True, trigger_rule='all_done')
    t7 = PythonOperator(task_id='registrar_en_audit_log', python_callable=registrar_en_audit_log, provide_context=True, trigger_rule='all_done')
    
    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
