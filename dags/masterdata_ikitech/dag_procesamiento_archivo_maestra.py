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
from io import StringIO

from masterdata_ikitech.parsers.fixed_width_parser import FixedWidthParser
from masterdata_ikitech.validators.validators import DataValidator, get_validator_for_table

DEFAULT_ARGS = {
    'owner': 'ikitech',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
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
    
    if not archivo_nombre_base.lower().endswith('.txt'):
        msg = f"Error Crítico: El parámetro de entrada '{archivo_nombre_base}' no tiene extensión .txt."
        logger.error(msg)
        resultado_error = {
            'registros': [], 'errores': [msg], 'total_lineas': 0, 'lineas_validas': 0,
            'lineas_invalidas': 0, 'tabla_destino': None, 'archivo': archivo_nombre_base, 
            'es_vacio': True 
        }
        context['task_instance'].xcom_push(key='resultado_parseo', value=resultado_error)
        return resultado_error

    nombre_busqueda = Path(archivo_nombre_base).stem
    input_dir = Path(GCP_INPUT_PATH)
    
    logger.info(f"Buscando archivo: {nombre_busqueda} con .txt en {input_dir}")
    
    candidatos = list(input_dir.glob(f"{nombre_busqueda}*"))
    
    archivos_encontrados = [
        f for f in candidatos 
        if f.suffix.lower() == '.txt'
    ]
    
    if not archivos_encontrados:
        logger.warning(f"No se encontró ningún archivo .txt válido para {nombre_busqueda}. (Se ignoraron {len(candidatos) - len(archivos_encontrados)} archivos sin extensión correcta).")
        raise AirflowSkipException(f"Archivo .txt no encontrado: {nombre_busqueda}")
    
    archivo_real = archivos_encontrados[0]
    archivo_ruta = str(archivo_real)
    
    logger.info(f"Archivo encontrado y validado: {archivo_real.name}")
    
    # Guardamos referencias
    context['task_instance'].xcom_push(key='archivo_real_nombre', value=archivo_real.name)
    context['task_instance'].xcom_push(key='archivo_real_ruta', value=archivo_ruta)
    
    layouts_config_path = str(Path(__file__).resolve().parent / 'config' / 'layouts.json')
    parser = FixedWidthParser(layouts_config_path)
    
    # Validación de estructura interna
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

def preparar_staging_y_copy(**context):

    logger = logging.getLogger("airflow")
    ti = context['task_instance']

    # --- XCom INPUTS ---
    res_parseo = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')
    res_valid = ti.xcom_pull(task_ids='validar_registros', key='resultado_validacion')

    if not res_parseo or not res_valid:
        logger.error("No hay datos para staging.")
        ti.xcom_push("staging_status", {"ok": False, "error": "sin_datos"})
        return {"ok": False}

    # Si falló validación → NO hace staging
    if res_valid.get("error_critico_integridad", False):
        logger.warning("Validación crítica falló. Staging NO generado.")
        ti.xcom_push("staging_status", {"ok": False, "error": "validacion_critica"})
        return {"ok": False}

    registros = res_valid.get("registros_validos", [])
    if not registros:
        logger.warning("No hay registros válidos para staging.")
        ti.xcom_push("staging_status", {"ok": False, "error": "sin_validos"})
        return {"ok": False}

    # Tabla destino
    tabla_destino = normalizar_tabla(res_parseo["tabla_destino"].lower())
    schema_name, table_name = tabla_destino.split('.', 1)

    # Nombres temporales
    staging_full = f"{schema_name}.stg_{table_name}"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_full = f"{schema_name}.bkp_{table_name}_{timestamp}"

    # Conexión
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()

    try:
        logger.info(f"[STAGING] Creando schema si no existe: {schema_name}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

        # BACKUP
        logger.info(f"[BACKUP] Creando backup seguro: {backup_full}")
        cur.execute(f"CREATE TABLE {backup_full} AS TABLE {tabla_destino};")

        # STAGING
        logger.info(f"[STAGING] Creando tabla staging: {staging_full}")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {staging_full} (LIKE {tabla_destino} INCLUDING DEFAULTS INCLUDING GENERATED);")
        cur.execute(f"TRUNCATE {staging_full};")

        # Normalizar datos
        registros_norm = [
            {k.lower(): ("" if v is None else str(v)) for k, v in row.items()}
            for row in registros
        ]
        columnas = list(registros_norm[0].keys())

        # COPY
        buf = StringIO()
        for row in registros_norm:
            buf.write("\t".join(row[c] for c in columnas) + "\n")
        buf.seek(0)

        logger.info(f"[STAGING] COPY {len(registros_norm)} filas → {staging_full}")
        cur.execute(f"SET search_path TO {schema_name}, public;")
        cur.copy_from(buf, f"stg_{table_name}", sep="\t", columns=columnas)

        conn.commit()

        # XCom OUT
        ti.xcom_push("staging_table", staging_full)
        ti.xcom_push("backup_table", backup_full)
        ti.xcom_push("staging_columns", columnas)
        ti.xcom_push("staging_status", {"ok": True})

        logger.info("[STAGING] Preparación completada OK")
        return {"ok": True}

    except Exception as e:
        logger.error(f"[STAGING ERROR] {str(e)}")
        conn.rollback()

        ti.xcom_push("staging_status", {"ok": False, "error": str(e)})
        return {"ok": False}

    finally:
        cur.close()

def cargar_datos_postgres(**context):

    logger = logging.getLogger("airflow")
    ti = context['task_instance']

    # Estatus de staging
    staging_status = ti.xcom_pull(task_ids='preparar_staging_y_copy', key='staging_status') or {}
    if not staging_status.get("ok", False):
        logger.warning("[POSTGRES] Swap cancelado: staging/copy falló.")
        ti.xcom_push('postgres_swap_status', {'ok': False, 'reason': 'staging_failed'})
        return {'ok': False}

    # Datos del parseo
    res_parseo = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')
    tabla_destino = normalizar_tabla(res_parseo["tabla_destino"].lower())

    staging = ti.xcom_pull(task_ids='preparar_staging_y_copy', key='staging_table')
    backup_table = ti.xcom_pull(task_ids='preparar_staging_y_copy', key='backup_table')
    columnas = ti.xcom_pull(task_ids='preparar_staging_y_copy', key='staging_columns')

    if not staging or not backup_table or not columnas:
        logger.error("[POSTGRES] Información de staging/backup incompleta.")
        ti.xcom_push('postgres_swap_status', {'ok': False, 'reason': 'missing_info'})
        return {'ok': False}

    col_str = ", ".join(columnas)

    # Conexión
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()

    swap_ok = False

    try:
        logger.info(f"[POSTGRES] Iniciando Transacción de SWAP en {tabla_destino}")

        cur.execute("BEGIN;")
        cur.execute(f"LOCK TABLE {tabla_destino} IN ACCESS EXCLUSIVE MODE;")
        
        # --- LOG NUEVO PARA VER EL TRUNCATE ---
        logger.info(f"[POSTGRES CRITICAL] Ejecutando TRUNCATE de tabla productiva: {tabla_destino}")
        cur.execute(f"TRUNCATE {tabla_destino};")

        # --- CARGA REAL ---
        logger.info(f"[POSTGRES] INSERT FROM {staging} → {tabla_destino}")
        cur.execute(
                f"INSERT INTO {tabla_destino} ({col_str}) "
                f"SELECT {col_str} FROM {staging};"
        )
        
        # PRUEBA FORZADA DEL ROLLBACK (activar para pruebas)
        
        #logger.info("[TEST] Forzando error para probar rollback...")
        #cur.execute("SELECT * FROM tabla_que_no_existe;")

        cur.execute("COMMIT;")
        swap_ok = True
        logger.info("[POSTGRES] Swap completado correctamente")

        ti.xcom_push('postgres_swap_status', {'ok': True})

    except Exception as e:
        logger.error(f"[POSTGRES ERROR] {str(e)}")
        conn.rollback()

        logger.info("[ROLLBACK] Restaurando destino desde backup de emergencia")

        try:
            cur.execute(f"TRUNCATE {tabla_destino};")
            cur.execute(f"INSERT INTO {tabla_destino} SELECT * FROM {backup_table};")
            conn.commit()
            logger.info("[ROLLBACK] Restauración completada.")
        except Exception as e2:
            logger.error(f"[ROLLBACK FATAL] No se pudo restaurar backup: {str(e2)}")

        ti.xcom_push('postgres_swap_status', {'ok': False, 'error': str(e)})

    finally:
        # --- CLEANUP DE STAGING Y BACKUP ---
        try:
            if staging:
                logger.info(f"[CLEANUP] Eliminando staging {staging}")
                cur.execute(f"DROP TABLE IF EXISTS {staging};")
        except Exception as e:
            logger.error(f"[CLEANUP ERROR] No se pudo eliminar staging: {str(e)}")

        try:
            if backup_table:
                logger.info(f"[CLEANUP] Eliminando backup {backup_table}")
                cur.execute(f"DROP TABLE IF EXISTS {backup_table};")
        except Exception as e:
            logger.error(f"[CLEANUP ERROR] No se pudo eliminar backup: {str(e)}")

        conn.commit()
        cur.close()

    return {'ok': swap_ok}


def publicar_en_kafka(**context):
    ti = context['task_instance']

    resultado_parseo = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')
    resultado_validacion = ti.xcom_pull(task_ids='validar_registros', key='resultado_validacion')
    archivo_real_nombre = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='archivo_real_nombre')

    staging_status = ti.xcom_pull(task_ids='preparar_staging_y_copy', key='staging_status') or {}
    postgres_status = ti.xcom_pull(task_ids='cargar_datos_postgres', key='postgres_swap_status') or {}

    # Si staging falló → NO Kafka
    if not staging_status.get("ok", True):
        logger.warning("Publicación a Kafka cancelada porque staging/copy falló.")
        ti.xcom_push('resultado_kafka', {'publicados': 0, 'motivo': 'staging_failed'})
        return None

    # Si el swap falló → NO Kafka
    if postgres_status and not postgres_status.get("ok", True):
        logger.warning("Publicación a Kafka cancelada porque swap en Postgres falló.")
        ti.xcom_push('resultado_kafka', {'publicados': 0, 'motivo': 'postgres_failed'})
        return None

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

        resultado = {
            'topic': kafka_topic,
            'publicados': len(registros_validos),
            'archivo': archivo_real_nombre
        }
        ti.xcom_push(key='resultado_kafka', value=resultado)
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
    ti = context['task_instance']

    staging_status = ti.xcom_pull(
        task_ids='preparar_staging_y_copy',
        key='staging_status'
    ) or {}

    postgres_status = ti.xcom_pull(
        task_ids='cargar_datos_postgres',
        key='postgres_swap_status'
    ) or {}

    archivo_real_nombre = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='archivo_real_nombre')
    archivo_real_ruta = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='archivo_real_ruta')
    res_parseo = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo') or {}
    res_validacion = ti.xcom_pull(task_ids='validar_registros', key='resultado_validacion') or {}

    if not archivo_real_nombre or not archivo_real_ruta:
        return None

    es_vacio = res_parseo.get('es_vacio', False)
    cantidad_validos = res_validacion.get('lineas_validas_count', 0)
    error_integridad = res_validacion.get('error_critico_integridad', False)

    # --- PRIORIDAD DE ERRORES ---
    if not staging_status.get("ok", True):
        destino_path = GCP_ERROR_PATH
        estado = 'error_staging'
        logger.error(f"STAGING/COPY falló para {archivo_real_nombre} -> ERROR")
    elif postgres_status and not postgres_status.get("ok", True):
        destino_path = GCP_ERROR_PATH
        estado = 'error_postgres_swap'
        logger.error(f"Swap Postgres falló para {archivo_real_nombre} -> ERROR")
    elif es_vacio:
        destino_path = GCP_ERROR_PATH
        estado = 'error_vacio'
        logger.error(f"Archivo VACÍO {archivo_real_nombre} -> ERROR")
    elif error_integridad:
        destino_path = GCP_ERROR_PATH
        estado = 'error_integridad_duplicados'
        logger.error(f"Archivo con DUPLICADOS {archivo_real_nombre} -> ERROR")
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
        # Si quieres que NUNCA marque rojo, no hagas raise aquí:
        # raise
        return {'estado': 'error_move', 'error': str(e)}


def registrar_en_audit_log(**context):
    ti = context['task_instance']
    
    archivo_real_nombre = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='archivo_real_nombre')
    resultado_parseo = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')
    
    if not resultado_parseo: return {'registrado': False}

    resultado_validacion = ti.xcom_pull(task_ids='validar_registros', key='resultado_validacion') or {}
    resultado_kafka = ti.xcom_pull(task_ids='publicar_en_kafka', key='resultado_kafka') or {}
    staging_status = ti.xcom_pull(task_ids='preparar_staging_y_copy', key='staging_status') or {}
    postgres_status = ti.xcom_pull(task_ids='cargar_datos_postgres', key='postgres_swap_status') or {}

    tiempo_inicio = context.get('data_interval_start') or context.get('execution_date')
    if hasattr(tiempo_inicio, '_get_current_object'):
        tiempo_inicio = tiempo_inicio._get_current_object()
    tiempo_fin = datetime.now(tiempo_inicio.tzinfo)

    status_final = 'OK'
    error_detalle = None

    if staging_status and not staging_status.get('ok', True):
        status_final = 'ERROR_STAGING'
        error_detalle = staging_status.get('error')
    elif postgres_status and not postgres_status.get('ok', True):
        status_final = 'ERROR_POSTGRES'
        error_detalle = postgres_status.get('error') or postgres_status.get('reason')
    elif resultado_validacion.get('error_critico_integridad'):
        status_final = 'ERROR_DUPLICADOS'
    elif resultado_validacion.get('es_vacio'):
        status_final = 'ERROR_VACIO'

    parametros = json.dumps({
        'archivo': archivo_real_nombre,
        'tabla': resultado_parseo.get('tabla_destino'),
        'run': context['dag_run'].run_id
    })
    
    info_adicional = json.dumps({'file': archivo_real_nombre})

    resultado_json = json.dumps({
        'total': resultado_parseo.get('total_lineas', 0),
        'validos': resultado_validacion.get('lineas_validas_count', 0),
        'publicados': resultado_kafka.get('publicados', 0),
        'status': status_final
    })

    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Agregamos ::timestamp y ::jsonb para que coincida con la firma del SP
        sql_call = """
            CALL db_masterdatahub.sp_registrar_logs(
                %s::text, 
                %s::timestamp, 
                %s::timestamp, 
                %s::text, 
                %s::text, 
                %s::jsonb, 
                %s::jsonb, 
                %s::jsonb
            )
        """
        
        cursor.execute(sql_call, (
            'procesamiento_archivo_maestra',  # 1. text
            tiempo_inicio,                    # 2. timestamp
            tiempo_fin,                       # 3. timestamp
            status_final,                     # 4. text
            error_detalle,                    # 5. text
            parametros,                       # 6. jsonb
            resultado_json,                   # 7. jsonb
            info_adicional                    # 8. jsonb
        ))

        conn.commit()
        cursor.close()
        logger.info(f"Audit Log registrado correctamente. Estado: {status_final}")

    except Exception as e:
        logger.error(f"Error llamando a SP Audit: {e}")
    
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
    t3_pre = PythonOperator(task_id='preparar_staging_y_copy',python_callable=preparar_staging_y_copy,provide_context=True)
    t3 = PythonOperator(task_id='cargar_datos_postgres',python_callable=cargar_datos_postgres,provide_context=True)
    t4 = PythonOperator(task_id='publicar_en_kafka', python_callable=publicar_en_kafka, provide_context=True)
    t5 = PythonOperator(task_id='registrar_errores', python_callable=registrar_errores, provide_context=True, trigger_rule='all_done')
    t6 = PythonOperator(task_id='mover_archivo', python_callable=mover_archivo, provide_context=True, trigger_rule='all_done')
    t7 = PythonOperator(task_id='registrar_en_audit_log', python_callable=registrar_en_audit_log, provide_context=True, trigger_rule='all_done')
    
    t1 >> t2 >> t3_pre >> t3 >> t4 >> t5 >> t6 >> t7
