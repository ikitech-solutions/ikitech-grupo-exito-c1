""""
DAG Hijo: Procesamiento de Archivo Individual de Maestra

Este DAG procesa un archivo individual de maestra:
1. Lee el archivo fixed-width o delimitado
2. Parsea y valida los registros
3. Publica en Kafka (mensajes en formato TOON)
4. Mueve el archivo a /processed/ o /error/
5. Registra en audit_log

Autor: IkiTech
Fecha: 2025-11-27
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException, AirflowException
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from io import StringIO
from pathlib import Path
from kafka import KafkaProducer
from masterdata_ikitech.utils.gcs_utils import export_to_parquet_and_upload
import logging
import json
import shutil
import hashlib
import re
import oracledb

from masterdata_ikitech.parsers.fixed_width_parser import FixedWidthParser
from masterdata_ikitech.validators.validators import DataValidator, get_validator_for_table
from masterdata_ikitech.utils.toon_encoder import encode as toon_encode

from toon_format import decode as toon_decode

DEFAULT_ARGS = {
    'owner': 'ikitech',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

GCS_BUCKET = Variable.get("gcs_bucket_masterdata", default_var="bucket-grupoexito")
GCS_CONN_ID = Variable.get("gcs_conn_id", default_var="gcs_masterdata")
GCP_INPUT_PATH = Variable.get("gcp_input_path", "/input/")
GCP_PROCESSED_PATH = Variable.get("gcp_processed_path", "/processed/")
GCP_ERROR_PATH = Variable.get("gcp_error_path", "/error/")
KAFKA_BOOTSTRAP_SERVERS = Variable.get("kafka_bootstrap_servers", "kafka:9092")
POSTGRES_CONN_ID = Variable.get("postgres_conn_id", "postgres_masterdata")

# Layouts de maestras en formato TOON desde Variables
LAYOUTS_TOON = Variable.get("maestras_layouts_toon", default_var=None)
LAYOUTS_DICT = toon_decode(LAYOUTS_TOON) if LAYOUTS_TOON else {}

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
    'db_masterdatahub.erp_segmentos': [],
    'db_masterdatahub.erp_subcategoria': []
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
        v = record.get(k) or record.get(k.upper()) or record.get(k.lower())
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
        logger.warning(f"No se encontró ningún archivo .txt válido para {nombre_busqueda}.")
        raise AirflowSkipException(f"Archivo .txt no encontrado: {nombre_busqueda}")

    archivo_real = archivos_encontrados[0]
    archivo_ruta = str(archivo_real)

    logger.info(f"Archivo encontrado: {archivo_real.name}")

    context['task_instance'].xcom_push(key='archivo_real_nombre', value=archivo_real.name)
    context['task_instance'].xcom_push(key='archivo_real_ruta', value=archivo_ruta)

    # Parser basado en layouts TOON cargados desde Variable
    parser = FixedWidthParser(layouts=LAYOUTS_DICT)

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

    # Normalizamos schema db_masterdata_hub -> db_masterdatahub para que machee con PK_MAPPING
    tabla = resultado.get('tabla_destino')
    if tabla:
        resultado['tabla_destino'] = tabla.replace('db_masterdata_hub', 'db_masterdatahub')

    if resultado.get('es_vacio'):
        logger.error(f"Archivo VACÍO: {archivo_real.name}.")
    else:
        logger.info(f"Parseo OK: {resultado.get('lineas_validas', 0)} registros.")

    if resultado.get('lineas_invalidas', 0) > 0:
        logger.warning(f"{resultado['lineas_invalidas']} registros con error.")

    context['task_instance'].xcom_push(key='resultado_parseo', value=resultado)
    return resultado


def validar_registros(**context):
    ti = context['task_instance']
    resultado_parseo = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')
    if not resultado_parseo:
        return None
    if resultado_parseo.get('es_vacio'):
        return {'lineas_validas_count': 0, 'es_vacio': True}

    registros = resultado_parseo['registros']
    tabla_destino = resultado_parseo['tabla_destino']

    logger.info(f"Validando {len(registros)} registros para {tabla_destino}")

    validator_func = get_validator_for_table(tabla_destino)
    pks = PK_MAPPING.get(normalizar_tabla(tabla_destino), [])

    registros_validos = []
    registros_invalidos = []

    seen_hashes = set()
    seen_pks = set()
    error_duplicidad_detectado = False

    for idx, record in enumerate(registros):

        # --- Duplicados exactos ---
        if pks:
            canonical = cadena_registro_canónico(record)
            row_hash = hashlib.sha256(canonical.encode('utf-8')).hexdigest()

            if row_hash in seen_hashes:
                msg = f"Registro DUPLICADO EXACTO detectado en línea {idx+1}"
                logger.error(msg)  # Error real de integridad
                registros_invalidos.append({'idx': idx, 'err': [msg]})
                error_duplicidad_detectado = True
                continue
            seen_hashes.add(row_hash)

            # --- PK duplicada ---
            pk_values = []
            for pk in pks:
                v = record.get(pk) or record.get(pk.upper()) or record.get(pk.lower())
                pk_values.append(normalizar_valores(v))
            pk_tuple = tuple(pk_values)

            if pk_tuple in seen_pks:
                msg = f"PK DUPLICADA {pk_tuple} detectada en línea {idx+1}"
                logger.error(msg)  # Error real de integridad
                registros_invalidos.append({'idx': idx, 'err': [msg]})
                error_duplicidad_detectado = True
                continue
            seen_pks.add(pk_tuple)

        # --- Validación de tipos ---
        if validator_func:
            try:
                es_valido, errores = validator_func(record)
            except Exception as e:
                msg = f"ERROR VALIDANDO TIPO en línea {idx+1}: {str(e)}"
                logger.error(msg)
                registros_invalidos.append({'idx': idx, 'err': [msg]})
                error_duplicidad_detectado = True
                continue

            if es_valido:
                registros_validos.append(record)
            else:
                registros_invalidos.append({'idx': idx, 'err': errores})
                logger.error(f"Registro rechazado línea {idx+1}: {errores}")
        else:
            registros_validos.append(record)

    if registros_invalidos:
        logger.warning(f"{len(registros_invalidos)} registros rechazados.")

    total = len(registros)
    validos = len(registros_validos)
    porcentaje = (validos / total) * 100 if total > 0 else 0

    VALID_PERCENTAGE = float(Variable.get("valid_percentage", "100"))
    error_tasa = porcentaje < VALID_PERCENTAGE

    error_archivo = error_duplicidad_detectado or error_tasa or len(registros_invalidos) > 0

    resultado_validacion = {
        'registros_validos': registros_validos,
        'registros_invalidos': registros_invalidos,
        'lineas_validas_count': validos,
        'es_vacio': False,
        'error_critico_integridad': error_archivo
    }

    ti.xcom_push(key='resultado_validacion', value=resultado_validacion)

    if error_archivo:
        logger.error(
            f"Validación crítica falló para {tabla_destino}. "
            f"Total={total}, válidos={validos}, rechazados={len(registros_invalidos)}. "
            "El archivo será rechazado y la tarea se marcará como FAILED."
        )
        raise AirflowException(
            "Error crítico de integridad en validación de registros "
            "(duplicados / porcentaje mínimo / registros inválidos)."
        )

    return resultado_validacion


def preparar_staging_y_copy(**context):

    logger = logging.getLogger("airflow")
    ti = context['task_instance']

    res_parseo = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')
    res_valid = ti.xcom_pull(task_ids='validar_registros', key='resultado_validacion')

    if not res_parseo or not res_valid:
        logger.error("No hay datos para staging.")
        ti.xcom_push("staging_status", {"ok": False, "error": "sin_datos"})
        # Error de flujo: marca la tarea como FAILED
        raise AirflowException("No hay datos para staging (XCom ausente).")

    if res_valid.get("error_critico_integridad", False):
        logger.error("Validación crítica falló. Staging NO generado.")
        ti.xcom_push("staging_status", {"ok": False, "error": "validacion_critica"})
        raise AirflowException("Validación crítica falló. Staging no se generó.")

    registros = res_valid.get("registros_validos", [])
    if not registros:
        logger.error("No hay registros válidos para staging.")
        ti.xcom_push("staging_status", {"ok": False, "error": "sin_validos"})
        raise AirflowException("No hay registros válidos para staging.")

    tabla_destino = normalizar_tabla(res_parseo["tabla_destino"].lower())
    schema_name, table_name = tabla_destino.split('.', 1)

    staging_full = f"{schema_name}.stg_{table_name}"

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()

    try:
        # --- STAGING ---
        logger.info(f"[STAGING] Creando tabla staging: {staging_full}")

        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {staging_full} "
            f"(LIKE {tabla_destino} INCLUDING DEFAULTS INCLUDING GENERATED);"
        )
        cur.execute(f"TRUNCATE {staging_full};")

        registros_norm = [
            {k.lower(): ("" if v is None else str(v)) for k, v in row.items()}
            for row in registros
        ]
        columnas = list(registros_norm[0].keys())

        buf = StringIO()
        for row in registros_norm:
            buf.write("\t".join(row[c] for c in columnas) + "\n")
        buf.seek(0)

        logger.info(f"[STAGING] COPY {len(registros_norm)} filas → {staging_full}")
        cur.execute(f"SET search_path TO {schema_name}, public;")
        cur.copy_from(buf, f"stg_{table_name}", sep="\t", columns=columnas)

        conn.commit()

        ti.xcom_push("staging_table", staging_full)
        ti.xcom_push("staging_columns", columnas)
        ti.xcom_push("staging_status", {"ok": True})

        logger.info("[STAGING] Preparación completada OK")
        return {"ok": True}

    except Exception as e:
        logger.exception(f"[STAGING ERROR] {str(e)}")
        conn.rollback()
        ti.xcom_push("staging_status", {"ok": False, "error": str(e)})
        # Error real: la tarea debe quedar en FAILED
        raise AirflowException(f"Error en preparación de staging: {e}")

    finally:
        cur.close()


def cargar_datos_postgres(**context):

    logger = logging.getLogger("airflow")
    ti = context['task_instance']

    # Validación previa: staging debe haber sido exitoso
    staging_status = ti.xcom_pull(task_ids='preparar_staging_y_copy', key='staging_status') or {}
    if not staging_status.get("ok", False):
        logger.error("[POSTGRES] Swap cancelado: staging falló.")
        ti.xcom_push('postgres_swap_status', {'ok': False, 'reason': 'staging_failed'})
        raise AirflowException("Swap en Postgres cancelado por fallo previo en staging.")

    # Datos del parseo
    res_parseo = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')
    tabla_destino = normalizar_tabla(res_parseo["tabla_destino"].lower())

    staging = ti.xcom_pull(task_ids='preparar_staging_y_copy', key='staging_table')
    columnas = ti.xcom_pull(task_ids='preparar_staging_y_copy', key='staging_columns')

    if not staging or not columnas:
        logger.error("[POSTGRES] Información de staging incompleta.")
        ti.xcom_push('postgres_swap_status', {'ok': False, 'reason': 'missing_staging'})
        raise AirflowException("Información de staging incompleta en carga a Postgres.")

    col_str = ", ".join(columnas)

    # Conexión
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()

    try:
        logger.info(f"[POSTGRES] Iniciando transacción atómica de SWAP en {tabla_destino}")

        cur.execute("BEGIN;")
        cur.execute(f"LOCK TABLE {tabla_destino} IN ACCESS EXCLUSIVE MODE;")

        #TRUNCATE dentro de la transacción (rollback-safe)
        logger.info(f"[POSTGRES] TRUNCATE productivo → {tabla_destino}")
        cur.execute(f"TRUNCATE {tabla_destino};")

        #Carga real
        logger.info(f"[POSTGRES] INSERT desde staging → {tabla_destino}")
        cur.execute(
            f"INSERT INTO {tabla_destino} ({col_str}) "
            f"SELECT {col_str} FROM {staging};"
        )
        
        # Forzar error para pruebas
        #tabla_destino_fake = f"{tabla_destino}_fake_no_existe"

        #logger.error(f"[POSTGRES] FORZANDO ERROR → insertando en tabla inexistente: {tabla_destino_fake}")

        #cur.execute(
        #    f"INSERT INTO {tabla_destino_fake} ({col_str}) "
        #    f"SELECT {col_str} FROM {staging};"
        #)
        
        # Commit atómico
        cur.execute("COMMIT;")
        logger.info("[POSTGRES] Swap completado correctamente")

        ti.xcom_push('postgres_swap_status', {'ok': True})
        swap_ok = True

    except Exception as e:
        logger.exception(f"[POSTGRES ERROR] {str(e)}")
        conn.rollback()

        logger.warning("[POSTGRES] ROLLBACK ejecutado. Tabla productiva restaurada automáticamente.")
        ti.xcom_push('postgres_swap_status', {'ok': False, 'error': str(e)})
        swap_ok = False
        raise

    finally:
        # Cleanup
        try:
            if staging:
                logger.info(f"[CLEANUP] Eliminando staging {staging}")
                cur.execute(f"DROP TABLE IF EXISTS {staging};")
        except Exception as e:
            logger.error(f"[CLEANUP ERROR] {str(e)}")

        conn.commit()
        cur.close()

    return {'ok': swap_ok}


def cargar_datos_oracle(**context):
    logger = logging.getLogger("airflow")
    ti = context['task_instance']

    # --- Obtener Datos ---
    res_valid = ti.xcom_pull(task_ids='validar_registros', key='resultado_validacion')
    res_parseo = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')

    if not res_valid or not res_parseo:
        logger.info("[ORACLE] No hay datos para procesar.")
        return

    registros = res_valid.get('registros_validos', [])
    if not registros:
        logger.warning("[ORACLE] 0 registros válidos. No se realizará carga.")
        return

    # --- Definir Nombre Base de Tabla (por maestra) ---
    tabla_base = res_parseo['tabla_destino'].split('.')[-1].upper().replace('ERP_', '')

    # --- Preparar Datos ---
    fecha_carga = datetime.now()
    datos_bind = []

    for r in registros:
        new_row = {k.upper(): v for k, v in r.items()}
        # Inyectar FEMOD si falta
        if 'FEMOD' not in new_row or not new_row['FEMOD']:
            new_row['FEMOD'] = fecha_carga
        datos_bind.append(new_row)

    # Columnas finales
    columnas_upper = list(datos_bind[0].keys())
    cols_str = ", ".join(columnas_upper)
    vals_str = ", ".join([f":{c}" for c in columnas_upper])

    sql_insert_staging = f"INSERT INTO {{tabla_staging}} ({cols_str}) VALUES ({vals_str})"

    # --- Conexión a Oracle ---
    conn_af = BaseHook.get_connection("oracle_masterdata")

    try:
        dsn = f"{conn_af.host}:{conn_af.port}/{conn_af.extra_dejson.get('service_name', 'FREE')}"

        with oracledb.connect(user=conn_af.login, password=conn_af.password, dsn=dsn) as conn:
            with conn.cursor() as cursor:

                schema = conn_af.login.upper()
                tabla_real = f"{schema}.ERP_{tabla_base}"
                tabla_staging = f"{schema}.STG_{tabla_base}"

                logger.info(f"[ORACLE] Destino: {tabla_real} | Staging: {tabla_staging}")

                # --- Asegurar que las tablas existen (auto-bootstrap) ---
                cols_def = ", ".join(f"{c} VARCHAR2(4000)" for c in columnas_upper)

                for nombre_tabla in (tabla_real, tabla_staging):
                    try:
                        cursor.execute(f"SELECT 1 FROM {nombre_tabla} WHERE 1 = 0")
                    except oracledb.DatabaseError as e:
                        error_obj, = e.args
                        if error_obj.code == 942:
                            logger.info(f"[ORACLE] Creando tabla {nombre_tabla} dinámicamente")
                            cursor.execute(f"CREATE TABLE {nombre_tabla} ({cols_def})")
                        else:
                            raise

                # --- Limpiar Staging ---
                logger.info(f"[ORACLE] Limpiando Staging: {tabla_staging}")
                cursor.execute(f"TRUNCATE TABLE {tabla_staging}")

                # --- Carga Masiva a Staging ---
                logger.info(f"[ORACLE] Cargando {len(datos_bind)} filas a Staging...")
                cursor.executemany(
                    sql_insert_staging.format(tabla_staging=tabla_staging),
                    datos_bind,
                    batcherrors=True
                )

                errores_batch = cursor.getbatcherrors()
                if errores_batch:
                    logger.warning(f"[ORACLE] Errores en carga Staging: {len(errores_batch)}")
                    for e in errores_batch[:3]:
                        logger.warning(f"[ORACLE] Batch error: {e.message}")

                # --- SWAP: TRUNCATE Real + INSERT /*+ APPEND */ ---
                logger.info(f"[ORACLE] Truncando tabla Real: {tabla_real}")
                cursor.execute(f"TRUNCATE TABLE {tabla_real}")

                logger.info(f"[ORACLE] Moviendo datos Staging -> Real (Direct Path)")
                sql_swap = f"""
                    INSERT /*+ APPEND */ INTO {tabla_real} ({cols_str})
                    SELECT {cols_str} FROM {tabla_staging}
                """
                cursor.execute(sql_swap)

                logger.info(f"[ORACLE] Limpieza Final: Vaciando {tabla_staging}")
                cursor.execute(f"TRUNCATE TABLE {tabla_staging}")

                # Commit Final
                conn.commit()
                logger.info(f"[ORACLE] ¡Éxito! Carga completada y Staging limpio. Tabla final: {tabla_real}")

    except Exception as e:
        logger.exception(f"[ORACLE FATAL] Error en persistencia: {e}")
        raise

    return {'ok': True, 'oracle_table': tabla_real}


def publicar_en_kafka(**context):
    ti = context['task_instance']

    # --- Recuperación de Datos y Estados ---
    resultado_parseo = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')
    resultado_validacion = ti.xcom_pull(task_ids='validar_registros', key='resultado_validacion')
    archivo_real_nombre = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='archivo_real_nombre')

    staging_status = ti.xcom_pull(task_ids='preparar_staging_y_copy', key='staging_status') or {}
    postgres_status = ti.xcom_pull(task_ids='cargar_datos_postgres', key='postgres_swap_status') or {}

    # --------------------------
    # VALIDACIONES (circuit breakers)
    # --------------------------
    if not staging_status.get("ok", True):
        logger.warning("Kafka cancelado: falla en staging/copy.")
        ti.xcom_push('resultado_kafka', {'publicados': 0, 'motivo': 'staging_failed'})
        return None

    if postgres_status and not postgres_status.get("ok", True):
        logger.warning("Kafka cancelado: falla en swap de Postgres.")
        ti.xcom_push('resultado_kafka', {'publicados': 0, 'motivo': 'postgres_failed'})
        return None

    if not resultado_parseo or not resultado_validacion:
        return None

    if resultado_validacion.get('es_vacio'):
        return None

    if resultado_validacion.get('error_critico_integridad'):
        logger.warning("Kafka cancelado por error crítico de integridad.")
        return None

    registros_validos = resultado_validacion.get('registros_validos', [])
    tabla_destino = resultado_parseo['tabla_destino']

    if not registros_validos:
        logger.info("No hay registros válidos para Kafka.")
        return None

    # --------------------------
    # TOPIC DESDE TOON
    # --------------------------
    kafka_topic = resultado_parseo.get('kafka_topic')

    if not kafka_topic:
        logger.info(f"[KAFKA] Layout sin kafka_topic -> No se publicará nada. Archivo={archivo_real_nombre}")
        ti.xcom_push('resultado_kafka', {
            'publicados': 0,
            'motivo': 'no_kafka_topic_en_layout',
            'archivo': archivo_real_nombre
        })
        return None

    # --------------------------
    # CAMPOS CLAVE PARA LOG COMPACTION
    # --------------------------
    campos_clave = resultado_parseo.get('campos_clave') or []

    logger.info(f"[KAFKA] Publicando archivo={archivo_real_nombre} -> topic={kafka_topic}")
    logger.info(f"Campos clave usados para la key: {campos_clave}")

    # --------------------------
    # ENVÍO A KAFKA
    # --------------------------
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            key_serializer=lambda k: None if k is None else str(k).encode('utf-8'),
            value_serializer=lambda v: toon_encode(v).encode('utf-8'),
            api_version=(0, 10, 2),
            client_id='airflow-producer'
        )

        publicados = 0

        for reg in registros_validos:

            if campos_clave:
                key_parts = []
                for pk in campos_clave:
                    val = reg.get(pk) or reg.get(pk.upper()) or reg.get(pk.lower())
                    key_parts.append("" if val is None else str(val).strip())
                kafka_key = "|".join(key_parts)
            else:
                kafka_key = None

            key = kafka_key.encode("utf-8") if kafka_key else None
            producer.send(kafka_topic, key=key, value=reg)
            publicados += 1

        producer.flush()
        producer.close()

        logger.info(f"[KAFKA] Envío exitoso: {publicados} mensajes al tópico {kafka_topic} (formato TOON).")

        resultado = {
            'topic': kafka_topic,
            'publicados': publicados,
            'archivo': archivo_real_nombre
        }

        ti.xcom_push(key='resultado_kafka', value=resultado)
        return resultado

    except Exception as e:
        logger.exception(f"[ERROR KAFKA] Fallo al enviar mensajes: {e}")
        raise


def generar_parquet_y_subir_a_gcs(**context):
    ti = context["task_instance"]

    # Recuperar estado y datos del pipeline
    resultado_parseo = ti.xcom_pull(
        task_ids="leer_y_parsear_archivo",
        key="resultado_parseo"
    ) or {}

    resultado_validacion = ti.xcom_pull(
        task_ids="validar_registros",
        key="resultado_validacion"
    ) or {}

    archivo_real_nombre = ti.xcom_pull(
        task_ids="leer_y_parsear_archivo",
        key="archivo_real_nombre"
    ) or "desconocido"

    # Registros ya validados
    registros_validos = resultado_validacion.get("registros_validos") or []
    tabla_destino = resultado_parseo.get("tabla_destino") or "tabla_desconocida"

    if not registros_validos:
        logger.info("[GCS] No hay registros válidos; no se genera parquet para GCS.")
        ti.xcom_push(
            key="resultado_gcs_parquet",
            value={
                "uploaded": False,
                "rows": 0,
                "motivo": "sin_registros",
                "tabla_destino": tabla_destino,
                "archivo": archivo_real_nombre,
            },
        )
        return

    nombre_tabla = tabla_destino.split(".")[-1].lower()
    object_name = f"masterdata/{nombre_tabla}/{archivo_real_nombre}.parquet"

    logger.info(
        f"[GCS] Generando parquet para tabla '{tabla_destino}' "
        f"archivo='{archivo_real_nombre}' -> gs://{GCS_BUCKET}/{object_name}"
    )

    resultado = export_to_parquet_and_upload(
        registros=registros_validos,
        bucket_name=GCS_BUCKET,
        object_name=object_name,
        gcp_conn_id=GCS_CONN_ID,
    )

    resultado.update(
        {
            "tabla_destino": tabla_destino,
            "archivo": archivo_real_nombre,
        }
    )

    ti.xcom_push(key="resultado_gcs_parquet", value=resultado)
    return resultado



def registrar_errores(**context):
    resultado_parseo = context['task_instance'].xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo') or {}
    resultado_validacion = context['task_instance'].xcom_pull(task_ids='validar_registros', key='resultado_validacion') or {}
    total = len(errores_parseo := resultado_parseo.get('errores', [])) + len(registros_invalidos := resultado_validacion.get('registros_invalidos', []))
    if total > 0:
        logger.warning(f"Registrando {total} errores.")
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
        logger.error("Moviendo a ERROR (0 registros válidos).")
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
        logger.exception(f"Error moviendo: {e}")
        return {'estado': 'error_move', 'error': str(e)}


def registrar_en_audit_log(**context):
    ti = context['task_instance']

    # --- Recuperación de XComs ---
    archivo_real_nombre = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='archivo_real_nombre')
    resultado_parseo = ti.xcom_pull(task_ids='leer_y_parsear_archivo', key='resultado_parseo')

    if not resultado_parseo:
        return {'registrado': False}

    resultado_validacion = ti.xcom_pull(task_ids='validar_registros', key='resultado_validacion') or {}
    resultado_kafka = ti.xcom_pull(task_ids='publicar_en_kafka', key='resultado_kafka') or {}
    staging_status = ti.xcom_pull(task_ids='preparar_staging_y_copy', key='staging_status') or {}
    postgres_status = ti.xcom_pull(task_ids='cargar_datos_postgres', key='postgres_swap_status') or {}

    # --- Recuperar estado de Oracle ---
    oracle_state = 'unknown'
    try:
        oracle_ti = context['dag_run'].get_task_instance('cargar_datos_oracle')
        if oracle_ti:
            oracle_state = oracle_ti.state
    except Exception:
        pass

    tiempo_inicio = context.get('data_interval_start') or context.get('execution_date')
    if hasattr(tiempo_inicio, '_get_current_object'):
        tiempo_inicio = tiempo_inicio._get_current_object()
    tiempo_fin = datetime.now(tiempo_inicio.tzinfo)

    status_final = 'OK'
    error_detalle = None

    # --- Lógica de prioridad de errores ---
    if staging_status and not staging_status.get('ok', True):
        status_final = 'ERROR_STAGING'
        error_detalle = staging_status.get('error')
    elif postgres_status and not postgres_status.get('ok', True):
        status_final = 'ERROR_POSTGRES'
        error_detalle = postgres_status.get('error') or postgres_status.get('reason')
    elif oracle_state == 'failed':
        status_final = 'ERROR_ORACLE'
        error_detalle = "Fallo crítico durante la persistencia en Oracle (ver logs de tarea)"
    elif resultado_validacion.get('error_critico_integridad'):
        status_final = 'ERROR_DUPLICADOS'
    elif resultado_validacion.get('es_vacio'):
        status_final = 'ERROR_VACIO'

    # --- Preparación de JSONs ---
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
        'status': status_final,
        'oracle_state': oracle_state
    })

    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

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
            'procesamiento_archivo_maestra',
            tiempo_inicio,
            tiempo_fin,
            status_final,
            error_detalle,
            parametros,
            resultado_json,
            info_adicional
        ))

        conn.commit()
        cursor.close()
        logger.info(f"Audit Log registrado correctamente en Postgres. Estado: {status_final}")

    except Exception as e:
        logger.exception(f"Error llamando a SP Audit en Postgres: {e}")
    try:
        conn_af = BaseHook.get_connection("oracle_masterdata")
        dsn = f"{conn_af.host}:{conn_af.port}/{conn_af.extra_dejson.get('service_name', 'FREE')}"

        with oracledb.connect(user=conn_af.login, password=conn_af.password, dsn=dsn) as conn:
            with conn.cursor() as cursor:

                sql_oracle = """
                    INSERT INTO AUDIT_LOG (
                        PROCEDURE_NAME,
                        EXECUTION_TIME,
                        END_TIME,
                        USER_NAME,
                        RESULT_STATUS,
                        ERROR_MESSAGE,
                        PARAMETERS,
                        RESULT,
                        DURATION,
                        ADDITIONAL_INFO
                    ) VALUES (
                        :procedure_name,
                        :execution_time,
                        :end_time,
                        :user_name,
                        :result_status,
                        :error_message,
                        :parameters,
                        :result,
                        NUMTODSINTERVAL(:duration_seconds, 'SECOND'),
                        :additional_info
                    )
                """

                duration_seconds = (tiempo_fin - tiempo_inicio).total_seconds()

                cursor.execute(sql_oracle, {
                    "procedure_name": "procesamiento_archivo_maestra",
                    "execution_time": tiempo_inicio,
                    "end_time": tiempo_fin,
                    "user_name": "airflow",
                    "result_status": status_final,
                    "error_message": error_detalle,
                    "parameters": parametros,
                    "result": resultado_json,
                    "duration_seconds": duration_seconds,
                    "additional_info": info_adicional
                })

                conn.commit()
                logger.info("[AUDIT ORACLE] Auditoría registrada correctamente en Oracle.")

    except Exception as e:
        logger.exception(f"[AUDIT ORACLE] Error registrando auditoría en Oracle: {e}")

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
    t3_pre = PythonOperator(task_id='preparar_staging_y_copy', python_callable=preparar_staging_y_copy, provide_context=True)
    t3 = PythonOperator(task_id='cargar_datos_postgres', python_callable=cargar_datos_postgres, provide_context=True)
    t4 = PythonOperator(task_id='cargar_datos_oracle', python_callable=cargar_datos_oracle, provide_context=True)
    t5 = PythonOperator(task_id='publicar_en_kafka', python_callable=publicar_en_kafka, provide_context=True)
    t6 = PythonOperator(task_id="generar_parquet_y_subir_a_gcs",python_callable=generar_parquet_y_subir_a_gcs,provide_context=True)
    t7 = PythonOperator(task_id='registrar_errores', python_callable=registrar_errores, provide_context=True, trigger_rule='all_done')
    t8 = PythonOperator(task_id='mover_archivo', python_callable=mover_archivo, provide_context=True, trigger_rule='all_done')
    t9 = PythonOperator(task_id='registrar_en_audit_log', python_callable=registrar_en_audit_log, provide_context=True, trigger_rule='all_done')
    t1 >> t2 >> t3_pre >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9
