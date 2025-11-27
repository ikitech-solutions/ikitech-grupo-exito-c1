"""
Este módulo proporciona utilidades para ejecutar SQL y procedimientos almacenados
desde Airflow usando conexiones PostgreSQL. Incluye funciones para:

- Ejecutar sentencias SQL.
- Obtener resultados de consultas SELECT.
- Llamar a procedimientos almacenados para logging estructurado.
- Serialización segura a JSON.
- Validación de nombres de procedimientos/funciones PostgreSQL.
"""

import json
import time
import logging
import psycopg2
import re
from contextlib import contextmanager
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

def is_valid_identifier(name):
    """
    Valida si el nombre es un identificador válido en PostgreSQL.

    Acepta nombres simples (ej. "mi_funcion") o calificados (ej. "schema.funcion").

    Args:
        name (str): Nombre a validar.

    Returns:
        bool: True si es válido, False en caso contrario.
    """
    return bool(re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)$', name) or
                re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name))

def safe_json(obj):
    """
    Serializa un objeto a JSON de forma segura.

    Si la serialización falla, se retorna un JSON con un mensaje de error.

    Args:
        obj (any): Objeto a serializar.

    Returns:
        str: Representación en JSON.
    """
    try:
        return json.dumps(obj)
    except Exception as e:
        log.warning(f"⚠️ No se pudo serializar objeto de tipo {type(obj)}: {obj} - Error: {e}")
        return json.dumps({'error': 'no serializable'})

def get_connection_params(connection_name):
    """
    Obtiene los parámetros de conexión a partir del nombre de conexión de Airflow.

    Args:
        connection_name (str): Nombre de la conexión definida en Airflow.

    Returns:
        dict: Diccionario con parámetros para conexión psycopg2.
    """
    connection = BaseHook.get_connection(connection_name)
    return {
        "host": connection.host,
        "dbname": connection.schema,
        "user": connection.login,
        "password": connection.password,
        "port": connection.port
    }

@contextmanager
def get_db_cursor(connection_name):
    """
    Context manager que abre una conexión y cursor a una base de datos PostgreSQL.

    Se encarga de abrir, cerrar y hacer commit o rollback automáticamente.

    Args:
        connection_name (str): Nombre de la conexión de Airflow.

    Yields:
        tuple: (conn, cursor) de psycopg2.
    """
    params = get_connection_params(connection_name)
    conn = psycopg2.connect(**params)
    cursor = conn.cursor()
    try:
        yield conn, cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def execute_sql(connection_name, sql):
    """
    Ejecuta una sentencia SQL (INSERT, UPDATE, DELETE, DDL).

    Args:
        connection_name (str): Nombre de la conexión Airflow.
        sql (str): Sentencia SQL a ejecutar.

    Raises:
        Exception: Si ocurre un error durante la ejecución.
    """
    with get_db_cursor(connection_name) as (_, cursor):
        start = time.time()
        try:
            cursor.execute(sql)
            duration = time.time() - start
            log.info(f"✅ SQL ejecutado correctamente en {duration:.2f} segundos.")
        except Exception as e:
            log.error(f"❌ Error al ejecutar SQL: {sql} - Error: {e}")
            raise

def fetch_results(connection_name, sql):
    """
    Ejecuta una sentencia SELECT y retorna los resultados.

    Args:
        connection_name (str): Nombre de la conexión Airflow.
        sql (str): Sentencia SELECT.

    Returns:
        list[tuple]: Resultados de la consulta.

    Raises:
        ValueError: Si la sentencia no es SELECT.
    """
    if not sql.strip().lower().startswith("select"):
        raise ValueError("❌ Solo se permiten consultas SELECT en fetch_results.")
    
    with get_db_cursor(connection_name) as (_, cursor):
        start = time.time()
        cursor.execute(sql)
        results = cursor.fetchall()
        duration = time.time() - start
        log.info(f"✅ SELECT ejecutado en {duration:.2f} segundos. Filas: {len(results)}")
        return results

def call_sp_logs(
    connection_name,
    sp_logs,
    nombre_proced,
    tiempo_ini,
    tiempo_fin,
    estado,
    mensaje_error,
    parametros,
    resultado,
    info_adicional
):
    """
    Llama a un procedimiento almacenado PostgreSQL para registrar logs estructurados.

    Args:
        connection_name (str): Nombre de la conexión Airflow.
        sp_logs (str): Nombre del procedimiento (puede estar calificado con schema).
        nombre_proced (str): Nombre del proceso que se está registrando.
        tiempo_ini (str): Timestamp de inicio.
        tiempo_fin (str): Timestamp de finalización.
        estado (str): Estado final del proceso (ej. "OK", "ERROR").
        mensaje_error (str): Mensaje de error si aplica.
        parametros (dict): Parámetros de entrada del proceso.
        resultado (dict): Resultado del proceso.
        info_adicional (dict): Información adicional relevante.

    Raises:
        ValueError: Si el nombre del procedimiento no es válido.
        Exception: Si ocurre un error al ejecutar el procedimiento.
    """
    if not is_valid_identifier(sp_logs):
        raise ValueError(f"❌ Nombre de procedimiento no válido: '{sp_logs}'")

    with get_db_cursor(connection_name) as (_, cursor):
        sql = f"""
            CALL {sp_logs}(
                %s::varchar, %s::timestamp, %s::timestamp, %s::varchar, %s::text, %s::jsonb, %s::jsonb, %s::jsonb
            );
        """
        args = [
            nombre_proced,
            tiempo_ini,
            tiempo_fin,
            estado,
            mensaje_error,
            safe_json(parametros),
            safe_json(resultado),
            safe_json(info_adicional)
        ]
        try:
            start = time.time()
            cursor.execute(sql, args)
            duration = time.time() - start
            log.info(f"📝 Stored procedure '{sp_logs}' ejecutado en {duration:.2f} segundos.")
        except Exception as e:
            log.error(f"❌ Error al ejecutar SQL: {sql} - Args: {args} - Error: {e}")
            raise
