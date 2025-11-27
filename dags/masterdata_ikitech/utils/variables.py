"""
Este módulo proporciona una función para obtener variables almacenadas en Airflow en formato JSON,
validando que contengan claves requeridas si se especifican.

Uso típico:
    from utils.variable_loader import get_variable_dict
    config = get_variable_dict("mi_variable_airflow", claves_requeridas=["host", "port"])
"""

from airflow.models import Variable
import json
from utils.logger import get_logger

log = get_logger()

def get_variable_dict(nombre_variable, claves_requeridas=None):
    """
    Obtiene una variable de Airflow en formato JSON y valida que contenga ciertas claves requeridas.

    Args:
        nombre_variable (str): Nombre de la variable almacenada en Airflow.
        claves_requeridas (list[str], opcional): Lista de claves que deben existir y no estar vacías en el JSON.

    Returns:
        dict: Diccionario parseado desde la variable JSON.

    Raises:
        KeyError: Si alguna de las claves requeridas no está presente o está vacía.
        Exception: Si ocurre cualquier otro error al obtener o parsear la variable.
    """
    try:
        raw_json = Variable.get(nombre_variable)
        log.info(f"Valor de la variable '{nombre_variable}': {raw_json}")
        parsed = json.loads(raw_json)

        if claves_requeridas:
            for clave in claves_requeridas:
                if clave not in parsed or parsed[clave] == "":
                    raise KeyError(f"La variable '{clave}' falta o está vacía en '{nombre_variable}'")

        log.info(f"Variable '{nombre_variable}' parseada correctamente: {parsed}")
        return parsed

    except KeyError as ke:
        log.error(f"Variable faltante o vacía en Airflow '{nombre_variable}': {ke}")
        raise
    except Exception as e:
        log.error(f"Error al obtener o parsear la variable de Airflow '{nombre_variable}': {e}")
        raise
