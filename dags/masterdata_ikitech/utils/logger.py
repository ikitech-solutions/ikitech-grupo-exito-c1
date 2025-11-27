"""
utils/logger.py

Este módulo proporciona una función para obtener un logger configurado con un formato específico.
Evita múltiples StreamHandlers y permite definir el nivel de log mediante una cadena de texto.

Uso:
    from utils.logger import get_logger
    logger = get_logger("mi_logger", "INFO")
    logger.info("Mensaje informativo")
"""

import logging

def get_logger(logger_name: str = "custom_airflow", log_level_str: str = "ERROR") -> logging.Logger:
    """
    Crea y devuelve un logger con un nombre y nivel de log especificados.

    Args:
        logger_name (str): Nombre del logger. Por defecto es 'custom_airflow'.
        log_level_str (str): Nivel de log como cadena (e.g. 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').

    Returns:
        logging.Logger: Objeto logger configurado.

    Nota:
        - Si el logger ya tiene un StreamHandler, no se agrega uno nuevo.
        - Si el nivel de log es inválido, se usa 'ERROR' por defecto y se emite una advertencia.
    """
    logger = logging.getLogger(logger_name)

    # Evitar agregar múltiples StreamHandlers si ya existe uno
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt='[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S %Z'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Validar y configurar el nivel de log
    log_level_str = log_level_str.upper()
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if log_level_str not in valid_levels:
        logger.warning(f"Nivel de log '{log_level_str}' no válido. Usando ERROR.")
        log_level_str = 'ERROR'

    logger.setLevel(getattr(logging, log_level_str))
    return logger
