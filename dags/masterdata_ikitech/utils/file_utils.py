"""
Este módulo contiene utilidades para manipulación básica de archivos y rutas.

Funciones incluidas:
- build_path: Construye rutas de archivos de forma segura y multiplataforma.
- find_file: Busca archivos según un patrón, con opciones de ordenamiento.
- move_file_safely: Mueve archivos asegurando que el directorio destino exista.
"""

import os
import glob
import shutil

def build_path(*args):
    """
    Construye una ruta de archivo uniendo múltiples segmentos.

    Args:
        *args: Segmentos de la ruta como strings.

    Returns:
        str: Ruta construida de forma segura según el sistema operativo.
    """
    return os.path.join(*args)

def find_file(pattern: str, sorted_by: str = "name") -> str:
    """
    Busca un archivo que coincida con un patrón y lo retorna según el orden especificado.

    Args:
        pattern (str): Patrón de búsqueda (ej. "*.csv", "data/*.txt").
        sorted_by (str): Criterio de ordenamiento, puede ser 'name' o 'ctime' (fecha de creación).

    Returns:
        str | None: Ruta del primer archivo encontrado según el criterio, o None si no se encuentra.
    """
    files = glob.glob(pattern)
    if not files:
        return None

    if sorted_by == "name":
        return sorted(files)[0]
    elif sorted_by == "ctime":
        return sorted(files, key=os.path.getctime)[0]

    # En caso de un criterio no reconocido, devuelve el primer archivo sin ordenar
    return files[0]

def move_file_safely(source_path, destination_dir, new_name=None):
    """
    Mueve un archivo asegurando que el directorio destino exista.

    Args:
        source_path (str): Ruta completa del archivo origen.
        destination_dir (str): Directorio donde se moverá el archivo.
        new_name (str, opcional): Nuevo nombre para el archivo. Si no se especifica, se conserva el original.

    Returns:
        None
    """
    if not os.path.exists(source_path):
        return
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)
    
    destination_path = os.path.join(destination_dir, new_name or os.path.basename(source_path))
    os.rename(source_path, destination_path)
