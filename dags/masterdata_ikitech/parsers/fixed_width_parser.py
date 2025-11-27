"""
Parser genérico para archivos de texto de ancho fijo (fixed-width).

Este módulo proporciona funciones para parsear archivos de texto donde cada campo
tiene una posición fija en la línea, basándose en una configuración de layout.
"""

import json
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class FixedWidthParser:
    """Parser para archivos de texto de ancho fijo."""
    
    def __init__(self, layouts_config_path: str):
        """
        Inicializa el parser con la configuración de layouts.
        
        Args:
            layouts_config_path: Ruta al archivo JSON con la configuración de layouts
        """
        self.layouts = self._load_layouts(layouts_config_path)
        logger.info(f"Layouts cargados: {list(self.layouts.keys())}")
    
    def _load_layouts(self, config_path: str) -> Dict[str, Any]:
        """Carga la configuración de layouts desde un archivo JSON."""
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def get_layout(self, filename: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene la configuración de layout para un archivo.
        
        Args:
            filename: Nombre del archivo (ej: 'XWECIA.txt')
        
        Returns:
            Dict con la configuración del layout o None si no se encuentra
        """
        return self.layouts.get(filename)
    
    def parse_file(self, file_path: str, filename: str) -> Dict[str, Any]:
        """
        Parsea un archivo completo de texto de ancho fijo.
        
        Args:
            file_path: Ruta completa al archivo
            filename: Nombre del archivo para identificar el layout
        
        Returns:
            Dict con:
                - registros: List[Dict] con los registros parseados
                - errores: List[Dict] con los errores encontrados
                - total_lineas: int
                - lineas_validas: int
                - lineas_invalidas: int
        """
        layout = self.get_layout(filename)
        if not layout:
            raise ValueError(f"No se encontró configuración de layout para: {filename}")
        
        encoding = layout.get('encoding', 'utf-8')
        registros = []
        errores = []
        linea_num = 0
        
        logger.info(f"Parseando archivo: {file_path} con encoding: {encoding}")
        
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                for linea in f:
                    linea_num += 1
                    
                    # Saltar líneas vacías
                    if not linea.strip():
                        continue
                    
                    try:
                        registro = self.parse_line(linea, layout)
                        registros.append(registro)
                    except Exception as e:
                        error = {
                            'linea_num': linea_num,
                            'linea': linea.rstrip('\n'),
                            'error': str(e)
                        }
                        errores.append(error)
                        logger.warning(f"Error en línea {linea_num}: {e}")
        
        except Exception as e:
            logger.error(f"Error leyendo archivo {file_path}: {e}")
            raise
        
        resultado = {
            'registros': registros,
            'errores': errores,
            'total_lineas': linea_num,
            'lineas_validas': len(registros),
            'lineas_invalidas': len(errores),
            'tabla_destino': layout.get('tabla_destino'),
            'archivo': filename
        }
        
        logger.info(f"Parseo completado: {len(registros)} válidos, {len(errores)} errores")
        
        return resultado
    
    def parse_line(self, linea: str, layout: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parsea una línea de texto según la configuración de layout.
        
        Args:
            linea: Línea de texto a parsear
            layout: Dict con la configuración del layout
        
        Returns:
            Dict con los campos parseados
        """
        registro = {}
        
        # Agregar campos hardcoded
        campos_hardcoded = layout.get('campos_hardcoded', {})
        for campo, valor in campos_hardcoded.items():
            registro[campo] = valor
        
        # Parsear campos según configuración
        campos = layout.get('campos', [])
        
        for campo_config in campos:
            nombre = campo_config['nombre']
            
            try:
                # Parseo por posición fija
                if 'start' in campo_config and 'end' in campo_config:
                    start = campo_config['start']
                    end = campo_config['end']
                    valor = linea[start:end].strip()
                
                # Parseo por índice (para archivos delimitados)
                elif 'index' in campo_config:
                    # Detectar delimitador (puede ser espacio, tab, pipe, etc.)
                    partes = linea.split()
                    index = campo_config['index']
                    if index < len(partes):
                        valor = partes[index].strip()
                    else:
                        raise ValueError(f"Índice {index} fuera de rango")
                
                else:
                    raise ValueError(f"Configuración inválida para campo {nombre}")
                
                # Convertir tipo si es necesario
                tipo = campo_config.get('tipo', 'string')
                if tipo == 'numeric' and valor:
                    valor = int(valor) if valor.isdigit() else float(valor)
                
                # Validar campo requerido
                if campo_config.get('required', False) and not valor:
                    raise ValueError(f"Campo requerido {nombre} está vacío")
                
                registro[nombre] = valor
            
            except Exception as e:
                raise ValueError(f"Error parseando campo {nombre}: {e}")
        
        return registro
    
    def validate_file_structure(self, file_path: str, filename: str) -> Dict[str, Any]:
        """
        Valida la estructura básica de un archivo antes de parsearlo.
        
        Args:
            file_path: Ruta completa al archivo
            filename: Nombre del archivo
        
        Returns:
            Dict con el resultado de la validación
        """
        layout = self.get_layout(filename)
        if not layout:
            return {
                'valido': False,
                'error': f"No se encontró configuración para {filename}"
            }
        
        try:
            encoding = layout.get('encoding', 'utf-8')
            with open(file_path, 'r', encoding=encoding) as f:
                primera_linea = f.readline()
                
                if not primera_linea:
                    return {
                        'valido': False,
                        'error': "Archivo vacío"
                    }
                
                # Validar longitud mínima (si está configurada)
                min_length = layout.get('min_line_length')
                if min_length and len(primera_linea.rstrip('\n')) < min_length:
                    return {
                        'valido': False,
                        'error': f"Línea muy corta. Esperado: >={min_length}, encontrado: {len(primera_linea)}"
                    }
                
                return {
                    'valido': True,
                    'encoding': encoding,
                    'primera_linea_length': len(primera_linea.rstrip('\n'))
                }
        
        except Exception as e:
            return {
                'valido': False,
                'error': f"Error validando archivo: {e}"
            }


def parse_fixed_width_file(file_path: str, filename: str, layouts_config_path: str) -> Dict[str, Any]:
    """
    Función de conveniencia para parsear un archivo de ancho fijo.
    
    Args:
        file_path: Ruta completa al archivo
        filename: Nombre del archivo
        layouts_config_path: Ruta al archivo de configuración de layouts
    
    Returns:
        Dict con los registros parseados y estadísticas
    """
    parser = FixedWidthParser(layouts_config_path)
    return parser.parse_file(file_path, filename)


# Ejemplo de uso
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Uso: python fixed_width_parser.py <archivo> <nombre_archivo>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    filename = sys.argv[2]
    layouts_config = "../config/layouts.json"
    
    logging.basicConfig(level=logging.INFO)
    
    try:
        resultado = parse_fixed_width_file(file_path, filename, layouts_config)
        print(f"\nResultado del parseo:")
        print(f"  Total líneas: {resultado['total_lineas']}")
        print(f"  Válidas: {resultado['lineas_validas']}")
        print(f"  Inválidas: {resultado['lineas_invalidas']}")
        print(f"  Tabla destino: {resultado['tabla_destino']}")
        
        if resultado['registros']:
            print(f"\nPrimer registro:")
            print(json.dumps(resultado['registros'][0], indent=2))
        
        if resultado['errores']:
            print(f"\nPrimeros errores:")
            for error in resultado['errores'][:5]:
                print(f"  Línea {error['linea_num']}: {error['error']}")
    
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
