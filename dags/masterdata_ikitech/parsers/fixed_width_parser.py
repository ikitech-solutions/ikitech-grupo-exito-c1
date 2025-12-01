"""
Parser híbrido para archivos de texto (Fixed-Width y Delimitados).

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
        self.layouts = self._load_layouts(layouts_config_path)
        logger.info(f"Layouts cargados: {list(self.layouts.keys())}")
    
    def _load_layouts(self, config_path: str) -> Dict[str, Any]:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def get_layout(self, filename: str) -> Optional[Dict[str, Any]]:
        return self.layouts.get(filename)
    
    def parse_file(self, file_path: str, filename: str) -> Dict[str, Any]:
        """
        Parsea un archivo completo según su configuración.
        """
        layout = self.get_layout(filename)
        if not layout:
            raise ValueError(f"No se encontró configuración de layout para: {filename}")
        
        encoding = layout.get('encoding', 'latin-1')
        tipo_archivo = layout.get('tipo', 'fixed')
        delimitador = layout.get('delimitador', '|')
        ignore_header = layout.get('ignore_header', False)
        
        registros = []
        errores = []
        linea_num = 0
        total_lineas_leidas = 0
        
        logger.info(f"Parseando archivo: {file_path} con encoding: {encoding} (Tipo: {tipo_archivo})")
        
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                todas_lineas = f.readlines()
                
                if not todas_lineas:
                    return {
                        'registros': [],
                        'errores': [],
                        'total_lineas': 0,
                        'lineas_validas': 0,
                        'lineas_invalidas': 0,
                        'tabla_destino': layout.get('tabla_destino'),
                        'archivo': filename,
                        'es_vacio': True
                    }
                
                inicio_lectura = 1 if ignore_header else 0
                
                for i, linea in enumerate(todas_lineas[inicio_lectura:], start=inicio_lectura + 1):
                    linea_num = i
                    total_lineas_leidas += 1
                    
                    if not linea.strip():
                        continue
                    
                    try:
                        if tipo_archivo == 'delimitado':
                            registro, advertencias = self._parse_delimited(linea, layout, delimitador)
                        else:
                            registro, advertencias = self._parse_fixed(linea, layout)
                        
                        registros.append(registro)
                        
                        if advertencias:
                            for adv in advertencias:
                                logger.warning(f"[Línea {linea_num}] {adv}")
                                
                    except Exception as e:
                        error = {
                            'linea_num': linea_num,
                            'linea': linea.rstrip('\n'),
                            'error': str(e)
                        }
                        errores.append(error)
                        logger.error(f"Error en línea {linea_num}: {e}")
        
        except Exception as e:
            logger.error(f"Error fatal leyendo archivo {file_path}: {e}")
            raise
        
        resultado = {
            'registros': registros,
            'errores': errores,
            'total_lineas': total_lineas_leidas,
            'lineas_validas': len(registros),
            'lineas_invalidas': len(errores),
            'tabla_destino': layout.get('tabla_destino'),
            'archivo': filename,
            'es_vacio': False
        }
        
        logger.info(f"Parseo completado: {len(registros)} válidos, {len(errores)} errores")
        
        return resultado
    
    def _parse_fixed(self, linea: str, layout: Dict[str, Any]):
        registro = {}
        advertencias = []
        campos_hardcoded = layout.get('campos_hardcoded', {})
        for campo, valor in campos_hardcoded.items():
            registro[campo] = valor
        
        campos = layout.get('campos', [])

        max_end = max([c.get('end', 0) for c in campos]) if campos else 0
        
        linea_util = linea.rstrip('\n').rstrip('\r')

        if len(linea_util) > max_end:
            contenido_extra = linea_util[max_end:]
            if contenido_extra.strip(): 
                advertencias.append(f"Datos no mapeados encontrados después de la posición {max_end}: '{contenido_extra[:20]}...'")
        # -----------------------------------------------------------
        
        for campo_config in campos:
            nombre = campo_config['nombre']
            start = campo_config.get('start')
            end = campo_config.get('end')
            
            if start is not None and end is not None:
                valor = linea[start:end].strip()
                
                tipo = campo_config.get('tipo', 'string')
                if tipo == 'numeric' and valor:
                    try:
                        valor = int(valor) if valor.isdigit() else float(valor)
                    except ValueError:
                        pass 
                
                if campo_config.get('required', False) and not str(valor):
                    raise ValueError(f"Campo requerido {nombre} está vacío")
                
                registro[nombre] = valor
            else:
                raise ValueError(f"Configuración inválida para campo {nombre} (fixed)")
        
        return registro, advertencias

    def _parse_delimited(self, linea: str, layout: Dict[str, Any], delimiter: str):
        registro = {}
        advertencias = []
        campos_hardcoded = layout.get('campos_hardcoded', {})
        for campo, valor in campos_hardcoded.items():
            registro[campo] = valor
            
        partes = linea.strip().split(delimiter)
        campos = layout.get('campos', [])

        if len(partes) > len(campos):
            cols_extra = partes[len(campos):]
            if any(col.strip() for col in cols_extra):
                advertencias.append(f"Se encontraron {len(partes)} columnas, pero se esperaban {len(campos)}. Hay datos extra.")

        for campo_config in campos:
            nombre = campo_config['nombre']
            index = campo_config.get('index')
            
            if index is not None:
                if index < len(partes):
                    valor = partes[index].strip()
                    
                    tipo = campo_config.get('tipo', 'string')
                    if tipo == 'numeric' and valor:
                        try:
                            valor = int(valor) if valor.isdigit() else float(valor)
                        except ValueError:
                            pass
                    
                    if campo_config.get('required', False) and not str(valor):
                        raise ValueError(f"Campo requerido {nombre} está vacío")
                    
                    registro[nombre] = valor
                else:
                    if campo_config.get('required', False):
                        raise ValueError(f"Índice {index} fuera de rango para campo requerido {nombre}")
                    registro[nombre] = None
            else:
                raise ValueError(f"Configuración inválida para campo {nombre} (delimitado)")
                
        return registro, advertencias
    
    def validate_file_structure(self, file_path: str, filename: str) -> Dict[str, Any]:
        layout = self.get_layout(filename)
        if not layout:
            return {
                'valido': False,
                'error': f"No se encontró configuración para {filename}"
            }
        
        try:
            encoding = layout.get('encoding', 'latin-1')
            with open(file_path, 'r', encoding=encoding) as f:
                primera_linea = f.readline()
                
                if not primera_linea:
                    return {
                        'valido': False,
                        'error': "Archivo vacío"
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
    parser = FixedWidthParser(layouts_config_path)
    return parser.parse_file(file_path, filename)


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
        print(f"  Es vacío: {resultado.get('es_vacio')}")
        
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