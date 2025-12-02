"""
Validadores para datos de maestras.

Este módulo proporciona funciones de validación para asegurar la calidad
de los datos antes de ser procesados.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
import re

logger = logging.getLogger(__name__)


class DataValidator:
    """Validador de datos de maestras."""
    
    def __init__(self):
        """Inicializa el validador."""
        self.errores = []
    
    def validate_record(self, record: Dict[str, Any], validation_rules: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Valida un registro según reglas de validación.
        """
        errores = []
        
        # Validar campos requeridos
        required_fields = validation_rules.get('required_fields', [])
        for field in required_fields:
            if field not in record or not record[field]:
                errores.append(f"Campo requerido '{field}' está vacío o no existe")
        
        # Validar tipos de datos
        field_types = validation_rules.get('field_types', {})
        for field, expected_type in field_types.items():
            if field in record and record[field]:
                if not self._validate_type(record[field], expected_type):
                    errores.append(f"Campo '{field}' ('{record[field]}') tiene tipo inválido. Esperado: {expected_type}")
        
        # Validar rangos numéricos
        numeric_ranges = validation_rules.get('numeric_ranges', {})
        for field, (min_val, max_val) in numeric_ranges.items():
            if field in record and record[field]:
                try:
                    val = float(record[field])
                    if val < min_val or val > max_val:
                        errores.append(f"Campo '{field}' fuera de rango [{min_val}, {max_val}]")
                except ValueError:
                    errores.append(f"Campo '{field}' no es numérico")
        
        # Validar longitudes de strings
        string_lengths = validation_rules.get('string_lengths', {})
        for field, max_length in string_lengths.items():
            if field in record and record[field]:
                if len(str(record[field])) > max_length:
                    errores.append(f"Campo '{field}' excede longitud máxima de {max_length}")
        
        # Validar patrones regex
        patterns = validation_rules.get('patterns', {})
        for field, pattern in patterns.items():
            if field in record and record[field]:
                if not re.match(pattern, str(record[field])):
                    errores.append(f"Campo '{field}' no coincide con el patrón esperado")
        
        return (len(errores) == 0, errores)
    
    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """Valida el tipo de un valor."""
        if expected_type == 'string':
            return isinstance(value, str)
        
        elif expected_type == 'string_alpha':
            return isinstance(value, str) and value.strip().isalpha()
        # ---------------------------------------------

        elif expected_type == 'numeric':
            try:
                float(value)
                return True
            except (ValueError, TypeError):
                return False
        elif expected_type == 'integer':
            try:
                int(value)
                return True
            except (ValueError, TypeError):
                return False
        elif expected_type == 'boolean':
            return isinstance(value, bool) or value in ['S', 'N', '0', '1', 'true', 'false']
        else:
            return True
    
    def validate_batch(self, records: List[Dict[str, Any]], validation_rules: Dict[str, Any]) -> Dict[str, Any]:
        """
        Valida un lote de registros.
        """
        registros_validos = []
        registros_invalidos = []
        
        for idx, record in enumerate(records):
            es_valido, errores = self.validate_record(record, validation_rules)
            
            if es_valido:
                registros_validos.append(record)
            else:
                registros_invalidos.append({
                    'indice': idx,
                    'registro': record,
                    'errores': errores
                })
        
        resultado = {
            'registros_validos': registros_validos,
            'registros_invalidos': registros_invalidos,
            'total': len(records),
            'validos': len(registros_validos),
            'invalidos': len(registros_invalidos),
            'tasa_validez': len(registros_validos) / len(records) if records else 0
        }
        
        logger.info(f"Validación completada: {resultado['validos']}/{resultado['total']} válidos ({resultado['tasa_validez']:.1%})")
        
        return resultado


def validate_compania(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Valida un registro de COMPANIA."""
    rules = {
        'required_fields': ['CODCIA', 'NOMBRE'],
        'field_types': {
            'CODCIA': 'numeric'
        },
        'numeric_ranges': {
            'CODCIA': (1, 99)
        },
        'string_lengths': {
            'NOMBRE': 30
        }
    }
    validator = DataValidator()
    return validator.validate_record(record, rules)


def validate_pais(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Valida un registro de PAIS."""
    rules = {
        'required_fields': ['COD_PAIS', 'NOMBRE'],
        'field_types': {
            'COD_PAIS': 'numeric',
            'COD_MON': 'numeric'
        },
        'numeric_ranges': {
            'COD_PAIS': (1, 999),
            'COD_MON': (1, 99)
        },
        'string_lengths': {
            'NOMBRE': 30
        }
    }
    validator = DataValidator()
    return validator.validate_record(record, rules)


def validate_ciudad(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Valida un registro de CIUDAD."""
    rules = {
        'required_fields': ['COD_PAIS', 'CIUDAD', 'NOMBRE'],
        'field_types': {
            'COD_PAIS': 'numeric',
            'SUBZONA': 'numeric',
            'CLIMA': 'numeric'
        },
        'string_lengths': {
            'CIUDAD': 5,
            'NOMBRE': 30
        }
    }
    validator = DataValidator()
    return validator.validate_record(record, rules)


def validate_cadena(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Valida un registro de CADENA."""
    rules = {
        'required_fields': ['CODCADE', 'NOMBRE'],
        'field_types': {
            'CODCADE': 'string_alpha' 
        },
        'string_lengths': {
            'CODCADE': 1,
            'NOMBRE': 30
        }
    }
    validator = DataValidator()
    return validator.validate_record(record, rules)


def validate_canal(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Valida un registro de CANAL."""
    rules = {
        'required_fields': ['CANAL'],
        'string_lengths': {
            'CANAL': 2,
            'NOMBRE': 30
        }
    }
    validator = DataValidator()
    return validator.validate_record(record, rules)


# Mapa de validadores por tabla
VALIDATORS = {
    'erp_COMPANIA': validate_compania,
    'erp_PAIS': validate_pais,
    'erp_CIUDAD': validate_ciudad,
    'erp_CADENA': validate_cadena,
    'erp_CANAL': validate_canal,
}


def get_validator_for_table(table_name: str):
    """
    Obtiene el validador apropiado para una tabla.
    
    Args:
        table_name: Nombre de la tabla
    
    Returns:
        Función validadora o None si no existe
    """
    # Extraer nombre de tabla sin schema
    if '.' in table_name:
        table_name = table_name.split('.')[-1]
    
    return VALIDATORS.get(table_name)