"""
Pruebas unitarias para el parser de fixed-width files.
"""

import pytest
import sys
from pathlib import Path

# Agregar el directorio de dags al path
sys.path.insert(0, str(Path(__file__).parent.parent / 'dags' / 'masterdata_ikitech'))

from parsers.fixed_width_parser import FixedWidthParser


class TestFixedWidthParser:
    """Pruebas para FixedWidthParser."""
    
    @pytest.fixture
    def parser(self):
        """Fixture que retorna un parser inicializado."""
        layouts_config = Path(__file__).parent.parent / 'dags' / 'masterdata_ikitech' / 'config' / 'layouts.json'
        return FixedWidthParser(str(layouts_config))
    
    @pytest.fixture
    def test_data_path(self):
        """Fixture que retorna la ruta a los datos de prueba."""
        return Path(__file__).parent / 'data'
    
    def test_parser_initialization(self, parser):
        """Prueba que el parser se inicialice correctamente."""
        assert parser is not None
        assert len(parser.layouts) > 0
    
    def test_get_layout(self, parser):
        """Prueba que se pueda obtener un layout."""
        layout = parser.get_layout('XCADENA.txt')
        assert layout is not None
        assert 'tabla_destino' in layout
        assert 'campos' in layout
    
    def test_parse_xcadena_file(self, parser, test_data_path):
        """Prueba el parseo del archivo XCADENA.txt."""
        file_path = test_data_path / 'XCADENA.txt'
        
        if not file_path.exists():
            pytest.skip("Archivo de prueba no encontrado")
        
        resultado = parser.parse_file(str(file_path), 'XCADENA.txt')
        
        assert resultado is not None
        assert 'registros' in resultado
        assert 'errores' in resultado
        assert resultado['lineas_validas'] > 0
        assert resultado['tabla_destino'] == 'db_masterdatahub.CADENA'
    
    def test_parse_xwecia_file(self, parser, test_data_path):
        """Prueba el parseo del archivo XWECIA.txt."""
        file_path = test_data_path / 'XWECIA.txt'
        
        if not file_path.exists():
            pytest.skip("Archivo de prueba no encontrado")
        
        resultado = parser.parse_file(str(file_path), 'XWECIA.txt')
        
        assert resultado is not None
        assert resultado['lineas_validas'] > 0
        assert resultado['tabla_destino'] == 'db_masterdata_hub.erp_COMPANIA'
        
        # Verificar estructura del primer registro
        if resultado['registros']:
            primer_registro = resultado['registros'][0]
            assert 'CODCIA' in primer_registro
            assert 'NOMBRE' in primer_registro
    
    def test_validate_file_structure(self, parser, test_data_path):
        """Prueba la validación de estructura de archivo."""
        file_path = test_data_path / 'XCADENA.txt'
        
        if not file_path.exists():
            pytest.skip("Archivo de prueba no encontrado")
        
        validacion = parser.validate_file_structure(str(file_path), 'XCADENA.txt')
        
        assert validacion is not None
        assert validacion['valido'] == True
    
    def test_parse_nonexistent_file(self, parser):
        """Prueba el manejo de archivos inexistentes."""
        with pytest.raises(Exception):
            parser.parse_file('/ruta/inexistente.txt', 'XCADENA.txt')
    
    def test_parse_unknown_layout(self, parser, test_data_path):
        """Prueba el manejo de layouts desconocidos."""
        file_path = test_data_path / 'XCADENA.txt'
        
        if not file_path.exists():
            pytest.skip("Archivo de prueba no encontrado")
        
        with pytest.raises(ValueError):
            parser.parse_file(str(file_path), 'ARCHIVO_DESCONOCIDO.txt')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
