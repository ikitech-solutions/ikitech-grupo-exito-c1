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
    """Pruebas para FixedWidthParser Híbrido."""
    
    @pytest.fixture
    def parser(self):
        """Fixture que retorna un parser inicializado con el layout real."""
        layouts_config = Path(__file__).parent.parent / 'dags' / 'masterdata_ikitech' / 'config' / 'layouts.toon'
        return FixedWidthParser(str(layouts_config))
    
    @pytest.fixture
    def test_data_path(self):
        """Fixture que retorna la ruta a los datos de prueba."""
        return Path(__file__).parent / 'data'
    
    def test_parser_initialization(self, parser):
        """Prueba que el parser se inicialice y cargue layouts."""
        assert parser is not None
        assert len(parser.layouts) > 0
        assert 'XWECIA.txt' in parser.layouts
    
    def test_get_layout(self, parser):
        """Prueba la recuperación de configuración."""
        layout = parser.get_layout('XCADENA.txt')
        assert layout is not None
        assert layout['tabla_destino'] == 'db_masterdata_hub.erp_CADENA'
        assert len(layout['campos']) > 0
    
    def test_parse_xcadena_file_fixed(self, parser, test_data_path):
        """Prueba el parseo de un archivo Fixed-Width (XCADENA)."""
        file_path = test_data_path / 'XCADENA.txt'
        
        if not file_path.exists():
            pytest.skip("Archivo XCADENA.txt no encontrado en tests/data")
        
        resultado = parser.parse_file(str(file_path), 'XCADENA.txt')
        
        assert resultado['es_vacio'] is False
        assert resultado['lineas_validas'] > 0
        # Validación de integridad con el manifiesto corregido
        assert resultado['tabla_destino'] == 'db_masterdata_hub.erp_CADENA'
        
        # Verificar contenido del primer registro
        first_row = resultado['registros'][0]
        assert 'CODCADE' in first_row
        assert 'NOMBRE' in first_row

    def test_parse_xwecia_file_fixed(self, parser, test_data_path):
        """Prueba el parseo de otro archivo Fixed-Width (XWECIA)."""
        file_path = test_data_path / 'XWECIA.txt'
        
        if not file_path.exists():
            pytest.skip("Archivo XWECIA.txt no encontrado")
        
        resultado = parser.parse_file(str(file_path), 'XWECIA.txt')
        
        assert resultado['lineas_validas'] > 0
        assert resultado['tabla_destino'] == 'db_masterdata_hub.erp_COMPANIA'
        
        if resultado['registros']:
            primer_registro = resultado['registros'][0]
            assert 'CODCIA' in primer_registro
            assert 'NOMBRE' in primer_registro

    def test_parse_delimited_file(self, parser, test_data_path):
        """
        Valida el soporte para archivos delimitados por '|'.
        Usa XLEXSUBC.txt o crea uno dummy si no existe.
        """
        filename = 'XLEXSUBC.txt'
        file_path = test_data_path / filename
        
        # Crear archivo dummy delimitado si no existe para asegurar la prueba
        if not file_path.exists():
            with open(file_path, 'w') as f:
                f.write("1001|00110|0000|0|INGRESOS ESTACIONES SERVICIO\n")
        
        try:
            resultado = parser.parse_file(str(file_path), filename)
            
            assert resultado['lineas_validas'] > 0
            assert resultado['tabla_destino'] == 'db_masterdata_hub.erp_SUBCATEGORIA'
            
            # Verificar que el split por '|' funcionó
            first_row = resultado['registros'][0]
            assert 'GRUPO' in first_row
            assert 'NOMBRE' in first_row
            # Verificar que no trajo basura del pipe
            assert '|' not in first_row.get('GRUPO', '') 
            
        finally:
            # Limpieza si creamos el dummy
            pass

    def test_parse_empty_file(self, parser, test_data_path):
        """Valida el manejo de archivos vacíos"""
        empty_file = test_data_path / 'EMPTY_TEST.txt'
        with open(empty_file, 'w') as f:
            pass # Crear vacío
            
        try:
            # Usamos cualquier layout válido para probar la lógica de vacío
            resultado = parser.parse_file(str(empty_file), 'XCADENA.txt')
            
            assert resultado['es_vacio'] is True
            assert resultado['lineas_validas'] == 0
            assert resultado['total_lineas'] == 0
            
        finally:
            if empty_file.exists():
                empty_file.unlink()

    def test_validate_file_structure(self, parser, test_data_path):
        """Prueba la validación de estructura previa."""
        file_path = test_data_path / 'XCADENA.txt'
        if not file_path.exists():
            pytest.skip("No data")
            
        validacion = parser.validate_file_structure(str(file_path), 'XCADENA.txt')
        assert validacion['valido'] is True
    
    def test_parse_nonexistent_file(self, parser):
        """Prueba que lance excepción si el archivo físico no existe."""
        with pytest.raises(Exception):
            parser.parse_file('/ruta/inexistente/falsa.txt', 'XCADENA.txt')
    
    def test_parse_unknown_layout(self, parser, test_data_path):
        """Prueba el fallo controlado si no hay configuración en JSON."""
        file_path = test_data_path / 'XCADENA.txt'
        if not file_path.exists(): pytest.skip("No data")
        
        with pytest.raises(ValueError) as excinfo:
            parser.parse_file(str(file_path), 'ARCHIVO_FANTASMA.txt')
        
        assert "No se encontró configuración" in str(excinfo.value) or "No layout" in str(excinfo.value)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])