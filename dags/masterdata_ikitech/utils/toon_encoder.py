"""
TOON Encoder Wrapper
Envuelve al encoder oficial de toon-format y expone encode() para usarlo desde Airflow.
"""

from toon_format import encode as toon_encode_lib

def encode(value, indent=2, delimiter="|", length_marker=False):
    """
    Encode Python dict to TOON format.
    
    Esta función solo envuelve el encoder oficial, pero te da un punto centralizado
    para modificar opciones sin alterar los DAGs.
    """
    options = {
        "indent": indent,
        "delimiter": delimiter,
        "lengthMarker": length_marker
    }
    return toon_encode_lib(value, options)
