import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)


class FixedWidthParser:

    def __init__(self, layouts: Optional[Dict[str, Any]] = None) -> None:
        self.layouts: Dict[str, Any] = layouts or {}
        logger.info(f"[FixedWidthParser] Layouts cargados: {list(self.layouts.keys())}")

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------
    def _find_layout_for_file(self, archivo_nombre: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Resuelve el layout a partir del nombre de archivo.
        - Intenta match exacto con la key (XWECIA.txt, etc.)
        - Si no, intenta por prefijo del stem (XWECIA_20251127.txt -> XWECIA.txt)
        """
        if not self.layouts:
            return None, None

        base_name = Path(archivo_nombre).name

        # Match exacto por key
        if base_name in self.layouts:
            return base_name, self.layouts[base_name]

        # Match por prefijo usando el stem
        stem = Path(base_name).stem
        for key in self.layouts.keys():
            key_stem = Path(key).stem
            if stem.startswith(key_stem):
                return key, self.layouts[key]

        return None, None

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------
    def validate_file_structure(self, file_path: str, archivo_nombre_base: str) -> Dict[str, Any]:
        """
        Verifica existencia de layout, archivo y que no esté vacío.
        Devuelve:
        {
          'valido': bool,
          'encoding': 'utf-8',
          'error': <str opcional>
        }
        """
        layout_key, layout = self._find_layout_for_file(archivo_nombre_base)
        if not layout:
            msg = f"Layout no encontrado para archivo '{archivo_nombre_base}'"
            logger.error(f"[VALIDATE] {msg}")
            return {"valido": False, "encoding": None, "error": msg}

        encoding = layout.get("encoding", "utf-8")

        try:
            with open(file_path, "r", encoding=encoding, errors="ignore") as f:
                lines = f.readlines()
        except FileNotFoundError:
            msg = f"Archivo no encontrado: {file_path}"
            logger.error(f"[VALIDATE] {msg}")
            return {"valido": False, "encoding": encoding, "error": msg}

        if not lines:
            msg = "Archivo vacío"
            logger.error(f"[VALIDATE] {msg}: {file_path}")
            return {"valido": False, "encoding": encoding, "error": msg}

        # Aquí se podrían agregar chequeos adicionales de longitud / columnas
        logger.info(f"[VALIDATE] Archivo válido para layout '{layout_key}' con encoding {encoding}")
        return {"valido": True, "encoding": encoding}

    def parse_file(self, file_path: str, archivo_nombre_base: str) -> Dict[str, Any]:
        """
        Parsea el archivo usando el layout correspondiente.

        Retorna un dict con:
        {
        'archivo': <str>,
        'tabla_destino': <str>,
        'kafka_topic': <str o None>,
        'campos_clave': <list o None>,
        'encoding': <str>,
        'registros': <list[dict]>,
        'errores': <list[str]>,
        'total_lineas': <int>,
        'lineas_validas': <int>,
        'lineas_invalidas': <int>,
        'es_vacio': <bool>
        }
        """
        layout_key, layout = self._find_layout_for_file(archivo_nombre_base)
        if not layout:
            raise ValueError(f"No se encontró layout para archivo '{archivo_nombre_base}'")

        encoding = layout.get("encoding", "utf-8")
        tipo = (layout.get("tipo") or "fixed").lower()
        tabla_destino = layout.get("tabla_destino")
        kafka_topic = layout.get("kafka_topic")
        campos_clave = layout.get("campos_clave")
        campos = layout.get("campos", []) or []
        campos_hardcoded = layout.get("campos_hardcoded") or {}
        ignore_header = bool(layout.get("ignore_header", False))
        delimitador = layout.get("delimitador", "|")

        registros: List[Dict[str, Any]] = []
        errores: List[str] = []
        total_lineas = 0
        lineas_validas = 0
        lineas_invalidas = 0

        logger.info(
            f"[PARSE] Archivo='{file_path}', layout='{layout_key}', tipo='{tipo}', "
            f"tabla_destino='{tabla_destino}'"
        )

        try:
            with open(file_path, "r", encoding=encoding, errors="ignore") as f:
                for idx_fisico, raw_line in enumerate(f, start=1):

                    # Header de archivos delimitados
                    if ignore_header and idx_fisico == 1:
                        continue

                    line = raw_line.rstrip("\n\r")
                    if not line.strip():
                        # línea en blanco -> se considera inválida
                        lineas_invalidas += 1
                        errores.append(f"Línea {idx_fisico}: línea en blanco.")
                        continue

                    total_lineas += 1

                    if tipo == "fixed":
                        record, errs = self._parse_fixed_line(line, campos, campos_hardcoded, idx_fisico)
                    elif tipo == "delimitado":
                        record, errs = self._parse_delimited_line(
                            line,
                            campos,
                            campos_hardcoded,
                            delimitador,
                            idx_fisico
                        )
                    else:
                        errs = [f"Línea {idx_fisico}: tipo de layout desconocido '{tipo}'"]
                        record = {}

                    if errs:
                        lineas_invalidas += 1
                        errores.extend(errs)
                    else:
                        lineas_validas += 1
                        registros.append(record)

        except FileNotFoundError:
            msg = f"Archivo no encontrado durante parseo: {file_path}"
            logger.error(f"[PARSE] {msg}")
            return {
                'archivo': Path(file_path).name,
                'tabla_destino': tabla_destino,
                'kafka_topic': kafka_topic,
                'campos_clave': campos_clave,
                'encoding': encoding,
                'registros': [],
                'errores': [msg],
                'total_lineas': 0,
                'lineas_validas': 0,
                'lineas_invalidas': 0,
                'es_vacio': True
            }

        es_vacio = (total_lineas == 0 or lineas_validas == 0 and lineas_invalidas == 0)

        resultado = {
            'archivo': Path(file_path).name,
            'tabla_destino': tabla_destino,
            'kafka_topic': kafka_topic,
            'campos_clave': campos_clave,
            'encoding': encoding,
            'registros': registros,
            'errores': errores,
            'total_lineas': total_lineas,
            'lineas_validas': lineas_validas,
            'lineas_invalidas': lineas_invalidas,
            'es_vacio': es_vacio,
        }

        logger.info(
            f"[PARSE] Resultado archivo='{resultado['archivo']}' "
            f"total={total_lineas}, validos={lineas_validas}, invalidos={lineas_invalidas}"
        )
        return resultado

    # ------------------------------------------------------------------
    # Parsers de línea
    # ------------------------------------------------------------------
    def _parse_fixed_line(
        self,
        line: str,
        campos: List[Dict[str, Any]],
        campos_hardcoded: Dict[str, Any],
        line_number: int
    ) -> Tuple[Dict[str, Any], List[str]]:
        record: Dict[str, Any] = {}
        errores: List[str] = []

        for campo in campos:
            nombre = campo.get("nombre")
            start = int(campo.get("start", 0))
            end = int(campo.get("end", 0))
            required = bool(campo.get("required", False))

            # Protección ante líneas más cortas
            raw_val = line[start:end] if start < len(line) else ""
            val = raw_val.rstrip()

            record[nombre] = val

            if required and not val.strip():
                errores.append(
                    f"Línea {line_number}: campo requerido '{nombre}' vacío "
                    f"(rango [{start}:{end}])."
                )

        # Campos hardcoded
        for k, v in campos_hardcoded.items():
            record[k] = v

        return record, errores

    def _parse_delimited_line(
        self,
        line: str,
        campos: List[Dict[str, Any]],
        campos_hardcoded: Dict[str, Any],
        delimitador: str,
        line_number: int
    ) -> Tuple[Dict[str, Any], List[str]]:
        record: Dict[str, Any] = {}
        errores: List[str] = []

        partes = line.split(delimitador)

        for campo in campos:
            nombre = campo.get("nombre")
            index = int(campo.get("index", 0))
            required = bool(campo.get("required", False))

            val = ""
            if 0 <= index < len(partes):
                val = partes[index].strip()

            record[nombre] = val

            if required and not val:
                errores.append(
                    f"Línea {line_number}: campo requerido '{nombre}' vacío "
                    f"(index={index})."
                )

        # Campos hardcoded
        for k, v in campos_hardcoded.items():
            record[k] = v

        return record, errores
