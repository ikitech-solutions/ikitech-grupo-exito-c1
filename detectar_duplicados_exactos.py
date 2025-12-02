#!/usr/bin/env python3
import hashlib
import re
from pathlib import Path

INPUT_DIR = "D:\Development\Data\Ikitech\Airflow-Exito\ikitech-grupo-exito-c1\input"   # Ajusta si tu ruta real es diferente


def normalize_value(line: str) -> str:
    """
    Normaliza una línea para comparación EXACTA:
    - Elimina \r
    - Reemplaza saltos de línea por espacio
    - Trim
    - Colapsa espacios/tabs múltiples a uno
    """
    if line is None:
        return ""
    
    s = line.replace("\r", "").replace("\n", " ")
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s


def canonical_hash(normalized_line: str) -> str:
    """
    Hash estable basado en SHA256 de la línea canonicalizada.
    """
    return hashlib.sha256(normalized_line.encode("utf-8")).hexdigest()


def detectar_archivo_y_duplicados():
    input_path = Path(INPUT_DIR)

    if not input_path.exists():
        print(f"❌ La carpeta {INPUT_DIR} NO existe.")
        return

    # Busca cualquier archivo dentro de /input
    archivos = list(input_path.glob("*"))

    if not archivos:
        print(f"❌ No se encontró ningún archivo dentro de {INPUT_DIR}")
        return

    # Toma el primero que encuentre
    archivo = archivos[0]
    print(f"📄 Archivo encontrado: {archivo.name}")

    # Procesar duplicados
    detectar_duplicados_en_archivo(archivo)


def detectar_duplicados_en_archivo(ruta_archivo: Path):
    seen = {}
    duplicates = {}

    print(f"Analizando líneas en: {ruta_archivo}")

    with ruta_archivo.open("r", encoding="latin-1", errors="ignore") as f:
        for num_linea, line in enumerate(f, start=1):
            norm = normalize_value(line)
            h = canonical_hash(norm)

            if h in seen:
                if h not in duplicates:
                    duplicates[h] = []
                duplicates[h].append(num_linea)
            else:
                seen[h] = (num_linea, norm)

    if not duplicates:
        print("\n✅ NO se encontraron registros EXACTAMENTE duplicados.\n")
        return

    print("\n⚠️ Se encontraron REGISTROS EXACTAMENTE DUPLICADOS:\n")

    for h, line_nums in duplicates.items():
        primera_linea, contenido = seen[h]

        print(f"--- DUPLICADO EXACTO ---")
        print(f"Primera aparición: línea {primera_linea}")
        print(f"Líneas duplicadas: {line_nums}")
        print(f"Contenido normalizado: '{contenido}'")
        print(f"Hash: {h}")
        print("------------------------\n")

    print(f"Total hashes duplicados: {len(duplicates)}")
    print("⚠️ Puedes pedirme un reporte CSV/JSON si lo quieres exportar.")


if __name__ == "__main__":
    detectar_archivo_y_duplicados()
