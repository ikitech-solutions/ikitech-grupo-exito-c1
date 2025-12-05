import logging
from typing import List, Dict, Any
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


def export_to_parquet_and_upload(
    registros: List[Dict[str, Any]],
    bucket_name: str,
    object_name: str,
    gcp_conn_id: str = "gcs_masterdata",
) -> Dict[str, Any]:

    # -------------------------------------------------------------------------
    # Validación inicial
    # -------------------------------------------------------------------------
    if not registros:
        logger.info("[GCS] No hay registros para exportar a parquet.")
        return {
            "uploaded": False,
            "rows": 0,
            "bucket": bucket_name,
            "object": object_name,
            "motivo": "sin_registros",
        }

    logger.info(f"[GCS] Iniciando exportación a parquet → bucket='{bucket_name}' objeto='{object_name}'")

    # -------------------------------------------------------------------------
    # Obtener credenciales de Airflow
    # -------------------------------------------------------------------------
    try:
        conn = BaseHook.get_connection(gcp_conn_id)
        keyfile_path = conn.extra_dejson.get("key_path")

        credentials = service_account.Credentials.from_service_account_file(
            keyfile_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        logger.info(f"[GCS] Credenciales cargadas correctamente desde la conexión '{gcp_conn_id}'.")

    except Exception as e:
        logger.error(f"[GCS ERROR] Fallo cargando credenciales GCP desde '{gcp_conn_id}': {e}")
        return {
            "uploaded": False,
            "rows": 0,
            "bucket": bucket_name,
            "object": object_name,
            "motivo": f"error_credenciales: {e}",
        }

    # -------------------------------------------------------------------------
    # Crear cliente de Google Storage
    # -------------------------------------------------------------------------
    try:
        client = storage.Client(credentials=credentials, project=credentials.project_id)
        logger.info("[GCS] Cliente de Google Cloud Storage creado correctamente.")
    except Exception as e:
        logger.error(f"[GCS ERROR] No se pudo crear el cliente GCS: {e}")
        return {
            "uploaded": False,
            "rows": 0,
            "bucket": bucket_name,
            "object": object_name,
            "motivo": f"error_creando_cliente: {e}",
        }

    # -------------------------------------------------------------------------
    # Obtener bucket
    # -------------------------------------------------------------------------
    try:
        bucket = client.get_bucket(bucket_name)
        logger.info(f"[GCS] Conexión al bucket '{bucket_name}' correcta.")
    except Exception as e:
        logger.error(f"[GCS ERROR] No se pudo acceder al bucket '{bucket_name}': {e}")
        return {
            "uploaded": False,
            "rows": len(registros),
            "bucket": bucket_name,
            "object": object_name,
            "motivo": f"bucket_inaccesible: {e}",
        }

    # -------------------------------------------------------------------------
    # Convertir registros a Parquet
    # -------------------------------------------------------------------------
    try:
        df = pd.DataFrame(registros)
        table = pa.Table.from_pandas(df)

        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        logger.info(f"[GCS] Archivo parquet generado exitosamente (rows={len(df)}).")

    except Exception as e:
        logger.error(f"[GCS ERROR] Fallo convirtiendo a Parquet: {e}")
        return {
            "uploaded": False,
            "rows": len(registros),
            "bucket": bucket_name,
            "object": object_name,
            "motivo": f"error_parquet: {e}",
        }

    # -------------------------------------------------------------------------
    # Subir archivo
    # -------------------------------------------------------------------------
    try:
        blob = bucket.blob(object_name)
        blob.upload_from_file(buffer, content_type="application/octet-stream")

        logger.info(f"[GCS] Subida completada con éxito: gs://{bucket_name}/{object_name}")

        return {
            "uploaded": True,
            "rows": len(registros),
            "bucket": bucket_name,
            "object": object_name,
        }

    except Exception as e:
        logger.error(f"[GCS ERROR] Fallo al subir archivo al bucket '{bucket_name}': {e}")
        return {
            "uploaded": False,
            "rows": len(registros),
            "bucket": bucket_name,
            "object": object_name,
            "motivo": f"error_subida: {e}",
        }
