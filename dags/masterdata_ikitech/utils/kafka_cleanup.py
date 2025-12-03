"""
Utilidad de Limpieza de Kafka
Autor: IkiTech
Descripción: Elimina tópicos obsoletos o mal nombrados para preparar el entorno.
"""

import os
import logging
from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# LISTA EXACTA DE TÓPICOS VIEJOS A ELIMINAR
TOPICOS_A_BORRAR = [
    "masterdata.erp.cadena",
    "masterdata.erp.canal",
    "masterdata.erp.categoria",
    "masterdata.erp.ciudad",
    "masterdata.erp.compania",
    "masterdata.erp.dependencia",
    "masterdata.erp.gerencia",
    "masterdata.erp.moneda",
    "masterdata.erp.pais",
    "masterdata.erp.segmentos",
    "masterdata.erp.subcategoria",
    "masterdata.erp.uen",
    "masterdata.erp.sublinea",
]

def borrar_topicos_viejos():
    logger.info(f"🔌 Conectando a Kafka en {KAFKA_BOOTSTRAP_SERVERS} para limpieza...")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='admin_cleanup_script'
        )
    except Exception as e:
        logger.error(f"❌ Error de conexión: {e}")
        return

    # Obtener qué tópicos existen realmente para no fallar intentando borrar fantasmas
    existing_topics = set(admin_client.list_topics())
    
    # Filtrar: Solo intentamos borrar los que realmente existen
    to_delete = [t for t in TOPICOS_A_BORRAR if t in existing_topics]
    ignored = [t for t in TOPICOS_A_BORRAR if t not in existing_topics]

    if ignored:
        logger.info(f"⚠️ {len(ignored)} tópicos de la lista no se encontraron (ya estaban borrados).")

    if not to_delete:
        logger.info("✅ No hay nada que borrar. El entorno está limpio.")
        admin_client.close()
        return

    logger.info(f"🗑️ Eliminando {len(to_delete)} tópicos obsoletos...")

    try:
        # Borrado en lote
        admin_client.delete_topics(topics=to_delete)
        
        logger.info("✅ Eliminación completada exitosamente:")
        for t in to_delete:
            logger.info(f"   - {t} [ELIMINADO]")
            
    except UnknownTopicOrPartitionError:
        logger.warning("Error de concurrencia: Alguien más borró un tópico mientras este script corría.")
    except Exception as e:
        logger.error(f"❌ Error eliminando tópicos: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    borrar_topicos_viejos()