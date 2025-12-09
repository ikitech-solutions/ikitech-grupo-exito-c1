"""
Utilidad de Administración de Kafka (Infrastructure as Code)
Autor: IkiTech
Descripción: Script para crear tópicos en Kafka con configuración predeterminada y con 
retención temporal para evitar acumulación de mensajes.
"""

import os
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DE ENTORNO ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_PREFIX = os.getenv('KAFKA_TOPIC_PREFIX', 'masterdata.erp.')
NUM_PARTITIONS = int(os.getenv('KAFKA_PARTITIONS', '1'))
REPLICATION_FACTOR = int(os.getenv('KAFKA_REPLICATION_FACTOR', '1'))

# --- CONFIGURACIÓN DE RETENCIÓN NORMAL ---
TOPIC_CONFIGS = {
    'cleanup.policy': 'delete',
    'retention.ms': '600000',
    'segment.ms': '300000',
    'retention.bytes': '-1'
}

# --- LISTA MAESTRA DE TABLAS ---
LISTA_MAESTRAS = [
    'compania',
    'moneda',
    'pais',
    'ciudad',
    'dependencia',
    'sublinea',
    'cadena',
    'gerencia',
    'categoria',
    'subcategoria',
    'uen',
    'canal',
    'segmentos'
]

def crear_topicos_maestras():
    logger.info(f"Iniciando configuración de tópicos en: {KAFKA_BOOTSTRAP_SERVERS}")

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='admin_setup_script'
        )
    except NoBrokersAvailable:
        logger.error("❌ No se pudo conectar a Kafka. Verifica que el servidor esté levantado.")
        return

    existing_topics = admin_client.list_topics()
    topic_list = []

    for tabla in LISTA_MAESTRAS:
        tabla_clean = tabla.lower().strip()
        topic_name = f"{TOPIC_PREFIX}{tabla_clean}"

        if topic_name in existing_topics:
            logger.info(f"⚠️ El tópico '{topic_name}' ya existe. Saltando...")
            continue

        logger.info(f"➕ Preparando creación de tópico normal: {topic_name}")

        new_topic = NewTopic(
            name=topic_name,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR,
            topic_configs=TOPIC_CONFIGS
        )

        topic_list.append(new_topic)

    if not topic_list:
        logger.info("✅ No hay tópicos nuevos para crear.")
        admin_client.close()
        return

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info(f"🚀 Se crearon exitosamente {len(topic_list)} tópicos.")
        for t in topic_list:
            logger.info(f"   - {t.name}")
    except TopicAlreadyExistsError:
        logger.warning("Algunos tópicos ya existían.")
    except Exception as e:
        logger.error(f"❌ Error creando tópicos: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    crear_topicos_maestras()
