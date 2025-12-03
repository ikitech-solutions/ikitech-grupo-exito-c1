"""
Utilidad de Administración de Kafka (Infrastructure as Code)
Autor: IkiTech
Descripción: Script para crear y configurar automáticamente tópicos de Kafka 
con políticas de Log Compaction. Soporta entornos Local y Prod.
"""

import os
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DE ENTORNO ---
# Por defecto local, (con variables para producción)
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_PREFIX = os.getenv('KAFKA_TOPIC_PREFIX', 'maestras.erp.')
NUM_PARTITIONS = int(os.getenv('KAFKA_PARTITIONS', '3'))
REPLICATION_FACTOR = int(os.getenv('KAFKA_REPLICATION_FACTOR', '1'))

# --- CONFIGURACIÓN DE COMPACTACIÓN ---
# Estas configuraciones se aplicarán a TODAS las maestras definidas abajo
TOPIC_CONFIGS = {
    'cleanup.policy': 'compact',                # Mantener solo última versión por PK
    'min.compaction.lag.ms': '60000',           # 1 minuto de espera antes de compactar
    'delete.retention.ms': '86400000',          # 24 horas para retener borrados (tombstones)
    'segment.ms': '3600000',                    # Rotar log cada 1 hora
    'min.cleanable.dirty.ratio': '0.5'          # Compactar cuando el 50% sea redundante
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
        logger.error("❌ No se pudo conectar a Kafka. Verifica que el servidor esté arriba.")
        return

    # Obtener tópicos existentes para no intentar crearlos de nuevo
    existing_topics = admin_client.list_topics()
    
    topic_list = []
    
    for tabla in LISTA_MAESTRAS:
        # Normalizamos nombre: minúsculas y sin espacios
        tabla_clean = tabla.lower().strip()
        topic_name = f"{TOPIC_PREFIX}{tabla_clean}"
        
        if topic_name in existing_topics:
            logger.info(f"⚠️ El tópico '{topic_name}' ya existe. Saltando...")
            continue
            
        logger.info(f"➕ Preparando creación de: {topic_name}")
        
        # Creamos el objeto NewTopic con las configs inyectadas
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR,
            topic_configs=TOPIC_CONFIGS
        )
        topic_list.append(new_topic)

    if not topic_list:
        logger.info("✅ No hay tópicos nuevos para crear. Todo está actualizado.")
        admin_client.close()
        return

    # Creación en BATCH
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info(f"🚀 Se crearon exitosamente {len(topic_list)} tópicos nuevos.")
        for t in topic_list:
            logger.info(f"   - {t.name} (Compactado)")
    except TopicAlreadyExistsError:
        logger.warning("Algunos tópicos ya existían (Condición de carrera). Verificar estado.")
    except Exception as e:
        logger.error(f"❌ Error creando tópicos: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    crear_topicos_maestras()