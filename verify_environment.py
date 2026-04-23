"""
verify_environment.py — Fase 0: Verificación de conectividad Spark ↔ Kafka.

Comprueba que Spark puede leer de un topic de Kafka usando el conector
spark-sql-kafka-0-10. Para esta prueba, Kafka debe estar corriendo.

Ejecución (Kafka debe estar activo):
    python verify_environment.py

Qué hace:
    1. Crea el topic 'crypto-trades' si no existe
    2. Publica un mensaje de prueba con kafka-python
    3. Lee ese mensaje desde Spark Structured Streaming (batch mode)
    4. Imprime el resultado y confirma que la integración funciona
"""

import json
import logging
import subprocess
import sys
import time
from typing import NoReturn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("verify_environment")


def check_kafka_running() -> bool:
    """Verifica si hay un proceso de Kafka corriendo."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "kafka.Kafka"],
            capture_output=True, text=True, timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def create_topic(topic: str, bootstrap: str = "localhost:9092") -> None:
    """Crea un topic de Kafka si no existe."""
    logger.info("Creando topic '%s' (si no existe)...", topic)
    subprocess.run(
        [
            "kafka-topics", "--bootstrap-server", bootstrap,
            "--create", "--if-not-exists",
            "--topic", topic,
            "--partitions", "1",
            "--replication-factor", "1",
        ],
        capture_output=True, text=True, timeout=15,
        check=True,
    )
    logger.info("Topic '%s' listo.", topic)


def produce_test_message(topic: str, bootstrap: str = "localhost:9092") -> dict:
    """Publica un mensaje de prueba en el topic."""
    from kafka import KafkaProducer

    test_msg = {
        "symbol": "TESTUSDT",
        "price": 99999.99,
        "quantity": 0.001,
        "timestamp": int(time.time() * 1000),
        "is_buyer_maker": False,
        "test": True,
    }

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    producer.send(topic, value=test_msg)
    producer.flush()
    producer.close()

    logger.info("Mensaje de prueba publicado en '%s': %s", topic, test_msg)
    return test_msg


def verify_spark_kafka(topic: str, bootstrap: str = "localhost:9092") -> bool:
    """
    Lee del topic con Spark en modo batch (no streaming continuo).
    Usa 'earliest' para leer desde el inicio y 'latest' como fin,
    lo que hace una lectura puntual de todo lo disponible en el topic.
    """
    from pyspark.sql import SparkSession

    # Importar configuración del proyecto
    sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))
    from config.spark_config import KAFKA_SPARK_PACKAGE

    logger.info("Iniciando SparkSession con conector Kafka...")
    logger.info("(La primera vez descarga el JAR de Maven, puede tardar ~1 min)")

    spark = (
        SparkSession.builder
        .appName("VerifyEnvironment")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_SPARK_PACKAGE)
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")  # No necesitamos UI para esta prueba
        .getOrCreate()
    )

    logger.info("SparkSession creada. Spark version: %s", spark.version)

    # Lectura batch desde Kafka (no streaming, solo verificación)
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    count = df.count()
    logger.info("Mensajes leídos del topic '%s': %d", topic, count)

    if count > 0:
        # Mostrar el último mensaje como string
        from pyspark.sql.functions import col
        sample = (
            df.select(col("value").cast("string").alias("json_value"))
            .orderBy(col("timestamp").desc())
            .limit(1)
            .collect()
        )
        if sample:
            logger.info("Último mensaje: %s", sample[0]["json_value"])

    spark.stop()
    return count > 0


def main() -> None:
    logger.info("═" * 60)
    logger.info("FASE 0 — Verificación de entorno: Spark ↔ Kafka")
    logger.info("═" * 60)

    # 1. Verificar que Kafka está corriendo
    if not check_kafka_running():
        logger.error(
            "Kafka no está corriendo. Levántalo primero con:\n"
            "  kafka-server-start /opt/homebrew/etc/kafka/server.properties"
        )
        sys.exit(1)

    logger.info("✓ Kafka está corriendo")

    # 2. Crear topic
    topic = "crypto-trades"
    create_topic(topic)
    logger.info("✓ Topic '%s' creado/verificado", topic)

    # 3. Publicar mensaje de prueba
    produce_test_message(topic)
    logger.info("✓ Mensaje de prueba publicado")

    # 4. Leer desde Spark
    success = verify_spark_kafka(topic)

    logger.info("═" * 60)
    if success:
        logger.info("✓ VERIFICACIÓN EXITOSA — Spark puede leer de Kafka")
        logger.info("  El entorno está listo para la Fase 1.")
    else:
        logger.warning(
            "⚠ Spark se conectó a Kafka pero no leyó mensajes. "
            "Esto puede pasar si el topic estaba vacío antes de la prueba. "
            "Intenta ejecutar de nuevo."
        )
    logger.info("═" * 60)


if __name__ == "__main__":
    main()
