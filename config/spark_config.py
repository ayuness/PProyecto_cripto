"""
config/spark_config.py
Configuración centralizada de Spark para todo el proyecto.

Se define aquí para que todos los scripts usen los mismos parámetros
y sea fácil ajustar la configuración al cambiar de arquitectura
(Mac local vs Linux con GPU).
"""

import os
import logging

logger = logging.getLogger(__name__)

# ── Versiones ────────────────────────────────────────────────────────────────
# Scala 2.12 porque Spark 3.5.4 se compiló con esa versión.
# El conector Kafka debe coincidir con la versión de Spark y Scala.
SPARK_VERSION = "3.5.4"
SCALA_VERSION = "2.12"
KAFKA_SPARK_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"

# ── Kafka ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_TRADES = "crypto-trades"

# ── Rutas de salida ──────────────────────────────────────────────────────────
# Relativas al directorio del proyecto (proyecto_final/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

OUTPUT_DIR = os.path.join(BASE_DIR, "output")
STATISTICS_DIR = os.path.join(OUTPUT_DIR, "statistics")
LABELED_DATA_DIR = os.path.join(OUTPUT_DIR, "labeled_data")
PREDICTIONS_DIR = os.path.join(OUTPUT_DIR, "predictions")
METRICS_DIR = os.path.join(OUTPUT_DIR, "metrics")

MODELS_DIR = os.path.join(BASE_DIR, "models")
KMEANS_MODEL_DIR = os.path.join(MODELS_DIR, "kmeans_model")
LOGISTIC_MODEL_DIR = os.path.join(MODELS_DIR, "logistic_model")

# Checkpoints para Structured Streaming (tolerancia a fallos)
CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoints")

# ── Spark Session builder ────────────────────────────────────────────────────

def get_spark_builder(app_name: str):
    """
    Retorna un SparkSession.builder preconfigurado.

    Se usa builder (no session) para que cada script pueda agregar
    configuración adicional antes de llamar .getOrCreate().

    Parámetros de memoria:
    - driver memory 2g: suficiente para un solo nodo local con 8 GB totales.
      En la máquina Linux con más RAM se puede subir.
    - shuffle partitions 4: valor bajo porque corremos en modo local.
      El default de Spark (200) genera demasiadas particiones pequeñas
      para un cluster de un solo nodo, lo que agrega overhead de scheduling.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Conector Kafka — se descarga automáticamente de Maven la primera vez
        .config("spark.jars.packages", KAFKA_SPARK_PACKAGE)
        # Memoria del driver ajustada al hardware disponible
        .config("spark.driver.memory", "2g")
        # Reducir particiones de shuffle para modo local
        .config("spark.sql.shuffle.partitions", "4")
        # Forzar filesystem local. Sin esto, Spark hereda la config de
        # Hadoop ($HADOOP_CONF_DIR/core-site.xml) que apunta a hdfs://localhost:9000
        # y falla con "RPC response has invalid length" porque HDFS no está corriendo.
        .config("spark.hadoop.fs.defaultFS", "file:///")
        # Habilitar UI para captura de métricas
        .config("spark.ui.enabled", "true")
    )

    logger.info(
        "SparkSession builder configurado: app=%s, kafka_package=%s",
        app_name, KAFKA_SPARK_PACKAGE
    )

    return builder
