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

# ── RAPIDS Accelerator (GPU) ─────────────────────────────────────────────────
# Se habilita con la variable de entorno SPARK_USE_GPU=1.
# Requiere el JAR de RAPIDS (rapids-4-spark_2.12-25.10.0.jar) en RAPIDS_JAR_PATH.
USE_GPU = os.environ.get("SPARK_USE_GPU", "0") == "1"
RAPIDS_JAR_PATH = os.environ.get(
    "RAPIDS_JAR_PATH",
    os.path.expanduser("~/tools/rapids/rapids-4-spark_2.12-25.10.0.jar"),
)

# Memoria del driver — configurable por arquitectura.
# Mac M1 (8 GB total): 2g; Linux WSL (7.6 GB): 3g; servidor Linux con más RAM: 4g+.
DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "2g")

# Sufijo para el app name, útil para distinguir corridas en Spark UI.
APP_NAME_SUFFIX = os.environ.get("SPARK_APP_SUFFIX", "")


# ── Spark Session builder ────────────────────────────────────────────────────

def get_spark_builder(app_name: str):
    """
    Retorna un SparkSession.builder preconfigurado.

    Se usa builder (no session) para que cada script pueda agregar
    configuración adicional antes de llamar .getOrCreate().

    Variables de entorno reconocidas:
    - SPARK_USE_GPU=1         → activa el plugin RAPIDS Accelerator
    - SPARK_DRIVER_MEMORY=Xg  → override de memoria del driver
    - SPARK_APP_SUFFIX=<txt>  → sufijo para distinguir corridas en Spark UI
    - RAPIDS_JAR_PATH=<path>  → ubicación del JAR de RAPIDS

    Parámetros de memoria:
    - driver memory por defecto 2g (Mac M1, 8 GB). Subir a 3-4g en Linux.
    - shuffle partitions 4: valor bajo porque corremos en modo local.
      El default de Spark (200) genera demasiadas particiones pequeñas
      para un cluster de un solo nodo, lo que agrega overhead de scheduling.
    """
    from pyspark.sql import SparkSession

    full_app_name = f"{app_name}-{APP_NAME_SUFFIX}" if APP_NAME_SUFFIX else app_name

    builder = (
        SparkSession.builder
        .appName(full_app_name)
        .master("local[*]")
        # Conector Kafka — se descarga automáticamente de Maven la primera vez
        .config("spark.jars.packages", KAFKA_SPARK_PACKAGE)
        # Memoria del driver ajustada al hardware disponible
        .config("spark.driver.memory", DRIVER_MEMORY)
        # Reducir particiones de shuffle para modo local
        .config("spark.sql.shuffle.partitions", "4")
        # Forzar filesystem local. Sin esto, Spark hereda la config de
        # Hadoop ($HADOOP_CONF_DIR/core-site.xml) que apunta a hdfs://localhost:9000
        # y falla con "RPC response has invalid length" porque HDFS no está corriendo.
        .config("spark.hadoop.fs.defaultFS", "file:///")
        # Habilitar UI para captura de métricas
        .config("spark.ui.enabled", "true")
    )

    if USE_GPU:
        if not os.path.isfile(RAPIDS_JAR_PATH):
            raise FileNotFoundError(
                f"SPARK_USE_GPU=1 pero no se encontró el JAR de RAPIDS en {RAPIDS_JAR_PATH}. "
                "Descarga el JAR o ajusta RAPIDS_JAR_PATH."
            )
        # Configuración mínima recomendada por la guía oficial de RAPIDS para
        # modo local (https://docs.nvidia.com/spark-rapids/user-guide/latest/
        # getting-started/local-mode.html).
        #
        # Importante: en `local[*]` NO se configuran spark.driver/executor/task
        # resource.gpu.amount — el resource scheduling de Spark queda
        # desactivado y los tasks no se quedan esperando una GPU "libre".
        # RAPIDS detecta la GPU directamente vía cuDF/CUDA y usa
        # `concurrentGpuTasks` para serializar las operaciones que sí caen
        # en GPU.
        builder = (
            builder
            .config("spark.jars", RAPIDS_JAR_PATH)
            .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
            .config("spark.rapids.sql.enabled", "true")
            # NONE para reducir ruido en logs; se cambia a NOT_ON_GPU si se
            # quiere debug del placement de operadores.
            .config("spark.rapids.sql.explain", "NONE")
            # Memoria GPU reservada para RAPIDS (fracción del total VRAM).
            # 0.5 deja margen para drivers/Windows host. RTX 4060 = 8 GB.
            .config("spark.rapids.memory.gpu.allocFraction", "0.5")
            .config("spark.rapids.memory.gpu.maxAllocFraction", "0.7")
            # Lecturas/escrituras de Parquet aceleradas por GPU.
            .config("spark.rapids.sql.format.parquet.enabled", "true")
            .config("spark.rapids.sql.format.parquet.read.enabled", "true")
            .config("spark.rapids.sql.format.parquet.write.enabled", "true")
            # Tasks concurrentes en GPU. 1 evita contención de memoria.
            .config("spark.rapids.sql.concurrentGpuTasks", "1")
            # Permitir operadores marcados "incompatibles" (resultado puede
            # diferir ligeramente del CPU por orden de operaciones float).
            # Es aceptable para este proyecto; la diferencia es < 1e-10.
            .config("spark.rapids.sql.incompatibleOps.enabled", "true")
            # Timezone UTC habilita JsonToStructs en GPU (de lo contrario
            # cae a CPU por ser timezone-sensitive). El producer envía
            # timestamps en epoch ms, así que UTC es el comportamiento
            # correcto y no afecta el resultado.
            .config("spark.sql.session.timeZone", "UTC")
        )
        logger.info(
            "RAPIDS Accelerator HABILITADO: jar=%s",
            RAPIDS_JAR_PATH,
        )

    logger.info(
        "SparkSession builder configurado: app=%s, gpu=%s, driver_mem=%s, kafka_package=%s",
        full_app_name, USE_GPU, DRIVER_MEMORY, KAFKA_SPARK_PACKAGE,
    )

    return builder
