"""
streaming/spark_inference.py — Fase 4: Inferencia en streaming con modelo supervisado.

Reactiva la lectura del topic 'crypto-trades' con Spark Structured Streaming,
calcula los mismos features por ventana de tiempo (idéntico a Fase 2), carga
el modelo de Logistic Regression entrenado en la Fase 3, y clasifica cada
nueva ventana como "normal" o "anómalo" en tiempo real.

Las predicciones se escriben a Parquet para análisis posterior y visualización
en Tableau.

Ejecución (Kafka y el producer deben estar activos):
    python streaming/spark_inference.py

Para detener: Ctrl+C
"""

import logging
import os
import signal
import sys
import time
from threading import Event
from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, BooleanType, TimestampType,
)
from pyspark.ml import PipelineModel

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import (
    get_spark_builder, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TRADES,
    PREDICTIONS_DIR, LOGISTIC_MODEL_DIR, CHECKPOINT_DIR,
)
# Reutilizar las funciones de feature engineering de la Fase 2
# para garantizar que los features sean idénticos
from streaming.spark_streaming import (
    compute_window_statistics, compute_features,
    TRADE_SCHEMA, WINDOW_DURATION, SLIDE_DURATION, WATERMARK_DELAY,
)

# ── Configuración ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("spark_inference")

# Duración de la fase de inferencia.
# 3 minutos es suficiente para demostrar que el modelo clasifica en tiempo real.
INFERENCE_SECONDS = 180

_shutdown = Event()


# ── Inferencia por micro-batch ───────────────────────────────────────────────

def create_batch_processor(model: PipelineModel):
    """
    Crea el callback de foreachBatch que aplica el modelo a cada micro-batch.

    Se usa un closure para capturar la referencia al modelo cargado.
    Cada micro-batch pasa por:
      1. Estadísticos por ventana (mismo cálculo que Fase 2)
      2. Feature engineering (mismas 4 features)
      3. Inferencia con el pipeline de Logistic Regression
      4. Escritura a Parquet (append)

    La columna 'prediction' del K-Means que pueda existir en los datos
    no afecta porque los features se calculan desde cero en cada batch.
    """
    predictions_dir = os.path.join(PREDICTIONS_DIR, "realtime_predictions")
    os.makedirs(predictions_dir, exist_ok=True)
    batch_count = [0]

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        row_count = batch_df.count()
        batch_count[0] += 1
        logger.info("Micro-batch #%d: %d trades recibidos", batch_id, row_count)

        # Calcular estadísticos y features (idéntico a Fase 2)
        stats = compute_window_statistics(batch_df)
        if stats.isEmpty():
            logger.info("  Sin ventanas completas en este batch")
            return

        features = compute_features(stats)
        window_count = features.count()

        # Aplicar modelo de Logistic Regression.
        # El pipeline incluye VectorAssembler + StandardScaler + LogisticRegression,
        # así que basta con llamar transform() sobre el DataFrame con las 4 features.
        predictions = model.transform(features)

        # Contar predicciones por clase
        normal_count = predictions.filter(F.col("prediction") == 0.0).count()
        anomaly_count = predictions.filter(F.col("prediction") == 1.0).count()

        logger.info(
            "  Ventanas: %d | Normal: %d | Anómalo: %d",
            window_count, normal_count, anomaly_count,
        )

        # Seleccionar columnas relevantes para guardar
        output = predictions.select(
            "symbol", "window_start", "window_end",
            "price_min", "price_max", "price_avg", "price_variance",
            "trade_count", "total_volume",
            "pct_return", "volatility", "volume_intensity", "price_speed",
            "prediction", "probability",
        )

        # Guardar predicciones (append para acumular a lo largo de la sesión)
        output.write.mode("append").parquet(predictions_dir)

    return process_batch, batch_count


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("═" * 60)
    logger.info("FASE 4 — Inferencia en streaming con Logistic Regression")
    logger.info("═" * 60)

    # Verificar que el modelo existe
    if not os.path.exists(LOGISTIC_MODEL_DIR):
        logger.error(
            "No se encontró el modelo en: %s\n"
            "Ejecuta primero la Fase 3: python training/train_model.py",
            LOGISTIC_MODEL_DIR,
        )
        sys.exit(1)

    # Handler de señales
    def signal_handler(sig: int, frame: Any) -> None:
        logger.info("Señal de cierre recibida...")
        _shutdown.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Crear SparkSession
    spark = get_spark_builder("CryptoInference-Phase4").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession iniciada. Spark version: %s", spark.version)
    logger.info("Spark UI: http://localhost:4040")

    try:
        # Cargar modelo
        logger.info("Cargando modelo desde: %s", LOGISTIC_MODEL_DIR)
        model = PipelineModel.load(LOGISTIC_MODEL_DIR)
        logger.info("Modelo cargado exitosamente.")

        # Leer stream de Kafka
        raw_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC_TRADES)
            # 'latest' para procesar solo datos nuevos desde este momento,
            # no reprocesar los de la Fase 2.
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", 10000)
            .load()
        )

        # Parsear JSON (mismo schema que Fase 2)
        trades_stream = (
            raw_stream
            .select(F.from_json(
                F.col("value").cast("string"), TRADE_SCHEMA
            ).alias("trade"))
            .select("trade.*")
            .withColumn("event_time", (F.col("timestamp") / 1000).cast(TimestampType()))
            .withWatermark("event_time", WATERMARK_DELAY)
        )

        # Configurar checkpoint con prefijo file://
        checkpoint_inference = os.path.join(CHECKPOINT_DIR, "inference")
        os.makedirs(checkpoint_inference, exist_ok=True)
        checkpoint_uri = f"file://{checkpoint_inference}"

        # Crear processor con el modelo
        process_batch, batch_count = create_batch_processor(model)

        # Iniciar streaming
        query = (
            trades_stream.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", checkpoint_uri)
            .trigger(processingTime="10 seconds")
            .start()
        )

        logger.info(
            "Inferencia en streaming iniciada. Ejecutando %d segundos (%d min)...",
            INFERENCE_SECONDS, INFERENCE_SECONDS // 60,
        )
        logger.info("(Puedes detener antes con Ctrl+C)")

        # Esperar
        start_time = time.time()
        try:
            while not _shutdown.is_set():
                elapsed = time.time() - start_time
                if elapsed >= INFERENCE_SECONDS:
                    logger.info("Tiempo de inferencia completado.")
                    break
                if int(elapsed) % 60 == 0 and int(elapsed) > 0:
                    logger.info(
                        "  Ejecutando... %.0f/%d segundos (batches: %d)",
                        elapsed, INFERENCE_SECONDS, batch_count[0],
                    )
                _shutdown.wait(timeout=1)
        except KeyboardInterrupt:
            logger.info("Interrupción recibida...")
            _shutdown.set()

        query.stop()

        # Resumen final
        predictions_path = os.path.join(PREDICTIONS_DIR, "realtime_predictions")
        if os.path.exists(predictions_path):
            results = spark.read.parquet(predictions_path)
            total = results.count()
            logger.info("═" * 60)
            logger.info("FASE 4 COMPLETADA")
            logger.info("  Batches procesados: %d", batch_count[0])
            logger.info("  Predicciones totales: %d", total)
            results.groupBy("prediction").count().show()
            logger.info("  Predicciones guardadas en: %s", predictions_path)
            logger.info("  → Siguiente paso: python metrics/capture_metrics.py (Fase 5)")
            logger.info("═" * 60)
        else:
            logger.warning("No se generaron predicciones.")

    finally:
        spark.stop()
        logger.info("SparkSession cerrada.")


if __name__ == "__main__":
    main()
