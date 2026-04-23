"""
streaming/spark_streaming.py — Fase 2: Estadísticos + K-Means en tiempo real.

Lee trades de criptomonedas del topic 'crypto-trades' de Kafka usando
Spark Structured Streaming y ejecuta tres etapas:

  Parte A — Estadísticos en ventanas de tiempo (1 minuto) por símbolo:
            min, max, promedio, varianza del precio; conteo de trades;
            volumen total. Se escriben a Parquet para Tableau.

  Parte B — Feature engineering por ventana para alimentar K-Means:
            retorno porcentual, volatilidad, ratio de volumen, velocidad
            de cambio del precio.

  Parte C — K-Means (no supervisado) para detección de anomalías:
            entrena con los datos acumulados, clasifica cada punto según
            su distancia al centroide más cercano, y etiqueta como
            "normal" o "anómalo" (percentil 95). Guarda todo en Parquet.

El flujo se divide en dos fases temporales:
  1) Acumulación (~5 minutos): recolecta datos del streaming, calcula
     estadísticos y features, los va guardando.
  2) Entrenamiento K-Means + etiquetado: una vez acumulados suficientes
     datos, entrena K-Means en batch sobre los features guardados,
     etiqueta todo y guarda el dataset final para la Fase 3.

Ejecución (Kafka y el producer deben estar activos):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
        streaming/spark_streaming.py

    O simplemente:
    python streaming/spark_streaming.py

Para detener la fase de acumulación: Ctrl+C o esperar el tiempo configurado.
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
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline

# Agregar directorio raíz del proyecto al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import (
    get_spark_builder, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TRADES,
    STATISTICS_DIR, LABELED_DATA_DIR, KMEANS_MODEL_DIR, CHECKPOINT_DIR,
)

# ── Configuración ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("spark_streaming")

# Duración de la fase de acumulación en segundos.
# 5 minutos da suficientes datos (~15,000-60,000 trades dependiendo del
# volumen del mercado) para que K-Means tenga ventanas significativas.
ACCUMULATION_SECONDS = 1200

# Tamaño de la ventana de tiempo para estadísticos y features.
# 1 minuto es un balance: suficientes trades por ventana para que los
# estadísticos sean estables, pero frecuencia alta para la visualización.
WINDOW_DURATION = "1 minute"

# Intervalo de slide: cada cuánto se emite una nueva ventana.
# 30 segundos genera ventanas solapadas (sliding window), lo que da
# el doble de puntos de datos y suaviza la serie temporal.
SLIDE_DURATION = "30 seconds"

# Watermark: cuánto tiempo esperar por datos tardíos antes de cerrar
# una ventana. 30 segundos es conservador para latencia de red local.
WATERMARK_DELAY = "30 seconds"

# Valores de k a probar en K-Means
# k=3: mercado en calma / volatilidad moderada / movimiento extremo
# k=4,5: subdivisiones más finas si los datos lo justifican
K_VALUES = [3, 4, 5]

# Percentil para umbral de anomalía.
# El 5% de los puntos más alejados de su centroide se consideran anómalos.
# Es un umbral estándar en detección de anomalías por distancia.
ANOMALY_PERCENTILE = 95

# Evento de shutdown
_shutdown = Event()

# ── Schema del JSON de trades ────────────────────────────────────────────────

# Schema explícito porque Structured Streaming no puede inferir schema
# de Kafka (los mensajes son bytes opacos). Debe coincidir exactamente
# con lo que publica binance_producer.py.
TRADE_SCHEMA = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("price", DoubleType(), nullable=False),
    StructField("quantity", DoubleType(), nullable=False),
    StructField("timestamp", LongType(), nullable=False),
    StructField("is_buyer_maker", BooleanType(), nullable=False),
])


# ── Parte A: Estadísticos en ventanas de tiempo ─────────────────────────────

def compute_window_statistics(trades_df: DataFrame) -> DataFrame:
    """
    Calcula estadísticos por símbolo en ventanas de tiempo.

    Agrupamos por (símbolo, ventana) y calculamos:
    - min/max/avg/varianza del precio: perfil básico del comportamiento
    - count: número de trades (proxy de actividad/liquidez)
    - sum(quantity): volumen total operado en la ventana

    La ventana deslizante (sliding window) con solapamiento genera más
    puntos de datos que una ventana tumbling, lo cual es mejor para
    visualización continua en Tableau.
    """
    return (
        trades_df
        .groupBy(
            F.col("symbol"),
            F.window(F.col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
        )
        .agg(
            F.min("price").alias("price_min"),
            F.max("price").alias("price_max"),
            F.avg("price").alias("price_avg"),
            F.variance("price").alias("price_variance"),
            F.count("*").alias("trade_count"),
            F.sum("quantity").alias("total_volume"),
            # Primer y último precio de la ventana para calcular retorno
            F.first("price").alias("price_first"),
            F.last("price").alias("price_last"),
            # Desviación estándar para volatilidad
            F.stddev("price").alias("price_stddev"),
        )
        # Extraer inicio y fin de la ventana como columnas separadas
        # para facilitar el consumo en Tableau
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )


# ── Parte B: Feature engineering para K-Means ───────────────────────────────

def compute_features(stats_df: DataFrame) -> DataFrame:
    """
    Calcula features derivados de los estadísticos por ventana.

    Cada feature captura un aspecto diferente del comportamiento del mercado:

    - pct_return: retorno porcentual del precio en la ventana.
      Mide la dirección y magnitud del movimiento. Un valor de 0.02
      significa que el precio subió 2% durante la ventana.

    - volatility: desviación estándar del precio normalizada por el promedio.
      Mide cuánto fluctúa el precio relativo a su nivel. Alta volatilidad
      indica incertidumbre o actividad inusual.

    - volume_intensity: volumen total normalizado por el número de trades.
      Un valor alto indica trades grandes (ballenas/institucionales);
      un valor bajo indica trades pequeños (retail).

    - price_speed: velocidad de cambio del precio (rango / promedio).
      Mide qué tan amplio fue el rango de precios relativo al nivel.
      Similar a volatilidad pero captura el rango extremo, no la dispersión.

    Se usa coalesce(..., 0.0) para manejar ventanas con un solo trade
    donde stddev/variance serían NULL.
    """
    return (
        stats_df
        .withColumn(
            "pct_return",
            F.when(F.col("price_first") != 0,
                   (F.col("price_last") - F.col("price_first")) / F.col("price_first"))
            .otherwise(0.0)
        )
        .withColumn(
            "volatility",
            F.when(F.col("price_avg") != 0,
                   F.coalesce(F.col("price_stddev"), F.lit(0.0)) / F.col("price_avg"))
            .otherwise(0.0)
        )
        .withColumn(
            "volume_intensity",
            F.when(F.col("trade_count") > 0,
                   F.col("total_volume") / F.col("trade_count"))
            .otherwise(0.0)
        )
        .withColumn(
            "price_speed",
            F.when(F.col("price_avg") != 0,
                   (F.col("price_max") - F.col("price_min")) / F.col("price_avg"))
            .otherwise(0.0)
        )
    )


# ── Parte C: K-Means y etiquetado de anomalías ──────────────────────────────

def train_kmeans_and_label(
    spark: SparkSession,
    features_df: DataFrame,
) -> DataFrame | None:
    """
    Entrena K-Means sobre los features acumulados y etiqueta anomalías.

    K-Means agrupa los datos en k clusters minimizando la suma de distancias
    al cuadrado de cada punto a su centroide más cercano. En este contexto:

    - Cada punto es un vector de 4 features de una ventana de tiempo
      (retorno, volatilidad, volumen, velocidad de cambio).
    - Los clusters representan estados del mercado:
      ej. calma, volatilidad moderada, movimiento extremo.
    - Un punto lejano a todos los centroides no encaja en ningún patrón
      normal → se etiqueta como anómalo.

    Detección de anomalías por distancia al centroide:
    - Calculamos la distancia euclidiana de cada punto a su centroide asignado.
    - Si la distancia supera el percentil 95, el punto es "anómalo".
    - El percentil 95 significa que ~5% de los datos se consideran anómalos,
      lo cual es un umbral estándar en detección de anomalías.

    Selección de k:
    - Probamos k=3,4,5 y elegimos el que maximize el Silhouette Score.
    - Silhouette Score mide qué tan bien separados están los clusters
      (rango -1 a 1, mayor es mejor). Un score > 0.5 indica buena separación.
    """
    feature_cols = ["pct_return", "volatility", "volume_intensity", "price_speed"]

    # Filtrar filas con NaN en features (pueden ocurrir en ventanas con pocos datos)
    clean_df = features_df.dropna(subset=feature_cols)
    row_count = clean_df.count()

    if row_count < 20:
        logger.warning(
            "Solo %d filas con features válidos — insuficiente para K-Means. "
            "Intenta acumular más tiempo.", row_count
        )
        return None

    logger.info("Datos disponibles para K-Means: %d filas", row_count)

    # Pipeline: VectorAssembler → StandardScaler → KMeans
    # VectorAssembler combina las 4 columnas de features en un solo vector.
    # StandardScaler normaliza (media=0, std=1) para que ningún feature
    # domine por su escala (ej: volumen puede ser mucho mayor que retorno).
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="raw_features",
        handleInvalid="skip",
    )
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="scaled_features",
        withStd=True,
        withMean=True,
    )

    # Probar diferentes valores de k y elegir el mejor
    best_k = K_VALUES[0]
    best_score = -1.0
    best_model = None
    evaluator = ClusteringEvaluator(
        predictionCol="prediction",
        featuresCol="scaled_features",
        metricName="silhouette",
    )

    # Preparar datos (assembler + scaler) una sola vez
    prep_pipeline = Pipeline(stages=[assembler, scaler])
    prep_model = prep_pipeline.fit(clean_df)
    prepared_df = prep_model.transform(clean_df)
    # Cachear porque se reutiliza para cada k
    prepared_df.cache()

    for k in K_VALUES:
        logger.info("Entrenando K-Means con k=%d ...", k)
        kmeans = KMeans(
            featuresCol="scaled_features",
            predictionCol="prediction",
            k=k,
            seed=42,
            maxIter=20,
        )
        km_model = kmeans.fit(prepared_df)
        predictions = km_model.transform(prepared_df)
        score = evaluator.evaluate(predictions)
        logger.info("  k=%d → Silhouette Score = %.4f", k, score)

        if score > best_score:
            best_score = score
            best_k = k
            best_model = km_model

    logger.info("Mejor k=%d con Silhouette Score=%.4f", best_k, best_score)

    # Aplicar el mejor modelo
    labeled_df = best_model.transform(prepared_df)

    # Calcular distancia al centroide asignado para detección de anomalías.
    # Spark MLlib no expone la distancia directamente, así que la calculamos
    # comparando cada punto con su centroide asignado.
    centers = best_model.clusterCenters()

    # Crear un broadcast de los centroides para eficiencia
    from pyspark.ml.linalg import Vectors, DenseVector
    import numpy as np

    centers_broadcast = spark.sparkContext.broadcast(
        [c.tolist() for c in centers]
    )

    @F.udf("double")
    def distance_to_centroid(features: DenseVector, cluster: int) -> float:
        """Distancia euclidiana del punto a su centroide asignado."""
        centroid = centers_broadcast.value[cluster]
        point = features.toArray()
        return float(np.sqrt(np.sum((point - np.array(centroid)) ** 2)))

    labeled_df = labeled_df.withColumn(
        "distance_to_centroid",
        distance_to_centroid(F.col("scaled_features"), F.col("prediction")),
    )

    # Calcular umbral de anomalía (percentil 95 de las distancias)
    threshold_row = labeled_df.approxQuantile(
        "distance_to_centroid", [ANOMALY_PERCENTILE / 100.0], 0.01
    )
    threshold = threshold_row[0] if threshold_row else 1.0
    logger.info("Umbral de anomalía (percentil %d): %.4f", ANOMALY_PERCENTILE, threshold)

    # Etiquetar: "anomalo" si distancia > umbral, "normal" si no
    labeled_df = labeled_df.withColumn(
        "label",
        F.when(F.col("distance_to_centroid") > threshold, "anomalo")
        .otherwise("normal")
    )

    # Columna binaria para el modelo supervisado de la Fase 3
    labeled_df = labeled_df.withColumn(
        "label_binary",
        F.when(F.col("label") == "anomalo", 1).otherwise(0)
    )

    # Contar distribución
    label_counts = labeled_df.groupBy("label").count().collect()
    for row in label_counts:
        logger.info("  %s: %d filas", row["label"], row["count"])

    # Liberar caché
    prepared_df.unpersist()

    # Guardar modelo K-Means
    best_model.write().overwrite().save(KMEANS_MODEL_DIR)
    logger.info("Modelo K-Means guardado en: %s", KMEANS_MODEL_DIR)

    # Seleccionar columnas relevantes para guardar
    output_df = labeled_df.select(
        "symbol", "window_start", "window_end",
        "price_min", "price_max", "price_avg", "price_variance",
        "trade_count", "total_volume",
        "pct_return", "volatility", "volume_intensity", "price_speed",
        "prediction", "distance_to_centroid", "label", "label_binary",
    )

    return output_df


# ── Streaming: acumulación de datos ──────────────────────────────────────────

def run_streaming_accumulation(spark: SparkSession) -> DataFrame | None:
    """
    Lee del topic de Kafka en modo streaming, calcula estadísticos y features
    por ventana, y acumula los resultados durante ACCUMULATION_SECONDS.

    Usa foreachBatch para procesar cada micro-batch: calcula estadísticos,
    features, y los guarda incrementalmente en Parquet (modo append).
    Al final, lee todo lo acumulado para la fase de K-Means.
    """
    # Leer stream de Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_TRADES)
        .option("startingOffsets", "latest")
        # maxOffsetsPerTrigger limita cuántos mensajes se procesan por micro-batch.
        # 10,000 es suficiente para no saturar la memoria en modo local (8 GB)
        # pero procesar suficientes trades para estadísticos significativos.
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )

    # Parsear el JSON del value de Kafka
    trades_stream = (
        raw_stream
        .select(F.from_json(
            F.col("value").cast("string"), TRADE_SCHEMA
        ).alias("trade"))
        .select("trade.*")
        # Convertir timestamp de epoch ms a TimestampType para ventanas de tiempo.
        # Spark necesita un tipo timestamp para la función window().
        .withColumn("event_time", (F.col("timestamp") / 1000).cast(TimestampType()))
        # Watermark: le dice a Spark cuánto tiempo esperar por datos tardíos.
        # Datos con event_time más viejo que (max_event_time - watermark) se descartan.
        # 30 segundos es conservador para latencia local.
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    # Directorios temporales para acumulación
    stats_accumulation_dir = os.path.join(STATISTICS_DIR, "_accumulating")
    features_accumulation_dir = os.path.join(LABELED_DATA_DIR, "_accumulating_features")

    os.makedirs(stats_accumulation_dir, exist_ok=True)
    os.makedirs(features_accumulation_dir, exist_ok=True)

    batch_count = [0]  # Mutable para acceso desde el closure

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        """
        Procesa cada micro-batch del streaming.

        foreachBatch recibe un DataFrame estático (no streaming) con los
        datos del micro-batch actual. Esto permite usar operaciones batch
        normales como aggregations complejas y escritura a Parquet.
        """
        if batch_df.isEmpty():
            return

        row_count = batch_df.count()
        batch_count[0] += 1
        logger.info("Micro-batch #%d: %d trades recibidos", batch_id, row_count)

        # Parte A: estadísticos por ventana
        stats = compute_window_statistics(batch_df)

        if stats.isEmpty():
            logger.info("  Sin ventanas completas en este batch (esperando más datos)")
            return

        # Parte B: features
        features = compute_features(stats)

        stats_count = stats.count()
        logger.info("  Ventanas calculadas: %d", stats_count)

        # Guardar estadísticos (para Tableau — Fase 6)
        stats.write.mode("append").parquet(stats_accumulation_dir)

        # Guardar features (para K-Means)
        features.write.mode("append").parquet(features_accumulation_dir)

    # Iniciar el streaming con foreachBatch
    # trigger(processingTime="10 seconds"): cada 10 segundos Spark procesa
    # un micro-batch con todos los mensajes acumulados desde el último.
    # Es un balance entre latencia (datos frescos) y overhead (costo de cada batch).
    checkpoint_streaming = os.path.join(CHECKPOINT_DIR, "streaming_stats")
    os.makedirs(checkpoint_streaming, exist_ok=True)
    # Prefijo file:// para que Spark use el filesystem local,
    # no intente conectarse a HDFS (que causa el error RPC invalid length).
    checkpoint_uri = f"file://{checkpoint_streaming}"

    query = (
        trades_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_uri)
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info(
        "Streaming iniciado. Acumulando datos durante %d segundos (%d min)...",
        ACCUMULATION_SECONDS, ACCUMULATION_SECONDS // 60,
    )
    logger.info("(Puedes detener antes con Ctrl+C)")

    # Esperar el tiempo de acumulación o hasta shutdown
    start_time = time.time()
    try:
        while not _shutdown.is_set():
            elapsed = time.time() - start_time
            if elapsed >= ACCUMULATION_SECONDS:
                logger.info("Tiempo de acumulación completado.")
                break
            remaining = ACCUMULATION_SECONDS - elapsed
            if int(elapsed) % 60 == 0 and int(elapsed) > 0:
                logger.info(
                    "  Acumulando... %.0f/%d segundos (batches procesados: %d)",
                    elapsed, ACCUMULATION_SECONDS, batch_count[0],
                )
            _shutdown.wait(timeout=1)
    except KeyboardInterrupt:
        logger.info("Interrupción recibida, deteniendo acumulación...")
        _shutdown.set()

    query.stop()
    logger.info(
        "Streaming detenido. Total de micro-batches procesados: %d",
        batch_count[0],
    )

    # Leer todos los features acumulados
    if not os.path.exists(features_accumulation_dir):
        logger.error("No se encontraron features acumulados.")
        return None

    try:
        all_features = spark.read.parquet(features_accumulation_dir)
        total_rows = all_features.count()
        logger.info("Total de filas con features acumuladas: %d", total_rows)

        if total_rows == 0:
            logger.error("No hay datos acumulados. ¿El producer estaba enviando?")
            return None

        return all_features
    except Exception as e:
        logger.error("Error leyendo features acumulados: %s", e)
        return None


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("═" * 60)
    logger.info("FASE 2 — Streaming: Estadísticos + K-Means")
    logger.info("═" * 60)

    # Handler de señales
    def signal_handler(sig: int, frame: Any) -> None:
        logger.info("Señal de cierre recibida...")
        _shutdown.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Crear SparkSession
    spark = get_spark_builder("CryptoStreaming-Phase2").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession iniciada. Spark version: %s", spark.version)
    logger.info("Spark UI: http://localhost:4040")

    try:
        # Fase de acumulación: streaming → estadísticos → features → Parquet
        logger.info("─" * 40)
        logger.info("ETAPA 1: Acumulación de datos del streaming")
        logger.info("─" * 40)
        features_df = run_streaming_accumulation(spark)

        if features_df is None:
            logger.error("No se pudieron acumular datos. Abortando.")
            return

        # Fase K-Means: entrenar + etiquetar
        logger.info("─" * 40)
        logger.info("ETAPA 2: Entrenamiento K-Means + etiquetado")
        logger.info("─" * 40)
        labeled_df = train_kmeans_and_label(spark, features_df)

        if labeled_df is None:
            logger.error("K-Means no pudo completarse. Abortando.")
            return

        # Guardar datos etiquetados en Parquet
        labeled_output = os.path.join(LABELED_DATA_DIR, "labeled_trades")
        labeled_df.write.mode("overwrite").parquet(labeled_output)
        logger.info("Datos etiquetados guardados en: %s", labeled_output)

        # Copiar estadísticos a su directorio final
        stats_final = os.path.join(STATISTICS_DIR, "window_statistics")
        stats_accum = os.path.join(STATISTICS_DIR, "_accumulating")
        if os.path.exists(stats_accum):
            stats_from_accum = spark.read.parquet(stats_accum)
            stats_from_accum.write.mode("overwrite").parquet(stats_final)
            logger.info("Estadísticos guardados en: %s", stats_final)

        # Resumen final
        total = labeled_df.count()
        logger.info("═" * 60)
        logger.info("FASE 2 COMPLETADA")
        logger.info("  Total de ventanas procesadas: %d", total)
        logger.info("  Datos etiquetados: %s", labeled_output)
        logger.info("  Estadísticos: %s", stats_final)
        logger.info("  Modelo K-Means: %s", KMEANS_MODEL_DIR)
        logger.info("  → Siguiente paso: python training/train_model.py (Fase 3)")
        logger.info("═" * 60)

    finally:
        spark.stop()
        logger.info("SparkSession cerrada.")


if __name__ == "__main__":
    main()
