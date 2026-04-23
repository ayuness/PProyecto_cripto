"""
visualization/prepare_tableau_data.py — Fase 6: Preparación de datos para Tableau.

Consolida todos los outputs del proyecto (estadísticos, predicciones, métricas)
en archivos CSV que Tableau pueda consumir directamente.

Tableau puede leer Parquet, pero CSV es más universal y evita problemas
de compatibilidad con versiones de Parquet/Spark. Además, CSV permite
inspección manual rápida.

Genera 4 archivos CSV:
  1. window_statistics.csv — Estadísticos por ventana (serie temporal para dashboard animado)
  2. labeled_data.csv — Datos etiquetados por K-Means (primera tanda)
  3. predictions.csv — Predicciones del modelo supervisado (segunda tanda)
  4. spark_metrics_summary.csv — Métricas de Spark UI de ambas máquinas (comparación)

Ejecución (requiere outputs de Fases 2, 3 y 4):
    python visualization/prepare_tableau_data.py

Output:
    output/tableau/
    ├── window_statistics.csv
    ├── labeled_data.csv
    ├── predictions.csv
    └── spark_metrics_summary.csv

Instrucciones para Tableau:
  - Conectar como "Text file" → seleccionar el CSV
  - Para la visualización animada: usar window_start como eje temporal,
    symbol como color/filtro, y las métricas como valores
"""

import glob
import json
import logging
import os
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import (
    STATISTICS_DIR, LABELED_DATA_DIR, PREDICTIONS_DIR, METRICS_DIR,
)

# ── Configuración ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("prepare_tableau")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TABLEAU_DIR = os.path.join(BASE_DIR, "output", "tableau")


# ── Utilidades ───────────────────────────────────────────────────────────────

def write_single_csv(df: DataFrame, output_path: str, order_by: str | None = None) -> int:
    """
    Escribe un DataFrame a un único archivo CSV.

    Spark por defecto escribe múltiples archivos (uno por partición).
    coalesce(1) fuerza un solo archivo, lo cual es necesario para que
    Tableau pueda abrir el CSV directamente sin tener que leer un directorio.

    Retorna el número de filas escritas.
    """
    count = df.count()
    if count == 0:
        logger.warning("DataFrame vacío, no se genera CSV: %s", output_path)
        return 0

    if order_by and order_by in df.columns:
        df = df.orderBy(order_by)

    # Escribir a directorio temporal, luego mover el archivo único
    temp_dir = output_path + "_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

    # Encontrar el archivo part-* generado y renombrarlo
    part_files = glob.glob(os.path.join(temp_dir, "part-*.csv"))
    if part_files:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        os.rename(part_files[0], output_path)

    # Limpiar directorio temporal
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)

    logger.info("  %s — %d filas", os.path.basename(output_path), count)
    return count


# ── Procesamiento de cada dataset ────────────────────────────────────────────

def export_statistics(spark: SparkSession) -> None:
    """
    Exporta los estadísticos por ventana de la primera tanda de streaming.
    Estos datos alimentan el dashboard animado de Tableau: serie temporal
    de precios con min/max/avg/varianza por símbolo.
    """
    stats_path = os.path.join(STATISTICS_DIR, "window_statistics")
    if not os.path.exists(stats_path):
        # Intentar con el directorio de acumulación
        stats_path = os.path.join(STATISTICS_DIR, "_accumulating")
    if not os.path.exists(stats_path):
        logger.warning("No se encontraron estadísticos. Saltando.")
        return

    df = spark.read.parquet(stats_path)

    # Convertir timestamps a strings legibles para Tableau
    df = (
        df
        .withColumn("window_start_str", F.date_format("window_start", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("window_end_str", F.date_format("window_end", "yyyy-MM-dd HH:mm:ss"))
    )

    output_path = os.path.join(TABLEAU_DIR, "window_statistics.csv")
    write_single_csv(df, output_path, order_by="window_start")


def export_labeled_data(spark: SparkSession) -> None:
    """
    Exporta los datos etiquetados por K-Means (primera tanda).
    Incluye features, cluster asignado, distancia al centroide y etiqueta.
    En Tableau se puede visualizar como scatter plot de features coloreado
    por etiqueta (normal/anómalo).
    """
    labeled_path = os.path.join(LABELED_DATA_DIR, "labeled_trades")
    if not os.path.exists(labeled_path):
        logger.warning("No se encontraron datos etiquetados. Saltando.")
        return

    df = spark.read.parquet(labeled_path)

    # Convertir timestamps
    if "window_start" in df.columns:
        df = (
            df
            .withColumn("window_start_str", F.date_format("window_start", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("window_end_str", F.date_format("window_end", "yyyy-MM-dd HH:mm:ss"))
        )

    output_path = os.path.join(TABLEAU_DIR, "labeled_data.csv")
    write_single_csv(df, output_path, order_by="window_start")


def export_predictions(spark: SparkSession) -> None:
    """
    Exporta las predicciones del modelo supervisado (segunda tanda).
    Incluye la probabilidad de cada clase para análisis de confianza.

    La columna 'probability' de Spark ML es un DenseVector con [P(0), P(1)].
    La extraemos como dos columnas separadas para Tableau.
    """
    predictions_path = os.path.join(PREDICTIONS_DIR, "realtime_predictions")
    if not os.path.exists(predictions_path):
        logger.warning("No se encontraron predicciones. Saltando.")
        return

    df = spark.read.parquet(predictions_path)

    # Extraer probabilidades del vector de Spark ML.
    # El tipo 'vector' de MLlib no se puede manipular con funciones SQL
    # estándar; se necesita vector_to_array() para convertirlo a un
    # array nativo de Spark, y luego extraer los elementos.
    if "probability" in df.columns:
        from pyspark.ml.functions import vector_to_array
        df = (
            df
            .withColumn("prob_array", vector_to_array("probability"))
            .withColumn("prob_normal", F.col("prob_array").getItem(0))
            .withColumn("prob_anomalo", F.col("prob_array").getItem(1))
            .drop("probability", "prob_array")
        )

    # Etiqueta legible
    df = df.withColumn(
        "label_predicted",
        F.when(F.col("prediction") == 1.0, "anomalo").otherwise("normal")
    )

    if "window_start" in df.columns:
        df = (
            df
            .withColumn("window_start_str", F.date_format("window_start", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("window_end_str", F.date_format("window_end", "yyyy-MM-dd HH:mm:ss"))
        )

    output_path = os.path.join(TABLEAU_DIR, "predictions.csv")
    write_single_csv(df, output_path, order_by="window_start")


def export_spark_metrics() -> None:
    """
    Consolida todos los JSON de métricas de Spark UI en un solo CSV
    con una fila por arquitectura para comparación directa en Tableau.

    Busca archivos spark_metrics_*.json en output/metrics/.
    """
    pattern = os.path.join(METRICS_DIR, "spark_metrics_*.json")
    metric_files = glob.glob(pattern)

    if not metric_files:
        logger.warning("No se encontraron archivos de métricas Spark. Saltando.")
        return

    rows = []
    for filepath in metric_files:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        summary = data.get("summary", {})
        row = {
            "architecture": data.get("architecture", "unknown"),
            "capture_timestamp": data.get("capture_timestamp", ""),
            "app_name": data.get("application", {}).get("name", ""),
        }
        row.update(summary)
        rows.append(row)

    if rows:
        # Escribir CSV manualmente (no necesita Spark para pocas filas)
        import csv
        output_path = os.path.join(TABLEAU_DIR, "spark_metrics_summary.csv")
        os.makedirs(TABLEAU_DIR, exist_ok=True)

        # Usar todas las keys encontradas como columnas
        all_keys = []
        for row in rows:
            for k in row.keys():
                if k not in all_keys:
                    all_keys.append(k)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys)
            writer.writeheader()
            writer.writerows(rows)

        logger.info("  spark_metrics_summary.csv — %d arquitecturas", len(rows))


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("═" * 60)
    logger.info("FASE 6 — Preparación de datos para Tableau")
    logger.info("═" * 60)

    os.makedirs(TABLEAU_DIR, exist_ok=True)

    # SparkSession solo para leer Parquet y escribir CSV
    from config.spark_config import get_spark_builder
    spark = (
        get_spark_builder("TableauExport-Phase6")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession iniciada.")

    try:
        logger.info("Exportando datasets a CSV...")

        export_statistics(spark)
        export_labeled_data(spark)
        export_predictions(spark)
        export_spark_metrics()  # No necesita Spark

        # Listar archivos generados
        logger.info("═" * 60)
        logger.info("FASE 6 COMPLETADA")
        logger.info("Archivos en %s:", TABLEAU_DIR)
        for f in sorted(os.listdir(TABLEAU_DIR)):
            if f.endswith(".csv"):
                size = os.path.getsize(os.path.join(TABLEAU_DIR, f))
                logger.info("  %s (%.1f KB)", f, size / 1024)

        logger.info("")
        logger.info("Instrucciones para Tableau:")
        logger.info("  1. Abrir Tableau → Connect → Text file")
        logger.info("  2. Seleccionar el CSV deseado desde output/tableau/")
        logger.info("  3. Dashboard animado: usar window_start_str como eje temporal")
        logger.info("  4. Comparación de arquitecturas: usar spark_metrics_summary.csv")
        logger.info("═" * 60)

    finally:
        spark.stop()
        logger.info("SparkSession cerrada.")


if __name__ == "__main__":
    main()
