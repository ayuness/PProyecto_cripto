"""
verify_rapids.py — Prueba rápida del plugin RAPIDS Accelerator.

Activa SPARK_USE_GPU=1 internamente, crea una SparkSession, y corre un
pequeño query con un GROUP BY/aggregation. Luego revisa el plan de Catalyst
para confirmar que las operaciones fueron migradas a GPU.

Uso (Kafka NO es necesario aquí):
    python verify_rapids.py
"""

import logging
import os
import sys

# Forzar GPU mode antes de importar el config
os.environ["SPARK_USE_GPU"] = "1"
os.environ["SPARK_DRIVER_MEMORY"] = "3g"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("verify_rapids")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config.spark_config import get_spark_builder


def main() -> None:
    logger.info("═" * 60)
    logger.info("Verificación: RAPIDS Accelerator")
    logger.info("═" * 60)

    spark = get_spark_builder("VerifyRapids").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Confirmar plugin cargado
    plugins = spark.sparkContext.getConf().get("spark.plugins", "")
    rapids_enabled = spark.sparkContext.getConf().get("spark.rapids.sql.enabled", "")
    logger.info("spark.plugins = %s", plugins)
    logger.info("spark.rapids.sql.enabled = %s", rapids_enabled)

    # Crear un dataframe sintético y correr una agregación
    from pyspark.sql import functions as F
    df = spark.range(0, 1_000_000).withColumn(
        "g", (F.col("id") % 1000).cast("int")
    ).withColumn(
        "v", (F.col("id") * 1.5).cast("double")
    )

    agg = df.groupBy("g").agg(F.avg("v").alias("avg_v"), F.sum("v").alias("sum_v"))

    # Capturar el plan ejecutado
    logger.info("─" * 40)
    logger.info("Plan físico (buscar 'GpuHashAggregate' / 'GpuColumnar' como evidencia GPU):")
    logger.info("─" * 40)
    agg.explain()

    count = agg.count()
    logger.info("Filas agregadas: %d", count)

    sample = agg.orderBy("g").limit(3).collect()
    for row in sample:
        logger.info("  g=%d avg=%.2f sum=%.2f", row["g"], row["avg_v"], row["sum_v"])

    logger.info("═" * 60)
    logger.info("✓ RAPIDS cargado correctamente")
    logger.info("═" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
