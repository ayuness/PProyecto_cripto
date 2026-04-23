"""
training/train_model.py — Fase 3: Entrenar Logistic Regression (supervisado).

Lee los datos etiquetados por K-Means (Fase 2) y entrena un modelo de
clasificación binaria para distinguir comportamiento "normal" vs "anómalo"
del mercado de criptomonedas.

Logistic Regression es un modelo lineal para clasificación binaria.
Modela la probabilidad de que un punto pertenezca a la clase "anómalo"
usando una función sigmoide: P(y=1|x) = 1 / (1 + exp(-w·x - b)).

Se eligió Logistic Regression porque:
  - Es el clasificador más simple de Spark ML, rápido de entrenar e inferir.
  - El foco del proyecto es la comparación de arquitecturas de ejecución,
    no la complejidad del modelo de ML.
  - Las etiquetas provienen de K-Means: esto conecta el aprendizaje no
    supervisado (Fase 2) con el supervisado (esta fase).

Pipeline: VectorAssembler → StandardScaler → LogisticRegression

Ejecución (requiere output de la Fase 2):
    python training/train_model.py

Output:
    - Modelo guardado en models/logistic_model/
    - Métricas impresas en consola (accuracy, precision, recall, F1, confusion matrix)
"""

import logging
import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import (
    get_spark_builder, LABELED_DATA_DIR, LOGISTIC_MODEL_DIR,
)

# ── Configuración ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("train_model")

# Features idénticos a los de K-Means (Fase 2) para consistencia.
# El modelo supervisado aprende a replicar la clasificación del no supervisado
# pero con una frontera de decisión explícita (hiperplano lineal).
FEATURE_COLS = ["pct_return", "volatility", "volume_intensity", "price_speed"]

# Columna objetivo: 0 = normal, 1 = anómalo (generada por K-Means en Fase 2)
LABEL_COL = "label_binary"

# Split de datos: 80% entrenamiento, 20% prueba.
# Seed fijo para reproducibilidad entre ejecuciones en ambas máquinas.
TRAIN_RATIO = 0.8
TEST_RATIO = 0.2
SPLIT_SEED = 42

# Hiperparámetros de Logistic Regression:
# - maxIter=20: suficiente para convergencia con datos de esta escala.
# - regParam=0.1: regularización L2 moderada para evitar sobreajuste
#   en un dataset pequeño (~400 filas).
# - elasticNetParam=0.0: L2 pura (Ridge). Se prefiere sobre L1 (Lasso)
#   porque no hay necesidad de selección de features (solo tenemos 4).
LR_MAX_ITER = 20
LR_REG_PARAM = 0.1
LR_ELASTIC_NET = 0.0


# ── Entrenamiento ────────────────────────────────────────────────────────────

def load_labeled_data(spark: SparkSession) -> DataFrame:
    """Carga los datos etiquetados por K-Means de la Fase 2."""
    labeled_path = os.path.join(LABELED_DATA_DIR, "labeled_trades")

    if not os.path.exists(labeled_path):
        logger.error(
            "No se encontraron datos etiquetados en: %s\n"
            "Ejecuta primero la Fase 2: python streaming/spark_streaming.py",
            labeled_path,
        )
        sys.exit(1)

    df = spark.read.parquet(labeled_path)
    logger.info("Datos cargados: %d filas desde %s", df.count(), labeled_path)
    return df


def build_pipeline() -> Pipeline:
    """
    Construye el pipeline de ML: VectorAssembler → StandardScaler → LogisticRegression.

    VectorAssembler: combina las 4 columnas de features en un solo vector
    denso, que es el formato que esperan los estimadores de Spark ML.

    StandardScaler: normaliza cada feature a media=0 y std=1. Necesario
    porque Logistic Regression es sensible a la escala de los features
    (el gradiente converge más rápido con features normalizados).

    LogisticRegression: clasificador binario lineal. La función de costo
    es cross-entropy con regularización L2.
    """
    assembler = VectorAssembler(
        inputCols=FEATURE_COLS,
        outputCol="raw_features",
        handleInvalid="skip",
    )

    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=True,
    )

    lr = LogisticRegression(
        featuresCol="features",
        labelCol=LABEL_COL,
        predictionCol="prediction",
        probabilityCol="probability",
        rawPredictionCol="rawPrediction",
        maxIter=LR_MAX_ITER,
        regParam=LR_REG_PARAM,
        elasticNetParam=LR_ELASTIC_NET,
    )

    return Pipeline(stages=[assembler, scaler, lr])


def evaluate_model(predictions: DataFrame) -> None:
    """
    Evalúa el modelo con múltiples métricas.

    - Accuracy: proporción de predicciones correctas (puede ser engañosa
      con clases desbalanceadas, pero es el baseline).
    - Precision: de todos los que el modelo dijo "anómalo", cuántos lo eran.
      Importante para no generar falsas alarmas.
    - Recall: de todos los anómalos reales, cuántos detectó el modelo.
      Importante para no dejar pasar anomalías.
    - F1-score: media armónica de precision y recall. Métrica balanceada
      para clases desiguales.
    - AUC-ROC: capacidad del modelo de distinguir entre clases, independiente
      del umbral de decisión.
    """
    # Métricas de clasificación multiclase (aplican a binario también)
    multi_eval = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
    )

    accuracy = multi_eval.evaluate(predictions, {multi_eval.metricName: "accuracy"})
    precision = multi_eval.evaluate(predictions, {multi_eval.metricName: "weightedPrecision"})
    recall = multi_eval.evaluate(predictions, {multi_eval.metricName: "weightedRecall"})
    f1 = multi_eval.evaluate(predictions, {multi_eval.metricName: "f1"})

    # AUC-ROC (evaluador binario)
    binary_eval = BinaryClassificationEvaluator(
        labelCol=LABEL_COL,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    auc_roc = binary_eval.evaluate(predictions)

    logger.info("── Métricas de evaluación ──")
    logger.info("  Accuracy:           %.4f", accuracy)
    logger.info("  Precision (weighted): %.4f", precision)
    logger.info("  Recall (weighted):    %.4f", recall)
    logger.info("  F1-score (weighted):  %.4f", f1)
    logger.info("  AUC-ROC:            %.4f", auc_roc)

    # Matriz de confusión
    logger.info("── Matriz de confusión ──")
    confusion = (
        predictions
        .groupBy(LABEL_COL, "prediction")
        .count()
        .orderBy(LABEL_COL, "prediction")
    )
    confusion.show()

    # Distribución de predicciones
    logger.info("── Distribución de predicciones ──")
    predictions.groupBy("prediction").count().orderBy("prediction").show()


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("═" * 60)
    logger.info("FASE 3 — Entrenamiento de Logistic Regression")
    logger.info("═" * 60)

    spark = get_spark_builder("CryptoTraining-Phase3").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession iniciada. Spark version: %s", spark.version)

    try:
        # 1. Cargar datos
        df = load_labeled_data(spark)

        # Renombrar columna 'prediction' de K-Means para evitar conflicto
        # con la columna que genera Logistic Regression
        if "prediction" in df.columns:
            df = df.withColumnRenamed("prediction", "kmeans_cluster")

        # Filtrar NaN en features
        clean_df = df.dropna(subset=FEATURE_COLS + [LABEL_COL])
        logger.info("Filas válidas después de limpiar NaN: %d", clean_df.count())

        # Distribución de clases
        logger.info("── Distribución de clases ──")
        clean_df.groupBy("label").count().show()

        # 2. Split train/test
        train_df, test_df = clean_df.randomSplit(
            [TRAIN_RATIO, TEST_RATIO], seed=SPLIT_SEED
        )
        train_count = train_df.count()
        test_count = test_df.count()
        logger.info("Train: %d filas | Test: %d filas", train_count, test_count)

        # Verificar que haya anomalías en ambos sets
        train_anomalies = train_df.filter(F.col(LABEL_COL) == 1).count()
        test_anomalies = test_df.filter(F.col(LABEL_COL) == 1).count()
        logger.info(
            "Anomalías — train: %d, test: %d", train_anomalies, test_anomalies
        )

        if train_anomalies == 0:
            logger.warning(
                "No hay anomalías en el set de entrenamiento. "
                "El modelo no podrá aprender la clase positiva. "
                "Considera acumular más datos en la Fase 2."
            )

        # 3. Entrenar pipeline
        logger.info("Entrenando pipeline: VectorAssembler → StandardScaler → LogisticRegression")
        pipeline = build_pipeline()
        pipeline_model = pipeline.fit(train_df)

        # Extraer coeficientes del modelo para interpretabilidad
        lr_model = pipeline_model.stages[-1]
        logger.info("── Coeficientes del modelo ──")
        coefficients = lr_model.coefficients.toArray()
        for feat, coef in zip(FEATURE_COLS, coefficients):
            logger.info("  %s: %.6f", feat, coef)
        logger.info("  intercepto: %.6f", lr_model.intercept)

        # 4. Predecir en test
        predictions = pipeline_model.transform(test_df)

        # 5. Evaluar
        evaluate_model(predictions)

        # 6. Guardar modelo
        pipeline_model.write().overwrite().save(LOGISTIC_MODEL_DIR)
        logger.info("Modelo guardado en: %s", LOGISTIC_MODEL_DIR)

        logger.info("═" * 60)
        logger.info("FASE 3 COMPLETADA")
        logger.info("  → Siguiente paso: python streaming/spark_inference.py (Fase 4)")
        logger.info("═" * 60)

    finally:
        spark.stop()
        logger.info("SparkSession cerrada.")


if __name__ == "__main__":
    main()
