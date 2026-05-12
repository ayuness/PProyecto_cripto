"""
extract_model_params.py — Extrae los coeficientes de LR y los parámetros del
StandardScaler del PipelineModel guardado en disco. Los serializa a JSON para
que el dashboard pueda aplicar el modelo manualmente sin necesidad de Spark.

Uso:
    python dashboard/extract_model_params.py

Salida: dashboard/model_params.json
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import get_spark_builder, LOGISTIC_MODEL_DIR

FEATURE_COLS = ["pct_return", "volatility", "volume_intensity", "price_speed"]
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "model_params.json")


def main() -> None:
    from pyspark.ml import PipelineModel

    spark = get_spark_builder("ExtractParams").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    model = PipelineModel.load(LOGISTIC_MODEL_DIR)
    scaler_model = model.stages[1]  # StandardScaler
    lr_model = model.stages[2]      # LogisticRegression

    params = {
        "feature_cols": FEATURE_COLS,
        "scaler": {
            "mean": list(scaler_model.mean),
            "std": list(scaler_model.std),
        },
        "lr": {
            "coefficients": list(lr_model.coefficients),
            "intercept": float(lr_model.intercept),
        },
    }

    with open(OUT, "w") as f:
        json.dump(params, f, indent=2)

    print(f"Parámetros guardados en {OUT}")
    print(json.dumps(params, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
