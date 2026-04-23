# Proyecto Final — Comparación de Arquitecturas de Ejecución en Tiempo Real

**Arquitectura de Grandes Volúmenes de Datos · ITAM · Prof. Wilmer Pereira · Primavera 2026**

---

## Descripción

Aplicación de procesamiento de datos en tiempo real que captura trades de criptomonedas desde Binance, calcula estadísticos y detecta anomalías usando modelos de machine learning, y compara el desempeño de la misma aplicación ejecutada en dos arquitecturas de hardware distintas.

El foco del proyecto es la **comparación de arquitecturas de ejecución**, no la complejidad de los modelos de ML. Los modelos se mantienen simples para que las diferencias de rendimiento sean atribuibles al hardware y la configuración de Spark.

---

## Stack Tecnológico

| Componente | Tecnología |
|---|---|
| Lenguaje | Python 3.11+ |
| Procesamiento distribuido | Apache Spark 3.5.4 (PySpark, Structured Streaming, MLlib) |
| Broker de mensajes | Apache Kafka 4.2.0 (modo KRaft, sin Zookeeper) |
| Fuente de datos | Binance WebSocket (trades en tiempo real, público, sin autenticación) |
| Visualización | Tableau |
| Monitoreo | Spark Application UI (API REST) |

### Arquitecturas comparadas

| | Máquina A | Máquina B |
|---|---|---|
| **OS** | macOS (Apple M1) | Linux |
| **CPU** | Apple M1, 8 cores | — |
| **RAM** | 8 GB | 16 GB |
| **GPU** | Sin GPU dedicada | NVIDIA (CUDA) |
| **Spark** | local[*] | local[*] + RAPIDS Accelerator (si compatible) |

---

## Fuente de Datos

**Binance WebSocket** — endpoint público que transmite trades en tiempo real:

- URL: `wss://stream.binance.com:9443/stream?streams=<symbol>@trade`
- Pares suscritos: BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT, XRPUSDT, DOGEUSDT, ADAUSDT, AVAXUSDT
- Cada mensaje contiene: símbolo, precio, cantidad, timestamp, dirección del trade
- Sin autenticación requerida
- Reconexión automática (Binance desconecta cada 24h)

---

## Arquitectura del Pipeline

El proyecto tiene **tres momentos secuenciales**. Cada uno depende de los resultados del anterior.

### Momento 1 — Primera tanda de streaming (captura + estadísticos + K-Means)

```
Binance WebSocket ──→ Kafka Producer ──→ Topic 'crypto-trades'
                                               │
                                    Spark Structured Streaming
                                               │
                              Estadísticos por ventana de tiempo
                              (min, max, promedio, varianza, volumen)
                                               │
                              Feature engineering por ventana:
                              · Retorno porcentual del precio
                              · Volatilidad (stddev normalizada)
                              · Intensidad de volumen
                              · Velocidad de cambio del precio
                                               │
                              Acumular datos en Parquet hasta
                              tener suficiente volumen
                                               │
                              Detener streaming ──→ K-Means (batch)
                                               │
                              Etiquetar cada ventana:
                              "normal" o "anómalo" (percentil 95)
                                               │
                              Guardar datos etiquetados en Parquet
```

**K-Means** agrupa las ventanas de tiempo en k clusters minimizando la distancia al centroide más cercano. Cada cluster representa un estado del mercado (ej: calma, volatilidad moderada, movimiento extremo). Los puntos con distancia al centroide mayor al percentil 95 se etiquetan como anómalos. Se prueban k=3, 4, 5 y se elige el mejor por Silhouette Score.

**Ventanas de tiempo:** 1 minuto de duración con 30 segundos de slide (solapadas). Watermark de 30 segundos para datos tardíos. Los estadísticos se emiten con alta frecuencia para alimentar la visualización animada.

### Momento 2 — Entrenamiento supervisado (batch, sin streaming)

```
Datos etiquetados (Parquet) ──→ VectorAssembler ──→ StandardScaler ──→ Logistic Regression
                                                                              │
                                                              Evaluar: accuracy, precision,
                                                              recall, F1, AUC-ROC
                                                                              │
                                                              Guardar modelo (PipelineModel)
```

**Logistic Regression** es un clasificador binario lineal que modela la probabilidad de anomalía. Se eligió por ser el modelo más simple de Spark ML — el foco es la arquitectura, no el ML. Las etiquetas provienen de K-Means, conectando el aprendizaje no supervisado con el supervisado. Pipeline completo: VectorAssembler → StandardScaler → LogisticRegression.

### Momento 3 — Segunda tanda de streaming (inferencia en tiempo real)

```
Binance WebSocket ──→ Kafka (sigue activo) ──→ Spark Structured Streaming
                                                         │
                                          Mismos features que Momento 1
                                                         │
                                          Cargar modelo entrenado
                                                         │
                                          Clasificar cada ventana:
                                          "normal" o "anómalo"
                                                         │
                                          Guardar predicciones en Parquet
```

No se re-entrena nada. El modelo del Momento 2 se aplica tal cual a los datos nuevos. Las predicciones incluyen la probabilidad de cada clase.

---

## Métricas de Comparación de Arquitecturas

Se capturan mediante la API REST de Spark UI (`http://localhost:4040/api/v1/`) mientras las fases de streaming están activas. Se comparan al menos dos de las siguientes:

| Métrica | Qué mide |
|---|---|
| Tiempo de ejecución de jobs | Duración total de procesamiento |
| Shuffle time | Tiempo en operaciones de redistribución de datos entre stages |
| I/O por stage | Volumen de lectura/escritura en cada etapa |
| Scheduler Delay | Tiempo esperando recursos disponibles |
| Executor Run Time | Tiempo real de cómputo |
| GC Time | Tiempo en recolección de basura de la JVM |
| Spill (Memory/Disk) | Datos derramados a disco por falta de memoria |
| Tasa de streaming | Input rate, processing rate, batch duration |
| Environment | Configuración de Spark en cada máquina |

---

## Estructura del Proyecto

```
proyecto_final/
├── README.md                          # Este archivo
├── requirements.txt                   # Dependencias Python con versiones
├── config/
│   └── spark_config.py                # Configuración centralizada de Spark
├── hardware_info.py                   # Captura specs de hardware → JSON
├── verify_environment.py              # Verifica conectividad Spark ↔ Kafka
├── producer/
│   └── binance_producer.py            # Binance WebSocket → Kafka
├── streaming/
│   ├── spark_streaming.py             # Momento 1: estadísticos + K-Means
│   └── spark_inference.py             # Momento 3: inferencia con modelo supervisado
├── training/
│   └── train_model.py                 # Momento 2: entrenamiento Logistic Regression
├── metrics/
│   └── capture_metrics.py             # Captura métricas de Spark UI (API REST)
├── visualization/
│   └── prepare_tableau_data.py        # Exporta datos a CSV para Tableau
├── models/                            # Modelos guardados (K-Means, LogReg)
├── output/                            # Outputs del pipeline
│   ├── statistics/                    #   Estadísticos por ventana
│   ├── labeled_data/                  #   Datos etiquetados por K-Means
│   ├── predictions/                   #   Predicciones en tiempo real
│   ├── metrics/                       #   Hardware info + métricas Spark UI
│   └── tableau/                       #   CSVs para Tableau
├── checkpoints/                       # Checkpoints de Structured Streaming
└── prueba/                            # Resultados de la corrida de prueba
    └── conclusion.ipynb               # Análisis y gráficas de la prueba
```

---

## Requisitos

```bash
pip install -r requirements.txt
```

Además se requiere:
- Apache Kafka 4.2.0 instalado y accesible en PATH
- Apache Spark 3.5.4 (binario o pip)
- Java 17+

---

## Ejecución

### Preparación (una sola vez)

```bash
# Terminal 1 — Levantar Kafka
kafka-server-start /opt/homebrew/etc/kafka/server.properties

# En otra terminal — Crear topic
kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic crypto-trades \
    --partitions 1 --replication-factor 1

# Verificar entorno
python hardware_info.py
python verify_environment.py
```

### Pipeline completo

```bash
# Terminal 2 — Momento 1: Producer (dejar corriendo)
python producer/binance_producer.py

# Terminal 3 — Momento 1: Streaming + K-Means
python streaming/spark_streaming.py

# Terminal 3 — Momento 2: Entrenamiento (batch)
python training/train_model.py

# Terminal 3 — Momento 3: Inferencia en streaming
python streaming/spark_inference.py

# Terminal 4 — Capturar métricas (mientras Momento 1 o 3 están activos)
python metrics/capture_metrics.py --arch mac-m1

# Al final — Exportar datos para Tableau
python visualization/prepare_tableau_data.py
```

### Repetir en la segunda máquina

Ejecutar exactamente el mismo pipeline en la máquina Linux con:
```bash
python metrics/capture_metrics.py --arch linux-gpu
```

---

## Visualización en Tableau

El script `prepare_tableau_data.py` genera CSVs en `output/tableau/`:

| Archivo | Contenido | Uso en Tableau |
|---|---|---|
| `window_statistics.csv` | Estadísticos por ventana y símbolo | Serie temporal animada de precios |
| `labeled_data.csv` | Datos con etiqueta K-Means | Scatter plot de features por cluster |
| `predictions.csv` | Predicciones del modelo supervisado | Serie temporal con detección de anomalías |
| `spark_metrics_summary.csv` | Métricas de Spark UI por arquitectura | Comparación de rendimiento |

Para conectar: Tableau → Connect → Text file → seleccionar el CSV.

---

## Resultados Esperados

### Datos y modelos

Con una acumulación de 20 minutos (8 pares de criptomonedas, ~50-200 trades/segundo):

- **~1,400+ ventanas** de estadísticos (8 símbolos × ~175 ventanas por símbolo)
- **K-Means:** Silhouette Score > 0.7 con k=3 clusters representando estados de mercado
- **~70 ventanas anómalas** (~5% del total, determinado por el umbral del percentil 95)
- **Logistic Regression:** accuracy y AUC-ROC altos. Es importante notar que el modelo supervisado aprende a **replicar las etiquetas de K-Means**, no a detectar anomalías reales del mercado. Si K-Means separó bien los clusters, el clasificador tendrá métricas altas casi por definición — está aprendiendo un patrón que ya fue definido de forma limpia por el modelo no supervisado. Esto es una limitación del diseño, no una validación de que el modelo detecte anomalías financieras reales.

### Hipótesis de comparación de arquitecturas

La comparación entre las dos máquinas es el objetivo principal del proyecto. Hipótesis antes de la ejecución final:

1. **CPU y RAM:** La máquina Linux tiene el doble de RAM (16 GB vs 8 GB), lo que significa menor presión de memoria para la JVM. Se esperaría menor GC time y menor probabilidad de spill a disco. La Mac M1 con solo 8 GB es limitada — con `spark.driver.memory=2g` queda poco margen, y Spark puede derramar datos a disco con datasets grandes. La máquina Linux puede usar `spark.driver.memory=4g` o más sin problemas.

2. **RAPIDS Accelerator (GPU):** Si es compatible con Spark 3.5.4, RAPIDS puede acelerar operaciones como joins, aggregations y shuffles ejecutándolas en la GPU. El impacto esperado es mayor en las fases de streaming (muchas aggregations por ventana) y en K-Means (operaciones vectoriales). Sin embargo, para datasets pequeños el overhead de transferir datos CPU→GPU puede anular la ganancia. Se espera beneficio solo si el volumen de datos es suficiente.

3. **Shuffle time:** En modo `local[*]` los shuffles son en memoria del mismo proceso, así que las diferencias deberían ser menores que en un cluster real. Aun así, más RAM permite buffers de shuffle más grandes y menos spill.

4. **Si RAPIDS no es compatible:** La comparación se hace solo con CPU, pero las diferencias de hardware (CPU, RAM, disco) siguen siendo medibles. En ese caso el foco sería tiempo de ejecución, GC time y spill.

---

## Notas Técnicas

- **Hadoop override:** `spark.hadoop.fs.defaultFS=file:///` es obligatorio si hay una instalación local de Hadoop con `core-site.xml` apuntando a HDFS sin daemon activo.
- **Checkpoints:** Las rutas de checkpoint de Structured Streaming deben llevar prefijo `file://` para forzar filesystem local.
- **Kafka conector:** `spark-sql-kafka-0-10_2.12:3.5.4` se descarga automáticamente de Maven la primera vez via `spark.jars.packages`.
