# Prompt para Claude Code — Proyecto de Spark Structured Streaming

## Contexto del Proyecto

Soy estudiante de Ingeniería en Ciencia de Datos en el ITAM. Este es el proyecto final de la materia **Arquitectura de Grandes Volúmenes de Datos** (Prof. Wilmer Pereira). El proyecto consiste en construir una aplicación de procesamiento de datos en tiempo real usando PySpark Structured Streaming + Kafka, y **comparar la ejecución de la misma aplicación en dos arquitecturas de hardware distintas**.

### Las dos máquinas (arquitecturas a comparar)

- **Máquina A (Mac, sin GPU):** Mi computadora personal. macOS, sin GPU dedicada.
- **Máquina B (Linux, con GPU NVIDIA):** La computadora de mi compañero. Linux, con GPU NVIDIA (CUDA disponible). Configurar RAPIDS Accelerator para Spark si es compatible, para aprovechar la GPU en el procesamiento.

### Stack técnico ya instalado en ambas máquinas

- Apache Kafka: instalado localmente, funcional
- Spark/PySpark: instalado (puede ser binario en PATH o pip, hay que verificar)
- Java: instalado en ambas
- IDE: VSCode
- Visualización: Tableau (licencia estudiantil)

---

## Objetivo principal

El núcleo del proyecto es la **comparación de arquitecturas de ejecución**, no la complejidad de los modelos de ML. Los modelos deben ser lo más simples posible. Lo importante es medir y comparar cómo se comporta la misma aplicación en hardware diferente.

---

## Fuente de datos

**Binance WebSocket** — datos reales de trades de criptomonedas en tiempo real.

- Endpoint público, gratuito, sin autenticación
- URL base: `wss://stream.binance.com:9443/ws/<stream>`
- Streams a suscribir: trades de múltiples pares (BTC/USDT, ETH/USDT, SOL/USDT, y más si es necesario para volumen)
- Cada mensaje es un trade real con: símbolo, precio, cantidad, timestamp, si fue compra o venta
- Manejar reconexión automática (Binance desconecta cada 24h)

---

## Arquitectura del pipeline

```
Binance WebSocket → Kafka Producer → Kafka Topic → Spark Structured Streaming
                                                        ↓
                                           Fase 1: Estadísticos + K-Means (no supervisado)
                                                        ↓
                                           Guardar datos etiquetados en Parquet
                                                        ↓
                                           Fase 2: Entrenar Logistic Regression (supervisado)
                                                        ↓
                                           Fase 3: Reactivar streaming + clasificar con modelo supervisado
```

---

## Fases de desarrollo (ejecutar en orden)

### Fase 0 — Diagnóstico del entorno

Antes de escribir cualquier código del proyecto:

1. Detectar cómo está instalado Spark/PySpark (binario en PATH, pip, o ambos). Si hay conflicto, resolverlo.
2. Verificar la versión de Spark (debe ser 3.x). Imprimir: `spark.version`
3. Verificar que Kafka está corriendo y funcional: crear un topic de prueba, producir y consumir un mensaje
4. Verificar que PySpark puede conectarse a Kafka: probar que el paquete `spark-sql-kafka-0-10` se descarga correctamente al configurar `spark.jars.packages`
5. **Generar un script `hardware_info.py`** que capture las specs de la máquina: OS, CPU (modelo, cores), RAM total, GPU (detectar con `nvidia-smi` si existe — capturar modelo, VRAM, versión de CUDA y driver), disco disponible. Exportar a JSON. Esto se ejecutará en ambas máquinas para la tabla comparativa del informe.

### Fase 1 — Productor de datos (Binance → Kafka)

Crear `producer/binance_producer.py`:

- Conectar al WebSocket público de Binance para recibir trades en tiempo real
- Suscribirse a múltiples pares simultáneamente (al menos BTC/USDT, ETH/USDT, SOL/USDT)
- Cada trade recibido se publica como mensaje JSON en un topic de Kafka llamado `crypto-trades`
- Esquema del mensaje JSON:
  ```json
  {
    "symbol": "BTCUSDT",
    "price": 67543.21,
    "quantity": 0.0023,
    "timestamp": 1714000000000,
    "is_buyer_maker": true
  }
  ```
- Implementar reconexión automática si el WebSocket se desconecta
- Logging con el módulo `logging` (no prints)
- Manejo de errores robusto

### Fase 2 — Streaming: Estadísticos + Modelo no supervisado (K-Means)

Crear `streaming/spark_streaming.py`:

**Parte A — Estadísticos en tiempo real:**
- Leer del topic `crypto-trades` con Spark Structured Streaming
- Parsear JSON con schema explícito
- Calcular en ventanas de tiempo (ej. 1 minuto):
  - Mínimo y máximo del precio por símbolo
  - Promedio y varianza del precio por símbolo
  - Conteo de trades por símbolo
  - Volumen total (suma de quantity) por símbolo
- Definir rangos de precio y reportar estadísticos por rango
- Usar watermark para manejar eventos tardíos
- Emitir estos reportes con alta frecuencia (servirán para la visualización en Tableau)
- Escribir los estadísticos a Parquet o CSV para que Tableau los consuma

**Parte B — Feature engineering para K-Means:**
- Sobre ventanas de tiempo, calcular features:
  - Retorno porcentual del precio en la ventana
  - Volatilidad (desviación estándar) en la ventana
  - Ratio de volumen actual vs promedio móvil
  - Velocidad de cambio de precio
- Estas features alimentan al modelo K-Means

**Parte C — K-Means (no supervisado):**
- Utilizar `pyspark.ml.clustering.KMeans`
- Entrenar con los datos acumulados en micro-batches
- Probar k=3, k=4, k=5 y evaluar con Silhouette Score para elegir el mejor
- Clasificar cada punto: calcular distancia al centroide más cercano
- Etiquetar como "anómalo" si la distancia supera el percentil 95 del entrenamiento, "normal" si no
- **Guardar todos los datos con sus etiquetas ("normal"/"anómalo") en Parquet** — esto es el dataset para el modelo supervisado

**Explicación detallada del modelo K-Means para incluir en el código como comentarios:**
- K-Means agrupa datos en k clusters minimizando la suma de distancias al cuadrado de cada punto a su centroide más cercano
- En este contexto: cada punto es un vector de features de una ventana de tiempo (retorno, volatilidad, volumen, velocidad)
- Los clusters representan estados del mercado (ej: calma, volatilidad moderada, movimiento extremo)
- La detección de anomalías se basa en: si un punto está muy lejos de todos los centroides, no encaja en ningún patrón normal → es anómalo
- El umbral (percentil 95) significa que el 5% de los datos más alejados se consideran anómalos
- Silhouette Score mide qué tan bien separados están los clusters (rango -1 a 1, mayor es mejor)

### Fase 3 — Modelo supervisado (Logistic Regression, batch)

Crear `training/train_model.py`:

- Leer los datos etiquetados del Parquet generado en la Fase 2
- Features: las mismas que K-Means (retorno, volatilidad, ratio de volumen, velocidad de cambio)
- Label: columna "normal" (0) / "anómalo" (1) generada por K-Means
- Utilizar `pyspark.ml.classification.LogisticRegression` — el modelo más simple de Spark ML
- Pipeline de Spark ML con: VectorAssembler → StandardScaler → LogisticRegression
- Split train/test (80/20)
- Evaluar con: accuracy, precision, recall, F1-score, matriz de confusión
- Guardar el modelo entrenado en `models/logistic_model/`

**Explicación detallada de Logistic Regression para incluir en el código como comentarios:**
- Logistic Regression es un modelo lineal para clasificación binaria
- Modela la probabilidad de que un punto pertenezca a la clase "anómalo" usando una función sigmoide
- En este contexto: recibe las mismas features de mercado y predice si el comportamiento es normal o anómalo
- Ventaja: es el clasificador más simple, rápido de entrenar y de inferir
- Se eligió Logistic Regression porque el foco del proyecto es la arquitectura de ejecución, no la complejidad del modelo
- Las etiquetas vienen del modelo K-Means: esto conecta el aprendizaje no supervisado con el supervisado

### Fase 4 — Inferencia en streaming con modelo supervisado

Crear `streaming/spark_inference.py`:

- Reactivar la lectura del topic `crypto-trades` con Spark Structured Streaming
- Calcular las mismas features en ventanas de tiempo (idéntico a Fase 2)
- Cargar el modelo de Logistic Regression guardado
- Clasificar cada nueva ventana como "normal" o "anómalo"
- Escribir predicciones a Parquet (`output/predictions.parquet`)
- Habilitar checkpointing para fault-tolerance

### Fase 5 — Captura de métricas de Spark UI para comparación de arquitecturas

Crear `metrics/capture_metrics.py`:

El profesor pide comparar la ejecución usando métricas de Spark UI (`http://localhost:4040`). Se deben capturar al menos **dos** de estas métricas en ambas máquinas:

1. **Tiempo de ejecución de los jobs** en ambas arquitecturas
2. **Tiempos de shuffle** entre cada stage (operaciones como join, reduceByKey)
3. **Cantidad de operaciones de I/O** en cada stage
4. **Scheduler Delay** (retraso esperando recursos), **Executor Run Time** (tiempo real de computación), **GC Time** (recolección de basura)
5. **Spill (Memory/Disk):** si los datos exceden la memoria del executor y Spark "derrama" a disco
6. **Tasa de streaming:** input rate, processing rate, batch duration, cola de batches pendientes
7. **Environment:** configuración de Spark en cada máquina

El script debe:
- Conectarse a la API REST de Spark UI (`http://localhost:4040/api/v1/applications/`)
- Descargar métricas de jobs, stages, streaming queries
- Exportar todo a JSON y/o CSV para análisis posterior
- Incluir instrucciones claras de cómo ejecutar en ambas máquinas

### Fase 6 — Preparación de datos para Tableau

Crear `visualization/prepare_tableau_data.py`:

- Consolidar los outputs de estadísticos, predicciones y métricas en archivos que Tableau pueda consumir (CSV o Parquet → CSV)
- Datos para visualización animada:
  - Serie temporal de precios por símbolo con marca de "normal"/"anómalo"
  - Estadísticos por ventana de tiempo (min, max, promedio, varianza)
  - Métricas de rendimiento de Spark de ambas máquinas
- Incluir instrucciones de cómo conectar Tableau a estos archivos y crear el dashboard

---

## Estructura de archivos del proyecto

```
proyecto-streaming-crypto/
├── README.md
├── requirements.txt
├── hardware_info.py                  # Fase 0: captura specs de hardware
├── producer/
│   └── binance_producer.py           # Fase 1: WebSocket → Kafka
├── streaming/
│   ├── spark_streaming.py            # Fase 2: estadísticos + K-Means
│   └── spark_inference.py            # Fase 4: inferencia con LogReg
├── training/
│   └── train_model.py                # Fase 3: entrenar Logistic Regression
├── metrics/
│   └── capture_metrics.py            # Fase 5: métricas de Spark UI
├── visualization/
│   └── prepare_tableau_data.py       # Fase 6: datos para Tableau
├── models/
│   ├── kmeans_model/                 # Modelo K-Means guardado
│   └── logistic_model/               # Modelo LogReg guardado
├── output/
│   ├── statistics/                   # Estadísticos del streaming
│   ├── labeled_data/                 # Datos etiquetados por K-Means
│   ├── predictions/                  # Predicciones del modelo supervisado
│   └── metrics/                      # Métricas de Spark UI
└── config/
    └── spark_config.py               # Configuración de Spark centralizada
```

---

## Reglas de código

- **Todo el código debe ser ejecutable tal cual** — nada de `# TODO` o `# implementar aquí`
- Usar **type hints** en funciones Python
- Usar módulo `logging` (no prints sueltos)
- **Comentarios detallados** explicando el porqué, no el qué. Especialmente en:
  - Configuración de Spark (por qué cada parámetro)
  - Features del modelo (por qué se eligió cada feature)
  - Modelo K-Means y Logistic Regression (explicación conceptual como comentarios)
  - Ventanas de tiempo y watermarks (por qué esos valores)
- Manejo de errores explícito (timeouts, conexiones perdidas, JSON malformado)
- `requirements.txt` con versiones pinneadas
- Compatible con **Spark 3.x** y **Python 3.10+**
- El paquete `spark-sql-kafka-0-10` se configura en código con `spark.jars.packages`, no requiere descarga manual
- **No asumir que ambas máquinas tienen GPU** — el código base debe funcionar sin GPU. Para la máquina con GPU NVIDIA, incluir un paso opcional claro para:
  - Verificar CUDA instalado y versión (`nvidia-smi`, `nvcc --version`)
  - Instalar y configurar RAPIDS Accelerator for Apache Spark (plugin que permite a Spark usar la GPU para operaciones como joins, aggregations, shuffles, etc.)
  - Configurar los parámetros de Spark para RAPIDS (`spark.plugins`, `spark.rapids.sql.enabled`, etc.)
  - Si RAPIDS no es compatible con la versión de Spark instalada, documentarlo y hacer la comparación solo con CPU (pero con las diferencias de hardware: CPU, RAM, disco)

---

## Instrucciones de ejecución

Cada fase debe incluir al final un bloque claro con:
1. El comando exacto para ejecutar (spark-submit o python)
2. Cómo verificar que funcionó antes de pasar a la siguiente fase
3. Qué output esperar (archivos generados, logs esperados)

---

## Ejecución paso a paso

Avanza **fase por fase**. En cada fase:
1. Explica brevemente qué vas a hacer y por qué
2. Entrega el código completo y funcional
3. Explica cómo probar que funciona
4. Espera mi confirmación antes de avanzar a la siguiente fase

**Empieza con la Fase 0 (diagnóstico del entorno).**
