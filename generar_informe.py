"""
Script para generar el informe preliminar del proyecto en PDF.
Ejecutar: python generar_informe.py
"""

from fpdf import FPDF


class InformePDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, "Proyecto Final - Arquitectura de Grandes Volumenes de Datos", align="C")
        self.ln(4)
        self.set_draw_color(0, 102, 204)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(6)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Página {self.page_no()}/{{nb}}", align="C")

    def titulo_seccion(self, numero, titulo):
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(0, 70, 140)
        self.cell(0, 10, f"{numero}. {titulo}", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(0, 102, 204)
        self.set_line_width(0.3)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def subtitulo(self, titulo):
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(50, 50, 50)
        self.cell(0, 8, titulo, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def parrafo(self, texto):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.5, texto)
        self.ln(3)

    def bullet(self, texto):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        x = self.get_x()
        self.cell(8, 5.5, "-")
        self.multi_cell(0, 5.5, texto)
        self.ln(1)

    def tabla(self, encabezados, filas, anchos=None):
        if anchos is None:
            anchos = [190 // len(encabezados)] * len(encabezados)
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(0, 70, 140)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(encabezados):
            self.cell(anchos[i], 7, h, border=1, fill=True, align="C")
        self.ln()
        self.set_font("Helvetica", "", 9)
        self.set_text_color(30, 30, 30)
        fill = False
        for fila in filas:
            if fill:
                self.set_fill_color(235, 241, 250)
            else:
                self.set_fill_color(255, 255, 255)
            for i, celda in enumerate(fila):
                align = "L" if i == 0 else "C"
                self.cell(anchos[i], 6.5, str(celda), border=1, fill=True, align=align)
            self.ln()
            fill = not fill
        self.ln(4)

    def codigo(self, texto):
        self.set_font("Courier", "", 8.5)
        self.set_fill_color(240, 240, 240)
        self.set_text_color(50, 50, 50)
        self.multi_cell(0, 5, texto, fill=True)
        self.ln(3)


def generar_informe():
    pdf = InformePDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # ── Portada ──
    pdf.ln(20)
    pdf.set_font("Helvetica", "B", 22)
    pdf.set_text_color(0, 70, 140)
    pdf.cell(0, 12, "Informe Preliminar", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)
    pdf.set_font("Helvetica", "", 14)
    pdf.set_text_color(60, 60, 60)
    pdf.cell(0, 10, "Pipeline de Streaming en Tiempo Real", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, "para Detección de Anomalías en Criptomonedas", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)
    pdf.set_draw_color(0, 102, 204)
    pdf.set_line_width(0.8)
    pdf.line(60, pdf.get_y(), 150, pdf.get_y())
    pdf.ln(10)
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 7, "Arquitectura de Grandes Volúmenes de Datos", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, "ITAM - Primavera 2026", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, "Prof. Wilmer Pereira", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)
    pdf.set_font("Helvetica", "B", 12)
    pdf.set_text_color(50, 50, 50)
    pdf.cell(0, 7, "Adolfo Yunes", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 7, "adolfoyunes1@gmail.com", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)
    pdf.set_font("Helvetica", "I", 10)
    pdf.set_text_color(120, 120, 120)
    pdf.cell(0, 7, "Fecha: 22 de abril de 2026", align="C", new_x="LMARGIN", new_y="NEXT")

    # ── 1. Introducción ──
    pdf.add_page()
    pdf.titulo_seccion("1", "Introducción")
    pdf.parrafo(
        "El presente informe describe el diseño e implementación de un pipeline de procesamiento "
        "de datos en tiempo real para la detección de anomalías en mercados de criptomonedas. "
        "El proyecto se desarrolla como trabajo final de la materia Arquitectura de Grandes "
        "Volúmenes de Datos en el ITAM."
    )
    pdf.parrafo(
        "El objetivo central del proyecto es la comparación de la ejecución de una misma aplicación "
        "de streaming en dos arquitecturas de hardware distintas: una máquina macOS con Apple M1 "
        "(sin GPU) y una máquina Linux con GPU NVIDIA. El foco está en medir y comparar métricas "
        "de rendimiento (tiempo de ejecución, shuffle, GC, spill, etc.), no en la complejidad "
        "de los modelos de machine learning."
    )
    pdf.parrafo(
        "La fuente de datos es el WebSocket público de Binance, que provee trades reales de "
        "criptomonedas en tiempo real sin requerir autenticación. Se monitorean 8 pares contra "
        "USDT: BTC, ETH, SOL, BNB, XRP, DOGE, ADA y AVAX."
    )

    # ── 2. Stack Tecnológico ──
    pdf.titulo_seccion("2", "Stack Tecnológico")
    pdf.tabla(
        ["Componente", "Versión", "Notas"],
        [
            ["Python", "3.11.7", "Anaconda"],
            ["PySpark", "3.5.4", "Binario local + pip"],
            ["Scala", "2.12", "Conector Kafka"],
            ["Java", "17", "OpenJDK (Homebrew)"],
            ["Apache Kafka", "4.2.0", "KRaft mode (sin Zookeeper)"],
            ["Hadoop", "3.3.6", "Solo libs, sin HDFS activo"],
            ["Tableau", "Estudiantil", "Visualización (Fase 6)"],
        ],
        [55, 35, 100],
    )

    pdf.subtitulo("Configuración clave de Spark")
    pdf.bullet("spark.hadoop.fs.defaultFS = file:/// - se fuerza filesystem local para evitar "
               "errores de conexión a HDFS (que no está activo).")
    pdf.bullet("spark.sql.shuffle.partitions = 4 - reducido del default 200 para modo local "
               "con un solo nodo.")
    pdf.bullet("spark.driver.memory = 2g - ajustado a los 8 GB de RAM total de la Máquina A.")
    pdf.bullet("spark.jars.packages - el conector Kafka se descarga automáticamente de Maven.")

    # ── 3. Arquitectura del Pipeline ──
    pdf.add_page()
    pdf.titulo_seccion("3", "Arquitectura General del Pipeline")
    pdf.parrafo(
        "El pipeline se ejecuta en tres momentos secuenciales (ninguna fase comienza hasta que "
        "la anterior ha terminado completamente):"
    )
    pdf.parrafo(
        "Momento 1 - Primer pase de streaming: ingesta de datos desde Binance vía Kafka, cálculo "
        "de estadísticos y features por ventana de tiempo, acumulación de datos, y entrenamiento "
        "de K-Means en batch para detección de anomalías no supervisada."
    )
    pdf.parrafo(
        "Momento 2 - Entrenamiento supervisado (batch): se entrena un modelo de Logistic Regression "
        "usando los datos etiquetados por K-Means como ground truth."
    )
    pdf.parrafo(
        "Momento 3 - Segundo pase de streaming: inferencia en tiempo real con el modelo supervisado "
        "entrenado, clasificando nuevas ventanas de datos como normales o anómalas."
    )
    pdf.ln(3)
    pdf.subtitulo("Diagrama de flujo")
    pdf.codigo(
        "Binance WebSocket --> Kafka Producer --> Topic 'crypto-trades'\n"
        "                                             |\n"
        "                                             v\n"
        "                                 Spark Structured Streaming\n"
        "                                             |\n"
        "                          Ventanas (1 min, slide 30s) + Features\n"
        "                                             |\n"
        "                                  Acumular en Parquet\n"
        "                                             |\n"
        "                              K-Means (batch, mejor k)\n"
        "                                             |\n"
        "                          Etiquetar: normal / anomalo (p95)\n"
        "                                             |\n"
        "                              Guardar datos etiquetados\n"
        "                                             |\n"
        "                         Logistic Regression (train, batch)\n"
        "                                             |\n"
        "                              Guardar modelo pipeline\n"
        "                                             |\n"
        "                    Segundo streaming --> Cargar modelo --> Inferencia"
    )

    # ── 4. Fases del Pipeline ──
    pdf.add_page()
    pdf.titulo_seccion("4", "Descripción Detallada de las Fases")

    # Fase 0
    pdf.subtitulo("Fase 0 - Diagnóstico del entorno")
    pdf.parrafo(
        "Antes de ejecutar el pipeline, se verifica la correcta instalación de todos los "
        "componentes: Spark, Kafka, Java, y la conectividad entre PySpark y Kafka. Se ejecuta "
        "un script hardware_info.py que captura las especificaciones del hardware (CPU, RAM, "
        "GPU si existe, disco) y las exporta a JSON. Este paso se repite en ambas máquinas "
        "para la tabla comparativa del informe final."
    )
    pdf.parrafo(
        "Adicionalmente, verify_environment.py prueba la cadena completa: crea un topic de "
        "prueba en Kafka, produce y consume un mensaje, y verifica que PySpark puede leer "
        "de Kafka correctamente."
    )

    # Fase 1
    pdf.subtitulo("Fase 1 - Productor de datos (Binance -> Kafka)")
    pdf.parrafo(
        "El script producer/binance_producer.py se conecta al WebSocket público de Binance y "
        "recibe trades en tiempo real de 8 pares de criptomonedas. Cada trade se parsea y publica "
        "como un mensaje JSON en el topic 'crypto-trades' de Kafka."
    )
    pdf.parrafo("El esquema de cada mensaje es:")
    pdf.codigo(
        '{\n'
        '  "symbol": "BTCUSDT",\n'
        '  "price": 67543.21,\n'
        '  "quantity": 0.0023,\n'
        '  "timestamp": 1714000000000,\n'
        '  "is_buyer_maker": true\n'
        '}'
    )
    pdf.parrafo(
        "El productor implementa reconexión automática (Binance desconecta cada 24 horas), "
        "manejo de señales para cierre limpio con Ctrl+C, y reportes de throughput cada 500 "
        "mensajes. La tasa observada es de aproximadamente 50-200 mensajes por segundo "
        "dependiendo de la actividad del mercado."
    )

    # Fase 2
    pdf.add_page()
    pdf.subtitulo("Fase 2 - Primer streaming: Estadísticos + K-Means")
    pdf.parrafo(
        "Esta es la fase más compleja del pipeline. Spark Structured Streaming lee del topic "
        "de Kafka y ejecuta tres etapas dentro de cada micro-batch:"
    )

    pdf.set_font("Helvetica", "BI", 10)
    pdf.cell(0, 7, "Parte A - Estadísticos por ventana de tiempo", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(1)
    pdf.parrafo(
        "Se agrupan los trades por símbolo y ventana de tiempo (1 minuto de duración, "
        "30 segundos de slide) y se calculan: mínimo, máximo, promedio y varianza del precio; "
        "conteo de trades; y volumen total. Se usa un watermark de 30 segundos para manejar "
        "datos tardíos."
    )

    pdf.set_font("Helvetica", "BI", 10)
    pdf.cell(0, 7, "Parte B - Feature engineering", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(1)
    pdf.parrafo("Se calculan 4 features por ventana para alimentar los modelos de ML:")
    pdf.tabla(
        ["Feature", "Descripción", "Intuición"],
        [
            ["pct_return", "Retorno % del precio", "Dirección y magnitud del movimiento"],
            ["volatility", "Stddev / precio promedio", "Fluctuación relativa al nivel"],
            ["volume_intensity", "Volumen / num. trades", "Tamaño promedio de transacción"],
            ["price_speed", "Rango / precio promedio", "Amplitud relativa del rango"],
        ],
        [40, 55, 95],
    )

    pdf.set_font("Helvetica", "BI", 10)
    pdf.cell(0, 7, "Parte C - K-Means (no supervisado)", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(1)
    pdf.parrafo(
        "Una vez acumulados suficientes datos (controlado por ACCUMULATION_SECONDS), se detiene "
        "el streaming y se entrena K-Means en modo batch sobre todos los features acumulados. "
        "K-Means agrupa los datos en k clusters minimizando la distancia al cuadrado de cada "
        "punto a su centroide."
    )
    pdf.parrafo(
        "Se prueban k = 3, 4 y 5, seleccionando el que maximice el Silhouette Score. "
        "Luego se calcula la distancia euclidiana de cada punto a su centroide asignado; "
        "los puntos con distancia superior al percentil 95 se etiquetan como 'anómalo' "
        "(el otro 95% como 'normal'). Los clusters representan estados del mercado: "
        "calma, volatilidad moderada, y movimiento extremo."
    )
    pdf.parrafo(
        "El pipeline incluye VectorAssembler (combina las 4 features en un vector) y "
        "StandardScaler (normaliza a media 0, std 1) antes de K-Means, para que ningún "
        "feature domine por su escala."
    )

    # Fase 3
    pdf.add_page()
    pdf.subtitulo("Fase 3 - Entrenamiento supervisado (Logistic Regression)")
    pdf.parrafo(
        "Se lee el dataset etiquetado por K-Means y se entrena un modelo de Logistic Regression "
        "para clasificación binaria: 0 = normal, 1 = anómalo. Se eligió Logistic Regression por "
        "ser el clasificador más simple de Spark ML, dado que el foco del proyecto es la "
        "comparación de arquitecturas, no la complejidad del modelo."
    )
    pdf.parrafo(
        "El pipeline de ML encadena tres etapas: VectorAssembler -> StandardScaler -> "
        "LogisticRegression. La regularización es L2 pura (Ridge) con regParam = 0.1. "
        "Se hace split 80/20 con semilla fija (42) para reproducibilidad."
    )
    pdf.parrafo("Las métricas de evaluación incluyen:")
    pdf.bullet("Accuracy: proporción de predicciones correctas.")
    pdf.bullet("Precision (weighted): de los predichos como anómalos, cuántos lo eran.")
    pdf.bullet("Recall (weighted): de los anómalos reales, cuántos se detectaron.")
    pdf.bullet("F1-score (weighted): media armónica de precision y recall.")
    pdf.bullet("AUC-ROC: capacidad de discriminación independiente del umbral.")

    # Fase 4
    pdf.subtitulo("Fase 4 - Segundo streaming: Inferencia en tiempo real")
    pdf.parrafo(
        "Se reactiva Spark Structured Streaming leyendo del mismo topic de Kafka, pero con "
        "startingOffsets = 'latest' para procesar solo datos nuevos. Se calculan exactamente "
        "las mismas features que en la Fase 2 (reutilizando las funciones compute_window_statistics "
        "y compute_features) y se aplica el modelo de Logistic Regression cargado desde disco."
    )
    pdf.parrafo(
        "Cada ventana de tiempo se clasifica como normal o anómala, y las predicciones se "
        "escriben a Parquet con la columna de probabilidad para análisis de confianza."
    )

    # Fase 5
    pdf.subtitulo("Fase 5 - Captura de métricas de Spark UI")
    pdf.parrafo(
        "Mientras corre la Fase 2 o 4, se ejecuta capture_metrics.py en paralelo. Este script "
        "se conecta a la API REST de Spark UI (http://localhost:4040/api/v1/) y descarga "
        "métricas de jobs, stages, executors, environment y streaming queries."
    )
    pdf.parrafo("Las métricas clave que se capturan para la comparación son:")
    pdf.bullet("Tiempo de ejecución de los jobs")
    pdf.bullet("Executor Run Time y Scheduler Delay")
    pdf.bullet("GC Time (garbage collection)")
    pdf.bullet("Shuffle Read/Write (bytes transferidos entre stages)")
    pdf.bullet("Spill a memoria y disco")
    pdf.bullet("Tasa de streaming (input rate, processing rate)")
    pdf.bullet("Configuración del entorno")

    # Fase 6
    pdf.add_page()
    pdf.subtitulo("Fase 6 - Visualización con Tableau")
    pdf.parrafo(
        "Se consolidan los outputs de estadísticos, predicciones y métricas en archivos CSV "
        "consumibles por Tableau. Se preparan datos para dashboards animados que muestren "
        "la serie temporal de precios con marcas de anomalía, estadísticos por ventana, "
        "y la comparación de métricas de rendimiento entre ambas máquinas."
    )

    # ── 5. Arquitecturas a comparar ──
    pdf.titulo_seccion("5", "Arquitecturas de Hardware a Comparar")
    pdf.tabla(
        ["Característica", "Máquina A (Mac)", "Máquina B (Linux)"],
        [
            ["Sistema Operativo", "macOS (Darwin)", "Linux"],
            ["CPU", "Apple M1", "x86_64 (por confirmar)"],
            ["Cores", "8", "Por confirmar"],
            ["RAM", "8 GB", "16 GB"],
            ["GPU", "No dedicada", "NVIDIA (CUDA)"],
            ["Aceleración GPU", "N/A", "RAPIDS si compatible"],
        ],
        [50, 70, 70],
    )
    pdf.parrafo(
        "El código base está diseñado para funcionar sin GPU. En la Máquina B se evaluará "
        "la posibilidad de usar RAPIDS Accelerator for Apache Spark. Si la versión de RAPIDS "
        "no es compatible con Spark 3.5.4, la comparación se hará solo con CPU pero "
        "aprovechando las diferencias de hardware (CPU, RAM, disco)."
    )

    # ── 6. Resultados Preliminares ──
    pdf.add_page()
    pdf.titulo_seccion("6", "Resultados Preliminares")
    pdf.parrafo(
        "Se ejecutó una corrida de prueba con aproximadamente 6 minutos de acumulación "
        "para validar la funcionalidad completa del pipeline. Los resultados a continuación "
        "son preliminares; la corrida final utilizará 20 minutos de acumulación para "
        "obtener una muestra más robusta."
    )

    pdf.subtitulo("6.1 Datos acumulados")
    pdf.tabla(
        ["Métrica", "Valor"],
        [
            ["Tiempo de acumulación", "~6 minutos"],
            ["Ventanas generadas", "430"],
            ["Símbolos monitoreados", "8 (BTC, ETH, SOL, BNB, XRP, DOGE, ADA, AVAX)"],
            ["Ventanas por símbolo", "~54-63"],
            ["Tipo de ventana", "Sliding (1 min duración, 30s slide)"],
        ],
        [70, 120],
    )

    pdf.subtitulo("6.2 Resultados de K-Means (Fase 2)")
    pdf.tabla(
        ["Métrica", "Valor"],
        [
            ["Mejor k", "3"],
            ["Silhouette Score", "0.8245"],
            ["Anomalías detectadas", "24 / 430 (5.6%)"],
            ["Umbral de anomalía (p95)", "2.2086"],
        ],
        [70, 120],
    )
    pdf.parrafo(
        "El Silhouette Score de 0.82 indica una separación muy buena entre los clusters. "
        "Los 3 clusters representan estados de mercado bien diferenciados. El 5.6% de "
        "ventanas etiquetadas como anómalas es consistente con el umbral del percentil 95."
    )

    pdf.subtitulo("6.3 Resultados de Logistic Regression (Fase 3)")
    pdf.tabla(
        ["Métrica", "Valor"],
        [
            ["Accuracy", "94.57%"],
            ["AUC-ROC", "1.0000"],
            ["F1-score (weighted)", "91.92%"],
            ["Split train/test", "338 / 92"],
            ["Anomalías en train", "19"],
            ["Anomalías en test", "5"],
        ],
        [70, 120],
    )

    pdf.subtitulo("Coeficientes del modelo")
    pdf.tabla(
        ["Feature", "Coeficiente", "Interpretación"],
        [
            ["price_speed", "0.53", "Más influyente - rango amplio de precios"],
            ["volatility", "0.43", "Alta fluctuación relativa"],
            ["pct_return", "Bajo", "Menor impacto en clasificación"],
            ["volume_intensity", "Bajo", "Menor impacto en clasificación"],
        ],
        [50, 40, 100],
    )
    pdf.parrafo(
        "Los coeficientes confirman que price_speed y volatility son los features más "
        "relevantes para la detección de anomalías, lo cual es coherente con la intuición "
        "de que cambios rápidos y amplios en el precio caracterizan comportamiento anómalo."
    )

    pdf.add_page()
    pdf.subtitulo("6.4 Resultados de Inferencia (Fase 4)")
    pdf.tabla(
        ["Métrica", "Valor"],
        [
            ["Ventanas clasificadas", "254"],
            ["Predicciones 'normal'", "232 (91.3%)"],
            ["Predicciones 'anómalo'", "22 (8.7%)"],
        ],
        [70, 120],
    )

    pdf.subtitulo("6.5 Observaciones y limitaciones de la prueba")
    pdf.parrafo(
        "Desbalance de clases: Con solo 5 anomalías en el set de test, el modelo tiende "
        "a predecir 'normal' por defecto con el umbral de 0.5. Sin embargo, el AUC-ROC "
        "perfecto (1.0) indica que el ranking de probabilidades es correcto - el modelo "
        "sí distingue las anomalías, pero el umbral de decisión necesita ajustarse para "
        "la tasa de anomalías real (~5%)."
    )
    pdf.parrafo(
        "Con más datos (corrida final de 20 minutos), se espera: mayor número de anomalías "
        "en ambos sets (train y test), mejor aprendizaje de la frontera de decisión, y "
        "métricas más representativas del rendimiento real del modelo."
    )

    # ── 7. Estructura del proyecto ──
    pdf.titulo_seccion("7", "Estructura del Proyecto")
    pdf.codigo(
        "proyecto_final/\n"
        "+-- config/spark_config.py          # Configuracion centralizada de Spark\n"
        "+-- hardware_info.py                # Fase 0: specs de hardware\n"
        "+-- verify_environment.py           # Fase 0: verificacion Spark-Kafka\n"
        "+-- producer/binance_producer.py    # Fase 1: WebSocket -> Kafka\n"
        "+-- streaming/spark_streaming.py    # Fase 2: estadisticos + K-Means\n"
        "+-- streaming/spark_inference.py    # Fase 4: inferencia en streaming\n"
        "+-- training/train_model.py         # Fase 3: Logistic Regression\n"
        "+-- metrics/capture_metrics.py      # Fase 5: metricas de Spark UI\n"
        "+-- visualization/prepare_tableau.py # Fase 6: datos para Tableau\n"
        "+-- models/kmeans_model/            # Modelo K-Means guardado\n"
        "+-- models/logistic_model/          # Modelo LR guardado\n"
        "+-- output/statistics/              # Estadisticos por ventana\n"
        "+-- output/labeled_data/            # Datos etiquetados (K-Means)\n"
        "+-- output/predictions/             # Predicciones (Fase 4)\n"
        "+-- output/metrics/                 # Metricas de Spark UI"
    )

    # ── 8. Ejecución ──
    pdf.add_page()
    pdf.titulo_seccion("8", "Instrucciones de Ejecución")
    pdf.parrafo("El pipeline se ejecuta en orden estricto. Cada fase debe completarse antes de iniciar la siguiente.")
    pdf.codigo(
        "# Fase 0 - Verificacion del entorno\n"
        "python hardware_info.py\n"
        "python verify_environment.py\n"
        "\n"
        "# Fase 1 - Productor (dejar corriendo en terminal separada)\n"
        "python producer/binance_producer.py\n"
        "\n"
        "# Fase 2 - Primer streaming (estadisticos + K-Means)\n"
        "python streaming/spark_streaming.py\n"
        "\n"
        "# Fase 3 - Entrenar modelo supervisado\n"
        "python training/train_model.py\n"
        "\n"
        "# Fase 4 - Segundo streaming (inferencia)\n"
        "python streaming/spark_inference.py\n"
        "\n"
        "# Fase 5 - Captura de metricas (mientras corre Fase 2 o 4)\n"
        "python metrics/capture_metrics.py --arch mac-m1"
    )

    # ── 9. Pasos Siguientes ──
    pdf.titulo_seccion("9", "Pasos Siguientes para la Entrega Final")
    pdf.bullet("Configurar ACCUMULATION_SECONDS = 1200 (20 minutos) para la corrida final.")
    pdf.bullet("Limpiar todos los outputs previos y ejecutar el pipeline completo de inicio a fin.")
    pdf.bullet("Capturar métricas de Spark UI durante las fases de streaming.")
    pdf.bullet("Ejecutar el mismo pipeline en la Máquina B (Linux + GPU).")
    pdf.bullet("Comparar métricas cuantitativamente entre ambas arquitecturas.")
    pdf.bullet("Construir dashboards en Tableau con los datos consolidados.")
    pdf.bullet("Redactar el informe final con análisis comparativo completo.")

    # Guardar
    output_path = "/Users/adolfo/Documents/ArquitecturaDatos/proyecto_final/informe_preliminar.pdf"
    pdf.output(output_path)
    print(f"Informe generado exitosamente: {output_path}")


if __name__ == "__main__":
    generar_informe()
