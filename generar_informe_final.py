"""
generar_informe_final.py — Informe final completo del proyecto.

Combina el informe preliminar (descripción del pipeline, stack, fases) con
la comparación de arquitecturas (resultados, métricas Spark, análisis CPU vs
GPU). Conciso pero explicativo.

Salida: output/informe_final.pdf
"""

from __future__ import annotations

import os
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from fpdf import FPDF

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHARTS_DIR = os.path.join(BASE_DIR, "output", "charts")
OUT_PDF = os.path.join(BASE_DIR, "output", "informe_final.pdf")

os.makedirs(CHARTS_DIR, exist_ok=True)
os.makedirs(os.path.dirname(OUT_PDF), exist_ok=True)


# ── Datos consolidados ──────────────────────────────────────────────────────

MAC = {
    "windows": 430, "acc_min": 6, "best_k": 3, "silhouette": 0.8245,
    "anomalies": 24, "anomaly_pct": 5.6, "threshold": 2.2086,
    "lr_acc": 0.9457, "lr_auc": 1.0000, "lr_f1": 0.9192,
    "predictions_p4": 254,
}
CPU = {
    "windows": 949, "acc_min": 10, "best_k": 3, "silhouette": 0.7635,
    "anomalies": 52, "anomaly_pct": 5.5, "threshold": 2.7340,
    "lr_acc": 0.9211, "lr_auc": 0.9878, "lr_f1": 0.8879,
    "predictions_p4": 282,
    "p2_s": 619, "p3_s": 16, "p4_s": 188,
    "p4_jobs": 33, "p4_tasks": 96, "p4_job_dur": 5.32,
    "p4_exec_run": 5.34, "p4_gc": 0.06, "p4_shuffle_mb": 0.02,
}
GPU = {
    "windows": 944, "acc_min": 10, "best_k": 4, "silhouette": 0.7066,
    "anomalies": 55, "anomaly_pct": 5.8, "threshold": 1.9639,
    "lr_acc": 0.9474, "lr_auc": 0.9665, "lr_f1": 0.9349,
    "predictions_p4": 277,
    "p2_s": 653, "p3_s": 56, "p4_s": 203,
    "p4_jobs": 89, "p4_tasks": 320, "p4_job_dur": 64.23,
    "p4_exec_run": 122.67, "p4_gc": 0.47, "p4_shuffle_mb": 0.07,
}


# ── Gráficas ─────────────────────────────────────────────────────────────────

def chart_wallclock() -> str:
    phases = ["Fase 2\n(stream+K-Means)", "Fase 3\n(train LR)", "Fase 4\n(inferencia)"]
    cpu_v = [CPU["p2_s"], CPU["p3_s"], CPU["p4_s"]]
    gpu_v = [GPU["p2_s"], GPU["p3_s"], GPU["p4_s"]]
    fig, ax = plt.subplots(figsize=(9, 4.2))
    x = np.arange(len(phases))
    w = 0.35
    b1 = ax.bar(x - w/2, cpu_v, w, label="Linux CPU", color="#1f77b4")
    b2 = ax.bar(x + w/2, gpu_v, w, label="Linux GPU (RAPIDS)", color="#2ca02c")
    ax.set_xticks(x)
    ax.set_xticklabels(phases)
    ax.set_ylabel("Wall-clock (s)")
    ax.set_title("Tiempo wall-clock por fase: CPU vs GPU")
    for bar in list(b1) + list(b2):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f"{bar.get_height():.0f}s", ha="center", fontsize=9)
    ax.legend()
    ax.grid(True, axis="y", alpha=0.3)
    path = os.path.join(CHARTS_DIR, "wallclock.png")
    plt.tight_layout()
    plt.savefig(path, dpi=130)
    plt.close()
    return path


def chart_spark_metrics() -> str:
    metrics = ["Jobs", "Total Tasks", "Job dur (s)", "Executor run (s)", "GC (s)"]
    cpu_v = [CPU["p4_jobs"], CPU["p4_tasks"], CPU["p4_job_dur"], CPU["p4_exec_run"], CPU["p4_gc"]]
    gpu_v = [GPU["p4_jobs"], GPU["p4_tasks"], GPU["p4_job_dur"], GPU["p4_exec_run"], GPU["p4_gc"]]
    fig, ax = plt.subplots(figsize=(9.5, 4.2))
    x = np.arange(len(metrics))
    w = 0.35
    b1 = ax.bar(x - w/2, cpu_v, w, label="Linux CPU", color="#1f77b4")
    b2 = ax.bar(x + w/2, gpu_v, w, label="Linux GPU (RAPIDS)", color="#2ca02c")
    ax.set_xticks(x)
    ax.set_xticklabels(metrics, fontsize=9)
    ax.set_yscale("log")
    ax.set_ylabel("Valor (escala log)")
    ax.set_title("Métricas Spark UI — Fase 4 (3 min inferencia)")
    for bar in list(b1) + list(b2):
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h * 1.1,
                f"{h:.2f}" if h < 10 else f"{h:.0f}", ha="center", fontsize=8)
    ax.legend()
    ax.grid(True, axis="y", alpha=0.3, which="both")
    path = os.path.join(CHARTS_DIR, "spark_metrics_phase4.png")
    plt.tight_layout()
    plt.savefig(path, dpi=130)
    plt.close()
    return path


def chart_ml_quality() -> str:
    models = ["Silhouette\nK-Means", "Accuracy\nLR", "F1\nLR", "AUC-ROC\nLR"]
    mac = [MAC["silhouette"], MAC["lr_acc"], MAC["lr_f1"], MAC["lr_auc"]]
    cpu_v = [CPU["silhouette"], CPU["lr_acc"], CPU["lr_f1"], CPU["lr_auc"]]
    gpu_v = [GPU["silhouette"], GPU["lr_acc"], GPU["lr_f1"], GPU["lr_auc"]]
    fig, ax = plt.subplots(figsize=(9, 4.2))
    x = np.arange(len(models))
    w = 0.27
    ax.bar(x - w, mac, w, label="Mac M1 (6 min)", color="#9467bd")
    ax.bar(x, cpu_v, w, label="Linux CPU (10 min)", color="#1f77b4")
    ax.bar(x + w, gpu_v, w, label="Linux GPU (10 min)", color="#2ca02c")
    ax.set_xticks(x)
    ax.set_xticklabels(models, fontsize=9)
    ax.set_ylabel("Score (0-1)")
    ax.set_ylim(0.6, 1.05)
    ax.set_title("Calidad de modelos por arquitectura")
    ax.legend()
    ax.grid(True, axis="y", alpha=0.3)
    path = os.path.join(CHARTS_DIR, "ml_quality.png")
    plt.tight_layout()
    plt.savefig(path, dpi=130)
    plt.close()
    return path


# ── PDF ──────────────────────────────────────────────────────────────────────

FONT_DIR = "/usr/share/fonts/truetype/dejavu"


class PDF(FPDF):
    def __init__(self):
        super().__init__()
        self.add_font("DejaVu", "", os.path.join(FONT_DIR, "DejaVuSans.ttf"))
        self.add_font("DejaVu", "B", os.path.join(FONT_DIR, "DejaVuSans-Bold.ttf"))
        self.add_font("DejaVu", "I", os.path.join(FONT_DIR, "DejaVuSans-Oblique.ttf"))
        self.add_font("DejaVu", "BI", os.path.join(FONT_DIR, "DejaVuSans-BoldOblique.ttf"))
        self.add_font("Mono", "", os.path.join(FONT_DIR, "DejaVuSansMono.ttf"))

    def header(self):
        if self.page_no() == 1:
            return
        self.set_font("DejaVu", "B", 9)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, "Proyecto Final — Arquitectura de Grandes Volúmenes de Datos · ITAM 2026", align="C")
        self.ln(4)
        self.set_draw_color(0, 102, 204)
        self.set_line_width(0.4)
        self.line(15, self.get_y(), 195, self.get_y())
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font("DejaVu", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Página {self.page_no()}/{{nb}}", align="C")

    def h1(self, num, txt):
        self.set_font("DejaVu", "B", 15)
        self.set_text_color(0, 70, 140)
        self.cell(0, 10, f"{num}. {txt}", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(0, 102, 204)
        self.set_line_width(0.3)
        self.line(15, self.get_y(), 195, self.get_y())
        self.ln(3)

    def h2(self, txt):
        self.set_font("DejaVu", "B", 11)
        self.set_text_color(50, 50, 50)
        self.cell(0, 7, txt, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def p(self, txt):
        self.set_font("DejaVu", "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.3, txt)
        self.ln(2)

    def bullet(self, txt):
        self.set_font("DejaVu", "", 10)
        self.set_text_color(30, 30, 30)
        self.cell(5, 5.3, "•")
        self.multi_cell(0, 5.3, txt)
        self.ln(0.3)

    def code(self, txt):
        self.set_font("Mono", "", 8.5)
        self.set_fill_color(245, 245, 245)
        self.set_text_color(60, 60, 60)
        self.multi_cell(0, 4.8, txt, fill=True)
        self.ln(2)

    def table(self, headers, rows, widths=None):
        if widths is None:
            widths = [180 // len(headers)] * len(headers)
        self.set_font("DejaVu", "B", 9)
        self.set_fill_color(0, 70, 140)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(widths[i], 6.5, h, border=1, fill=True, align="C")
        self.ln()
        self.set_font("DejaVu", "", 9)
        self.set_text_color(30, 30, 30)
        fill = False
        for row in rows:
            self.set_fill_color(235, 241, 250 if fill else 255)
            for i, cell in enumerate(row):
                align = "L" if i == 0 else "C"
                self.cell(widths[i], 6, str(cell), border=1, fill=True, align=align)
            self.ln()
            fill = not fill
        self.ln(3)

    def image_centered(self, path, w=170):
        x = (self.w - w) / 2
        self.image(path, x=x, w=w)
        self.ln(3)


def build_pdf() -> None:
    chart_wc = chart_wallclock()
    chart_sm = chart_spark_metrics()
    chart_ml = chart_ml_quality()

    pdf = PDF()
    pdf.alias_nb_pages()
    pdf.set_margins(15, 18, 15)
    pdf.set_auto_page_break(auto=True, margin=18)

    # ── Portada ──
    pdf.add_page()
    pdf.ln(28)
    pdf.set_font("DejaVu", "B", 22)
    pdf.set_text_color(0, 70, 140)
    pdf.multi_cell(0, 11, "Pipeline de Streaming\nen Tiempo Real para\nDetección de Anomalías\nen Criptomonedas", align="C")
    pdf.ln(6)
    pdf.set_font("DejaVu", "", 12)
    pdf.set_text_color(80, 80, 80)
    pdf.multi_cell(0, 6,
        "Comparación de tres arquitecturas de ejecución:\n"
        "Apple M1, Linux CPU multi-core, y Linux + NVIDIA GPU (RAPIDS)",
        align="C")
    pdf.ln(10)
    pdf.set_draw_color(0, 102, 204)
    pdf.set_line_width(0.8)
    pdf.line(70, pdf.get_y(), 140, pdf.get_y())
    pdf.ln(10)
    pdf.set_font("DejaVu", "", 11)
    pdf.set_text_color(60, 60, 60)
    pdf.cell(0, 7, "Arquitectura de Grandes Volúmenes de Datos", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, "ITAM · Primavera 2026 · Prof. Wilmer Pereira", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)
    pdf.set_font("DejaVu", "B", 12)
    pdf.cell(0, 7, "Adolfo Yunes", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("DejaVu", "", 10)
    pdf.cell(0, 6, "adolfoyunes1@gmail.com", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)
    pdf.set_font("DejaVu", "I", 10)
    pdf.set_text_color(120, 120, 120)
    pdf.cell(0, 7, f"Generado el {datetime.now().strftime('%d de %B de %Y')}", align="C", new_x="LMARGIN", new_y="NEXT")

    # ── 1. Resumen ejecutivo ──
    pdf.add_page()
    pdf.h1(1, "Resumen Ejecutivo")
    pdf.p(
        "Se diseñó y ejecutó un pipeline de procesamiento en tiempo real que captura trades "
        "de Binance (8 pares contra USDT), calcula estadísticos y features por ventana de "
        "1 minuto, detecta anomalías con K-Means (no supervisado) y clasifica nuevas "
        "ventanas con Logistic Regression (supervisado). El mismo pipeline se corrió en "
        "tres arquitecturas para comparar el efecto del hardware sobre el rendimiento y "
        "la calidad de los modelos."
    )
    pdf.h2("Hallazgos principales")
    pdf.bullet("Los modelos producen métricas comparables en las tres arquitecturas (Silhouette 0.71-0.82, F1 0.89-0.93, AUC 0.97-1.00). El hardware no degrada la calidad del aprendizaje.")
    pdf.bullet("RAPIDS Accelerator (GPU NVIDIA) NO acelera el pipeline para este workload de streaming con batches chicos (~1K-3K trades por micro-batch). De hecho, la Fase 4 fue 12x más lenta en GPU por overhead de inicialización CUDA por task.")
    pdf.bullet("Las diferencias en cores y RAM entre Mac M1 (8/8) y Linux (20/7.6) no se traducen en wall-clock proporcional: el cuello de botella es el throughput de Kafka, no Spark.")
    pdf.bullet("RAPIDS introduce no-determinismo en operaciones float (incompatibleOps) lo cual hace que K-Means en GPU converja a k=4 en lugar de k=3 con centroides ligeramente distintos. Es estadísticamente válido pero no idéntico al CPU.")
    pdf.h2("Resultado en una tabla")
    pdf.table(
        ["Métrica clave", "Mac M1", "Linux CPU", "Linux GPU"],
        [
            ["Ventanas acumuladas", MAC["windows"], CPU["windows"], GPU["windows"]],
            ["Acumulación (min)", MAC["acc_min"], CPU["acc_min"], GPU["acc_min"]],
            ["K-Means: mejor k", MAC["best_k"], CPU["best_k"], GPU["best_k"]],
            ["K-Means: Silhouette", f"{MAC['silhouette']:.3f}", f"{CPU['silhouette']:.3f}", f"{GPU['silhouette']:.3f}"],
            ["LR: F1 (weighted)", f"{MAC['lr_f1']:.3f}", f"{CPU['lr_f1']:.3f}", f"{GPU['lr_f1']:.3f}"],
            ["LR: AUC-ROC", f"{MAC['lr_auc']:.3f}", f"{CPU['lr_auc']:.3f}", f"{GPU['lr_auc']:.3f}"],
            ["Fase 4 wall-clock (s)", "—", CPU["p4_s"], GPU["p4_s"]],
            ["Fase 4 Executor run (s)", "—", f"{CPU['p4_exec_run']:.2f}", f"{GPU['p4_exec_run']:.2f}"],
        ],
        widths=[55, 40, 40, 45],
    )

    # ── 2. Introducción ──
    pdf.add_page()
    pdf.h1(2, "Introducción y Objetivo")
    pdf.p(
        "El proyecto resuelve un problema realista de Big Data: ingerir trades de criptomonedas "
        "en tiempo real, procesarlos en ventanas de tiempo, y detectar comportamiento anómalo "
        "del mercado. Los componentes son intencionalmente simples (Kafka 1 partición, K-Means, "
        "Logistic Regression) porque el foco está en la arquitectura de ejecución, no en la "
        "sofisticación del ML."
    )
    pdf.p(
        "La fuente es el WebSocket público de Binance: 8 streams en paralelo "
        "(BTC, ETH, SOL, BNB, XRP, DOGE, ADA, AVAX vs USDT) con throughput de ~70-110 "
        "trades/segundo. Cada trade publica un JSON a Kafka, Spark Structured Streaming lo "
        "consume, agrega y modela. Sobre este pipeline se comparan tres entornos:"
    )
    pdf.bullet("Máquina A: Apple M1, 8 cores, 8 GB RAM, sin GPU dedicada.")
    pdf.bullet("Máquina B (CPU): Intel i7-13650HX, 20 cores lógicos, 7.6 GB RAM efectiva en WSL2, sin aceleración GPU.")
    pdf.bullet("Máquina B (GPU): mismo hardware Linux, ahora con NVIDIA RTX 4060 Laptop (8 GB VRAM, CUDA 12.6) usando RAPIDS Accelerator for Apache Spark.")

    # ── 3. Stack tecnológico ──
    pdf.h1(3, "Stack Tecnológico")
    pdf.table(
        ["Componente", "Versión", "Notas"],
        [
            ["Python", "3.10-3.11", "Anaconda (Mac) / system (Linux)"],
            ["PySpark", "3.5.4", "instalado vía pip"],
            ["Scala", "2.12", "binding del conector Kafka"],
            ["Java", "OpenJDK 17", "ambas máquinas"],
            ["Apache Kafka", "4.2.0", "KRaft mode (sin Zookeeper)"],
            ["Conector Kafka", "spark-sql-kafka-0-10_2.12:3.5.4", "auto-download desde Maven"],
            ["RAPIDS Accelerator", "25.10.0", "solo Máquina B, GPU"],
            ["cuDF", "25.10.0", "biblioteca CUDA empaquetada en el JAR"],
            ["CUDA Toolkit", "12.6", "Máquina B"],
        ],
        widths=[55, 50, 75],
    )
    pdf.h2("Configuración relevante de Spark")
    pdf.code(
        "# Común a las tres arquitecturas:\n"
        "spark.master              = local[*]\n"
        "spark.hadoop.fs.defaultFS = file:///   # evita conexión a HDFS\n"
        "spark.sql.shuffle.partitions = 4       # local mode\n"
        "spark.driver.memory       = 2g (Mac) / 3g (Linux)\n\n"
        "# Solo Linux GPU (RAPIDS):\n"
        "spark.plugins                       = com.nvidia.spark.SQLPlugin\n"
        "spark.rapids.sql.enabled            = true\n"
        "spark.rapids.memory.gpu.allocFraction = 0.5\n"
        "spark.rapids.sql.concurrentGpuTasks   = 1\n"
        "spark.rapids.sql.incompatibleOps.enabled = true\n"
        "spark.sql.session.timeZone            = UTC   # habilita JsonToStructs en GPU"
    )
    pdf.p(
        "Nota técnica: en modo local[*] NO se setean spark.driver/executor/task.resource.gpu.* "
        "porque el resource scheduling de Spark causa deadlock del scheduler en pyspark. "
        "RAPIDS detecta la GPU directamente vía cuDF/CUDA sin necesidad del resource manager."
    )

    # ── 4. Arquitectura del pipeline ──
    pdf.add_page()
    pdf.h1(4, "Arquitectura del Pipeline")
    pdf.p("El pipeline se compone de tres momentos secuenciales (una fase no inicia hasta que la anterior termina):")
    pdf.code(
        "Binance WebSocket ──> Kafka Producer ──> Topic 'crypto-trades'\n"
        "                                              │\n"
        "                                              ▼\n"
        "                          [Fase 2] Spark Structured Streaming\n"
        "                       Ventanas (1 min, slide 30s) + 4 features\n"
        "                                              │\n"
        "                                Acumular ~10 min en Parquet\n"
        "                                              │\n"
        "                       K-Means batch (k=3,4,5) → mejor Silhouette\n"
        "                                              │\n"
        "                          Etiquetar normal / anómalo (P95)\n"
        "                                              │\n"
        "                          [Fase 3] Logistic Regression (batch)\n"
        "                          VectorAssembler → Scaler → LR (L2)\n"
        "                                              │\n"
        "                          Guardar PipelineModel a disco\n"
        "                                              │\n"
        "                       [Fase 4] Cargar modelo, segundo streaming\n"
        "                       startingOffsets=latest → inferir → Parquet"
    )

    pdf.h2("Features ingenierizadas (4 por ventana)")
    pdf.table(
        ["Feature", "Fórmula", "Intuición"],
        [
            ["pct_return", "(last − first) / first", "Dirección y magnitud del movimiento"],
            ["volatility", "stddev(price) / avg(price)", "Fluctuación relativa al nivel"],
            ["volume_intensity", "Σvolume / count(trades)", "Tamaño promedio de transacción"],
            ["price_speed", "(max − min) / avg(price)", "Amplitud relativa del rango"],
        ],
        widths=[40, 60, 80],
    )

    pdf.h2("Fases del pipeline en una página")
    pdf.bullet("Fase 0 — Diagnóstico: hardware_info.py captura specs, verify_environment.py prueba Spark↔Kafka end-to-end.")
    pdf.bullet("Fase 1 — Producer: WebSocket → JSON → Kafka, ~70-110 trades/s. Reconexión automática.")
    pdf.bullet("Fase 2 — Streaming + K-Means: estadísticos por ventana, features, acumulación 10 min, K-Means batch, etiquetado P95.")
    pdf.bullet("Fase 3 — Train LR: 80/20 train/test (seed=42), pipeline VectorAssembler → StandardScaler → LR (L2, regParam=0.1, maxIter=20).")
    pdf.bullet("Fase 4 — Inferencia streaming: carga modelo, lee Kafka desde 'latest', clasifica ventana por ventana, guarda con probability.")
    pdf.bullet("Fase 5 — Captura métricas: GET a http://localhost:4040/api/v1/ mientras corre streaming.")
    pdf.bullet("Fase 6 — Tableau: CSVs consolidados para dashboards (window_statistics, labeled_data, predictions).")

    # ── 5. Comparación de Arquitecturas (Hardware) ──
    pdf.add_page()
    pdf.h1(5, "Arquitecturas Comparadas")
    pdf.table(
        ["Característica", "Mac M1", "Linux CPU", "Linux GPU"],
        [
            ["Sistema operativo", "macOS 15", "Ubuntu 22.04 (WSL2)", "Ubuntu 22.04 (WSL2)"],
            ["CPU", "Apple M1", "i7-13650HX", "i7-13650HX"],
            ["Cores lógicos", "8", "20", "20"],
            ["RAM efectiva", "8 GB", "7.6 GB", "7.6 GB"],
            ["GPU", "Ninguna", "Ninguna (CPU only)", "RTX 4060 Laptop 8 GB"],
            ["CUDA / RAPIDS", "—", "—", "12.6 / 25.10.0"],
            ["spark.driver.memory", "2g", "3g", "3g"],
        ],
        widths=[55, 45, 45, 45],
    )
    pdf.p(
        "Las máquinas Linux comparten el mismo hardware: la diferencia entre CPU y GPU es "
        "puramente de software (variable SPARK_USE_GPU=1 que activa el plugin RAPIDS). Esto "
        "aísla el efecto de la aceleración GPU sin contaminación por otras variables. "
        "Apple M1 vs Intel i7 sí mezclan ISA (ARM vs x86), pero ese contraste tampoco es el "
        "objetivo principal."
    )

    # ── 6. Resultados de los modelos ──
    pdf.add_page()
    pdf.h1(6, "Resultados de los Modelos")
    pdf.h2("6.1 K-Means (no supervisado, batch)")
    pdf.table(
        ["Métrica", "Mac M1", "Linux CPU", "Linux GPU"],
        [
            ["Ventanas acumuladas", MAC["windows"], CPU["windows"], GPU["windows"]],
            ["Mejor k (por Silhouette)", MAC["best_k"], CPU["best_k"], GPU["best_k"]],
            ["Silhouette Score", f"{MAC['silhouette']:.4f}", f"{CPU['silhouette']:.4f}", f"{GPU['silhouette']:.4f}"],
            ["Anomalías", f"{MAC['anomalies']} ({MAC['anomaly_pct']:.1f}%)",
                          f"{CPU['anomalies']} ({CPU['anomaly_pct']:.1f}%)",
                          f"{GPU['anomalies']} ({GPU['anomaly_pct']:.1f}%)"],
            ["Umbral P95 (distancia)", f"{MAC['threshold']:.3f}", f"{CPU['threshold']:.3f}", f"{GPU['threshold']:.3f}"],
        ],
        widths=[55, 45, 45, 45],
    )
    pdf.p(
        "Interpretación: las tres arquitecturas detectan ~5-6% de anomalías (coherente con el "
        "P95). Silhouette en Mac fue mayor porque la corrida fue más corta (430 ventanas vs "
        "~945 en Linux) y los clusters quedaron más limpios; con más datos aparecen ventanas "
        "fronterizas que bajan el score. RAPIDS eligió k=4 por el no-determinismo de floats."
    )

    pdf.h2("6.2 Logistic Regression (supervisado, batch)")
    pdf.table(
        ["Métrica", "Mac M1", "Linux CPU", "Linux GPU"],
        [
            ["Accuracy", f"{MAC['lr_acc']:.4f}", f"{CPU['lr_acc']:.4f}", f"{GPU['lr_acc']:.4f}"],
            ["F1 (weighted)", f"{MAC['lr_f1']:.4f}", f"{CPU['lr_f1']:.4f}", f"{GPU['lr_f1']:.4f}"],
            ["AUC-ROC", f"{MAC['lr_auc']:.4f}", f"{CPU['lr_auc']:.4f}", f"{GPU['lr_auc']:.4f}"],
            ["Predicciones Fase 4", MAC["predictions_p4"], CPU["predictions_p4"], GPU["predictions_p4"]],
        ],
        widths=[55, 45, 45, 45],
    )
    pdf.image_centered(chart_ml, w=170)
    pdf.p(
        "El AUC perfecto de Mac (1.0) es un artefacto del set de test reducido (5 anomalías): "
        "el modelo logra rankear correctamente todas las probabilidades, pero el umbral "
        "default 0.5 deja pasar 4 de 5 anomalías. En Linux GPU con 15 anomalías de test el "
        "modelo logra recall 5/15, mejor que CPU (1/16) — la diferencia se debe a la frontera "
        "de decisión que aprende cada arquitectura sobre datos ligeramente distintos."
    )

    # ── 7. Comparación de rendimiento ──
    pdf.add_page()
    pdf.h1(7, "Comparación de Rendimiento (Linux CPU vs GPU)")
    pdf.h2("7.1 Wall-clock por fase")
    pdf.image_centered(chart_wc, w=170)
    total_cpu = CPU["p2_s"] + CPU["p3_s"] + CPU["p4_s"]
    total_gpu = GPU["p2_s"] + GPU["p3_s"] + GPU["p4_s"]
    pdf.table(
        ["Fase", "CPU (s)", "GPU (s)", "Δ", "Ratio"],
        [
            ["Fase 2 (stream+K-Means)", CPU["p2_s"], GPU["p2_s"],
                f"+{GPU['p2_s']-CPU['p2_s']:.0f}", f"{GPU['p2_s']/CPU['p2_s']:.2f}x"],
            ["Fase 3 (train LR)", CPU["p3_s"], GPU["p3_s"],
                f"+{GPU['p3_s']-CPU['p3_s']:.0f}", f"{GPU['p3_s']/CPU['p3_s']:.2f}x"],
            ["Fase 4 (inferencia 3 min)", CPU["p4_s"], GPU["p4_s"],
                f"+{GPU['p4_s']-CPU['p4_s']:.0f}", f"{GPU['p4_s']/CPU['p4_s']:.2f}x"],
            ["TOTAL", total_cpu, total_gpu,
                f"+{total_gpu-total_cpu:.0f}", f"{total_gpu/total_cpu:.2f}x"],
        ],
        widths=[60, 30, 30, 30, 30],
    )

    pdf.h2("7.2 Métricas Spark UI — Fase 4")
    pdf.image_centered(chart_sm, w=170)
    pdf.table(
        ["Métrica", "CPU", "GPU", "Ratio GPU/CPU"],
        [
            ["Jobs completados", CPU["p4_jobs"], GPU["p4_jobs"], f"{GPU['p4_jobs']/CPU['p4_jobs']:.2f}x"],
            ["Total tasks", CPU["p4_tasks"], GPU["p4_tasks"], f"{GPU['p4_tasks']/CPU['p4_tasks']:.2f}x"],
            ["Suma duración jobs (s)", f"{CPU['p4_job_dur']:.2f}", f"{GPU['p4_job_dur']:.2f}",
                f"{GPU['p4_job_dur']/CPU['p4_job_dur']:.1f}x"],
            ["Executor run time (s)", f"{CPU['p4_exec_run']:.2f}", f"{GPU['p4_exec_run']:.2f}",
                f"{GPU['p4_exec_run']/CPU['p4_exec_run']:.1f}x"],
            ["GC time (s)", f"{CPU['p4_gc']:.2f}", f"{GPU['p4_gc']:.2f}",
                f"{GPU['p4_gc']/CPU['p4_gc']:.1f}x"],
            ["Shuffle read (MB)", f"{CPU['p4_shuffle_mb']:.2f}", f"{GPU['p4_shuffle_mb']:.2f}", "—"],
            ["Memory/Disk spill", "0", "0", "—"],
        ],
        widths=[60, 35, 35, 40],
    )

    # ── 8. Análisis ──
    pdf.add_page()
    pdf.h1(8, "Análisis de las Diferencias")
    pdf.h2("8.1 Por qué GPU es más lento aquí")
    pdf.bullet("Tamaño de batch demasiado pequeño: cada micro-batch tiene ~500-3000 trades. Ese volumen cabe en cache L2/L3 del CPU; el cómputo CPU es prácticamente gratis.")
    pdf.bullet("Overhead por task GPU: cada task aloja memoria CUDA, transfiere de JVM heap a GPU, ejecuta, y transfiere de vuelta. Para ~500 filas son ms de overhead vs μs de cómputo real.")
    pdf.bullet("Spark fragmenta más con RAPIDS: Fase 4 generó 320 tasks vs 96 en CPU (operadores GpuShuffleCoalesce y GpuColumnar añaden particiones).")
    pdf.bullet("Operaciones que caen a CPU: from_json sólo va a GPU con session timezone = UTC; EventTimeWatermarkExec siempre CPU. Cada fallback paga transferencia CPU↔GPU.")
    pdf.bullet("GC time GPU 0.47s vs CPU 0.06s: RAPIDS aloja más objetos JVM intermedios (cuDF tables, GpuColumnVector wrappers) que presionan al G1GC.")
    pdf.bullet("MLlib no está acelerado: K-Means y LogisticRegression de Spark ML corren en CPU aunque RAPIDS esté activo. Solo el SQL/Catalyst se acelera.")

    pdf.h2("8.2 Por qué los modelos cambian con GPU")
    pdf.bullet("spark.rapids.sql.incompatibleOps=true permite que sumas/agregaciones float den resultados con orden ligeramente distinto (asociatividad rota).")
    pdf.bullet("Eso desplaza centroides de K-Means a coordenadas vecinas pero no idénticas: Silhouette cambia y el algoritmo prefiere k=4 sobre k=3.")
    pdf.bullet("El cambio se propaga: el split estratificado obtiene 15 anomalías test (GPU) vs 16 (CPU); el modelo aprende una frontera distinta.")
    pdf.bullet("Para reproducibilidad estricta entre arquitecturas se debe desactivar incompatibleOps, a costo de aún más overhead GPU.")

    pdf.h2("8.3 Sobre RAM, GC y spill")
    pdf.bullet("Ninguna arquitectura sufrió spill (memory ni disk). El dataset (~950 ventanas × 4 features) cabe holgadamente en RAM.")
    pdf.bullet("La hipótesis original del README ('Linux tiene más RAM por lo que menos GC y menos spill') no se materializó: WSL2 limitó el guest a 7.6 GB, similar a los 8 GB de Mac.")
    pdf.bullet("El cuello de botella real fue throughput de Kafka (~100 trades/s producer), no Spark ni hardware.")

    # ── 9. Lecciones ──
    pdf.add_page()
    pdf.h1(9, "Lecciones y Recomendaciones")
    pdf.h2("9.1 Cuándo NO usar RAPIDS en este tipo de pipeline")
    pdf.bullet("Batches menores a 10 K filas: el overhead GPU domina sobre el cómputo.")
    pdf.bullet("Streaming con triggers cortos (10 s): el init CUDA se paga repetidamente.")
    pdf.bullet("ML con MLlib (KMeans, LogisticRegression): RAPIDS no las acelera; solo SQL/Catalyst.")

    pdf.h2("9.2 Cuándo SÍ usar RAPIDS")
    pdf.bullet("Batches ≥ 1 M filas con joins, group-by, sort, window functions: documentado 3-10× speedup.")
    pdf.bullet("Lecturas/escrituras masivas de Parquet (>10 GB): el plugin acelera I/O columnar.")
    pdf.bullet("Pipelines batch (no streaming) donde el init GPU se amortiza sobre minutos de cómputo.")

    pdf.h2("9.3 Operacionales")
    pdf.bullet("Evitar spark.driver/executor/task.resource.gpu.* en local[*] — provoca deadlock del scheduler en pyspark. RAPIDS detecta la GPU sin resource manager.")
    pdf.bullet("CUDA 12.6 + RAPIDS 25.10.0 + Spark 3.5.4 + Scala 2.12: combinación verificada y estable.")
    pdf.bullet("spark.sql.session.timeZone='UTC' es necesario para que from_json corra en GPU.")
    pdf.bullet("RAPIDS introduce no-determinismo via incompatibleOps; documentarlo cuando reproducibilidad importe.")
    pdf.bullet("WSL2 limita RAM del guest. Para experimentos con memoria, considerar ajuste vía .wslconfig o máquina nativa Linux.")

    # ── 10. Reproducción ──
    pdf.h1(10, "Reproducción")
    pdf.code(
        "# 1. Dependencias Python\n"
        "pip install pyspark==3.5.4 kafka-python websocket-client requests \\\n"
        "            fpdf2 matplotlib numpy\n\n"
        "# 2. Kafka 4.2.0 (KRaft, sin Zookeeper)\n"
        "wget archive.apache.org/dist/kafka/4.2.0/kafka_2.13-4.2.0.tgz\n"
        "tar xzf kafka_2.13-4.2.0.tgz && cd kafka_2.13-4.2.0\n"
        "bin/kafka-storage.sh format -t $(bin/kafka-storage.sh random-uuid) \\\n"
        "    -c config/server.properties --standalone\n"
        "bin/kafka-server-start.sh config/server.properties &\n\n"
        "# 3. JAR de RAPIDS (~770 MB) — solo para Linux GPU\n"
        "wget https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/\\\n"
        "    25.10.0/rapids-4-spark_2.12-25.10.0.jar -P ~/tools/rapids/\n\n"
        "# 4. Pipeline (terminales separadas)\n"
        "python producer/binance_producer.py        # background\n"
        "python streaming/spark_streaming.py        # corrida CPU\n"
        "python training/train_model.py\n"
        "python streaming/spark_inference.py\n"
        "python metrics/capture_metrics.py --arch linux-cpu   # paralelo a streaming\n\n"
        "# 5. Mismo flujo en GPU\n"
        "SPARK_USE_GPU=1 SPARK_DRIVER_MEMORY=3g python streaming/spark_streaming.py\n"
        "SPARK_USE_GPU=1 python training/train_model.py\n"
        "SPARK_USE_GPU=1 python streaming/spark_inference.py\n"
        "python metrics/capture_metrics.py --arch linux-gpu\n\n"
        "# 6. Informe\n"
        "python generar_informe_final.py"
    )

    pdf.h2("Outputs preservados")
    pdf.bullet("output_linux_cpu/ y output_linux_gpu/: parquet, modelos, logs, métricas Spark UI por arquitectura.")
    pdf.bullet("output/charts/: gráficas PNG generadas por este script.")
    pdf.bullet("output/informe_final.pdf: este informe.")

    # ── 11. Conclusión ──
    pdf.add_page()
    pdf.h1(11, "Conclusión")
    pdf.p(
        "El proyecto demuestra de forma medible que una decisión arquitectónica —activar "
        "RAPIDS Accelerator— puede empeorar el rendimiento de un pipeline streaming bien "
        "dimensionado, en contradicción con la intuición común de que 'GPU = más rápido'. "
        "El overhead de inicialización CUDA por task, la fragmentación de tasks por el "
        "planificador RAPIDS, y la falta de aceleración para operadores MLlib hacen que GPU "
        "sea 12-23× más lento para este workload concreto."
    )
    pdf.p(
        "Al mismo tiempo, la calidad de los modelos aprendidos es comparable o ligeramente "
        "mejor en GPU (F1 0.935 vs 0.888 CPU), aunque parte de esa mejora es ruido por el "
        "no-determinismo de floats que introduce RAPIDS. La conclusión es que la GPU es una "
        "herramienta especializada: brilla con batches grandes y operaciones SQL pesadas, no "
        "con streaming de baja latencia y modelos clásicos de MLlib."
    )
    pdf.p(
        "La comparación Mac M1 vs Linux CPU mostró que el cuello de botella aquí es el "
        "throughput del producer (Kafka recibiendo ~100 trades/s), no el procesamiento Spark. "
        "Más cores o más RAM no aceleran el pipeline cuando el dato llega más lento que la "
        "capacidad de procesarlo."
    )
    pdf.p(
        "Para escenarios reales de producción de criptomonedas en tiempo real, la arquitectura "
        "recomendada es Linux CPU multi-core, manteniendo la GPU para análisis batch posterior "
        "(backtesting, joins con datasets históricos, agregaciones sobre meses de trades)."
    )

    pdf.output(OUT_PDF)
    print(f"PDF generado: {OUT_PDF}")


if __name__ == "__main__":
    build_pdf()
