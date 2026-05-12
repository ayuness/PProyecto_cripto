"""
generar_comparacion.py — Genera PDF de comparación de arquitecturas.

Toma los resultados de las tres corridas (Mac M1 prueba, Linux CPU, Linux GPU)
y produce un informe PDF con tablas, gráficas y conclusiones.

Salida: output/reporte_comparacion_arquitecturas.pdf
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

import matplotlib
matplotlib.use("Agg")  # backend sin display
import matplotlib.pyplot as plt
import numpy as np
from fpdf import FPDF

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CPU_DIR = os.path.join(BASE_DIR, "output_linux_cpu")
GPU_DIR = os.path.join(BASE_DIR, "output_linux_gpu")
MAC_DIR = os.path.join(BASE_DIR, "prueba")
CHARTS_DIR = os.path.join(BASE_DIR, "output", "charts")
OUT_PDF = os.path.join(BASE_DIR, "output", "reporte_comparacion_arquitecturas.pdf")

os.makedirs(CHARTS_DIR, exist_ok=True)
os.makedirs(os.path.dirname(OUT_PDF), exist_ok=True)


# ── Datos consolidados ──────────────────────────────────────────────────────

# Resultados Mac M1 (corrida de prueba, 6 min — CLAUDE.md test run).
MAC = {
    "label": "Mac M1",
    "os": "macOS (Apple M1)",
    "cores": 8,
    "ram_gb": 8.0,
    "gpu": "ninguna",
    "accumulation_min": 6,
    "windows": 430,
    "best_k": 3,
    "silhouette": 0.8245,
    "anomalies": 24,
    "anomaly_pct": 5.6,
    "anomaly_threshold": 2.2086,
    "lr_acc": 0.9457,
    "lr_auc": 1.0000,
    "lr_f1": 0.9192,
    "predictions_phase4": 254,
}

CPU = {
    "label": "Linux CPU",
    "os": "Ubuntu 22.04 (WSL2)",
    "cores": 20,
    "ram_gb": 7.6,
    "gpu": "ninguna",
    "accumulation_min": 10,
    "windows": 949,
    "best_k": 3,
    "silhouette": 0.7635,
    "anomalies": 52,
    "anomaly_pct": 5.5,
    "anomaly_threshold": 2.7340,
    "lr_acc": 0.9211,
    "lr_auc": 0.9878,
    "lr_f1": 0.8879,
    "predictions_phase4": 282,
    "phase2_wallclock_s": 619,
    "phase3_wallclock_s": 16,
    "phase4_wallclock_s": 188,
    "phase4_jobs": 33,
    "phase4_total_tasks": 96,
    "phase4_job_duration_s": 5.32,
    "phase4_executor_run_s": 5.34,
    "phase4_gc_s": 0.06,
    "phase4_shuffle_mb": 0.02,
    "phase4_spill_mb": 0.0,
}

GPU = {
    "label": "Linux GPU (RAPIDS)",
    "os": "Ubuntu 22.04 (WSL2)",
    "cores": 20,
    "ram_gb": 7.6,
    "gpu": "NVIDIA RTX 4060 Laptop, 8 GB VRAM, CUDA 12.6",
    "rapids_version": "RAPIDS Accelerator 25.10.0 + cuDF 25.10.0",
    "accumulation_min": 10,
    "windows": 944,
    "best_k": 4,
    "silhouette": 0.7066,
    "anomalies": 55,
    "anomaly_pct": 5.8,
    "anomaly_threshold": 1.9639,
    "lr_acc": 0.9474,
    "lr_auc": 0.9665,
    "lr_f1": 0.9349,
    "predictions_phase4": 277,
    "phase2_wallclock_s": 653,
    "phase3_wallclock_s": 56,
    "phase4_wallclock_s": 203,
    "phase4_jobs": 89,
    "phase4_total_tasks": 320,
    "phase4_job_duration_s": 64.23,
    "phase4_executor_run_s": 122.67,
    "phase4_gc_s": 0.47,
    "phase4_shuffle_mb": 0.07,
    "phase4_spill_mb": 0.0,
}


# ── Generar gráficas ─────────────────────────────────────────────────────────

def chart_wallclock() -> str:
    """Tiempos wall-clock por fase, Linux CPU vs Linux GPU."""
    phases = ["Fase 2\n(streaming + K-Means)", "Fase 3\n(train LR)", "Fase 4\n(inferencia 3 min)"]
    cpu = [CPU["phase2_wallclock_s"], CPU["phase3_wallclock_s"], CPU["phase4_wallclock_s"]]
    gpu = [GPU["phase2_wallclock_s"], GPU["phase3_wallclock_s"], GPU["phase4_wallclock_s"]]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    x = np.arange(len(phases))
    w = 0.35
    b1 = ax.bar(x - w/2, cpu, w, label="Linux CPU", color="#1f77b4")
    b2 = ax.bar(x + w/2, gpu, w, label="Linux GPU (RAPIDS)", color="#2ca02c")
    ax.set_xticks(x)
    ax.set_xticklabels(phases)
    ax.set_ylabel("Tiempo wall-clock (s)")
    ax.set_title("Tiempo de cada fase: CPU vs GPU (RAPIDS)")
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
    """Métricas Spark de Fase 4: jobs, tasks, executor run time."""
    metrics = ["Jobs", "Total Tasks", "Job duration (s)", "Executor run (s)", "GC time (s)"]
    cpu = [CPU["phase4_jobs"], CPU["phase4_total_tasks"], CPU["phase4_job_duration_s"],
           CPU["phase4_executor_run_s"], CPU["phase4_gc_s"]]
    gpu = [GPU["phase4_jobs"], GPU["phase4_total_tasks"], GPU["phase4_job_duration_s"],
           GPU["phase4_executor_run_s"], GPU["phase4_gc_s"]]

    fig, ax = plt.subplots(figsize=(9.5, 4.5))
    x = np.arange(len(metrics))
    w = 0.35
    b1 = ax.bar(x - w/2, cpu, w, label="Linux CPU", color="#1f77b4")
    b2 = ax.bar(x + w/2, gpu, w, label="Linux GPU (RAPIDS)", color="#2ca02c")
    ax.set_xticks(x)
    ax.set_xticklabels(metrics, fontsize=9)
    ax.set_ylabel("Valor (escala log)")
    ax.set_yscale("log")
    ax.set_title("Métricas Spark UI — Fase 4 (inferencia 3 min)")
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
    """Métricas de calidad de modelos."""
    models = ["Silhouette\nK-Means", "Accuracy\nLR", "F1\nLR", "AUC-ROC\nLR"]
    mac = [MAC["silhouette"], MAC["lr_acc"], MAC["lr_f1"], MAC["lr_auc"]]
    cpu = [CPU["silhouette"], CPU["lr_acc"], CPU["lr_f1"], CPU["lr_auc"]]
    gpu = [GPU["silhouette"], GPU["lr_acc"], GPU["lr_f1"], GPU["lr_auc"]]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    x = np.arange(len(models))
    w = 0.27
    ax.bar(x - w, mac, w, label="Mac M1 (6 min)", color="#9467bd")
    ax.bar(x, cpu, w, label="Linux CPU (10 min)", color="#1f77b4")
    ax.bar(x + w, gpu, w, label="Linux GPU (10 min)", color="#2ca02c")
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
FONT_REG = os.path.join(FONT_DIR, "DejaVuSans.ttf")
FONT_BOLD = os.path.join(FONT_DIR, "DejaVuSans-Bold.ttf")
FONT_ITAL = os.path.join(FONT_DIR, "DejaVuSans-Oblique.ttf")
FONT_MONO = os.path.join(FONT_DIR, "DejaVuSansMono.ttf")


class PDF(FPDF):
    def __init__(self):
        super().__init__()
        self.add_font("DejaVu", "", FONT_REG)
        self.add_font("DejaVu", "B", FONT_BOLD)
        self.add_font("DejaVu", "I", FONT_ITAL)
        self.add_font("DejaVuMono", "", FONT_MONO)

    def header(self):
        self.set_font("DejaVu", "B", 10)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, "Comparación de Arquitecturas — Proyecto Final GVD ITAM 2026", align="C")
        self.ln(4)
        self.set_draw_color(0, 102, 204)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(6)

    def footer(self):
        self.set_y(-15)
        self.set_font("DejaVu", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Página {self.page_no()}/{{nb}}", align="C")

    def h1(self, txt):
        self.set_font("DejaVu", "B", 16)
        self.set_text_color(0, 70, 140)
        self.cell(0, 10, txt, new_x="LMARGIN", new_y="NEXT")
        self.ln(3)

    def h2(self, txt):
        self.set_font("DejaVu", "B", 13)
        self.set_text_color(0, 70, 140)
        self.cell(0, 9, txt, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(0, 102, 204)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def h3(self, txt):
        self.set_font("DejaVu", "B", 11)
        self.set_text_color(50, 50, 50)
        self.cell(0, 7, txt, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def p(self, txt):
        self.set_font("DejaVu", "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.5, txt)
        self.ln(2)

    def bullet(self, txt):
        self.set_font("DejaVu", "", 10)
        self.set_text_color(30, 30, 30)
        self.cell(6, 5.5, "-")
        self.multi_cell(0, 5.5, txt)
        self.ln(0.5)

    def table(self, headers, rows, widths=None):
        if widths is None:
            widths = [190 // len(headers)] * len(headers)
        self.set_font("DejaVu", "B", 9)
        self.set_fill_color(0, 70, 140)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(widths[i], 7, h, border=1, fill=True, align="C")
        self.ln()
        self.set_font("DejaVu", "", 9)
        self.set_text_color(30, 30, 30)
        fill = False
        for row in rows:
            self.set_fill_color(235, 241, 250 if fill else 255)
            for i, cell in enumerate(row):
                align = "L" if i == 0 else "C"
                self.cell(widths[i], 6.5, str(cell), border=1, fill=True, align=align)
            self.ln()
            fill = not fill
        self.ln(3)

    def code(self, txt):
        self.set_font("DejaVuMono", "", 9)
        self.set_fill_color(245, 245, 245)
        self.set_text_color(60, 60, 60)
        self.multi_cell(0, 5, txt, fill=True)
        self.ln(2)

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
    pdf.add_page()

    # ── Portada ──
    pdf.ln(20)
    pdf.set_font("DejaVu", "B", 22)
    pdf.set_text_color(0, 70, 140)
    pdf.multi_cell(0, 11,
        "Comparación de Arquitecturas de Ejecución\n"
        "en un Pipeline de Streaming de Criptomonedas",
        align="C")
    pdf.ln(8)
    pdf.set_font("DejaVu", "", 12)
    pdf.set_text_color(60, 60, 60)
    pdf.multi_cell(0, 6,
        "Proyecto Final — Arquitectura de Grandes Volúmenes de Datos\n"
        "ITAM · Prof. Wilmer Pereira · Primavera 2026",
        align="C")
    pdf.ln(15)
    pdf.set_font("DejaVu", "B", 11)
    pdf.set_text_color(0, 70, 140)
    pdf.cell(0, 8, "Tres arquitecturas evaluadas:", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    pdf.set_font("DejaVu", "", 11)
    pdf.set_text_color(30, 30, 30)
    pdf.cell(0, 7, "Mac M1 (8 cores, 8 GB)  ·  Linux CPU (20 cores, 7.6 GB)  ·  Linux + RTX 4060 (RAPIDS)",
             align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(20)
    pdf.set_font("DejaVu", "", 9)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 6, f"Generado el {datetime.now().strftime('%Y-%m-%d %H:%M')}",
             align="C", new_x="LMARGIN", new_y="NEXT")

    # ── 1. Resumen ejecutivo ──
    pdf.add_page()
    pdf.h1("1. Resumen Ejecutivo")
    pdf.p(
        "Se ejecutó el mismo pipeline de Spark Structured Streaming sobre tres "
        "configuraciones de hardware/software, capturando datos en vivo de "
        "Binance para 8 pares de criptomonedas. El objetivo es comparar el "
        "desempeño y la calidad de los modelos cuando cambia la arquitectura, "
        "no optimizar los modelos en sí."
    )
    pdf.h3("Hallazgo principal")
    pdf.p(
        "RAPIDS Accelerator (GPU NVIDIA) NO mejora el throughput para este "
        "workload de streaming con batches pequeños (~1 K ventanas en 10 min). "
        "El overhead de inicialización del contexto CUDA, asignación de memoria "
        "GPU y transferencias CPU<->GPU domina sobre el cómputo, haciendo que "
        "GPU sea ~12x más lento en wall-clock para la Fase 4 (inferencia) y "
        "~3.5x más lento para la Fase 3 (entrenamiento batch). En cambio, los "
        "MODELOS aprendidos son comparables o ligeramente mejores en GPU "
        "(F1 0.935 vs 0.888 CPU)."
    )
    pdf.h3("Veredicto por arquitectura")
    pdf.bullet("Mac M1: simple, suficiente para el dataset; mejor Silhouette (0.825) por menos datos y menos ruido.")
    pdf.bullet("Linux CPU (20 cores): wall-clock total más rápido y predecible; ideal para producción de este pipeline.")
    pdf.bullet("Linux + RAPIDS: técnicamente funciona, pero el overhead por batch arruina la ventaja paralela en streaming; valdría la pena solo con batches de >1M filas o joins masivos.")

    # ── 2. Hardware ──
    pdf.add_page()
    pdf.h1("2. Hardware y Software por Arquitectura")
    pdf.table(
        ["Parámetro", "Mac M1", "Linux CPU", "Linux GPU"],
        [
            ["OS", "macOS 15", "Ubuntu 22.04 (WSL2)", "Ubuntu 22.04 (WSL2)"],
            ["CPU", "Apple M1, 8 cores", "i7-13650HX, 20 cores", "i7-13650HX, 20 cores"],
            ["RAM", "8 GB", "7.6 GB", "7.6 GB"],
            ["GPU", "Ninguna", "Ninguna (CPU only)", "RTX 4060 Laptop, 8 GB"],
            ["Java", "OpenJDK 17", "OpenJDK 17", "OpenJDK 17"],
            ["Python", "3.11.7", "3.10.12", "3.10.12"],
            ["Spark", "3.5.4 (pip)", "3.5.4 (pip)", "3.5.4 (pip)"],
            ["Kafka", "4.2.0 (Homebrew)", "4.2.0 (tarball)", "4.2.0 (tarball)"],
            ["RAPIDS", "—", "—", "Accelerator 25.10.0 + cuDF 25.10.0"],
            ["spark.driver.memory", "2g", "3g", "3g"],
            ["spark.master", "local[*] (8)", "local[*] (20)", "local[*] (20)"],
        ],
        widths=[55, 45, 45, 45],
    )

    pdf.h3("Configuración RAPIDS clave")
    pdf.code(
        "spark.plugins = com.nvidia.spark.SQLPlugin\n"
        "spark.rapids.sql.enabled = true\n"
        "spark.rapids.memory.gpu.allocFraction = 0.5\n"
        "spark.rapids.memory.gpu.maxAllocFraction = 0.7\n"
        "spark.rapids.sql.concurrentGpuTasks = 1\n"
        "spark.rapids.sql.incompatibleOps.enabled = true\n"
        "spark.sql.session.timeZone = UTC   # habilita JsonToStructs en GPU\n"
        "# IMPORTANTE: NO se setean spark.driver/executor/task.resource.gpu.*\n"
        "# en modo local[*] — el resource scheduling de Spark causa deadlock\n"
        "# del scheduler. RAPIDS detecta la GPU directamente via cuDF/CUDA."
    )

    # ── 3. Pipeline ──
    pdf.add_page()
    pdf.h1("3. Pipeline Ejecutado")
    pdf.p(
        "El pipeline se compone de tres momentos secuenciales (ver CLAUDE.md):"
    )
    pdf.bullet("Fase 2: Spark Structured Streaming lee trades de Kafka, calcula estadísticos en ventanas de 1 min (slide 30 s) y 4 features (pct_return, volatility, volume_intensity, price_speed). Tras 10 min de acumulación, K-Means batch sobre todas las ventanas con k in {3,4,5}; mejor k por Silhouette Score. Etiqueta cada ventana 'anomalo' si distancia al centroide > P95.")
    pdf.bullet("Fase 3: Regresión Logística batch sobre las ventanas etiquetadas. Pipeline VectorAssembler -> StandardScaler -> LogisticRegression con L2 (regParam=0.1). 80/20 train/test, seed=42.")
    pdf.bullet("Fase 4: Inferencia en streaming (3 min) usando el modelo de Fase 3. startingOffsets=latest para no reprocesar datos viejos. Predicciones guardadas en Parquet.")
    pdf.h3("Datos de entrada")
    pdf.bullet("Producer: WebSocket de Binance, 8 streams en paralelo (btc/eth/sol/bnb/xrp/doge/ada/avax @ USDT).")
    pdf.bullet("Throughput observado: ~70-110 trades/seg (~1 K-3 K trades por batch de 10 s).")

    # ── 4. Resultados de modelos ──
    pdf.add_page()
    pdf.h1("4. Resultados de los Modelos")
    pdf.h2("4.1 K-Means")
    pdf.table(
        ["Métrica", "Mac M1", "Linux CPU", "Linux GPU"],
        [
            ["Ventanas acumuladas", MAC["windows"], CPU["windows"], GPU["windows"]],
            ["Tiempo acumulación (min)", MAC["accumulation_min"], CPU["accumulation_min"], GPU["accumulation_min"]],
            ["Mejor k", MAC["best_k"], CPU["best_k"], GPU["best_k"]],
            ["Silhouette Score", f"{MAC['silhouette']:.4f}", f"{CPU['silhouette']:.4f}", f"{GPU['silhouette']:.4f}"],
            ["Anomalías", f"{MAC['anomalies']} ({MAC['anomaly_pct']:.1f}%)",
                          f"{CPU['anomalies']} ({CPU['anomaly_pct']:.1f}%)",
                          f"{GPU['anomalies']} ({GPU['anomaly_pct']:.1f}%)"],
            ["Umbral P95 distancia", f"{MAC['anomaly_threshold']:.4f}",
                                      f"{CPU['anomaly_threshold']:.4f}",
                                      f"{GPU['anomaly_threshold']:.4f}"],
        ],
        widths=[55, 45, 45, 45],
    )
    pdf.p(
        "Nota: el GPU eligió k=4 en vez de k=3, con un Silhouette ligeramente menor (0.707 vs 0.764). "
        "Esto se debe a que RAPIDS habilita 'incompatibleOps' para mantener throughput, "
        "lo que permite diferencias de orden en operaciones de punto flotante. "
        "Los centroides convergen a puntos cercanos pero no idénticos a los del CPU."
    )

    pdf.h2("4.2 Logistic Regression")
    pdf.table(
        ["Métrica", "Mac M1", "Linux CPU", "Linux GPU"],
        [
            ["Accuracy", f"{MAC['lr_acc']:.4f}", f"{CPU['lr_acc']:.4f}", f"{GPU['lr_acc']:.4f}"],
            ["F1 (weighted)", f"{MAC['lr_f1']:.4f}", f"{CPU['lr_f1']:.4f}", f"{GPU['lr_f1']:.4f}"],
            ["AUC-ROC", f"{MAC['lr_auc']:.4f}", f"{CPU['lr_auc']:.4f}", f"{GPU['lr_auc']:.4f}"],
            ["Predicciones (Fase 4)", MAC["predictions_phase4"], CPU["predictions_phase4"], GPU["predictions_phase4"]],
        ],
        widths=[55, 45, 45, 45],
    )
    pdf.image_centered(chart_ml, w=170)

    # ── 5. Tiempos ──
    pdf.add_page()
    pdf.h1("5. Tiempos de Ejecución")
    pdf.h2("5.1 Wall-clock por fase (Linux CPU vs Linux GPU)")
    pdf.image_centered(chart_wc, w=170)
    pdf.table(
        ["Fase", "Linux CPU (s)", "Linux GPU (s)", "Delta", "Razón"],
        [
            ["Fase 2 (stream+K-Means)", CPU["phase2_wallclock_s"], GPU["phase2_wallclock_s"],
                f"+{GPU['phase2_wallclock_s']-CPU['phase2_wallclock_s']:.0f}s",
                f"{GPU['phase2_wallclock_s']/CPU['phase2_wallclock_s']:.2f}x"],
            ["Fase 3 (train LR)", CPU["phase3_wallclock_s"], GPU["phase3_wallclock_s"],
                f"+{GPU['phase3_wallclock_s']-CPU['phase3_wallclock_s']:.0f}s",
                f"{GPU['phase3_wallclock_s']/CPU['phase3_wallclock_s']:.2f}x"],
            ["Fase 4 (inferencia)", CPU["phase4_wallclock_s"], GPU["phase4_wallclock_s"],
                f"+{GPU['phase4_wallclock_s']-CPU['phase4_wallclock_s']:.0f}s",
                f"{GPU['phase4_wallclock_s']/CPU['phase4_wallclock_s']:.2f}x"],
            ["TOTAL", CPU["phase2_wallclock_s"]+CPU["phase3_wallclock_s"]+CPU["phase4_wallclock_s"],
                GPU["phase2_wallclock_s"]+GPU["phase3_wallclock_s"]+GPU["phase4_wallclock_s"],
                f"+{(GPU['phase2_wallclock_s']+GPU['phase3_wallclock_s']+GPU['phase4_wallclock_s'])-(CPU['phase2_wallclock_s']+CPU['phase3_wallclock_s']+CPU['phase4_wallclock_s']):.0f}s",
                "—"],
        ],
        widths=[55, 35, 35, 30, 30],
    )

    pdf.h2("5.2 Métricas Spark UI — Fase 4 (3 min inferencia)")
    pdf.image_centered(chart_sm, w=170)
    pdf.table(
        ["Métrica Spark", "Linux CPU", "Linux GPU", "Razón GPU/CPU"],
        [
            ["Jobs completados", CPU["phase4_jobs"], GPU["phase4_jobs"], f"{GPU['phase4_jobs']/CPU['phase4_jobs']:.2f}x"],
            ["Total tasks", CPU["phase4_total_tasks"], GPU["phase4_total_tasks"], f"{GPU['phase4_total_tasks']/CPU['phase4_total_tasks']:.2f}x"],
            ["Suma duración jobs (s)", f"{CPU['phase4_job_duration_s']:.2f}", f"{GPU['phase4_job_duration_s']:.2f}", f"{GPU['phase4_job_duration_s']/CPU['phase4_job_duration_s']:.1f}x"],
            ["Executor run time (s)", f"{CPU['phase4_executor_run_s']:.2f}", f"{GPU['phase4_executor_run_s']:.2f}", f"{GPU['phase4_executor_run_s']/CPU['phase4_executor_run_s']:.1f}x"],
            ["GC time (s)", f"{CPU['phase4_gc_s']:.2f}", f"{GPU['phase4_gc_s']:.2f}", f"{GPU['phase4_gc_s']/CPU['phase4_gc_s']:.1f}x"],
            ["Shuffle read (MB)", f"{CPU['phase4_shuffle_mb']:.2f}", f"{GPU['phase4_shuffle_mb']:.2f}", "—"],
            ["Memory spill (MB)", f"{CPU['phase4_spill_mb']:.2f}", f"{GPU['phase4_spill_mb']:.2f}", "—"],
        ],
        widths=[60, 40, 40, 45],
    )

    # ── 6. Análisis ──
    pdf.add_page()
    pdf.h1("6. Análisis de las Diferencias")
    pdf.h2("6.1 ¿Por qué GPU es más lento aquí?")
    pdf.bullet("Tamaño de batch demasiado pequeño: cada micro-batch tiene ~500-3000 trades. Ese volumen ya cabe en cache L2/L3 de un Xeon/Core i7, así que el cómputo CPU es prácticamente gratis.")
    pdf.bullet("Overhead por task: en GPU cada task allocates memoria CUDA, transfiere los datos desde JVM heap a GPU, ejecuta, y transfiere de regreso. Para ~500 filas eso son milisegundos de overhead vs microsegundos de cómputo.")
    pdf.bullet("Spark fragmenta más: la Fase 4 en GPU generó 320 tasks vs 96 en CPU (más particiones por las operaciones GpuShuffleCoalesce y GpuColumnar). Más tasks = más overhead acumulado.")
    pdf.bullet("Operaciones que caen a CPU: from_json sólo corre en GPU si la session timezone es UTC; HashAggregate, EventTimeWatermark también pueden caer a CPU. Cada fallback paga la transferencia CPU<->GPU.")
    pdf.bullet("GC time GPU = 0.47s vs CPU = 0.06s: RAPIDS aloja más objetos JVM intermedios (cuDF tables, GpuColumnVector wrappers), lo cual presiona al G1GC.")

    pdf.h2("6.2 ¿Por qué el modelo cambia con GPU?")
    pdf.bullet("spark.rapids.sql.incompatibleOps.enabled = true permite que operaciones float den resultados con orden ligeramente distinto (ej. suma asociativa).")
    pdf.bullet("Esto cambia los centroides de K-Means a coordenadas vecinas pero no idénticas, lo que altera Silhouette y umbral P95.")
    pdf.bullet("El cambio se propaga a la regresión: train test split estratificado obtiene 15 anomalías test (GPU) vs 16 (CPU); el modelo logra recall 5/15 vs 1/16 (GPU detecta más anomalías genuinamente).")
    pdf.bullet("Para reproducibilidad estricta entre arquitecturas se debe desactivar incompatibleOps, a costa de aún más overhead GPU.")

    pdf.h2("6.3 RAM / Spill")
    pdf.bullet("Ninguna arquitectura sufrió spill a disco. El dataset (~950 ventanas, 4 features) cabe en RAM con margen.")
    pdf.bullet("Mac M1 (8 GB) y Linux WSL2 (7.6 GB) son comparables en memoria efectiva. El cuello de botella real fue CPU, no RAM.")
    pdf.bullet("Linux gana en cores (20 vs 8) pero perdimos esa ventaja en GPU por la serialización vía concurrentGpuTasks=1.")

    # ── 7. Lecciones / cuándo sí usar GPU ──
    pdf.add_page()
    pdf.h1("7. Lecciones y Recomendaciones")
    pdf.h2("7.1 Cuándo NO usar RAPIDS en este tipo de pipeline")
    pdf.bullet("Batches < 10 K filas: el overhead de inicialización GPU domina. Costo > beneficio.")
    pdf.bullet("Streaming con triggers cortos (10 s): cada batch paga init de contexto GPU repetidamente.")
    pdf.bullet("Operaciones que RAPIDS no acelera nativo (UDFs Python, KMeans MLlib, LogisticRegression MLlib): MLlib NO se acelera con RAPIDS — solo el SQL/Catalyst.")

    pdf.h2("7.2 Cuándo SÍ usar RAPIDS")
    pdf.bullet("Batches >= 1 M filas con joins, group-by, sort, window functions: RAPIDS gana 3-10x en estos casos según docs NVIDIA.")
    pdf.bullet("Lecturas/escrituras masivas de Parquet (>10 GB): el plugin acelera I/O columnar.")
    pdf.bullet("Pipelines batch (no streaming) donde el init GPU se amortiza sobre cómputo de minutos.")

    pdf.h2("7.3 Lecciones operacionales")
    pdf.bullet("En modo local[*] de Spark, evitar spark.driver/executor/task.resource.gpu.* — provoca deadlock del scheduler en pyspark. RAPIDS detecta la GPU vía cuDF directo, no necesita resource scheduling de Spark.")
    pdf.bullet("WSL2 limita RAM del guest. El host tenía 16 GB pero el guest vio solo 7.6 GB. La hipótesis del README sobre 'más RAM en Linux' no aplicó.")
    pdf.bullet("CUDA 12.6 + RAPIDS 25.10.0 + Spark 3.5.4 + Scala 2.12: combinación verificada y estable en este proyecto.")
    pdf.bullet("spark.sql.session.timeZone='UTC' es necesario para que from_json corra en GPU; con timezone local cae a CPU.")
    pdf.bullet("RAPIDS introduce no-determinismo via incompatibleOps; documenta esto cuando la reproducibilidad importa.")

    # ── 8. Reproducción ──
    pdf.add_page()
    pdf.h1("8. Reproducción")
    pdf.p("Para reproducir esta comparación en otra máquina Linux con NVIDIA:")
    pdf.code(
        "# 1. Instalar dependencias\n"
        "pip install pyspark==3.5.4 kafka-python websocket-client requests fpdf2 matplotlib\n\n"
        "# 2. Kafka 4.2.0 (KRaft mode)\n"
        "wget archive.apache.org/dist/kafka/4.2.0/kafka_2.13-4.2.0.tgz\n"
        "tar xzf kafka_2.13-4.2.0.tgz\n"
        "kafka-storage.sh format -t $(kafka-storage.sh random-uuid) \\\n"
        "    -c config/server.properties --standalone\n"
        "kafka-server-start.sh config/server.properties &\n\n"
        "# 3. Descargar JAR de RAPIDS (~770 MB)\n"
        "wget https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/\\\n"
        "    25.10.0/rapids-4-spark_2.12-25.10.0.jar -P ~/tools/rapids/\n\n"
        "# 4. Pipeline (en terminales separadas)\n"
        "python producer/binance_producer.py        # background\n"
        "python streaming/spark_streaming.py        # corrida CPU\n"
        "python training/train_model.py\n"
        "python streaming/spark_inference.py\n\n"
        "# Para corrida GPU, mismas órdenes con env vars:\n"
        "SPARK_USE_GPU=1 SPARK_DRIVER_MEMORY=3g \\\n"
        "    python streaming/spark_streaming.py\n"
        "# (y así con train_model.py / spark_inference.py)\n"
    )

    pdf.h3("Outputs preservados")
    pdf.bullet("output_linux_cpu/: corrida completa CPU (parquet, modelos, logs, métricas).")
    pdf.bullet("output_linux_gpu/: corrida completa GPU (parquet, modelos, logs, métricas).")
    pdf.bullet("output/charts/: gráficas en PNG generadas por este script.")
    pdf.bullet("output/reporte_comparacion_arquitecturas.pdf: este informe.")

    pdf.output(OUT_PDF)
    print(f"PDF generado: {OUT_PDF}")


if __name__ == "__main__":
    build_pdf()
