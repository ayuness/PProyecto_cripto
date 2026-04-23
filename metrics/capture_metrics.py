"""
metrics/capture_metrics.py — Fase 5: Captura de métricas de Spark UI.

Conecta a la API REST de Spark UI (http://localhost:4040/api/v1/)
y descarga métricas de jobs, stages y streaming queries para la
comparación de arquitecturas que pide el proyecto.

La API REST de Spark UI expone en JSON toda la información que se ve
en la interfaz web. Este script la descarga de forma programática
para poder comparar ejecuciones entre las dos máquinas de forma
objetiva y reproducible.

IMPORTANTE: Este script debe ejecutarse MIENTRAS una aplicación Spark
está corriendo (Fase 2 o Fase 4), porque Spark UI solo está disponible
mientras existe una SparkSession activa.

Modo de uso recomendado:
  1. En Terminal 1: ejecutar la fase de streaming (Fase 2 o 4)
  2. En Terminal 2: ejecutar este script para capturar métricas
  3. Repetir en ambas máquinas

Ejecución:
    python metrics/capture_metrics.py

    Con etiqueta de arquitectura (para el nombre del archivo):
    python metrics/capture_metrics.py --arch mac-m1
    python metrics/capture_metrics.py --arch linux-gpu

Output:
    output/metrics/spark_metrics_<arch>_<timestamp>.json
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import METRICS_DIR

# ── Configuración ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("capture_metrics")

# URL base de la API REST de Spark UI.
# Por defecto Spark expone la UI en el puerto 4040.
# Si hay múltiples aplicaciones Spark corriendo, se usan 4041, 4042, etc.
SPARK_UI_BASE = "http://localhost:4040/api/v1"

# Timeout para requests HTTP (segundos)
REQUEST_TIMEOUT = 10


# ── Funciones de captura ─────────────────────────────────────────────────────

def api_get(endpoint: str) -> Any | None:
    """
    Hace GET a un endpoint de la API de Spark UI.
    Retorna el JSON parseado, o None si falla.
    """
    url = f"{SPARK_UI_BASE}{endpoint}"
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.ConnectionError:
        logger.error(
            "No se pudo conectar a Spark UI en %s. "
            "¿Hay una aplicación Spark corriendo?", SPARK_UI_BASE
        )
        return None
    except requests.RequestException as e:
        logger.warning("Error en %s: %s", url, e)
        return None


def capture_applications() -> list[dict] | None:
    """Lista de aplicaciones Spark (normalmente solo una en modo local)."""
    return api_get("/applications")


def capture_jobs(app_id: str) -> list[dict] | None:
    """
    Todos los jobs de la aplicación.
    Cada job tiene: jobId, status, submissionTime, completionTime,
    numTasks, numCompletedTasks, stageIds, etc.
    """
    return api_get(f"/applications/{app_id}/jobs")


def capture_stages(app_id: str) -> list[dict] | None:
    """
    Todos los stages de la aplicación.
    Cada stage tiene métricas detalladas: executorRunTime, jvmGcTime,
    shuffleReadBytes, shuffleWriteBytes, inputBytes, outputBytes,
    memoryBytesSpilled, diskBytesSpilled, etc.
    """
    return api_get(f"/applications/{app_id}/stages")


def capture_stage_detail(app_id: str, stage_id: int, attempt: int = 0) -> dict | None:
    """
    Detalle de un stage específico incluyendo métricas por task.
    Aquí están Scheduler Delay, Executor Run Time, GC Time, etc.
    """
    return api_get(f"/applications/{app_id}/stages/{stage_id}/{attempt}")


def capture_environment(app_id: str) -> dict | None:
    """
    Configuración del entorno Spark: properties, classpath, etc.
    Útil para documentar las diferencias de configuración entre máquinas.
    """
    return api_get(f"/applications/{app_id}/environment")


def capture_executors(app_id: str) -> list[dict] | None:
    """
    Información de executors: memoria, cores, tasks completados,
    shuffle read/write, GC time acumulado, etc.
    """
    return api_get(f"/applications/{app_id}/allexecutors")


def capture_streaming(app_id: str) -> dict | None:
    """
    Estadísticas de Structured Streaming: queries activas, input rate,
    processing rate, batch duration, etc.

    Endpoint: /applications/{id}/streaming/statistics
    Nota: solo disponible si hay un streaming query activo.
    """
    # Intentar el endpoint de streaming statistics
    stats = api_get(f"/applications/{app_id}/streaming/statistics")
    if stats:
        return stats

    # Alternativa: SQL streaming queries
    sql = api_get(f"/applications/{app_id}/sql")
    if sql:
        return {"sql_queries": sql}

    return None


# ── Métricas derivadas ───────────────────────────────────────────────────────

def compute_summary(metrics: dict[str, Any]) -> dict[str, Any]:
    """
    Calcula un resumen con las métricas clave para la comparación
    de arquitecturas que pide el profesor:

    1. Tiempo total de ejecución de jobs
    2. Tiempo de shuffle acumulado
    3. I/O total
    4. Scheduler Delay, Executor Run Time, GC Time
    5. Spill (Memory/Disk)
    6. Configuración del entorno
    """
    summary: dict[str, Any] = {}

    # ── Métricas de Jobs ──
    jobs = metrics.get("jobs", [])
    if jobs:
        completed = [j for j in jobs if j.get("status") == "SUCCEEDED"]
        summary["total_jobs"] = len(jobs)
        summary["completed_jobs"] = len(completed)

        # Tiempo total de ejecución (sum de duración de cada job)
        total_job_ms = 0
        for job in completed:
            start = job.get("submissionTime", "")
            end = job.get("completionTime", "")
            if start and end:
                from datetime import datetime as dt
                try:
                    t_start = dt.fromisoformat(start.replace("GMT", "+00:00").rstrip("Z"))
                    t_end = dt.fromisoformat(end.replace("GMT", "+00:00").rstrip("Z"))
                    total_job_ms += (t_end - t_start).total_seconds() * 1000
                except (ValueError, TypeError):
                    pass
        summary["total_job_duration_ms"] = total_job_ms

    # ── Métricas de Stages ──
    stages = metrics.get("stages", [])
    if stages:
        summary["total_stages"] = len(stages)

        # Acumular métricas de todos los stages
        total_executor_run_time = 0
        total_gc_time = 0
        total_shuffle_read = 0
        total_shuffle_write = 0
        total_input_bytes = 0
        total_output_bytes = 0
        total_memory_spill = 0
        total_disk_spill = 0
        total_scheduler_delay = 0

        for stage in stages:
            total_executor_run_time += stage.get("executorRunTime", 0)
            total_gc_time += stage.get("jvmGcTime", 0)
            total_shuffle_read += stage.get("shuffleReadBytes", 0)
            total_shuffle_write += stage.get("shuffleWriteBytes", 0)
            total_input_bytes += stage.get("inputBytes", 0)
            total_output_bytes += stage.get("outputBytes", 0)
            total_memory_spill += stage.get("memoryBytesSpilled", 0)
            total_disk_spill += stage.get("diskBytesSpilled", 0)
            # Scheduler delay no siempre está en el resumen del stage,
            # se acumula del detalle de tasks cuando está disponible
            total_scheduler_delay += stage.get("schedulerDelay", 0)

        summary["executor_run_time_ms"] = total_executor_run_time
        summary["gc_time_ms"] = total_gc_time
        summary["shuffle_read_bytes"] = total_shuffle_read
        summary["shuffle_write_bytes"] = total_shuffle_write
        summary["input_bytes"] = total_input_bytes
        summary["output_bytes"] = total_output_bytes
        summary["memory_bytes_spilled"] = total_memory_spill
        summary["disk_bytes_spilled"] = total_disk_spill

        # Convertir a unidades legibles
        summary["shuffle_read_mb"] = round(total_shuffle_read / (1024 * 1024), 2)
        summary["shuffle_write_mb"] = round(total_shuffle_write / (1024 * 1024), 2)
        summary["memory_spill_mb"] = round(total_memory_spill / (1024 * 1024), 2)
        summary["disk_spill_mb"] = round(total_disk_spill / (1024 * 1024), 2)

    # ── Métricas de Executors ──
    executors = metrics.get("executors", [])
    if executors:
        for ex in executors:
            if ex.get("id") == "driver":
                summary["driver_max_memory_mb"] = round(
                    ex.get("maxMemory", 0) / (1024 * 1024), 2
                )
                summary["driver_total_gc_time_ms"] = ex.get("totalGCTime", 0)
                summary["driver_total_tasks"] = ex.get("totalTasks", 0)

    return summary


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    global SPARK_UI_BASE

    parser = argparse.ArgumentParser(
        description="Captura métricas de Spark UI para comparación de arquitecturas"
    )
    parser.add_argument(
        "--arch", default="local",
        help="Etiqueta de arquitectura (ej: mac-m1, linux-gpu). Se usa en el nombre del archivo."
    )
    parser.add_argument(
        "--spark-ui", default=SPARK_UI_BASE,
        help=f"URL base de Spark UI API (default: {SPARK_UI_BASE})"
    )
    args = parser.parse_args()

    SPARK_UI_BASE = args.spark_ui

    logger.info("═" * 60)
    logger.info("FASE 5 — Captura de métricas de Spark UI")
    logger.info("═" * 60)
    logger.info("Spark UI API: %s", SPARK_UI_BASE)
    logger.info("Arquitectura: %s", args.arch)

    # 1. Obtener aplicaciones
    apps = capture_applications()
    if not apps:
        logger.error(
            "No se encontraron aplicaciones Spark. "
            "Este script debe ejecutarse mientras corre la Fase 2 o 4."
        )
        sys.exit(1)

    # Usar la aplicación más reciente
    app = apps[0]
    app_id = app["id"]
    app_name = app.get("name", "unknown")
    logger.info("Aplicación: %s (id: %s)", app_name, app_id)

    # 2. Capturar todas las métricas
    logger.info("Capturando métricas...")

    metrics: dict[str, Any] = {
        "capture_timestamp": datetime.now(timezone.utc).isoformat(),
        "architecture": args.arch,
        "application": app,
    }

    jobs = capture_jobs(app_id)
    if jobs:
        metrics["jobs"] = jobs
        logger.info("  Jobs: %d capturados", len(jobs))

    stages = capture_stages(app_id)
    if stages:
        metrics["stages"] = stages
        logger.info("  Stages: %d capturados", len(stages))

        # Capturar detalle de los primeros 20 stages (para Scheduler Delay por task)
        stage_details = []
        for stage in stages[:20]:
            detail = capture_stage_detail(
                app_id, stage["stageId"], stage.get("attemptId", 0)
            )
            if detail:
                stage_details.append(detail)
        if stage_details:
            metrics["stage_details"] = stage_details
            logger.info("  Stage details: %d capturados", len(stage_details))

    executors = capture_executors(app_id)
    if executors:
        metrics["executors"] = executors
        logger.info("  Executors: %d capturados", len(executors))

    environment = capture_environment(app_id)
    if environment:
        metrics["environment"] = environment
        logger.info("  Environment: capturado")

    streaming = capture_streaming(app_id)
    if streaming:
        metrics["streaming"] = streaming
        logger.info("  Streaming stats: capturado")

    # 3. Resumen
    summary = compute_summary(metrics)
    metrics["summary"] = summary

    logger.info("── Resumen de métricas ──")
    for key, value in summary.items():
        logger.info("  %s: %s", key, value)

    # 4. Guardar
    os.makedirs(METRICS_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"spark_metrics_{args.arch}_{timestamp}.json"
    output_path = os.path.join(METRICS_DIR, filename)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False, default=str)

    logger.info("Métricas guardadas en: %s", output_path)

    # 5. Mostrar métricas clave para la comparación
    logger.info("═" * 60)
    logger.info("MÉTRICAS CLAVE PARA COMPARACIÓN DE ARQUITECTURAS")
    logger.info("═" * 60)
    logger.info("  1. Tiempo total de jobs:        %.2f s", summary.get("total_job_duration_ms", 0) / 1000)
    logger.info("  2. Executor Run Time:           %.2f s", summary.get("executor_run_time_ms", 0) / 1000)
    logger.info("  3. GC Time:                     %.2f s", summary.get("gc_time_ms", 0) / 1000)
    logger.info("  4. Shuffle Read:                %.2f MB", summary.get("shuffle_read_mb", 0))
    logger.info("  5. Shuffle Write:               %.2f MB", summary.get("shuffle_write_mb", 0))
    logger.info("  6. Memory Spill:                %.2f MB", summary.get("memory_spill_mb", 0))
    logger.info("  7. Disk Spill:                  %.2f MB", summary.get("disk_spill_mb", 0))
    logger.info("  8. Total Tasks:                 %d", summary.get("driver_total_tasks", 0))
    logger.info("═" * 60)
    logger.info("→ Ejecuta en la otra máquina con: python metrics/capture_metrics.py --arch linux-gpu")
    logger.info("═" * 60)


if __name__ == "__main__":
    main()
