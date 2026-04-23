"""
hardware_info.py — Fase 0: Captura de especificaciones de hardware.

Genera un JSON con las specs de la máquina donde se ejecuta.
Se corre en ambas máquinas (Mac y Linux) para construir la tabla
comparativa que pide el profesor en el informe.

Ejecución:
    python hardware_info.py

Output:
    output/metrics/hardware_info.json
"""

import json
import logging
import os
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("hardware_info")

# Ruta de salida
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(BASE_DIR, "output", "metrics", "hardware_info.json")


# ── Utilidades ───────────────────────────────────────────────────────────────

def _run(cmd: list[str], timeout: int = 10) -> str | None:
    """Ejecuta un comando y retorna stdout limpio, o None si falla."""
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


# ── Detección de OS y CPU ────────────────────────────────────────────────────

def get_os_info() -> dict[str, str]:
    """Información del sistema operativo."""
    return {
        "system": platform.system(),
        "release": platform.release(),
        "version": platform.version(),
        "machine": platform.machine(),
        "hostname": platform.node(),
    }


def get_cpu_info() -> dict[str, Any]:
    """
    Modelo, cores físicos y lógicos.
    En macOS se usa sysctl; en Linux se parsea /proc/cpuinfo.
    """
    info: dict[str, Any] = {
        "logical_cores": os.cpu_count(),
    }

    system = platform.system()

    if system == "Darwin":
        info["model"] = _run(["sysctl", "-n", "machdep.cpu.brand_string"]) or "Unknown"
        physical = _run(["sysctl", "-n", "hw.physicalcpu"])
        if physical:
            info["physical_cores"] = int(physical)
    elif system == "Linux":
        model = _run(["sh", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2"])
        if model:
            info["model"] = model.strip()
        physical = _run(["sh", "-c", "grep 'cpu cores' /proc/cpuinfo | head -1 | cut -d: -f2"])
        if physical:
            info["physical_cores"] = int(physical.strip())

    return info


# ── RAM ──────────────────────────────────────────────────────────────────────

def get_ram_gb() -> float | None:
    """RAM total en GB."""
    system = platform.system()
    if system == "Darwin":
        raw = _run(["sysctl", "-n", "hw.memsize"])
        if raw:
            return round(int(raw) / (1024 ** 3), 1)
    elif system == "Linux":
        raw = _run(["sh", "-c", "grep MemTotal /proc/meminfo | awk '{print $2}'"])
        if raw:
            return round(int(raw) / (1024 ** 2), 1)
    return None


# ── GPU (NVIDIA) ─────────────────────────────────────────────────────────────

def get_gpu_info() -> dict[str, Any] | None:
    """
    Detecta GPU NVIDIA usando nvidia-smi.
    Retorna None si no hay GPU NVIDIA disponible (caso Mac sin GPU dedicada).
    """
    # nvidia-smi con formato CSV para parseo limpio
    raw = _run([
        "nvidia-smi",
        "--query-gpu=name,memory.total,driver_version",
        "--format=csv,noheader,nounits",
    ])
    if not raw:
        logger.info("No se detectó GPU NVIDIA (nvidia-smi no disponible)")
        return None

    parts = [p.strip() for p in raw.split(",")]
    gpu_info: dict[str, Any] = {
        "model": parts[0] if len(parts) > 0 else "Unknown",
        "vram_mb": int(parts[1]) if len(parts) > 1 else None,
        "driver_version": parts[2] if len(parts) > 2 else None,
    }

    # Versión de CUDA toolkit (nvcc)
    cuda_raw = _run(["nvcc", "--version"])
    if cuda_raw:
        for line in cuda_raw.splitlines():
            if "release" in line.lower():
                # Formato típico: "Cuda compilation tools, release 12.2, V12.2.140"
                gpu_info["cuda_version"] = line.strip()
                break

    logger.info("GPU detectada: %s, VRAM: %s MB", gpu_info["model"], gpu_info.get("vram_mb"))
    return gpu_info


# ── Disco ────────────────────────────────────────────────────────────────────

def get_disk_info() -> dict[str, Any]:
    """Espacio en disco del volumen donde vive el proyecto."""
    usage = shutil.disk_usage(BASE_DIR)
    return {
        "total_gb": round(usage.total / (1024 ** 3), 1),
        "free_gb": round(usage.free / (1024 ** 3), 1),
        "used_percent": round((usage.used / usage.total) * 100, 1),
    }


# ── Software ─────────────────────────────────────────────────────────────────

def get_software_info() -> dict[str, str | None]:
    """Versiones de las herramientas clave del proyecto."""
    java_raw = _run(["java", "-version"])
    # java -version escribe a stderr, así que intentamos también ahí
    if not java_raw:
        try:
            result = subprocess.run(
                ["java", "-version"], capture_output=True, text=True, timeout=10
            )
            java_raw = result.stderr.strip().splitlines()[0] if result.stderr else None
        except (FileNotFoundError, subprocess.TimeoutExpired):
            java_raw = None

    spark_raw = _run(["sh", "-c", "spark-submit --version 2>&1 | grep -oE 'version [0-9]+\\.[0-9]+\\.[0-9]+' | head -1"])

    kafka_raw = _run(["kafka-topics", "--version"])

    python_pyspark = None
    try:
        import pyspark
        python_pyspark = pyspark.__version__
    except ImportError:
        pass

    return {
        "python": platform.python_version(),
        "java": java_raw,
        "spark_binary": spark_raw,
        "pyspark_pip": python_pyspark,
        "kafka": kafka_raw,
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def collect_hardware_info() -> dict[str, Any]:
    """Recopila toda la información de hardware y software."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "os": get_os_info(),
        "cpu": get_cpu_info(),
        "ram_gb": get_ram_gb(),
        "gpu": get_gpu_info(),
        "disk": get_disk_info(),
        "software": get_software_info(),
    }


def main() -> None:
    logger.info("Recopilando información de hardware y software...")

    info = collect_hardware_info()

    # Asegurar que el directorio de salida existe
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(info, f, indent=2, ensure_ascii=False)

    logger.info("Hardware info guardado en: %s", OUTPUT_PATH)

    # Resumen en consola
    logger.info("── Resumen ──")
    logger.info("OS:     %s %s (%s)", info["os"]["system"], info["os"]["release"], info["os"]["machine"])
    logger.info("CPU:    %s — %s cores lógicos", info["cpu"].get("model", "?"), info["cpu"].get("logical_cores", "?"))
    logger.info("RAM:    %s GB", info.get("ram_gb", "?"))
    if info["gpu"]:
        logger.info("GPU:    %s — %s MB VRAM", info["gpu"]["model"], info["gpu"].get("vram_mb", "?"))
    else:
        logger.info("GPU:    No detectada (sin NVIDIA GPU)")
    logger.info("Disco:  %.1f GB libres de %.1f GB", info["disk"]["free_gb"], info["disk"]["total_gb"])
    logger.info("Spark:  %s (pip: %s)", info["software"].get("spark_binary", "?"), info["software"].get("pyspark_pip", "?"))
    logger.info("Kafka:  %s", info["software"].get("kafka", "?"))


if __name__ == "__main__":
    main()
