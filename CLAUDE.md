# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Final project for **Arquitectura de Grandes Volúmenes de Datos** (ITAM, Prof. Wilmer Pereira, Spring 2026). Real-time crypto streaming pipeline comparing execution across two hardware architectures.

**Data source:** Binance WebSocket (public, no auth) — real-time trades from 8 crypto pairs (BTC, ETH, SOL, BNB, XRP, DOGE, ADA, AVAX vs USDT).

**Two architectures compared:**
- Machine A: macOS, Apple M1, 8 cores, 8 GB RAM, no GPU
- Machine B: Linux, 16 GB RAM, NVIDIA GPU (RAPIDS Accelerator if compatible)

## Pipeline Flow — Three Sequential Moments

The project follows three strictly sequential phases. No phase starts until the previous one completes.

### 1. First Streaming Pass (Phases 1-2)

```
Binance WebSocket → Kafka Producer → topic 'crypto-trades'
                                          ↓
                              Spark Structured Streaming
                                          ↓
                    Windowed statistics + feature engineering (per symbol)
                                          ↓
                          Accumulate to Parquet until enough data
                                          ↓
                              Stop streaming, train K-Means (batch)
                                          ↓
                      Label each window: "normal" or "anomalo" (p95 threshold)
                                          ↓
                              Save labeled data to Parquet
```

- The producer runs continuously, pushing trades to Kafka.
- Spark reads from Kafka, computes stats and 4 features per time window (1 min windows, 30s slide): `pct_return`, `volatility`, `volume_intensity`, `price_speed`.
- Accumulation stops based on data volume (number of windows), not wall-clock time. The `ACCUMULATION_SECONDS` constant controls the streaming duration but should be calibrated so that enough windows are collected.
- K-Means runs in batch over all accumulated data. Best k chosen by Silhouette Score (k=3,4,5).
- Anomaly detection: points with distance to centroid > 95th percentile are "anomalo".

### 2. Supervised Training (Phase 3, batch, no streaming)

```
Labeled Parquet (from step 1) → VectorAssembler → StandardScaler → LogisticRegression
                                                                          ↓
                                                              Evaluate (accuracy, F1, AUC-ROC)
                                                                          ↓
                                                              Save pipeline model
```

- Single training run, no streaming involved.
- Labels come from K-Means (step 1): binary 0=normal, 1=anomalo.
- Same 4 features as K-Means.
- Model saved as a Spark PipelineModel (includes assembler + scaler + LR).

### 3. Second Streaming Pass (Phase 4, inference only)

```
Binance WebSocket → Kafka (still running) → Spark Structured Streaming
                                                      ↓
                                    Same feature engineering as step 1
                                                      ↓
                              Load trained model → classify each window
                                                      ↓
                                    Write predictions to Parquet
```

- No re-training. The model from step 2 is loaded and applied to new data.
- `startingOffsets=latest` so it only processes new trades, not old ones.
- Predictions include probability column for confidence analysis.

## Test Run Results (430 windows, ~6 min accumulation)

These are **test results only** — the final run will use 20+ minutes of accumulation for more data.

| Metric | Value |
|---|---|
| Windows accumulated | 430 (8 symbols, ~54-63 per symbol) |
| K-Means best k | 3 (Silhouette: 0.8245) |
| Anomalies detected (K-Means) | 24 / 430 (5.6%) |
| Anomaly threshold | 2.2086 (p95 distance) |
| LR Accuracy | 94.57% |
| LR AUC-ROC | 1.0000 |
| LR F1 (weighted) | 91.92% |
| Train/Test split | 338/92 (19/5 anomalies) |
| Inference predictions | 254 windows (232 normal, 22 anomalo) |

**Known issue with test data:** Only 5 anomalies in test set — all predicted as normal by default threshold. AUC-ROC is perfect (probability ranking works) but the 0.5 threshold is too conservative for the 5.6% anomaly rate. With more data (20 min) there will be more anomalies for the model to learn the decision boundary.

**LR Coefficients (test):** price_speed (0.53) and volatility (0.43) are the most influential features, which aligns with the intuition that rapid price changes and high volatility signal anomalous market behavior.

## Final Run Checklist

For the actual submission run (not test):
- [ ] Set `ACCUMULATION_SECONDS = 1200` (20 min) in `streaming/spark_streaming.py`
- [ ] Clean all outputs: `rm -rf output/ models/ checkpoints/`
- [ ] Run all 3 moments sequentially, verify outputs between each
- [ ] Capture Spark UI metrics during each phase (Phase 5)
- [ ] Run the same pipeline on Machine B (Linux + GPU)
- [ ] Compare metrics across architectures
- [ ] Prepare Tableau dashboards (Phase 6)

## Tech Stack

- **Python 3.11.7** (Anaconda at `/opt/anaconda3/bin/python`)
- **PySpark 3.5.4** (binary at `/opt/spark-3.5.4`, same version via pip)
- **Scala 2.12** — Kafka connector: `spark-sql-kafka-0-10_2.12:3.5.4`
- **Java 17** (OpenJDK Homebrew)
- **Apache Kafka 4.2.0** (Homebrew, KRaft mode, `/opt/homebrew/bin/`)
- **Hadoop 3.3.6** at `/opt/hadoop-3.3.6` — **NOT running HDFS**. The `core-site.xml` points to `hdfs://localhost:9000` but we override with `spark.hadoop.fs.defaultFS=file:///` in Spark config to use local filesystem.
- Spark UI at `http://localhost:4040`
- Tableau (student license) for visualization

## Project Structure

```
proyecto_final/
├── config/
│   └── spark_config.py               # Centralized Spark config (versions, paths, builder)
├── hardware_info.py                   # Phase 0: capture hardware specs → JSON
├── verify_environment.py              # Phase 0: verify Spark ↔ Kafka connectivity
├── producer/
│   └── binance_producer.py            # Phase 1: Binance WebSocket → Kafka
├── streaming/
│   ├── spark_streaming.py             # Phase 2: stats + K-Means (first streaming pass)
│   └── spark_inference.py             # Phase 4: inference with LR (second streaming pass)
├── training/
│   └── train_model.py                 # Phase 3: train Logistic Regression (batch)
├── metrics/
│   └── capture_metrics.py             # Phase 5: Spark UI metrics
├── visualization/
│   └── prepare_tableau_data.py        # Phase 6: data for Tableau (pending)
├── models/
│   ├── kmeans_model/                  # Saved K-Means model
│   └── logistic_model/                # Saved LR pipeline (assembler+scaler+LR)
├── output/
│   ├── statistics/                    # Windowed stats (for Tableau)
│   ├── labeled_data/                  # K-Means labeled data (for Phase 3)
│   ├── predictions/                   # Real-time predictions (Phase 4)
│   └── metrics/                       # Hardware info + Spark UI metrics
├── checkpoints/                       # Structured Streaming checkpoints
└── requirements.txt
```

## Running the Pipeline

```bash
# Phase 0 — Environment
python hardware_info.py
# Start Kafka in a separate terminal:
#   kafka-server-start /opt/homebrew/etc/kafka/server.properties
# Create topic (once):
#   kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
#       --topic crypto-trades --partitions 1 --replication-factor 1
python verify_environment.py

# Phase 1 — Producer (leave running in its own terminal)
python producer/binance_producer.py

# Phase 2 — First streaming pass (stats + K-Means)
python streaming/spark_streaming.py

# Phase 3 — Train supervised model (batch)
python training/train_model.py

# Phase 4 — Second streaming pass (inference)
python streaming/spark_inference.py

# Phase 5 — Capture metrics (run WHILE Phase 2 or 4 is active)
python metrics/capture_metrics.py --arch mac-m1
```

## Code Conventions

- All code must be fully executable — no TODOs or stubs
- Type hints on all functions
- `logging` module (no bare prints)
- Comments explain *why*, not *what* — especially for Spark config, ML choices, window/watermark values
- Spanish for comments and reports in the final deliverable

## Important Config Notes

- **Hadoop override:** `spark.hadoop.fs.defaultFS=file:///` is mandatory. Without it, Spark uses the Hadoop config at `$HADOOP_CONF_DIR` which points to HDFS and causes `RPC response has invalid length` errors.
- **Shuffle partitions:** Set to 4 (default 200 is too many for single-node local mode).
- **Driver memory:** 2g (appropriate for 8 GB total RAM on Machine A).
- **Kafka connector:** Auto-downloaded from Maven via `spark.jars.packages`. Cached in `~/.ivy2/`.
- **Checkpoints:** Must use `file://` prefix for local filesystem paths.

## Architecture Comparison Metrics (at least 2 required)

Job execution time, shuffle time, I/O per stage, Scheduler Delay, Executor Run Time, GC Time, Spill (Memory/Disk), streaming rate (input/processing rate, batch duration, pending batches), environment config.
