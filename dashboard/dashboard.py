"""
dashboard/dashboard.py — Dashboard en tiempo real para detección de anomalías.

Conecta directamente a Kafka, acumula trades en buffers por símbolo, calcula
los 4 features cada vez que una ventana de 60s se completa, y aplica el modelo
de Logistic Regression manualmente (sin Spark) usando los coeficientes
extraídos en dashboard/model_params.json.

Uso:
    # 1. Asegurar que Kafka y el producer estén corriendo
    python producer/binance_producer.py &  # en otra terminal

    # 2. Asegurar que existe el JSON con parámetros del modelo
    python dashboard/extract_model_params.py

    # 3. Lanzar el dashboard
    streamlit run dashboard/dashboard.py

El dashboard se abre en http://localhost:8501.
"""

from __future__ import annotations

import json
import math
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh

# ── Configuración ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "crypto-trades"
PARAMS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "model_params.json")

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",
           "XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT"]

# Configuración del modelado por ventana (debe coincidir con el pipeline)
WINDOW_SECONDS = 60   # ventana de 1 minuto
SLIDE_SECONDS = 30    # nueva inferencia cada 30 s
REFRESH_MS = 3000     # auto-refresh del UI cada 3s

# Cuántos trades retener en memoria por símbolo (para gráficas de precio)
BUFFER_SIZE = 5000
# Cuántas predicciones recientes mostrar
MAX_PREDICTIONS_DISPLAY = 50

# ── Page setup ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Crypto Anomaly Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Carga de parámetros del modelo ───────────────────────────────────────────

@st.cache_resource
def load_model_params() -> dict[str, Any]:
    """Carga los coeficientes del modelo desde JSON."""
    if not os.path.isfile(PARAMS_PATH):
        st.error(
            f"No se encontró {PARAMS_PATH}. "
            "Corre primero: python dashboard/extract_model_params.py"
        )
        st.stop()
    with open(PARAMS_PATH) as f:
        return json.load(f)


@st.cache_resource
def get_kafka_consumer() -> KafkaConsumer:
    """Crea (una vez por sesión Streamlit) el consumer de Kafka."""
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=200,
        group_id=f"dashboard-{int(time.time())}",  # único por sesión
    )


# ── Estado persistente entre reruns ──────────────────────────────────────────

if "trades_buffer" not in st.session_state:
    # {symbol: deque[(timestamp_ms, price, quantity)]}
    st.session_state.trades_buffer = defaultdict(lambda: deque(maxlen=BUFFER_SIZE))

if "predictions" not in st.session_state:
    # list[dict] de predicciones recientes
    st.session_state.predictions = deque(maxlen=MAX_PREDICTIONS_DISPLAY)

if "stats" not in st.session_state:
    st.session_state.stats = {
        "total_trades": 0,
        "total_windows": 0,
        "total_anomalies": 0,
        "session_start": datetime.now(timezone.utc).isoformat(),
    }

if "last_window_emit" not in st.session_state:
    # {symbol: timestamp_ms del último emit} para forzar slide de 30s
    st.session_state.last_window_emit = {}


# ── Lógica de modelado ───────────────────────────────────────────────────────

def compute_features(trades: list[tuple]) -> dict[str, float] | None:
    """
    Calcula los 4 features de una ventana a partir de la lista de trades.
    trades: [(ts_ms, price, quantity), ...]
    """
    if len(trades) < 2:
        return None

    trades_sorted = sorted(trades, key=lambda t: t[0])
    prices = np.array([t[1] for t in trades_sorted])
    quantities = np.array([t[2] for t in trades_sorted])

    price_first = prices[0]
    price_last = prices[-1]
    price_min = prices.min()
    price_max = prices.max()
    price_avg = prices.mean()
    price_stddev = prices.std() if len(prices) > 1 else 0.0
    total_volume = quantities.sum()
    trade_count = len(trades)

    pct_return = (price_last - price_first) / price_first if price_first != 0 else 0.0
    volatility = price_stddev / price_avg if price_avg != 0 else 0.0
    volume_intensity = total_volume / trade_count if trade_count > 0 else 0.0
    price_speed = (price_max - price_min) / price_avg if price_avg != 0 else 0.0

    return {
        "pct_return": float(pct_return),
        "volatility": float(volatility),
        "volume_intensity": float(volume_intensity),
        "price_speed": float(price_speed),
        "price_first": float(price_first),
        "price_last": float(price_last),
        "price_min": float(price_min),
        "price_max": float(price_max),
        "price_avg": float(price_avg),
        "trade_count": trade_count,
        "total_volume": float(total_volume),
    }


def predict(features: dict[str, float], params: dict) -> tuple[int, float]:
    """
    Aplica StandardScaler + LogisticRegression manualmente.
    Retorna (etiqueta_binaria, probabilidad_anomalo).
    """
    cols = params["feature_cols"]
    mean = np.array(params["scaler"]["mean"])
    std = np.array(params["scaler"]["std"])
    coefs = np.array(params["lr"]["coefficients"])
    intercept = params["lr"]["intercept"]

    raw = np.array([features[c] for c in cols])
    scaled = (raw - mean) / std

    logit = float(np.dot(coefs, scaled) + intercept)
    # Sigmoid con guard contra overflow
    if logit >= 0:
        prob = 1.0 / (1.0 + math.exp(-logit))
    else:
        e = math.exp(logit)
        prob = e / (1.0 + e)

    label = 1 if prob >= 0.5 else 0
    return label, prob


# ── Lectura de Kafka ─────────────────────────────────────────────────────────

def drain_kafka(consumer: KafkaConsumer, max_records: int = 5000) -> int:
    """
    Vacía el consumer (no-blocking) y agrega trades al buffer por símbolo.
    Retorna número de mensajes consumidos.
    """
    n = 0
    msgs = consumer.poll(timeout_ms=200, max_records=max_records)
    for _tp, records in msgs.items():
        for record in records:
            v = record.value
            symbol = v.get("symbol")
            if symbol not in SYMBOLS:
                continue
            try:
                ts_ms = int(v["timestamp"])
                price = float(v["price"])
                qty = float(v["quantity"])
            except (KeyError, ValueError, TypeError):
                continue
            st.session_state.trades_buffer[symbol].append((ts_ms, price, qty))
            n += 1
    return n


def emit_windows_if_ready(params: dict) -> int:
    """
    Para cada símbolo, si han pasado >= SLIDE_SECONDS desde el último emit
    y hay suficientes trades en la ventana, computa features + predicción
    y agrega a predictions. Retorna número de ventanas emitidas.
    """
    now_ms = int(time.time() * 1000)
    window_ms = WINDOW_SECONDS * 1000
    slide_ms = SLIDE_SECONDS * 1000
    emitted = 0

    for symbol in SYMBOLS:
        buf = st.session_state.trades_buffer[symbol]
        if not buf:
            continue

        last_emit = st.session_state.last_window_emit.get(symbol, 0)
        if now_ms - last_emit < slide_ms:
            continue

        # Ventana = últimos WINDOW_SECONDS
        cutoff = now_ms - window_ms
        window_trades = [t for t in buf if t[0] >= cutoff]
        if len(window_trades) < 3:
            continue  # ventana muy chica

        feats = compute_features(window_trades)
        if feats is None:
            continue

        label, prob = predict(feats, params)
        record = {
            "timestamp": datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat(),
            "ts_ms": now_ms,
            "symbol": symbol,
            "trade_count": feats["trade_count"],
            "price_avg": feats["price_avg"],
            "pct_return": feats["pct_return"],
            "volatility": feats["volatility"],
            "volume_intensity": feats["volume_intensity"],
            "price_speed": feats["price_speed"],
            "label": label,
            "label_str": "anomalo" if label == 1 else "normal",
            "probability": prob,
        }
        st.session_state.predictions.append(record)
        st.session_state.stats["total_windows"] += 1
        if label == 1:
            st.session_state.stats["total_anomalies"] += 1
        st.session_state.last_window_emit[symbol] = now_ms
        emitted += 1

    return emitted


# ── Layout ───────────────────────────────────────────────────────────────────

params = load_model_params()

# Auto-refresh
st_autorefresh(interval=REFRESH_MS, key="auto_refresh")

# Title
st.title("Detección de Anomalías en Criptomonedas — Tiempo Real")
st.caption(
    "Logistic Regression aplicado a ventanas de 60s con slide de 30s. "
    "Inferencia manual con coeficientes extraídos del PipelineModel entrenado en GPU."
)

# Pull data
try:
    consumer = get_kafka_consumer()
    new_msgs = drain_kafka(consumer)
    st.session_state.stats["total_trades"] += new_msgs
    new_windows = emit_windows_if_ready(params)
except Exception as e:
    st.error(f"Error conectando con Kafka: {e}")
    st.stop()

# ── Sidebar ──
with st.sidebar:
    st.subheader("Estado de la sesión")
    st.metric("Trades consumidos", f"{st.session_state.stats['total_trades']:,}")
    st.metric("Ventanas clasificadas", st.session_state.stats["total_windows"])
    anomalies = st.session_state.stats["total_anomalies"]
    total_win = max(st.session_state.stats["total_windows"], 1)
    st.metric(
        "Anomalías detectadas",
        f"{anomalies}",
        delta=f"{100 * anomalies / total_win:.1f}% del total",
    )
    st.metric("Últimos 3s", f"+{new_msgs} trades, +{new_windows} ventanas")

    st.divider()
    st.subheader("Modelo cargado")
    st.code(
        f"intercept = {params['lr']['intercept']:.4f}\n"
        f"pct_return       = {params['lr']['coefficients'][0]:+.4f}\n"
        f"volatility       = {params['lr']['coefficients'][1]:+.4f}\n"
        f"volume_intensity = {params['lr']['coefficients'][2]:+.4f}\n"
        f"price_speed      = {params['lr']['coefficients'][3]:+.4f}",
        language="text",
    )
    st.caption("Threshold default: P(anomalía) ≥ 0.5")

    st.divider()
    st.caption(
        f"Conectado a Kafka {KAFKA_BOOTSTRAP} · tópico `{KAFKA_TOPIC}` · "
        f"refresh cada {REFRESH_MS // 1000}s"
    )

# ── Main panel ──

# Tarjetas con últimos precios por símbolo
st.subheader("Precios actuales por símbolo")
cols = st.columns(4)
for i, sym in enumerate(SYMBOLS):
    buf = st.session_state.trades_buffer.get(sym)
    with cols[i % 4]:
        if buf and len(buf) > 0:
            last_price = buf[-1][1]
            # Calcular variación de los últimos 60s
            now_ms = int(time.time() * 1000)
            older = [t for t in buf if t[0] >= now_ms - 60_000]
            if len(older) >= 2:
                first = older[0][1]
                delta_pct = (last_price - first) / first * 100
                st.metric(sym, f"${last_price:,.4f}", delta=f"{delta_pct:+.3f}% / 60s")
            else:
                st.metric(sym, f"${last_price:,.4f}", delta="—")
        else:
            st.metric(sym, "—")

# Gráfica de precios con anomalías superpuestas
st.subheader("Serie temporal de precios (últimos 5 min)")

# Calcular qué símbolos tienen anomalías recientes (últimos 5 min)
_now = int(time.time() * 1000)
_window_ms = 5 * 60 * 1000
symbols_with_anomalies = sorted({
    p["symbol"] for p in st.session_state.predictions
    if p["label"] == 1 and p["ts_ms"] >= _now - _window_ms
})

# Banner de alerta si hay anomalías recientes
if symbols_with_anomalies:
    st.error(
        f"🚨 **Anomalías detectadas en los últimos 5 min**: "
        f"{', '.join(symbols_with_anomalies)}",
        icon="🚨",
    )

c_a, c_b, c_c = st.columns([2.5, 1.2, 1.2])
with c_a:
    user_selected = st.multiselect(
        "Símbolos a mostrar",
        SYMBOLS,
        default=["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT"],
        key="symbol_selector",
    )
with c_b:
    auto_include = st.checkbox(
        "Auto-incluir anómalos",
        value=True,
        help="Agrega automáticamente al gráfico los símbolos donde el modelo "
             "detectó anomalías en los últimos 5 min (aunque no estén "
             "seleccionados arriba).",
    )
with c_c:
    chart_mode = st.radio(
        "Vista",
        ["Multi-panel", "% cambio"],
        index=0,
        key="chart_mode",
        help="Multi-panel: una gráfica por símbolo con su propia escala. "
             "% cambio: todos los símbolos comparten escala mostrando "
             "el % de cambio desde el inicio de la ventana.",
    )

# Si el toggle está activo, agrega los símbolos con anomalías al set
if auto_include:
    selected = sorted(set(user_selected) | set(symbols_with_anomalies),
                      key=lambda s: SYMBOLS.index(s))
else:
    selected = user_selected

if selected:
    now_ms = int(time.time() * 1000)
    window_ms = 5 * 60 * 1000  # últimos 5 min

    # Recolectar datos válidos
    data_per_sym = {}
    for sym in selected:
        buf = st.session_state.trades_buffer.get(sym)
        if not buf:
            continue
        recent = [t for t in buf if t[0] >= now_ms - window_ms]
        if len(recent) < 2:
            continue
        data_per_sym[sym] = recent

    if not data_per_sym:
        st.info("Aún no hay datos suficientes en los últimos 5 minutos.")

    elif chart_mode.startswith("Multi-panel"):
        # ── Modo small multiples: una fila por símbolo, cada una con su escala ──
        n = len(data_per_sym)
        fig = make_subplots(
            rows=n, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.04,
            subplot_titles=[f"<b>{s}</b>" for s in data_per_sym.keys()],
        )
        palette = ["#3498db", "#9b59b6", "#1abc9c", "#e67e22",
                   "#f1c40f", "#2ecc71", "#e91e63", "#00bcd4"]

        for idx, (sym, recent) in enumerate(data_per_sym.items()):
            row = idx + 1
            ts = [datetime.fromtimestamp(t[0] / 1000) for t in recent]
            prices = [t[1] for t in recent]
            color = palette[idx % len(palette)]

            fig.add_trace(
                go.Scatter(
                    x=ts, y=prices, mode="lines",
                    line=dict(color=color, width=1.5),
                    name=sym, showlegend=False,
                    hovertemplate=f"<b>{sym}</b><br>%{{x|%H:%M:%S}}<br>$%{{y:,.4f}}<extra></extra>",
                ),
                row=row, col=1,
            )

            # Anomalías de este símbolo en la ventana visible
            anom = [p for p in st.session_state.predictions
                    if p["symbol"] == sym and p["label"] == 1
                    and p["ts_ms"] >= now_ms - window_ms]
            if anom:
                anom_x = [datetime.fromtimestamp(p["ts_ms"] / 1000) for p in anom]
                anom_y = [p["price_avg"] for p in anom]
                # Halo blanco grande detrás para que destaque sobre la línea
                fig.add_trace(
                    go.Scatter(
                        x=anom_x, y=anom_y, mode="markers",
                        marker=dict(size=24, symbol="circle", color="rgba(255,255,255,0.85)",
                                    line=dict(color="red", width=2)),
                        showlegend=False, hoverinfo="skip",
                    ),
                    row=row, col=1,
                )
                # X grande encima
                fig.add_trace(
                    go.Scatter(
                        x=anom_x, y=anom_y, mode="markers+text",
                        marker=dict(size=18, symbol="x-thin", color="red",
                                    line=dict(color="red", width=4)),
                        text=[f"P={p['probability']:.2f}" for p in anom],
                        textposition="top center",
                        textfont=dict(color="red", size=10, family="Arial Black"),
                        name="🚨 anomalía" if idx == 0 else None,
                        showlegend=(idx == 0),
                        hovertemplate=(f"<b>{sym} 🚨 ANOMALÍA</b><br>"
                                       "%{x|%H:%M:%S}<br>"
                                       "Precio: $%{y:,.4f}<br>"
                                       "P(anom): %{customdata:.3f}<extra></extra>"),
                        customdata=[p["probability"] for p in anom],
                    ),
                    row=row, col=1,
                )

            fig.update_yaxes(title_text="$", row=row, col=1,
                             tickformat=",.2f" if max(prices) > 10 else ",.4f")
            # Resaltar el subplot si hay anomalía
            if anom:
                fig.layout.annotations[idx].update(
                    text=f"<b>🚨 {sym}</b>",
                    font=dict(color="red", size=14),
                )

        fig.update_layout(
            height=max(220 * n, 320),
            margin=dict(l=20, r=20, t=40, b=20),
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig, use_container_width=True)

    else:
        # ── Modo % cambio normalizado: todos en mismo eje ──
        fig = go.Figure()
        palette = ["#3498db", "#9b59b6", "#1abc9c", "#e67e22",
                   "#f1c40f", "#2ecc71", "#e91e63", "#00bcd4"]

        for idx, (sym, recent) in enumerate(data_per_sym.items()):
            ts = [datetime.fromtimestamp(t[0] / 1000) for t in recent]
            prices = np.array([t[1] for t in recent])
            base = prices[0]
            pct_change = (prices - base) / base * 100  # % cambio desde t0
            color = palette[idx % len(palette)]

            fig.add_trace(go.Scatter(
                x=ts, y=pct_change, mode="lines", name=sym,
                line=dict(color=color, width=1.5),
                hovertemplate=f"<b>{sym}</b><br>%{{x|%H:%M:%S}}<br>%{{y:+.3f}}%<extra></extra>",
            ))

            anom = [p for p in st.session_state.predictions
                    if p["symbol"] == sym and p["label"] == 1
                    and p["ts_ms"] >= now_ms - window_ms]
            if anom:
                anom_x = [datetime.fromtimestamp(p["ts_ms"] / 1000) for p in anom]
                anom_pct = [(p["price_avg"] - base) / base * 100 for p in anom]
                # Halo blanco
                fig.add_trace(go.Scatter(
                    x=anom_x, y=anom_pct, mode="markers",
                    marker=dict(size=24, symbol="circle", color="rgba(255,255,255,0.85)",
                                line=dict(color="red", width=2)),
                    showlegend=False, hoverinfo="skip",
                ))
                # X grande encima
                fig.add_trace(go.Scatter(
                    x=anom_x, y=anom_pct, mode="markers+text",
                    marker=dict(size=18, symbol="x-thin", color="red",
                                line=dict(color="red", width=4)),
                    text=[f"{sym} P={p['probability']:.2f}" for p in anom],
                    textposition="top center",
                    textfont=dict(color="red", size=10, family="Arial Black"),
                    name=f"🚨 {sym}",
                    showlegend=True,
                    hovertemplate=(f"<b>{sym} 🚨 ANOMALÍA</b><br>%{{x|%H:%M:%S}}<br>"
                                   "%{y:+.3f}% vs t₀<br>"
                                   "P(anom): %{customdata:.3f}<extra></extra>"),
                    customdata=[p["probability"] for p in anom],
                ))

        fig.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5)
        fig.update_layout(
            height=420,
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            xaxis_title=None,
            yaxis_title="% cambio desde t₀",
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Selecciona al menos un símbolo para ver la serie de precios.")

# Última ventana clasificada — radar de features + verdicto
st.subheader("Última ventana clasificada")
if st.session_state.predictions:
    last = list(st.session_state.predictions)[-1]
    c1, c2, c3 = st.columns([1, 2, 1])

    with c1:
        verdict = "🚨 ANOMALÍA" if last["label"] == 1 else "✅ Normal"
        color = "red" if last["label"] == 1 else "green"
        st.markdown(
            f"<div style='padding:18px;border-radius:12px;"
            f"background:{'#ffe5e5' if last['label'] == 1 else '#e8f5e9'};"
            f"text-align:center;'>"
            f"<div style='font-size:14px;color:#555;'>{last['symbol']}</div>"
            f"<div style='font-size:28px;font-weight:bold;color:{color};margin:6px 0;'>{verdict}</div>"
            f"<div style='font-size:14px;color:#666;'>P(anomalía) = {last['probability']:.3f}</div>"
            f"<div style='font-size:12px;color:#888;margin-top:8px;'>"
            f"{last['trade_count']} trades · ${last['price_avg']:,.4f}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )

    with c2:
        # Radar de features normalizadas
        feat_names = params["feature_cols"]
        mean = np.array(params["scaler"]["mean"])
        std = np.array(params["scaler"]["std"])
        raw = np.array([last[c] for c in feat_names])
        scaled = (raw - mean) / std
        contributions = scaled * np.array(params["lr"]["coefficients"])

        radar_df = pd.DataFrame({
            "feature": feat_names,
            "valor_escalado": scaled,
            "contribución_logit": contributions,
        })
        fig_radar = go.Figure(go.Bar(
            x=feat_names,
            y=contributions,
            marker_color=["#e74c3c" if c > 0 else "#3498db" for c in contributions],
            text=[f"{c:+.2f}" for c in contributions],
            textposition="outside",
        ))
        fig_radar.update_layout(
            title="Contribución de cada feature al logit",
            yaxis_title="coef × feature_escalado",
            height=280,
            margin=dict(l=20, r=20, t=40, b=20),
            showlegend=False,
        )
        fig_radar.add_hline(y=0, line_dash="dash", line_color="gray")
        st.plotly_chart(fig_radar, use_container_width=True)

    with c3:
        st.markdown("**Features crudos**")
        st.write(f"pct_return: `{last['pct_return']:+.5f}`")
        st.write(f"volatility: `{last['volatility']:.5f}`")
        st.write(f"volume_intensity: `{last['volume_intensity']:.3f}`")
        st.write(f"price_speed: `{last['price_speed']:.5f}`")
else:
    st.info("Aún no se ha completado ninguna ventana. Espera ~30 segundos después del primer trade.")

# Log de predicciones recientes
st.subheader("Historial de predicciones recientes")
if st.session_state.predictions:
    df = pd.DataFrame(list(st.session_state.predictions)[::-1])
    df_display = df[["timestamp", "symbol", "label_str", "probability",
                     "trade_count", "price_avg", "pct_return", "volatility",
                     "volume_intensity", "price_speed"]].copy()
    df_display["timestamp"] = pd.to_datetime(df_display["timestamp"]).dt.strftime("%H:%M:%S")
    # Indicador visible en la columna de etiqueta
    df_display["label_str"] = df_display["label_str"].map({
        "anomalo": "🚨 ANOMALÍA",
        "normal": "✅ normal",
    })
    df_display = df_display.rename(columns={
        "timestamp": "hora",
        "symbol": "símbolo",
        "label_str": "etiqueta",
        "probability": "P(anom)",
        "trade_count": "trades",
        "price_avg": "precio",
    })

    def style_row(row):
        """Toda la fila anómala se pinta con contraste alto y texto blanco."""
        if "ANOMALÍA" in str(row["etiqueta"]):
            return ["background-color: #c62828; color: #ffffff; font-weight: 600"] * len(row)
        return [""] * len(row)

    def style_label_cell(val):
        """La celda de la etiqueta recibe estilo extra para que destaque."""
        if "ANOMALÍA" in str(val):
            return "background-color: #b71c1c; color: #ffffff; font-weight: 700; text-align: center"
        return "color: #2e7d32; font-weight: 500"

    styled = (
        df_display.style
        .apply(style_row, axis=1)
        .map(style_label_cell, subset=["etiqueta"])
        .format({
            "P(anom)": "{:.3f}",
            "precio": "${:,.4f}",
            "pct_return": "{:+.5f}",
            "volatility": "{:.5f}",
            "volume_intensity": "{:.2f}",
            "price_speed": "{:.5f}",
        })
    )

    st.dataframe(
        styled,
        use_container_width=True,
        height=320,
        hide_index=True,
    )

    n_anom = int((df["label"] == 1).sum())
    if n_anom > 0:
        st.caption(f"🚨 {n_anom} anomalía(s) en las últimas {len(df)} predicciones · "
                   f"tasa: {100 * n_anom / len(df):.1f}%")
else:
    st.info("Sin predicciones aún.")
