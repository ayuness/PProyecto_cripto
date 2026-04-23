"""
producer/binance_producer.py — Fase 1: Binance WebSocket → Kafka.

Conecta al WebSocket público de Binance para recibir trades en tiempo real
de múltiples pares de criptomonedas y los publica en el topic 'crypto-trades'
de Kafka.

Binance envía ~50-200 mensajes/segundo dependiendo de la actividad del mercado
y la cantidad de pares suscritos. Con 6+ pares se supera fácilmente el umbral
de 4096 registros/segundo que pide el proyecto en momentos de alta actividad.

Endpoint: wss://stream.binance.com:9443/ws/<stream>
Para múltiples streams se usa el endpoint combinado:
    wss://stream.binance.com:9443/stream?streams=<stream1>/<stream2>/...

Cada stream de trades se llama <symbol>@trade (ej: btcusdt@trade).

Ejecución (Kafka debe estar activo):
    python producer/binance_producer.py

Para detener: Ctrl+C (cierra el WebSocket y el producer de Kafka limpiamente).
"""

import json
import logging
import os
import signal
import sys
import time
from threading import Event
from typing import Any

import websocket
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Agregar el directorio raíz del proyecto al path para imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TRADES

# ── Configuración ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("binance_producer")

# Pares de criptomonedas a monitorear.
# Se eligen pares con alto volumen para garantizar flujo constante.
# btcusdt y ethusdt son los de mayor volumen en Binance;
# los demás aportan diversidad y volumen adicional.
SYMBOLS = [
    "btcusdt",
    "ethusdt",
    "solusdt",
    "bnbusdt",
    "xrpusdt",
    "dogeusdt",
    "adausdt",
    "avaxusdt",
]

# Construir URL del WebSocket combinado.
# El endpoint /stream?streams= permite suscribirse a múltiples streams
# en una sola conexión, lo cual es más eficiente que abrir una por par.
STREAMS = "/".join(f"{s}@trade" for s in SYMBOLS)
WS_URL = f"wss://stream.binance.com:9443/stream?streams={STREAMS}"

# Intervalo de reconexión en segundos.
# Binance desconecta WebSockets cada 24 horas; también puede haber
# cortes de red temporales. Esperamos 5 segundos antes de reconectar
# para no saturar con intentos inmediatos.
RECONNECT_DELAY_SECONDS = 5

# Evento para señalizar cierre limpio desde el handler de señales
_shutdown = Event()


# ── Kafka Producer ───────────────────────────────────────────────────────────

def create_kafka_producer() -> KafkaProducer:
    """
    Crea un KafkaProducer configurado para el proyecto.

    - value_serializer: convierte dict → bytes JSON UTF-8
    - acks='all': espera confirmación de todas las réplicas antes de
      considerar el mensaje enviado. En nuestro caso con replication-factor=1
      es equivalente a acks=1, pero es buena práctica para producción.
    - retries=3: reintenta hasta 3 veces si hay error transitorio de red.
    - linger_ms=10: espera hasta 10ms para agrupar mensajes en un batch,
      mejorando throughput sin agregar latencia perceptible.
    """
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=10,
    )
    logger.info("KafkaProducer conectado a %s", KAFKA_BOOTSTRAP_SERVERS)
    return producer


# ── Procesamiento de mensajes ────────────────────────────────────────────────

class TradeCounter:
    """Contador simple para reportar throughput cada N mensajes."""

    def __init__(self, report_every: int = 500):
        self.count = 0
        self.report_every = report_every
        self.start_time = time.time()
        self.last_report_time = self.start_time

    def increment(self) -> None:
        self.count += 1
        if self.count % self.report_every == 0:
            now = time.time()
            elapsed = now - self.last_report_time
            rate = self.report_every / elapsed if elapsed > 0 else 0
            total_elapsed = now - self.start_time
            total_rate = self.count / total_elapsed if total_elapsed > 0 else 0
            logger.info(
                "Trades enviados: %d | Tasa actual: %.1f msg/s | Tasa promedio: %.1f msg/s",
                self.count, rate, total_rate,
            )
            self.last_report_time = now


def parse_trade(raw_data: dict[str, Any]) -> dict[str, Any] | None:
    """
    Extrae los campos relevantes de un mensaje de trade de Binance.

    El endpoint combinado (/stream?streams=) envuelve el payload en:
    {"stream": "btcusdt@trade", "data": {<trade>}}

    Campos del trade de Binance que usamos:
    - s: symbol (ej: "BTCUSDT")
    - p: price (string, lo convertimos a float)
    - q: quantity (string, lo convertimos a float)
    - T: trade time (epoch ms)
    - m: is buyer maker (bool — true si el comprador hizo la orden limit,
         lo que indica que el trade fue iniciado por un vendedor)
    """
    try:
        trade = raw_data.get("data", raw_data)
        return {
            "symbol": trade["s"],
            "price": float(trade["p"]),
            "quantity": float(trade["q"]),
            "timestamp": trade["T"],
            "is_buyer_maker": trade["m"],
        }
    except (KeyError, ValueError, TypeError) as e:
        logger.warning("Error parseando trade: %s — data: %s", e, raw_data)
        return None


# ── WebSocket callbacks ──────────────────────────────────────────────────────

def create_ws_app(kafka_producer: KafkaProducer, counter: TradeCounter) -> websocket.WebSocketApp:
    """
    Crea el WebSocketApp con los callbacks configurados.

    Se separa en función para facilitar la reconexión: al desconectarse,
    se crea un nuevo WebSocketApp y se vuelve a llamar run_forever().
    """

    def on_open(ws: websocket.WebSocket) -> None:
        logger.info("WebSocket conectado a Binance")
        logger.info("Streams suscritos: %s", [f"{s}@trade" for s in SYMBOLS])

    def on_message(ws: websocket.WebSocket, message: str) -> None:
        """Callback por cada mensaje recibido del WebSocket."""
        if _shutdown.is_set():
            return

        raw = json.loads(message)
        trade = parse_trade(raw)
        if trade is None:
            return

        try:
            kafka_producer.send(KAFKA_TOPIC_TRADES, value=trade)
            counter.increment()
        except KafkaError as e:
            logger.error("Error enviando a Kafka: %s", e)

    def on_error(ws: websocket.WebSocket, error: Exception) -> None:
        logger.error("WebSocket error: %s", error)

    def on_close(ws: websocket.WebSocket, close_status_code: int | None, close_msg: str | None) -> None:
        logger.info(
            "WebSocket cerrado (code=%s, msg=%s)",
            close_status_code, close_msg,
        )

    return websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )


# ── Loop principal con reconexión ────────────────────────────────────────────

def run_with_reconnect(kafka_producer: KafkaProducer) -> None:
    """
    Ejecuta el WebSocket con reconexión automática.

    Binance cierra conexiones WebSocket cada 24 horas. También puede haber
    cortes de red temporales. Este loop reconecta automáticamente hasta
    que se reciba una señal de shutdown (Ctrl+C).
    """
    counter = TradeCounter(report_every=500)

    while not _shutdown.is_set():
        ws_app = create_ws_app(kafka_producer, counter)
        logger.info("Conectando a %s ...", WS_URL[:60] + "...")

        # run_forever() bloquea hasta que el WebSocket se cierra.
        # ping_interval=30: envía ping cada 30s para mantener la conexión viva.
        # ping_timeout=10: si no recibe pong en 10s, considera la conexión muerta.
        ws_app.run_forever(ping_interval=30, ping_timeout=10)

        if _shutdown.is_set():
            break

        logger.info(
            "Reconectando en %d segundos...", RECONNECT_DELAY_SECONDS
        )
        # Usar wait() en vez de sleep() para que el shutdown interrumpa la espera
        _shutdown.wait(timeout=RECONNECT_DELAY_SECONDS)


# ── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("═" * 60)
    logger.info("FASE 1 — Producer: Binance WebSocket → Kafka")
    logger.info("═" * 60)
    logger.info("Topic: %s", KAFKA_TOPIC_TRADES)
    logger.info("Símbolos: %s", [s.upper() for s in SYMBOLS])

    # Handler de señales para cierre limpio con Ctrl+C
    def signal_handler(sig: int, frame: Any) -> None:
        logger.info("Señal de cierre recibida, deteniendo...")
        _shutdown.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    kafka_producer = create_kafka_producer()

    try:
        run_with_reconnect(kafka_producer)
    finally:
        # flush() envía todos los mensajes pendientes en el buffer antes de cerrar
        kafka_producer.flush()
        kafka_producer.close()
        logger.info("Producer cerrado limpiamente. Total de la sesión completado.")


if __name__ == "__main__":
    main()
