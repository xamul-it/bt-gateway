import zmq
import msgpack
import logging
import os
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict

# Configurazione ZeroMQ:
# Priorita':
# 1) proxy_pub_addr (compatibile con env/zmq)
# 2) ZMQ_SERVER_ADDR (override esplicito)
# 3) default locale
ZMQ_SERVER_ADDR = (
    os.environ.get("proxy_pub_addr")
    or os.environ.get("ZMQ_SERVER_ADDR")
    or "tcp://127.0.0.1:5556"
)

# Configurazione logging
OUTPUT_DIR = Path("out") / "dump"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUTPUT_DIR / "zmq_messages.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()  # Opzionale: stampa anche a terminale
    ]
)


def _format_epoch(epoch: Any) -> Any:
    """Converte un timestamp epoch (float/int) in ISO8601 UTC se possibile."""
    if epoch is None:
        return None
    try:
        return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        return str(epoch)


def _write_entry(symbol: str, entry: Dict[str, Any]) -> None:
    """Scrive una riga JSON per simbolo mantenendo un file per feed."""
    # I simboli crypto includono "/" (es. BTC/USD): normalizza per usarli come nome file.
    safe_symbol = (symbol or "UNKNOWN").replace("/", "_")
    file_path = OUTPUT_DIR / f"{safe_symbol}.json"
    with file_path.open("a", encoding="utf-8") as f:
        json.dump(entry, f, ensure_ascii=False)
        f.write("\n")


def _build_log_entry(message: Dict[str, Any]) -> Dict[str, Any]:
    """Prepara una riga pronta per essere salvata (formato flat msgpack)."""
    recv_ts = datetime.now(timezone.utc).isoformat()
    symbol = message.get("symbol") or "UNKNOWN"
    return {
        "recv_ts":     recv_ts,
        "proxy_ts":    _format_epoch(message.get("proxy_ts")),
        "timeframe":   "daily" if message.get("daily") else "intraday",
        "symbol":      symbol,
        "timestamp":   message.get("ts"),
        "open":        message.get("open"),
        "high":        message.get("high"),
        "low":         message.get("low"),
        "close":       message.get("close"),
        "volume":      message.get("volume"),
        "trade_count": message.get("trade_count"),
        "vwap":        message.get("vwap"),
    }


def setup_zmq_subscriber():
    """Configura un subscriber ZMQ per messaggi msgpack+multipart."""
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.connect(ZMQ_SERVER_ADDR)
    subscriber.setsockopt_string(zmq.SUBSCRIBE, "")  # riceve tutti i topic (inclusi EOD '*')

    logging.info(f"ZMQ Logger avviato. In ascolto su {ZMQ_SERVER_ADDR}...")

    try:
        while True:
            try:
                frames = subscriber.recv_multipart()
                if len(frames) != 2:
                    logging.warning("Frame count inatteso: %d", len(frames))
                    continue
                _, payload = frames
                message = msgpack.unpackb(payload, raw=False)

                if not isinstance(message, dict):
                    logging.warning("Messaggio non riconosciuto: %s", message)
                    continue

                # Segnale EOD: logga e ignora (non scrivere su file dati)
                if message.get('type') == 'EOD':
                    logging.info("EOD signal (reason=%s)", message.get('reason'))
                    continue

                entry = _build_log_entry(message)
                _write_entry(entry["symbol"], entry)

            except (msgpack.UnpackException, zmq.ZMQError) as e:
                logging.exception(e)
                logging.error(f"Errore deserializzazione: {e}")
            except Exception as e:
                logging.exception(e)
                logging.error(f"Errore generico: {e}")

    except KeyboardInterrupt:
        logging.info("Logger interrotto manualmente.")
    finally:
        subscriber.close()
        context.term()

if __name__ == "__main__":
    setup_zmq_subscriber()
