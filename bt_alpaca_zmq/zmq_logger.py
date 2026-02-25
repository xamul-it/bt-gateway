import zmq
import logging
import os
from datetime import datetime, timezone
import pickle  # Necessario se il server usa send_pyobj con pickle
import json
from pathlib import Path
from decimal import Decimal
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


def _serialize_value(value: Any) -> Any:
    """Rende serializzabile qualsiasi valore rilevante (datetime, Decimal, Enum, ecc.)."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    if hasattr(value, "value"):
        raw = getattr(value, "value")
        if isinstance(raw, (int, float, str, bool)):
            return raw
    if isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_value(v) for v in value]
    return str(value)


def _format_epoch(epoch: Any) -> Any:
    """Converte un timestamp epoch (float/int) in ISO8601 UTC se possibile."""
    if epoch is None:
        return None
    try:
        return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        return _serialize_value(epoch)


def _normalize_bar(bar: Any) -> Dict[str, Any]:
    """Converte il payload della barra in un dict serializzabile e confrontabile."""
    if bar is None:
        return {}
    raw: Dict[str, Any]
    if hasattr(bar, "model_dump"):
        raw = bar.model_dump()
    elif isinstance(bar, dict):
        raw = dict(bar)
    else:
        try:
            raw = dict(bar)  # type: ignore[arg-type]
        except Exception:
            return {"raw": _serialize_value(bar)}
    return {k: _serialize_value(v) for k, v in raw.items()}


def _write_entry(symbol: str, entry: Dict[str, Any]) -> None:
    """Scrive una riga JSON per simbolo mantenendo un file per feed."""
    # I simboli crypto includono "/" (es. BTC/USD): normalizza per usarli come nome file.
    safe_symbol = (symbol or "UNKNOWN").replace("/", "_")
    file_path = OUTPUT_DIR / f"{safe_symbol}.json"
    with file_path.open("a", encoding="utf-8") as f:
        json.dump(entry, f, ensure_ascii=False)
        f.write("\n")


def _build_log_entry(message: Dict[str, Any]) -> Dict[str, Any]:
    """Prepara una riga pronta per essere salvata."""
    recv_ts = datetime.now(timezone.utc).isoformat()
    bar_payload = _normalize_bar(message.get("data"))
    symbol = message.get("symbol") or bar_payload.get("symbol") or "UNKNOWN"
    # Evita duplicazioni tra entry e payload
    bar_payload.pop("symbol", None)

    entry: Dict[str, Any] = {
        "recv_ts": recv_ts,
        "proxy_ts": _format_epoch(message.get("timestamp")),
        "timeframe": "daily" if message.get("daily") else "intraday",
        "symbol": symbol,
    }
    # Porta le colonne OHLC sullo stesso livello per facilitare il confronto
    entry.update(bar_payload)
    return entry


def setup_zmq_subscriber():
    """Configura un subscriber ZMQ per messaggi serializzati con send_pyobj."""
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.connect(ZMQ_SERVER_ADDR)
    subscriber.setsockopt_string(zmq.SUBSCRIBE, "")  # Sottoscrizione a tutti i messaggi
    
    logging.info(f"ZMQ Logger avviato. In ascolto su {ZMQ_SERVER_ADDR}...")

    try:
        while True:
            try:
                # Ricevi il messaggio serializzato
                message = subscriber.recv_pyobj()
                if not isinstance(message, dict):
                    logging.warning("Messaggio non riconosciuto: %s", message)
                    continue

                entry = _build_log_entry(message)
                _write_entry(entry["symbol"], entry)
            
            except (pickle.PickleError, zmq.ZMQError) as e:
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
