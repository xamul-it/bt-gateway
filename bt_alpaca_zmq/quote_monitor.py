"""Persistent observation subscriptions, owned by the existing flow logger."""
import json
import os
import re
import time
import uuid
from pathlib import Path
from collections import deque

import zmq


ROOT = Path(__file__).resolve().parents[2]
CONFIG = Path(os.getenv("BT_QUOTE_MONITOR_CONFIG", ROOT / "config-common/quote-monitor.json"))
STATUS = Path(os.getenv("BT_QUOTE_MONITOR_STATUS", ROOT / "out/quote-monitor-status.json"))


class QuoteMonitor:
    def __init__(self, context):
        self.context = context
        self.socket = None
        self.identity = f"quote-monitor-{uuid.uuid4().hex}".encode()
        self.symbols = []
        self.last_tick = 0
        self.last_subscription = 0
        self.latest = {}
        self.error = None
        self.connected = False
        self.feed = None
        self.proxy = {}
        self.last_control = 0
        self.session_id = uuid.uuid4().hex
        self.sequence = 0
        self.events = deque(maxlen=500)
        self.counters = {}

    def command(self, value):
        if self.socket is None:
            self.socket = self.context.socket(zmq.REQ)
            self.socket.setsockopt(zmq.IDENTITY, self.identity)
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.setsockopt(zmq.SNDTIMEO, 500)
            self.socket.setsockopt(zmq.RCVTIMEO, 500)
            self.socket.connect(os.getenv("PROXY_ADDR", "tcp://127.0.0.1:5555"))
        try:
            self.socket.send_string(value)
            frames = self.socket.recv_multipart()
            if not frames or frames[0].startswith(b"ERR"):
                raise ValueError("Richiesta rifiutata dal proxy")
            return frames
        except Exception:
            self.socket.close()
            self.socket = None
            raise

    def observe(self, entry):
        symbol = entry.get("symbol")
        kind = entry.get("event_type", "bar")
        self.sequence += 1
        self.events.append({**entry, "sequence": self.sequence})
        key = f"{symbol}:{kind}"
        self.counters[key] = self.counters.get(key, 0) + 1
        if symbol in self.symbols and entry.get("timeframe") in ("quote", "intraday", "trade"):
            self.latest.setdefault(symbol, {})[kind] = entry

    def tick(self, force=False):
        now = time.time()
        if not force and now - self.last_tick < 1:
            return
        self.last_tick = now
        if force or now - self.last_control >= 5:
            self.last_control = now
            try:
                config = json.loads(CONFIG.read_text()) if CONFIG.exists() else {"symbols": []}
                groups = [config.get("symbols", []), *config.get("sources", {}).values()]
                if any(not isinstance(group, list) for group in groups):
                    raise ValueError("Lista simboli non valida")
                values = [symbol for group in groups for symbol in group]
                if any(not isinstance(s, str) or not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,14}", s) for s in values):
                    raise ValueError("Lista simboli non valida")
                symbols = list(dict.fromkeys(values))
                if len(symbols) > 100:
                    raise ValueError("Massimo 100 simboli complessivi")
                if symbols != self.symbols:
                    if self.symbols:
                        self.command("DISCONNECT")
                    self.symbols = symbols
                    self.last_subscription = 0
                self.command('REGISTER:{"application":"Registratore persistente L1 / Watchtower"}')
                frames = self.command("ADMIN")
                self.proxy = json.loads(frames[-1])
                self.feed = self.proxy.get("feed")
                if now - self.last_subscription >= 15:
                    for symbol in self.symbols:
                        for channel in ("MIN", "QUOTE", "TRADE"):
                            self.command(f"{channel}:{symbol}")
                    self.last_subscription = now
                self.connected = True
                self.error = None
            except Exception as exc:
                self.connected = False
                self.error = str(exc) or type(exc).__name__
                self.last_subscription = 0
        STATUS.parent.mkdir(parents=True, exist_ok=True)
        temporary = STATUS.with_suffix(".tmp")
        temporary.write_text(json.dumps({
            "updated_at": now, "proxy_connected": self.connected,
            "error": self.error, "feed": self.feed, "symbols": self.symbols,
            "latest": {s: self.latest.get(s, {}) for s in self.symbols},
            "proxy": self.proxy, "session_id": self.session_id, "sequence": self.sequence,
            "events": list(self.events), "counters": self.counters,
        }))
        temporary.replace(STATUS)

    def close(self):
        try:
            if self.socket:
                self.command("DISCONNECT")
        except Exception:
            # The proxy also releases this client's subscriptions on heartbeat expiry.
            pass
        finally:
            if self.socket:
                self.socket.close()
