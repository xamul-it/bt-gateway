#!/usr/bin/env python3
"""
Replay ZMQ Proxy
================
Riproduce giornate di trading da file di log (output di zmq_logger.py).
Interfaccia ZMQ compatibile con AlpacaSmartProxy, su porte separate.

Comportamento:
  - Porte default: REQ/REP=5557, PUB/SUB=5558 (configurabile)
  - Al primo client → replay parte dall'apertura mercato (14:30 UTC = 09:30 EST)
  - Speed: 1.0 = real-time, 10.0 = 10x più veloce, 0 = massima velocità
  - No clients → reset (prossimo client riparte da tick 0)
  - Late-join symbol → fast-forward al clock corrente
  - HISTORICAL request → risponde con bar dai log file (no Alpaca REST)
  - Fine giornata → broadcast segnale EOD a tutti i client

Uso:
  python replay_zmq_proxy.py --date 2026-01-12 --speed 60

Per usarlo con btmain.py:
  export PROXY_ADDR=tcp://localhost:5557
  export proxy_pub_addr=tcp://localhost:5558
  python btmain.py --mode backtest --provider alpaca --alpaca-mode proxy ...
"""

import sys
import zmq
import msgpack
import threading
import time
import json
import pickle
import logging
import argparse
from datetime import date, datetime, timezone
from pathlib import Path
from collections import defaultdict
import pandas as pd


# ---------------------------------------------------------------------------
# Market hours (UTC)
# ---------------------------------------------------------------------------
MARKET_OPEN_UTC  = (14, 30)   # 09:30 EST = 14:30 UTC
MARKET_CLOSE_UTC = (21, 0)    # 16:00 EST = 21:00 UTC


# ---------------------------------------------------------------------------
# ReplayProxy
# ---------------------------------------------------------------------------
class ReplayProxy:
    DEFAULT_REQ_PORT = 5557
    DEFAULT_PUB_PORT = 5558
    HEARTBEAT_TIMEOUT = 60   # secondi prima di considerare un client morto

    def __init__(self, replay_date: date, speed: float, log_dir: str,
                 req_port: int, pub_port: int, log_level=logging.INFO):
        self.replay_date = replay_date
        # Delta giorni per il remapping: sposta ogni timestamp a oggi
        self._date_delta = date.today() - replay_date
        self.speed       = speed        # 0 = max, else 60/speed secondi per bar
        self.log_dir     = Path(log_dir)
        self.req_port    = req_port
        self.pub_port    = pub_port

        self._setup_logging(log_level)
        self.logger = logging.getLogger('ReplayProxy')

        # ZMQ
        self.context   = zmq.Context()
        self.router    = self.context.socket(zmq.ROUTER)
        self.router.setsockopt(zmq.ROUTER_HANDOVER, 1)
        self.router.setsockopt(zmq.RCVTIMEO, 500)   # 500ms timeout su recv
        self.router.bind(f"tcp://*:{req_port}")
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.bind(f"tcp://*:{pub_port}")

        # Symbol data
        # symbol -> {'day': {ts: bar_dict}, 'all': {ts: bar_dict}}
        self.symbol_bars: dict = {}
        self.all_ticks: list   = []       # sorted datetimes for replay_date
        self.market_open_ts: datetime = None

        # State
        self._state     = 'IDLE'    # IDLE | REPLAYING | EOD_SIGNALED
        self._clock_idx = 0
        self._lock      = threading.RLock()
        self._pub_lock  = threading.Lock()

        # Client management
        self.clients     = {}   # bytes -> set of symbols
        self.subscribers = {}   # symbol -> set of client_ids
        self.heartbeats  = {}   # bytes -> float (last seen)

        # Thread control
        self._start_event = threading.Event()   # set → replay starts
        self._reset_event = threading.Event()   # set → abort current replay
        self._running = True

        self.logger.info(
            f"ReplayProxy: data={replay_date}, speed={speed}x, "
            f"log={log_dir}, REQ={req_port}, PUB={pub_port}"
        )

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    def _setup_logging(self, log_level):
        logger = logging.getLogger('ReplayProxy')
        logger.setLevel(log_level)
        if not logger.handlers:
            h = logging.StreamHandler()
            h.setFormatter(logging.Formatter(
                '%(asctime)s [%(levelname)s] %(message)s', '%H:%M:%S'
            ))
            logger.addHandler(h)

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------
    def _parse_ts(self, s) -> datetime | None:
        if not s:
            return None
        s = str(s)
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def load_log_files(self):
        """
        Carica tutti i *.json dalla log_dir.
        Indicizza per symbol e timestamp.
        Costruisce all_ticks per il replay_date.
        """
        tick_set = set()
        symbols_found = []

        for json_file in sorted(self.log_dir.glob('*.json')):
            symbol = json_file.stem
            bars_all = {}
            bars_day = {}

            with open(json_file) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    ts = self._parse_ts(entry.get('timestamp'))
                    if ts is None:
                        continue

                    bar_dict = {
                        'timestamp':   ts,
                        'open':        float(entry.get('open',        0)),
                        'high':        float(entry.get('high',        0)),
                        'low':         float(entry.get('low',         0)),
                        'close':       float(entry.get('close',       0)),
                        'volume':      float(entry.get('volume',      0)),
                        'trade_count': float(entry.get('trade_count', 0)),
                        'vwap':        float(entry.get('vwap',        0)),
                    }
                    bars_all[ts] = bar_dict

                    if ts.date() == self.replay_date:
                        if ts in bars_day:
                            self.logger.debug(
                                f"CORRECTION {symbol} {ts.strftime('%H:%M')}: "
                                f"Alpaca ha inviato barra corretta (close "
                                f"{bars_day[ts]['close']} → {bar_dict['close']})"
                            )
                        bars_day[ts] = bar_dict
                        tick_set.add(ts)

            if bars_day:
                self.symbol_bars[symbol] = {'day': bars_day, 'all': bars_all}
                symbols_found.append(symbol)
                self.logger.info(f"  {symbol}: {len(bars_day)} bar ({self.replay_date}), "
                                 f"{len(bars_all)} totali")

        self.all_ticks = sorted(tick_set)

        # market_open_ts = 14:30 UTC del replay_date
        mo = datetime(
            self.replay_date.year, self.replay_date.month, self.replay_date.day,
            MARKET_OPEN_UTC[0], MARKET_OPEN_UTC[1], tzinfo=timezone.utc
        )
        self.market_open_ts = mo

        self.logger.info(
            f"Caricati {len(symbols_found)} simboli, "
            f"{len(self.all_ticks)} tick per {self.replay_date}"
        )
        if self.all_ticks:
            self.logger.info(f"  Primo: {self.all_ticks[0].isoformat()}")
            self.logger.info(f"  Ultimo: {self.all_ticks[-1].isoformat()}")
            warmup_count = sum(1 for t in self.all_ticks if t < mo)
            trading_count = len(self.all_ticks) - warmup_count
            self.logger.info(f"  Warmup: {warmup_count} bar | Trading: {trading_count} bar")

        if not self.all_ticks:
            self.logger.warning(f"Nessun bar trovato per {self.replay_date} in {self.log_dir}")

    # ------------------------------------------------------------------
    # Timestamp remapping: replay_date → oggi, orario invariato
    # ------------------------------------------------------------------
    def _remap_ts(self, ts: datetime) -> datetime:
        """Trasla il timestamp di _date_delta giorni: porta la data a oggi
        mantenendo esattamente l'orario e il fuso orario originale.
        Così la strategia vede barre 'di oggi' e i controlli di orario
        (trading_start / trading_end) funzionano correttamente."""
        from datetime import timedelta
        return ts + timedelta(days=self._date_delta.days)

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------
    def _publish_bar(self, symbol: str, bar_dict: dict):
        """Pubblica un bar via PUB. Thread-safe.

        OPT-C/D: flat msgpack message con topic = symbol (per-symbol ZMQ filter).
        Compatibile con il formato del proxy di produzione.
        """
        ts = self._remap_ts(bar_dict['timestamp'])
        msg = {
            'type':        'bar',
            'daily':       False,
            'asset_class': 'stock',
            'symbol':      symbol,
            'ts':          ts.isoformat() if hasattr(ts, 'isoformat') else str(ts),
            'open':        float(bar_dict['open']),
            'high':        float(bar_dict['high']),
            'low':         float(bar_dict['low']),
            'close':       float(bar_dict['close']),
            'volume':      float(bar_dict['volume']),
            'trade_count': float(bar_dict.get('trade_count', 0)),
            'vwap':        float(bar_dict.get('vwap', 0.0)),
            'proxy_ts':    time.time(),
        }
        with self._pub_lock:
            self.publisher.send_multipart([symbol.encode(), msgpack.packb(msg)])

    def _broadcast_eod(self, reason: str = 'day_complete'):
        """Invia segnale EOD broadcast a tutti i subscriber."""
        # OPT-C/D: topic '*' + msgpack flat payload
        msg = {
            'type':        'EOD',
            'daily':       False,
            'asset_class': 'stock',
            'symbol':      '*',
            'ts':          None,
            'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0, 'volume': 0.0,
            'trade_count': 0.0, 'vwap': 0.0,
            'proxy_ts':    time.time(),
            'reason':      reason,
        }
        with self._pub_lock:
            self.publisher.send_multipart([b'*', msgpack.packb(msg)])
        self.logger.info(f"EOD broadcast: {reason}")

    # ------------------------------------------------------------------
    # Fast-forward per late-joining symbols
    # ------------------------------------------------------------------
    def _fast_forward(self, symbol: str):
        """
        Invia rapidamente le bar del simbolo dal tick 0 al clock corrente.
        Chiamato in thread separato dal request handler.
        """
        with self._lock:
            current_idx = self._clock_idx

        sym_data = self.symbol_bars.get(symbol)
        if not sym_data:
            return

        day_bars = sym_data['day']
        sent = 0
        for i in range(current_idx):
            ts = self.all_ticks[i]
            bar = day_bars.get(ts)
            if bar:
                self._publish_bar(symbol, bar)
                sent += 1

        if sent > 0:
            self.logger.info(f"{symbol}: fast-forward {sent} bar (fino al tick {current_idx})")

    # ------------------------------------------------------------------
    # Replay loop (thread principale)
    # ------------------------------------------------------------------
    def _replay_loop(self):
        """Thread: gestisce l'intera logica di replay, stato IDLE ↔ REPLAYING."""
        while self._running:
            # Attendi che almeno un client si connetta
            self.logger.info("IDLE — in attesa di client...")
            self._start_event.wait()
            if not self._running:
                break
            self._start_event.clear()

            with self._lock:
                self._state     = 'REPLAYING'
                self._clock_idx = 0

            self.logger.info(
                f"REPLAY START — {self.replay_date} | {len(self.all_ticks)} tick | "
                f"speed={self.speed}x"
            )

            for i, ts in enumerate(self.all_ticks):
                if self._reset_event.is_set() or not self._running:
                    self.logger.info(f"Replay interrotto al tick {i}")
                    break

                with self._lock:
                    self._clock_idx = i
                    active_symbols = list(self.subscribers.keys())

                is_warmup = ts < self.market_open_ts

                # Pubblica bar per tutti i simboli subscribed a questo timestamp
                for symbol in active_symbols:
                    sym_data = self.symbol_bars.get(symbol)
                    if not sym_data:
                        continue
                    bar = sym_data['day'].get(ts)
                    if bar:
                        self._publish_bar(symbol, bar)
                    elif not is_warmup:
                        self.logger.debug(
                            f"MISSING {symbol} {ts.strftime('%H:%M')}: "
                            f"nessuna barra Alpaca per questo minuto"
                        )

                # Log progresso ogni 60 tick
                if i % 60 == 0:
                    self.logger.debug(f"Tick {i}/{len(self.all_ticks)} [{ts.strftime('%H:%M')}]"
                                      f"{' (warmup)' if is_warmup else ''}")

                # Controllo velocità: solo su bar di trading (non warmup)
                if not is_warmup and self.speed > 0 and not self._reset_event.is_set():
                    time.sleep(60.0 / self.speed)

            # ----- Fine ciclo -----
            if not self._reset_event.is_set() and self._running:
                # Replay completato normalmente
                self.logger.info("Replay completato. Invio EOD e attendo disconnect client.")
                with self._lock:
                    self._state = 'EOD_SIGNALED'
                self._broadcast_eod('day_complete')

                # Attendi max 120s che i client si disconnettano
                for _ in range(120):
                    with self._lock:
                        if not self.clients:
                            break
                    time.sleep(1)

            # ----- Reset -----
            self._reset_event.clear()
            with self._lock:
                self._clock_idx = 0
                self._state     = 'IDLE'
                self.clients.clear()
                self.subscribers.clear()
                self.heartbeats.clear()

            self.logger.info("Reset completato. Pronto per nuova sessione.")

    # ------------------------------------------------------------------
    # Client management
    # ------------------------------------------------------------------
    def _get_client_id_str(self, client_id) -> str:
        if isinstance(client_id, bytes):
            return client_id.hex()[:8]
        return str(client_id)[:8]

    def _handle_subscribe(self, client_id: bytes, symbol: str, timeframe: str = 'intraday'):
        """Registra subscription e avvia fast-forward se replay già in corso."""
        with self._lock:
            is_first_client = not self.clients
            already_subscribed = (
                client_id in self.clients and
                symbol in self.clients.get(client_id, set())
            )
            if already_subscribed:
                return

            self.clients.setdefault(client_id, set()).add(symbol)
            self.subscribers.setdefault(symbol, set()).add(client_id)
            self.heartbeats[client_id] = time.time()
            state     = self._state
            clock_idx = self._clock_idx

        cid = self._get_client_id_str(client_id)
        self.logger.info(f"Subscribe {symbol} da {cid} (state={state})")

        if is_first_client:
            self.logger.info("Primo client: avvio replay")
            self._start_event.set()
        elif state == 'REPLAYING' and clock_idx > 0:
            # Late joiner: fast-forward in background
            threading.Thread(
                target=self._fast_forward,
                args=(symbol,),
                daemon=True
            ).start()

    def _remove_client(self, client_id: bytes):
        """Rimuove client e triggera reset se è l'ultimo."""
        cid = self._get_client_id_str(client_id)
        with self._lock:
            for symbol in list(self.clients.get(client_id, [])):
                if symbol in self.subscribers:
                    self.subscribers[symbol].discard(client_id)
                    if not self.subscribers[symbol]:
                        del self.subscribers[symbol]
            for reg in [self.clients, self.heartbeats]:
                reg.pop(client_id, None)
            no_clients = not self.clients

        self.logger.info(f"Client {cid} rimosso. Clients rimasti: {len(self.clients)}")

        if no_clients:
            self.logger.info("Nessun client: reset")
            self._reset_event.set()

    # ------------------------------------------------------------------
    # Historical request handler
    # ------------------------------------------------------------------
    def _handle_historical(self, msg_str: str) -> dict:
        """
        Gestisce HISTORICAL|asset|symbol|timeframe|from|to|limit.
        Restituisce bar dai log file (pre-replay_date) come DataFrame pickle.
        Ignora il range di date nella request — usa sempre le bar prima del replay_date.
        """
        try:
            parts = msg_str.split('|')
            if len(parts) != 7 or parts[0] != 'HISTORICAL':
                return {'ok': False, 'error': f'Formato non valido: {msg_str[:80]}', 'df': None}

            _, asset_class, symbol, timeframe, _, _, limit_str = parts
            limit = int(limit_str)

            sym_data = self.symbol_bars.get(symbol)
            if not sym_data:
                self.logger.warning(f"HISTORICAL: simbolo {symbol} non trovato")
                return {'ok': True, 'df': pd.DataFrame()}

            # Bar PRIMA del replay_date (per warmup indicatori)
            replay_start = datetime(
                self.replay_date.year, self.replay_date.month, self.replay_date.day,
                tzinfo=timezone.utc
            )
            pre_bars = sorted(
                (b for b in sym_data['all'].values() if b['timestamp'] < replay_start),
                key=lambda x: x['timestamp']
            )
            pre_bars = pre_bars[-limit:]   # Ultime N prima del replay_date

            if not pre_bars:
                self.logger.warning(f"HISTORICAL: nessuna bar pre-{self.replay_date} per {symbol}")
                return {'ok': True, 'df': pd.DataFrame()}

            rows = [{
                'timestamp':   self._remap_ts(b['timestamp']),
                'open':        b['open'],
                'high':        b['high'],
                'low':         b['low'],
                'close':       b['close'],
                'volume':      b['volume'],
                'trade_count': b.get('trade_count', 0),
                'vwap':        b.get('vwap', 0),
                'symbol':      symbol,
            } for b in pre_bars]

            df = pd.DataFrame(rows).set_index('timestamp')
            self.logger.info(
                f"HISTORICAL {symbol}: {len(df)} bar pre-{self.replay_date} (richieste {limit})"
            )
            return {'ok': True, 'df': df}

        except Exception as e:
            self.logger.error(f"Errore HISTORICAL: {e}")
            return {'ok': False, 'error': str(e), 'df': None}

    # ------------------------------------------------------------------
    # Request thread (REQ/REP)
    # ------------------------------------------------------------------
    def _request_thread(self):
        """Thread: gestisce messaggi REQ/REP dei client."""
        self.logger.info(f"Request thread avviato (porta {self.req_port})")
        while self._running:
            try:
                parts = self.router.recv_multipart()
            except zmq.Again:
                continue
            except zmq.ZMQError as e:
                if self._running:
                    self.logger.error(f"ZMQ error: {e}")
                    time.sleep(0.5)
                continue
            except Exception as e:
                if self._running:
                    self.logger.error(f"Request thread error: {e}")
                continue

            if len(parts) == 3:
                client_id, _, message = parts
            elif len(parts) == 2:
                client_id, message = parts
            else:
                continue

            cid = self._get_client_id_str(client_id)
            with self._lock:
                self.heartbeats[client_id] = time.time()

            try:
                msg_str = message.decode('utf-8', errors='replace').strip()
            except Exception:
                msg_str = ''

            # ----- Dispatch -----
            try:
                if msg_str.upper() == 'HEARTBEAT':
                    self.router.send_multipart([client_id, b'', b'PONG'])

                elif msg_str.startswith('MIN:') or msg_str.startswith('SUB:'):
                    symbol = msg_str.split(':', 1)[1]
                    self._handle_subscribe(client_id, symbol, 'intraday')
                    self.router.send_multipart([client_id, b'', b'ACK'])

                elif msg_str.startswith('MIN_CRYPTO:'):
                    symbol = msg_str.split(':', 1)[1]
                    self._handle_subscribe(client_id, symbol, 'intraday')
                    self.router.send_multipart([client_id, b'', b'ACK-CRYPTO'])

                elif msg_str.startswith('DAY:'):
                    # Daily non supportato in replay intraday, ACK comunque
                    self.router.send_multipart([client_id, b'', b'ACK-DAILY'])
                    self.logger.debug(f"DAY subscription ignorata (replay intraday only)")

                elif msg_str.startswith('DAY_CRYPTO:'):
                    self.router.send_multipart([client_id, b'', b'ERR:DAY_CRYPTO_NOT_SUPPORTED'])

                elif msg_str.startswith('HISTORICAL|') or msg_str.startswith('HISTORICAL:'):
                    payload = self._handle_historical(msg_str)
                    self.router.send_multipart([client_id, b'', pickle.dumps(payload)])

                elif msg_str == 'DISCONNECT':
                    self._remove_client(client_id)
                    self.router.send_multipart([client_id, b'', b'BYE'])
                    self.logger.info(f"Disconnessione {cid}")

                else:
                    self.logger.debug(f"Messaggio non riconosciuto da {cid}: {msg_str[:60]}")
                    self.router.send_multipart([client_id, b'', b'UNKNOWN'])

            except zmq.ZMQError as e:
                self.logger.error(f"ZMQ error sending to {cid}: {e}")
            except Exception as e:
                self.logger.error(f"Error handling message from {cid}: {e}")

    # ------------------------------------------------------------------
    # Heartbeat cleanup thread
    # ------------------------------------------------------------------
    def _cleanup_thread(self):
        """Thread: rimuove client con heartbeat scaduto."""
        while self._running:
            time.sleep(15)
            now = time.time()
            with self._lock:
                dead = [
                    cid for cid, ts in list(self.heartbeats.items())
                    if now - ts > self.HEARTBEAT_TIMEOUT
                ]
            for client_id in dead:
                self.logger.warning(
                    f"Client {self._get_client_id_str(client_id)} timeout ({self.HEARTBEAT_TIMEOUT}s)"
                )
                self._remove_client(client_id)

    # ------------------------------------------------------------------
    # Start / Stop
    # ------------------------------------------------------------------
    def start(self):
        """Avvia tutti i thread e blocca fino a Ctrl+C."""
        if not self.all_ticks:
            self.logger.error(f"Nessun dato caricato per {self.replay_date}. "
                              f"Verifica --log-dir e --date.")
            return

        self.logger.info(
            f"Proxy in ascolto su REQ=tcp://*:{self.req_port}, PUB=tcp://*:{self.pub_port}"
        )
        self.logger.info(
            f"Per usarlo con btmain.py:\n"
            f"  export PROXY_ADDR=tcp://localhost:{self.req_port}\n"
            f"  export proxy_pub_addr=tcp://localhost:{self.pub_port}"
        )

        req_t     = threading.Thread(target=self._request_thread, name='req',     daemon=True)
        cleanup_t = threading.Thread(target=self._cleanup_thread, name='cleanup', daemon=True)
        replay_t  = threading.Thread(target=self._replay_loop,    name='replay',  daemon=False)

        req_t.start()
        cleanup_t.start()
        replay_t.start()

        try:
            replay_t.join()
        except KeyboardInterrupt:
            self.logger.info("Ctrl+C: arresto...")
        finally:
            self._running = False
            self._start_event.set()   # sblocca thread in wait
            self._reset_event.set()
            try:
                self.router.close(linger=0)
                self.publisher.close(linger=0)
                self.context.term()
            except Exception:
                pass
            self.logger.info("Proxy fermato.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description='Replay ZMQ Proxy — riproduce giornate di trading da file di log',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  python replay_zmq_proxy.py --date 2026-01-12 --speed 60
  python replay_zmq_proxy.py --date 2026-01-12 --speed 0   # massima velocità
  python replay_zmq_proxy.py --date 2026-01-12 --speed 1   # real-time

Per usarlo con btmain.py:
  export PROXY_ADDR=tcp://localhost:5557
  export proxy_pub_addr=tcp://localhost:5558
  python btmain.py --strat intraday.HMADynamic --ticker LCID \\
    --mode backtest --provider alpaca --alpaca-mode proxy --timeframe minutes
"""
    )
    p.add_argument('--date', required=True,
                   help='Data da replicare (YYYY-MM-DD)')
    p.add_argument('--speed', type=float, default=10.0,
                   help='Velocità: 1=real-time, 60=60x, 0=max (default: 10)')
    p.add_argument('--log-dir', default='out/dump',
                   help='Directory log di zmq_logger (default: out/dump)')
    p.add_argument('--req-port', type=int, default=ReplayProxy.DEFAULT_REQ_PORT,
                   help=f'Porta REQ/REP (default: {ReplayProxy.DEFAULT_REQ_PORT})')
    p.add_argument('--pub-port', type=int, default=ReplayProxy.DEFAULT_PUB_PORT,
                   help=f'Porta PUB/SUB (default: {ReplayProxy.DEFAULT_PUB_PORT})')
    p.add_argument('--log-level', default='INFO',
                   choices=['DEBUG', 'INFO', 'WARNING'],
                   help='Livello di logging (default: INFO)')
    return p.parse_args()


def main():
    args = parse_args()

    try:
        replay_date = date.fromisoformat(args.date)
    except ValueError:
        print(f"Errore: data non valida '{args.date}'. Formato atteso: YYYY-MM-DD")
        sys.exit(1)

    proxy = ReplayProxy(
        replay_date=replay_date,
        speed=args.speed,
        log_dir=args.log_dir,
        req_port=args.req_port,
        pub_port=args.pub_port,
        log_level=getattr(logging, args.log_level),
    )
    proxy.load_log_files()
    proxy.start()


if __name__ == '__main__':
    main()
