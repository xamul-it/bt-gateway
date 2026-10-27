#!/usr/bin/env python3
"""Replay degli archivi ZMQ su porte separate, senza accesso a broker o Alpaca.

REQ 5557 / PUB 5558, loopback per default. Il primo client avvia il clock.
MIN, QUOTE e TRADE richiedono dati presenti; SUBSCRIBE registra più canali
atomicamente. Speed 1 conserva gli intervalli, 60 li divide per 60, 0=max.
I timestamp originali sono conservati; --remap-today è una scelta esplicita.
"""
import argparse
from collections import deque
from datetime import date, datetime, time as day_time, timedelta, timezone
import json
import logging
import math
import os
from pathlib import Path
import pickle
import re
import signal
import sqlite3
import tempfile
import time
from zoneinfo import ZoneInfo

import msgpack
import pandas as pd
import zmq


ROOT = Path(__file__).resolve().parents[2]
CHANNELS = {'minute', 'quote', 'trade'}


def parse_timestamp(value):
    if not isinstance(value, str):
        raise ValueError('Timestamp mancante')
    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    fraction = re.search(r':\d{2}\.(\d+)', value)
    nanos = int(fraction.group(1)[:9].ljust(9, '0')) if fraction else 0
    return dt, int(dt.replace(microsecond=0).timestamp()) * 1_000_000_000 + nanos


class ReplayProxy:
    DEFAULT_REQ_PORT = 5557
    DEFAULT_PUB_PORT = 5558
    HEARTBEAT_TIMEOUT = 60

    def __init__(self, replay_date, speed, log_dir, req_port=5557, pub_port=5558,
                 log_level=logging.INFO, symbols=None, session='all', status_file=None,
                 remap_today=False, parent_pid=None):
        if not math.isfinite(speed) or speed < 0:
            raise ValueError('Velocità non valida')
        if req_port == pub_port or req_port in (5555, 5556) or pub_port in (5555, 5556):
            raise ValueError('Usa porte separate dal proxy live')
        if session not in ('all', 'rth', 'ah'):
            raise ValueError('Sessione non valida')
        self.replay_date, self.speed, self.log_dir = replay_date, speed, Path(log_dir)
        self.req_port, self.pub_port = req_port, pub_port
        self.symbols = symbols
        self.session, self.status_file = session, Path(status_file) if status_file else None
        self.parent_pid = parent_pid
        self.remap_today = remap_today
        today = datetime.now(ZoneInfo('America/New_York')).date()
        self.date_delta = timedelta(days=(today - replay_date).days) if remap_today else timedelta(0)
        ny = ZoneInfo('America/New_York')
        start_hour, end_hour = {'all': (0, 24), 'rth': (9.5, 16), 'ah': (16, 20)}[session]
        self.session_start = datetime.combine(replay_date, day_time(int(start_hour), int(start_hour % 1 * 60)), ny)
        self.session_end = (datetime.combine(replay_date + timedelta(days=1), day_time(0), ny) if end_hour == 24
                    else datetime.combine(replay_date, day_time(end_hour), ny))
        self.logger = logging.getLogger('ReplayProxy')
        self.logger.setLevel(log_level)
        self.context = zmq.Context()
        self.router = self.context.socket(zmq.ROUTER)
        self.router.setsockopt(zmq.ROUTER_HANDOVER, 1)
        self.router.setsockopt(zmq.LINGER, 0)
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.setsockopt(zmq.LINGER, 0)
        self.router.bind(f'tcp://127.0.0.1:{req_port}')
        self.publisher.bind(f'tcp://127.0.0.1:{pub_port}')
        self.temporary = tempfile.TemporaryDirectory(prefix='bt-replay-')
        self.db = sqlite3.connect(str(Path(self.temporary.name) / 'events.sqlite'))
        self.db.execute('PRAGMA journal_mode=OFF')
        self.db.execute('PRAGMA cache_size=-8192')
        self.db.execute('CREATE TABLE events (id INTEGER PRIMARY KEY, ns INTEGER, symbol TEXT, channel TEXT, payload TEXT)')
        self.db.execute('CREATE TABLE history (id INTEGER PRIMARY KEY, ns INTEGER, symbol TEXT, payload TEXT)')
        self.available = {}
        self.clients, self.names, self.heartbeats = {}, {}, {}
        self.state, self.error, self.loading_symbol = 'PREPARING', None, None
        self.invalid_lines, self.scanned_bytes, self.total_events = 0, 0, 0
        self.published, self.sequence, self.generation = 0, 0, 0
        self.recent = deque(maxlen=500)
        self.running = True
        self.cursor, self.next_event = None, None
        self.clock_origin_ns, self.wall_origin = 0, 0
        self.last_status, self.last_cleanup = 0, 0
        self.preview_requested = False
        self.preview_generation = 0
        self.preview_socket = None
        self.preview_rows = deque(maxlen=500)
        self._status(force=True)

    def _parent_alive(self):
        return self.parent_pid is None or os.getppid() == self.parent_pid

    def load_log_files(self):
        files = [self.log_dir / f'{s}.json' for s in self.symbols] if self.symbols else sorted(self.log_dir.glob('*.json'))
        since_status = 0
        for path in files:
            if path.is_symlink() or not path.is_file():
                raise ValueError(f'Archivio assente o non valido: {path.stem}')
            symbol = path.stem
            if not re.fullmatch(r'[A-Z][A-Z0-9.-]{0,14}', symbol):
                continue
            self.loading_symbol = symbol
            history = deque(maxlen=10000)
            # Freeze the file size: a live recording may continue growing.
            size = path.stat().st_size
            with path.open('rb') as handle:
                while handle.tell() < size:
                    line = handle.readline(1_000_001)
                    if not line:
                        break
                    if handle.tell() > size:
                        break
                    if len(line) > 1_000_000:
                        raise ValueError(f'Riga troppo lunga in {symbol}')
                    self.scanned_bytes += len(line)
                    since_status += 1
                    if since_status >= 2000:
                        if not self._parent_alive() or not self.running:
                            raise RuntimeError('Preparazione interrotta')
                        self.db.commit()
                        self._status(force=True)
                        since_status = 0
                    if not line.endswith(b'\n'):
                        self.invalid_lines += 1
                        continue
                    try:
                        entry = json.loads(line)
                        if not isinstance(entry, dict):
                            raise ValueError()
                        dt, ns = parse_timestamp(entry.get('timestamp'))
                        kind = entry.get('event_type') or entry.get('type') or entry.get('timeframe', 'bar')
                        channel = 'quote' if kind == 'quote' else 'trade' if kind == 'trade' else 'minute'
                        if entry.get('timeframe') == 'daily' or entry.get('daily'):
                            continue
                        if channel == 'minute' and not all(entry.get(k) is not None for k in ('open', 'high', 'low', 'close', 'volume')):
                            raise ValueError()
                        if channel == 'quote' and not all(entry.get(k) is not None for k in ('bid_price', 'ask_price')):
                            raise ValueError()
                        if channel == 'trade' and entry.get('price') is None:
                            raise ValueError()
                        entry['symbol'] = symbol
                        if channel == 'minute' and dt < self.session_start:
                            history.append((ns, entry))
                        if not self.session_start <= dt < self.session_end:
                            continue
                        self.db.execute('INSERT INTO events(ns,symbol,channel,payload) VALUES(?,?,?,?)',
                                        (ns, symbol, channel, json.dumps(entry)))
                        self.available.setdefault(symbol, {}).setdefault(channel, 0)
                        self.available[symbol][channel] += 1
                        self.total_events += 1
                    except (ValueError, TypeError, OverflowError):
                        self.invalid_lines += 1
            self.db.executemany('INSERT INTO history(ns,symbol,payload) VALUES(?,?,?)',
                                ((ns, symbol, json.dumps(entry)) for ns, entry in history))
        self.db.execute('CREATE INDEX event_clock ON events(ns,id)')
        self.db.execute('CREATE INDEX history_clock ON history(symbol,ns,id)')
        self.db.commit()
        self.loading_symbol = None
        if not self.total_events:
            raise ValueError(f'Nessun evento nella sessione {self.session} del {self.replay_date}')
        self.state = 'IDLE'
        self._status(force=True)

    def _status(self, force=False):
        now = time.time()
        if not force and now - self.last_status < 0.5:
            return
        self.last_status = now
        if not self.status_file:
            return
        payload = {'pid': os.getpid(), 'updated_at': now, 'state': self.state, 'error': self.error,
                   'date': self.replay_date.isoformat(), 'session': self.session, 'speed': self.speed,
                   'req_addr': f'tcp://127.0.0.1:{self.req_port}', 'pub_addr': f'tcp://127.0.0.1:{self.pub_port}',
                   'timestamp_mode': 'remap_today' if self.remap_today else 'original',
                   'available': self.available, 'total_events': self.total_events, 'published': self.published,
                   'loading_symbol': self.loading_symbol, 'scanned_bytes': self.scanned_bytes,
                   'invalid_lines': self.invalid_lines, 'generation': self.generation,
                   'clients': [{'id': cid.hex(), 'application': self.names.get(cid, 'Client senza nome'),
                                'subscriptions': [{'symbol': s, 'channel': c} for s, c in sorted(subs)]}
                               for cid, subs in self.clients.items()],
                   'preview': {'active': self.preview_requested, 'generation': self.preview_generation,
                               'received': self.sequence, 'rows': list(self.preview_rows)}}
        self.status_file.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.status_file.with_suffix('.tmp')
        temporary.write_text(json.dumps(payload))
        temporary.replace(self.status_file)

    def _start_clock(self):
        self.cursor = self.db.execute('SELECT ns,symbol,channel,payload FROM events ORDER BY ns,id')
        self.next_event = self.cursor.fetchone()
        self.state = 'REPLAYING'
        self.generation += 1
        self.clock_origin_ns = self.next_event[0]
        # Allow SUB sockets to complete the subscription handshake before the first event.
        self.wall_origin = time.monotonic() + 0.3
        self.published = 0
        self.recent.clear()

    def _register(self, client_id, symbols, channels, application=None):
        if not isinstance(symbols, list) or not symbols or len(symbols) > 20:
            return b'ERR:INVALID_SYMBOLS'
        if not isinstance(channels, list) or not channels or any(c not in CHANNELS for c in channels):
            return b'ERR:UNSUPPORTED_CHANNEL'
        for symbol in symbols:
            if not isinstance(symbol, str) or symbol not in self.available:
                return b'ERR:NO_DATA_FOR_SYMBOL'
            if any(channel not in self.available[symbol] for channel in channels):
                return b'ERR:NO_DATA_FOR_CHANNEL'
        if self.state == 'EOD_SIGNALED':
            return b'ERR:REPLAY_COMPLETE_DISCONNECT_OR_RESET'
        self.clients.setdefault(client_id, set()).update((s, c) for s in symbols for c in channels)
        self.heartbeats[client_id] = time.time()
        if application:
            self.names[client_id] = str(application)[:120]
        if self.state == 'IDLE':
            self._start_clock()
        # Late subscribers join the current clock; do not republish history to existing clients.
        return b'ACK'

    def _remove_client(self, client_id):
        for registry in (self.clients, self.names, self.heartbeats):
            registry.pop(client_id, None)
        if not self.clients:
            self.state = 'IDLE'
            self.next_event = None
            if self.cursor:
                self.cursor.close()
                self.cursor = None

    def _handle_historical(self, message):
        try:
            parts = message.split('|')
            if len(parts) != 7 or parts[0] != 'HISTORICAL':
                raise ValueError('Formato richiesta storico non valido')
            _, asset, symbol, timeframe, _, _, limit = parts
            if asset != 'stock' or timeframe.lower() not in ('minute', 'minutes', '1min'):
                raise ValueError('Replay: storico disponibile solo per barre minute azionarie')
            limit = int(limit)
            if not 1 <= limit <= 10000:
                raise ValueError('Limite storico non valido')
            if symbol not in self.available:
                raise ValueError('Nessun dato per il simbolo')
            rows = []
            history = list(self.db.execute('SELECT payload FROM history WHERE symbol=? ORDER BY ns DESC,id DESC LIMIT ?',
                                           (symbol, limit)))
            for raw, in reversed(history):
                entry = json.loads(raw)
                dt, _ = parse_timestamp(entry['timestamp'])
                rows.append({**entry, 'timestamp': self._remap_datetime(dt), 'symbol': symbol})
            frame = pd.DataFrame(rows)
            if not frame.empty:
                frame = frame.set_index('timestamp')
            return {'ok': True, 'df': frame}
        except (ValueError, TypeError) as exc:
            return {'ok': False, 'error': str(exc), 'df': None}

    def _preview(self, enabled):
        client_id = b'dashboard-replay'
        if not enabled:
            self._remove_client(client_id)
            self.preview_requested = False
            if self.preview_socket:
                self.preview_socket.close(0)
                self.preview_socket = None
            return b'ACK'
        if self.preview_requested:
            return b'ACK'
        self.preview_socket = self.context.socket(zmq.SUB)
        self.preview_socket.setsockopt(zmq.LINGER, 0)
        self.preview_socket.setsockopt(zmq.RCVHWM, 10000)
        self.preview_socket.setsockopt(zmq.SUBSCRIBE, b'')
        self.preview_socket.connect(f'tcp://127.0.0.1:{self.pub_port}')
        self.preview_requested = True
        self.preview_generation += 1
        self.preview_rows.clear()
        self.sequence = 0
        # Register exactly the channels available per symbol, atomically before starting the clock.
        self.clients[client_id] = {(symbol, channel) for symbol, channels in self.available.items() for channel in channels}
        self.names[client_id] = 'Dashboard replay'
        self.heartbeats[client_id] = time.time()
        if self.state == 'IDLE':
            self._start_clock()
        return b'ACK'

    def _dispatch(self, client_id, message):
        if message.startswith('HISTORICAL'):
            return pickle.dumps(self._handle_historical(message))
        if message in ('CONFIG', 'HELLO', 'ADMIN'):
            return json.dumps({'mode': 'replay', 'state': self.state, 'available': self.available,
                               'heartbeat_timeout_s': self.HEARTBEAT_TIMEOUT, 'recommended_heartbeat_s': 15}).encode()
        if message == 'HEARTBEAT':
            if client_id in self.clients:
                self.heartbeats[client_id] = time.time()
            return b'PONG'
        if message == 'DISCONNECT':
            self._remove_client(client_id)
            return b'BYE'
        if message == 'PREVIEW:START':
            if self.state == 'EOD_SIGNALED':
                return b'ERR:REPLAY_COMPLETE_DISCONNECT_OR_RESET'
            return self._preview(True)
        if message == 'PREVIEW:STOP':
            return self._preview(False)
        if message == 'RESET':
            if any(cid != b'dashboard-replay' for cid in self.clients):
                return b'ERR:OTHER_CLIENTS_CONNECTED'
            self._preview(False)
            self.state = 'IDLE'
            return b'ACK'
        if message.startswith('REGISTER:'):
            application = json.loads(message.split(':', 1)[1]).get('application')
            if not isinstance(application, str) or not application or len(application) > 120:
                return b'ERR:INVALID_APPLICATION'
            self.names[client_id] = application
            return b'ACK-REGISTER'
        if message.startswith('SUBSCRIBE:'):
            payload = json.loads(message.split(':', 1)[1])
            return self._register(client_id, payload.get('symbols'), payload.get('channels'), payload.get('application'))
        for prefix, channel in [('MIN:', 'minute'), ('SUB:', 'minute'), ('QUOTE:', 'quote'), ('TRADE:', 'trade')]:
            if message.startswith(prefix):
                return self._register(client_id, [message.split(':', 1)[1]], [channel])
        return b'ERR:UNSUPPORTED_COMMAND'

    def _remap_datetime(self, dt):
        if not self.remap_today:
            return dt
        return (dt.astimezone(ZoneInfo('America/New_York')) + self.date_delta).astimezone(timezone.utc)

    def _publish(self, symbol, channel, entry):
        timestamp = entry['timestamp']
        if self.remap_today:
            dt, _ = parse_timestamp(timestamp)
            fraction = re.search(r':\d{2}(\.\d+)', timestamp)
            timestamp = self._remap_datetime(dt).strftime('%Y-%m-%dT%H:%M:%S') + (fraction.group(1) if fraction else '') + '+00:00'
        payload = {**entry, 'type': 'bar' if channel == 'minute' else channel,
                   'daily': False, 'asset_class': 'stock', 'symbol': symbol, 'ts': timestamp,
                   'timestamp': timestamp, 'recorded_timestamp': entry['timestamp'],
                   'replay': True, 'proxy_ts': time.time()}
        self.publisher.send_multipart([symbol.encode(), msgpack.packb(payload, use_bin_type=True)])
        self.published += 1

    def _advance(self):
        if self.state != 'REPLAYING' or not self.next_event:
            return
        active = set().union(*self.clients.values()) if self.clients else set()
        # Limit each burst to keep commands, heartbeats and the preview consumer responsive.
        for _ in range(100):
            ns, symbol, channel, raw = self.next_event
            due = self.wall_origin + ((ns - self.clock_origin_ns) / 1e9 / self.speed if self.speed else 0)
            if time.monotonic() < due:
                break
            if (symbol, channel) in active:
                self._publish(symbol, channel, json.loads(raw))
            self.next_event = self.cursor.fetchone()
            if not self.next_event:
                self.state = 'EOD_SIGNALED'
                self._eod('day_complete')
                break

    def _eod(self, reason):
        eod = {'type': 'EOD', 'symbol': '*', 'ts': None, 'replay': True, 'reason': reason}
        self.publisher.send_multipart([b'*', msgpack.packb(eod, use_bin_type=True)])

    def _receive_preview(self):
        if not self.preview_socket:
            return
        for _ in range(1000):
            try:
                _, raw = self.preview_socket.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                break
            entry = msgpack.unpackb(raw, raw=False)
            if entry.get('type') == 'EOD':
                continue
            self.sequence += 1
            self.preview_rows.append({**entry, 'event_type': entry['type'], 'sequence': self.sequence})

    def start(self):
        try:
            while self.running and self._parent_alive():
                if self.router.poll(1 if self.state == 'REPLAYING' else 50):
                    frames = self.router.recv_multipart()
                    if len(frames) in (2, 3):
                        client_id, raw = frames[0], frames[-1]
                        try:
                            reply = self._dispatch(client_id, raw.decode().strip())
                        except (ValueError, TypeError, KeyError, AttributeError):
                            reply = b'ERR:INVALID_REQUEST'
                        self.router.send_multipart([client_id, b'', reply])
                self._advance()
                self._receive_preview()
                now = time.time()
                if now - self.last_cleanup > 5:
                    self.last_cleanup = now
                    for cid, heartbeat in list(self.heartbeats.items()):
                        if cid != b'dashboard-replay' and now - heartbeat > self.HEARTBEAT_TIMEOUT:
                            self._remove_client(cid)
                self._status()
        finally:
            self.close()

    def close(self):
        self.running = False
        if self.state == 'REPLAYING' and self.clients:
            self._eod('replay_stop')
        if self.preview_socket:
            self.preview_socket.close(0)
            self.preview_socket = None
        self.preview_requested = False
        self.state = 'STOPPED' if not self.error else 'ERROR'
        self._status(force=True)
        self.router.close(0)
        self.publisher.close(250)
        self.context.term()
        self.db.close()
        self.temporary.cleanup()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', required=True)
    parser.add_argument('--speed', type=float, default=10)
    parser.add_argument('--symbols', help='Ticker separati da virgola; senza opzione legge tutti gli archivi')
    parser.add_argument('--session', choices=['all', 'rth', 'ah'], default='all')
    parser.add_argument('--log-dir', default=str(ROOT / 'out/dump'))
    parser.add_argument('--req-port', type=int, default=5557)
    parser.add_argument('--pub-port', type=int, default=5558)
    parser.add_argument('--status-file')
    parser.add_argument('--parent-pid', type=int)
    parser.add_argument('--remap-today', action='store_true')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING'], default='INFO')
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    symbols = args.symbols.split(',') if args.symbols else None
    if symbols and (len(symbols) > 20 or any(not re.fullmatch(r'[A-Z][A-Z0-9.-]{0,14}', s) for s in symbols)):
        raise SystemExit('Lista simboli non valida')
    proxy = None
    try:
        proxy = ReplayProxy(date.fromisoformat(args.date), args.speed, args.log_dir,
                            args.req_port, args.pub_port, getattr(logging, args.log_level),
                            symbols, args.session, args.status_file, args.remap_today, args.parent_pid)
        def stop(*_):
            proxy.running = False
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        proxy.load_log_files()
        proxy.start()
    except Exception as exc:
        if proxy and proxy.running:
            proxy.error = str(exc)
            proxy.close()
        logging.exception('Replay non avviato')
        raise SystemExit(1)


if __name__ == '__main__':
    main()
