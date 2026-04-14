import argparse
import datetime
import logging
import os
import pickle
import signal as _signal
import subprocess
import sys
import threading
import time
from collections import defaultdict

import msgpack
import zmq
from alpaca.data.enums import DataFeed
from alpaca.data.historical import CryptoHistoricalDataClient, StockHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from process_monitor import ProcessMonitor

API_KEY = os.environ.get("ALPACA_API_KEY") or os.environ.get("ALPACA_KEY")
SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY") or os.environ.get("ALPACA_SECRET")
FEED = os.environ.get("ALPACA_DATA_FEED", "sip")

if not API_KEY or not SECRET_KEY:
    raise RuntimeError(
        "Chiavi Alpaca mancanti nel proxy: imposta ALPACA_API_KEY/ALPACA_SECRET_KEY "
        "oppure ALPACA_KEY/ALPACA_SECRET"
    )


class AlpacaSmartProxy:
    def __init__(self, log_level, router_bind, pub_bind, cmd_endpoint, event_endpoint, monitor_seconds):
        self._setup_logging(log_level)
        self.logger = logging.getLogger("AlpacaProxy")
        self.monitor = ProcessMonitor("CTRL", self.logger, monitor_seconds, self._monitor_metrics)

        self.context = zmq.Context()
        self.router = self.context.socket(zmq.ROUTER)
        self.router.setsockopt(zmq.ROUTER_HANDOVER, 1)
        self.router.setsockopt(zmq.ROUTER_MANDATORY, 1)
        self.router.setsockopt(zmq.IMMEDIATE, 1)
        self.router.setsockopt(zmq.RCVTIMEO, 1000)
        self.router.setsockopt(zmq.LINGER, 0)
        self.router.bind(router_bind)

        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.setsockopt(zmq.LINGER, 0)
        self.publisher.bind(pub_bind)

        self.router_bind = router_bind
        self.pub_bind = pub_bind
        self.worker_cmd_endpoint = cmd_endpoint
        self.worker_event_endpoint = event_endpoint
        self.worker_command_socket = self.context.socket(zmq.PUSH)
        self.worker_command_socket.setsockopt(zmq.IMMEDIATE, 1)
        self.worker_command_socket.setsockopt(zmq.SNDTIMEO, 1000)
        self.worker_command_socket.setsockopt(zmq.LINGER, 0)
        self.worker_command_socket.bind(self.worker_cmd_endpoint)

        self.worker_event_socket = self.context.socket(zmq.PULL)
        self.worker_event_socket.setsockopt(zmq.RCVTIMEO, 1000)
        self.worker_event_socket.setsockopt(zmq.LINGER, 0)
        self.worker_event_socket.bind(self.worker_event_endpoint)

        self.worker_process = None
        self.worker_log_level = logging.getLevelName(log_level)
        self.monitor_seconds = monitor_seconds

        self.heartbeats = {}
        self.client_assets = defaultdict(set)
        self.asset_subscribers = defaultdict(set)
        self.active_alpaca_symbols = set()
        self.active_alpaca_crypto_symbols = set()
        self.client_daily_assets = defaultdict(set)
        self.daily_asset_subscribers = defaultdict(set)
        self.active_alpaca_daily_symbols = set()

        if FEED == "sip":
            data_feed = DataFeed.SIP
        elif FEED == "iex":
            data_feed = DataFeed.IEX
        else:
            data_feed = DataFeed.SIP

        self._stock_hist_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
        self._crypto_hist_client = CryptoHistoricalDataClient(API_KEY, SECRET_KEY)
        self._stock_hist_client._session.verify = False
        self._crypto_hist_client._session.verify = False
        self._data_feed = data_feed

        self.running = False
        self._stopping = False
        self._requested_stop_reason = None
        self.cleanup_thread = None
        self.client_thread = None
        self.worker_event_thread = None
        self.alive_log_interval_s = 300
        self._last_alive_log = 0.0

    def _setup_logging(self, log_level=logging.INFO):
        logger = logging.getLogger("AlpacaProxy")
        logger.setLevel(log_level)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(log_level)
        logger.handlers.clear()
        logger.addHandler(console_handler)

    def _monitor_metrics(self):
        return {
            "clients": len(self.heartbeats),
            "intraday_syms": len(self.active_alpaca_symbols) + len(self.active_alpaca_crypto_symbols),
            "daily_syms": len(self.active_alpaca_daily_symbols),
            "worker_pid": self.worker_process.pid if self.worker_process else "na",
        }

    def _broadcast_eod(self, reason="proxy_stop"):
        eod_msg = {
            "type": "EOD",
            "daily": False,
            "asset_class": "stock",
            "symbol": "*",
            "ts": None,
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "close": 0.0,
            "volume": 0.0,
            "trade_count": 0.0,
            "vwap": 0.0,
            "proxy_ts": time.time(),
            "reason": reason,
        }
        self.publisher.send_multipart([b"*", msgpack.packb(eod_msg, use_bin_type=True)])
        self.logger.info("EOD broadcast inviato (reason=%s)", reason)

    def request_stop(self, reason="proxy_stop"):
        self._requested_stop_reason = reason
        self.running = False

    def _get_client_id_str(self, client_id):
        try:
            if isinstance(client_id, bytes):
                return client_id.hex()[:8]
            if isinstance(client_id, str):
                return client_id[:8]
            return str(client_id)[:8]
        except Exception:
            return "UNKNOWN_ID"

    def _send_worker_command(self, action, symbol=None, timeframe="minute", asset_class="stock"):
        command = {
            "action": action,
            "symbol": symbol,
            "timeframe": timeframe,
            "asset_class": asset_class,
        }
        self.worker_command_socket.send(msgpack.packb(command, use_bin_type=True))

    def _start_worker(self):
        worker_path = os.path.join(os.path.dirname(__file__), "alpaca_stream_worker.py")
        cmd = [
            sys.executable,
            worker_path,
            "--cmd-endpoint",
            self.worker_cmd_endpoint,
            "--event-endpoint",
            self.worker_event_endpoint,
            "--log-level",
            self.worker_log_level,
            "--monitor-seconds",
            str(self.monitor_seconds),
        ]
        self.worker_process = subprocess.Popen(cmd, cwd=os.path.dirname(__file__))
        self.logger.info("Worker Alpaca lanciato pid=%s", self.worker_process.pid)

    def _stop_worker(self):
        if not self.worker_process:
            return
        try:
            self._send_worker_command("shutdown")
        except Exception as exc:
            self.logger.warning("Invio shutdown al worker fallito: %s", exc)
        try:
            self.worker_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.logger.warning("Worker non terminato in tempo, invio SIGTERM")
            self.worker_process.terminate()
            try:
                self.worker_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.logger.warning("Worker non terminato dopo SIGTERM, invio SIGKILL")
                self.worker_process.kill()
                self.worker_process.wait(timeout=5)
        self.logger.info("Worker Alpaca arrestato")

    def _safe_remove_client(self, client_id):
        if client_id not in self.heartbeats:
            self.logger.debug("Tentativo rimozione client inesistente: %s", self._get_client_id_str(client_id))
            return

        client_id_str = self._get_client_id_str(client_id)
        try:
            for symbol in list(self.client_assets.get(client_id, set())):
                self.asset_subscribers[symbol].discard(client_id)
                if not self.asset_subscribers[symbol]:
                    asset_class = "crypto" if symbol in self.active_alpaca_crypto_symbols else "stock"
                    self._send_worker_command("unsubscribe", symbol=symbol, timeframe="minute", asset_class=asset_class)
                    if asset_class == "crypto":
                        self.active_alpaca_crypto_symbols.discard(symbol)
                    else:
                        self.active_alpaca_symbols.discard(symbol)
                    self.logger.info("Rimosso simbolo intraday %s (nessun client)", symbol)

            for symbol in list(self.client_daily_assets.get(client_id, set())):
                self.daily_asset_subscribers[symbol].discard(client_id)
                if not self.daily_asset_subscribers[symbol]:
                    self._send_worker_command("unsubscribe", symbol=symbol, timeframe="daily", asset_class="stock")
                    self.active_alpaca_daily_symbols.discard(symbol)
                    self.logger.info("Rimosso simbolo giornaliero %s (nessun client)", symbol)

            for registry in [self.client_assets, self.client_daily_assets, self.heartbeats]:
                if client_id in registry:
                    del registry[client_id]

            self.logger.info("Client %s rimosso correttamente", client_id_str)
        except Exception as exc:
            self.logger.error("Errore rimozione %s: %s", client_id_str, exc, exc_info=True)

    def _add_subscription(self, client_id, symbol, timeframe=None, asset_class="stock"):
        try:
            is_crypto = asset_class == "crypto"
            if timeframe == "daily":
                if is_crypto:
                    raise ValueError("Sottoscrizione daily crypto non supportata in live stream")
                prev_count = len(self.daily_asset_subscribers.get(symbol, set()))
                registry = self.client_daily_assets
                subscribers = self.daily_asset_subscribers
                active_symbols = self.active_alpaca_daily_symbols
                sub_type = "daily"
            else:
                prev_count = len(self.asset_subscribers.get(symbol, set()))
                registry = self.client_assets
                subscribers = self.asset_subscribers
                active_symbols = self.active_alpaca_crypto_symbols if is_crypto else self.active_alpaca_symbols
                sub_type = "intraday-crypto" if is_crypto else "intraday"

            registry.setdefault(client_id, set()).add(symbol)
            subscribers.setdefault(symbol, set()).add(client_id)
            self.heartbeats[client_id] = time.time()

            self.logger.debug(
                "Client %s sottoscritto a %s (%s)",
                self._get_client_id_str(client_id),
                symbol,
                sub_type,
            )

            if prev_count == 0 and symbol not in active_symbols:
                worker_timeframe = "daily" if timeframe == "daily" else "minute"
                self._send_worker_command(
                    "subscribe",
                    symbol=symbol,
                    timeframe=worker_timeframe,
                    asset_class=asset_class,
                )
                active_symbols.add(symbol)
                self.logger.info("Nuovo simbolo registrato: %s (%s)", symbol, sub_type)
        except Exception as exc:
            self.logger.error("Errore in _add_subscription: %s", exc, exc_info=True)
            raise

    def _cleanup_dead_clients(self):
        while self.running:
            time.sleep(10)
            try:
                now = time.time()
                if now - self._last_alive_log >= self.alive_log_interval_s:
                    total_clients = len(self.heartbeats)
                    total_intraday = len(self.active_alpaca_symbols) + len(self.active_alpaca_crypto_symbols)
                    total_daily = len(self.active_alpaca_daily_symbols)
                    total_symbols = total_intraday + total_daily
                    self.logger.info(
                        "Alive: %s client, %s intraday + %s daily = %s titoli",
                        total_clients,
                        total_intraday,
                        total_daily,
                        total_symbols,
                    )
                    self._last_alive_log = now
                dead_clients = [
                    client_id
                    for client_id, last_hb in list(self.heartbeats.items())
                    if now - last_hb > 30
                ]
                for client_id in dead_clients:
                    self.logger.warning(
                        "Client [%s] inattivo da %ss",
                        self._get_client_id_str(client_id),
                        int(now - self.heartbeats[client_id]),
                    )
                    self._safe_remove_client(client_id)
            except Exception as exc:
                self.logger.error("Errore in cleanup: %s", exc, exc_info=True)

    def _handle_worker_events(self):
        while self.running:
            try:
                parts = self.worker_event_socket.recv_multipart()
            except zmq.Again:
                continue
            except zmq.ZMQError as exc:
                if self.running:
                    self.logger.error("Errore socket eventi worker: %s", exc, exc_info=True)
                break
            if len(parts) != 2:
                self.logger.warning("Evento worker con formato errato: %s", parts)
                continue
            topic, payload = parts
            try:
                self.publisher.send_multipart([topic, payload])
            except Exception as exc:
                self.logger.error("Errore publish verso consumer: %s", exc, exc_info=True)

    def _handle_client_messages(self):
        while self.running:
            try:
                parts = self.router.recv_multipart()
            except zmq.Again:
                continue
            except zmq.ZMQError as exc:
                if self.running:
                    self.logger.error("Errore ZMQ: %s", exc, exc_info=True)
                break

            try:
                if len(parts) == 3:
                    client_id, _, message = parts
                elif len(parts) == 2:
                    client_id, message = parts
                else:
                    self.logger.warning("Messaggio con formato errato: %s", parts)
                    continue

                client_id_str = self._get_client_id_str(client_id)
                self.heartbeats[client_id] = time.time()
                try:
                    msg_str = message.decode("utf-8", errors="replace") if isinstance(message, bytes) else message
                    msg_str = msg_str.strip()
                except Exception:
                    msg_str = f"BINARY_DATA[{len(message)}]"

                if msg_str.upper() == "HEARTBEAT":
                    self.router.send_multipart([client_id, b"", b"PONG"])
                elif msg_str.startswith("MIN:") or message.startswith(b"SUB:"):
                    symbol = msg_str.split(":", 1)[1] if ":" in msg_str else "UNKNOWN"
                    self._add_subscription(client_id, symbol)
                    self.router.send_multipart([client_id, b"", b"ACK"])
                    self.logger.info("Sottoscrizione intraday %s da %s", symbol, client_id_str)
                elif msg_str.startswith("MIN_CRYPTO:"):
                    symbol = msg_str.split(":", 1)[1] if ":" in msg_str else "UNKNOWN"
                    self._add_subscription(client_id, symbol, asset_class="crypto")
                    self.router.send_multipart([client_id, b"", b"ACK-CRYPTO"])
                    self.logger.info("Sottoscrizione intraday crypto %s da %s", symbol, client_id_str)
                elif msg_str.startswith("DAY_CRYPTO:"):
                    self.router.send_multipart([client_id, b"", b"ERR:DAY_CRYPTO_NOT_SUPPORTED"])
                    self.logger.warning("Richiesta DAY_CRYPTO non supportata da %s", client_id_str)
                elif msg_str.startswith("DAY:"):
                    symbol = msg_str.split(":", 1)[1] if ":" in msg_str else "UNKNOWN"
                    self._add_subscription(client_id, symbol, timeframe="daily")
                    self.router.send_multipart([client_id, b"", b"ACK-DAILY"])
                    self.logger.info("Sottoscrizione giornaliera %s da %s", symbol, client_id_str)
                elif msg_str == "DISCONNECT":
                    self._safe_remove_client(client_id)
                    self.router.send_multipart([client_id, b"", b"BYE"])
                    self.logger.info("Disconnessione pulita %s", client_id_str)
                elif msg_str.startswith("HISTORICAL:") or msg_str.startswith("HISTORICAL|"):
                    payload = self._handle_historical_request(msg_str)
                    self.router.send_multipart([client_id, b"", pickle.dumps(payload)])
                else:
                    self.logger.warning("Messaggio non riconosciuto da %s: %s", client_id_str, msg_str[:100])
            except Exception as exc:
                self.logger.error("Errore generico: %s", exc, exc_info=True)

    def _parse_proxy_datetime(self, value):
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.datetime.fromisoformat(value)

    def _to_timeframe(self, timeframe):
        mapping = {
            "minute": TimeFrame.Minute,
            "minutes": TimeFrame.Minute,
            "1min": TimeFrame.Minute,
            "day": TimeFrame.Day,
            "days": TimeFrame.Day,
            "1day": TimeFrame.Day,
            "week": TimeFrame.Week,
            "month": TimeFrame.Month,
        }
        key = timeframe.strip().lower()
        if key not in mapping:
            raise ValueError(f"timeframe non supportato: {timeframe}")
        return mapping[key]

    def _get_historical_data(self, symbol, timeframe, fromdate, todate, limit, asset_class="stock"):
        tf = self._to_timeframe(timeframe)
        start_dt = self._parse_proxy_datetime(fromdate)
        end_dt = self._parse_proxy_datetime(todate)

        if asset_class == "crypto":
            request = CryptoBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf,
                start=start_dt,
                end=end_dt,
                limit=limit,
            )
            barset = self._crypto_hist_client.get_crypto_bars(request)
        else:
            request = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf,
                start=start_dt,
                end=end_dt,
                limit=limit,
                feed=self._data_feed,
            )
            barset = self._stock_hist_client.get_stock_bars(request)

        return barset.df

    def _handle_historical_request(self, msg_str):
        try:
            if "|" in msg_str:
                parts = msg_str.split("|")
                if len(parts) != 7 or parts[0] != "HISTORICAL":
                    raise ValueError(f"Formato HISTORICAL non valido: {msg_str}")
                _, asset_class, symbol, timeframe, fromdate, todate, limit = parts
                limit = int(limit)
            else:
                _, symbol, timeframe, fromdate, todate = msg_str.split(":")
                asset_class = "stock"
                limit = 1000

            df = self._get_historical_data(
                symbol=symbol,
                timeframe=timeframe,
                fromdate=fromdate,
                todate=todate,
                limit=limit,
                asset_class=asset_class,
            )
            return {"ok": True, "df": df}
        except Exception as exc:
            emsg = str(exc)
            if "401" in emsg and "Authorization" in emsg:
                emsg = (
                    "401 Authorization Required su storico Alpaca dal proxy. "
                    "Verifica chiavi del processo proxy (ALPACA_API_KEY/ALPACA_SECRET_KEY "
                    "o ALPACA_KEY/ALPACA_SECRET)."
                )
            self.logger.error("Errore richiesta storico proxy: %s", emsg)
            return {"ok": False, "error": emsg, "df": None}

    def start(self):
        self.logger.info("Avvio proxy Alpaca control plane...")
        self.running = True
        self.monitor.start()
        self._start_worker()
        try:
            self.cleanup_thread = threading.Thread(target=self._cleanup_dead_clients, daemon=True)
            self.cleanup_thread.start()

            self.client_thread = threading.Thread(target=self._handle_client_messages, daemon=True)
            self.client_thread.start()

            self.worker_event_thread = threading.Thread(target=self._handle_worker_events, daemon=True)
            self.worker_event_thread.start()

            self.logger.info(
                "Proxy pronto. Control plane attivo su %s / %s",
                self.router_bind,
                self.pub_bind,
            )
            while self.running:
                if self.worker_process and self.worker_process.poll() is not None:
                    raise RuntimeError(f"Worker Alpaca terminato con codice {self.worker_process.returncode}")
                time.sleep(1)
            if self._requested_stop_reason and not self._stopping:
                self.stop(eod_reason=self._requested_stop_reason)
        except Exception as exc:
            self.logger.critical("Errore durante l'avvio: %s", exc, exc_info=True)
            self.stop(eod_reason="proxy_error")
            raise

    def stop(self, eod_reason="proxy_stop"):
        if self._stopping:
            self.logger.info("stop() già in corso, ignoro chiamata duplicata")
            return
        self._stopping = True
        self.logger.info("Arresto proxy in corso...")
        self.running = False

        try:
            self._broadcast_eod(reason=eod_reason)
            time.sleep(0.5)
        except Exception as exc:
            self.logger.warning("EOD broadcast fallito in stop(): %s", exc)

        for thread in [self.cleanup_thread, self.client_thread, self.worker_event_thread]:
            if thread:
                thread.join(timeout=5)

        try:
            self._stop_worker()
        finally:
            self.monitor.stop()
            self.worker_command_socket.close(0)
            self.worker_event_socket.close(0)
            self.router.close(0)
            self.publisher.close(0)
            self.context.term()
            self.logger.info("Proxy arrestato correttamente")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Livello di logging",
    )
    parser.add_argument(
        "--router-bind",
        default="tcp://*:5555",
        help="Endpoint pubblico ROUTER",
    )
    parser.add_argument(
        "--pub-bind",
        default="tcp://*:5556",
        help="Endpoint pubblico PUB",
    )
    parser.add_argument(
        "--worker-cmd-endpoint",
        default="tcp://127.0.0.1:5560",
        help="Endpoint interno A->B per comandi",
    )
    parser.add_argument(
        "--worker-event-endpoint",
        default="tcp://127.0.0.1:5561",
        help="Endpoint interno B->A per eventi",
    )
    parser.add_argument(
        "--monitor-seconds",
        type=float,
        default=0.0,
        help="Intervallo monitor processi; 0 disabilita",
    )
    args = parser.parse_args()

    log_level = getattr(logging, args.log_level)
    proxy = AlpacaSmartProxy(
        log_level=log_level,
        router_bind=args.router_bind,
        pub_bind=args.pub_bind,
        cmd_endpoint=args.worker_cmd_endpoint,
        event_endpoint=args.worker_event_endpoint,
        monitor_seconds=args.monitor_seconds,
    )

    def _handle_sigusr1(signum, frame):
        del signum, frame
        proxy.logger.info("SIGUSR1: invio EOD ai client e arresto controllato...")
        proxy.request_stop(reason="eod_manual")

    _signal.signal(_signal.SIGUSR1, _handle_sigusr1)

    def _handle_stop_signal(signum, frame):
        del frame
        sname = _signal.Signals(signum).name
        proxy.logger.info("Segnale %s ricevuto: stop proxy con EOD", sname)
        proxy.request_stop(reason="proxy_stop")

    _signal.signal(_signal.SIGTERM, _handle_stop_signal)
    _signal.signal(_signal.SIGINT, _handle_stop_signal)

    try:
        proxy.start()
    except KeyboardInterrupt:
        proxy.logger.info("Ricevuto CTRL+C, arresto...")
        proxy.stop(eod_reason="proxy_stop")
    except Exception as exc:
        proxy.logger.critical("Errore irreversibile: %s", exc, exc_info=True)
        proxy.stop(eod_reason="proxy_error")
