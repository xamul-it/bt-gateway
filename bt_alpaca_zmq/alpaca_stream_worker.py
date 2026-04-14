import argparse
import logging
import os
import signal
import threading
import time

import msgpack
import zmq
from alpaca.data.enums import DataFeed
from alpaca.data.live import CryptoDataStream, StockDataStream

from process_monitor import ProcessMonitor

API_KEY = os.environ.get("ALPACA_API_KEY") or os.environ.get("ALPACA_KEY")
SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY") or os.environ.get("ALPACA_SECRET")
FEED = os.environ.get("ALPACA_DATA_FEED", "sip")


class AlpacaStreamWorker:
    def __init__(self, cmd_endpoint, event_endpoint, log_level, monitor_seconds):
        self._setup_logging(log_level)
        self.logger = logging.getLogger("AlpacaStreamWorker")
        self.monitor = ProcessMonitor("STREAM", self.logger, monitor_seconds, self._monitor_metrics)

        self.context = zmq.Context()
        self.command_socket = self.context.socket(zmq.PULL)
        self.command_socket.setsockopt(zmq.RCVTIMEO, 1000)
        self.command_socket.setsockopt(zmq.LINGER, 0)
        self.command_socket.connect(cmd_endpoint)

        self.event_socket = self.context.socket(zmq.PUSH)
        self.event_socket.setsockopt(zmq.IMMEDIATE, 1)
        self.event_socket.setsockopt(zmq.SNDTIMEO, 1000)
        self.event_socket.setsockopt(zmq.LINGER, 0)
        self.event_socket.connect(event_endpoint)

        if FEED == "sip":
            data_feed = DataFeed.SIP
        elif FEED == "iex":
            data_feed = DataFeed.IEX
        else:
            data_feed = DataFeed.SIP

        self.data_feed = data_feed
        self.alpaca_stream = self._build_stock_stream()
        self.alpaca_crypto_stream = self._build_crypto_stream()

        self.running = False
        self.stock_stream_started = False
        self.crypto_stream_started = False
        self.stock_thread = None
        self.crypto_thread = None

        self.active_alpaca_symbols = set()
        self.active_alpaca_crypto_symbols = set()
        self.active_alpaca_daily_symbols = set()

    def _build_stock_stream(self):
        return StockDataStream(API_KEY, SECRET_KEY, feed=self.data_feed)

    def _build_crypto_stream(self):
        return CryptoDataStream(API_KEY, SECRET_KEY)

    def _reset_stock_stream(self):
        if self.stock_stream_started:
            try:
                self.alpaca_stream.stop()
            except Exception as exc:
                self.logger.warning("Reset stock stream fallito: %s", exc)
            if self.stock_thread:
                self.stock_thread.join(timeout=5)
        self.alpaca_stream = self._build_stock_stream()
        self.stock_thread = None
        self.stock_stream_started = False
        self.logger.info("Stream Alpaca stock/daily riportato in idle")

    def _reset_crypto_stream(self):
        if self.crypto_stream_started:
            try:
                self.alpaca_crypto_stream.stop()
            except Exception as exc:
                self.logger.warning("Reset crypto stream fallito: %s", exc)
            if self.crypto_thread:
                self.crypto_thread.join(timeout=5)
        self.alpaca_crypto_stream = self._build_crypto_stream()
        self.crypto_thread = None
        self.crypto_stream_started = False
        self.logger.info("Stream Alpaca crypto riportato in idle")

    def _setup_logging(self, log_level):
        logger = logging.getLogger("AlpacaStreamWorker")
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
            "stock_syms": len(self.active_alpaca_symbols),
            "crypto_syms": len(self.active_alpaca_crypto_symbols),
            "daily_syms": len(self.active_alpaca_daily_symbols),
            "stock_stream_started": int(self.stock_stream_started),
            "crypto_stream_started": int(self.crypto_stream_started),
        }

    async def _alpaca_callback(self, bar):
        await self._on_bar(bar)

    async def _alpaca_daily_callback(self, bar):
        await self._on_bar(bar, daily=True)

    async def _alpaca_crypto_callback(self, bar):
        await self._on_bar(bar, daily=False, asset_class="crypto")

    def _ensure_streams_started(self, asset_class, timeframe):
        if asset_class == "crypto":
            if not self.crypto_stream_started:
                self.crypto_thread = threading.Thread(target=self.alpaca_crypto_stream.run, daemon=True)
                self.crypto_thread.start()
                self.crypto_stream_started = True
                self.logger.info("Stream Alpaca crypto avviato")
            return

        if not self.stock_stream_started:
            self.stock_thread = threading.Thread(target=self.alpaca_stream.run, daemon=True)
            self.stock_thread.start()
            self.stock_stream_started = True
            self.logger.info("Stream Alpaca stock/daily avviato")

    def _normalize_symbol(self, value):
        return str(value).replace("/", "").upper()

    async def _on_bar(self, bar, daily=False, asset_class="stock"):
        try:
            symbol = bar.symbol
            matched_symbol = symbol
            if asset_class == "crypto" and symbol not in self.active_alpaca_crypto_symbols:
                normalized = self._normalize_symbol(symbol)
                for subscribed in self.active_alpaca_crypto_symbols:
                    if self._normalize_symbol(subscribed) == normalized:
                        matched_symbol = subscribed
                        break

            ts = bar.timestamp
            payload = {
                "type": "bar",
                "daily": daily,
                "asset_class": asset_class,
                "symbol": matched_symbol,
                "ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": float(bar.volume),
                "trade_count": float(bar.trade_count) if bar.trade_count is not None else 0.0,
                "vwap": float(bar.vwap) if bar.vwap is not None else 0.0,
                "proxy_ts": time.time(),
            }
            self.event_socket.send_multipart(
                [
                    matched_symbol.encode("utf-8"),
                    msgpack.packb(payload, use_bin_type=True),
                ]
            )
        except Exception as exc:
            self.logger.error("Errore invio barra a control plane: %s", exc, exc_info=True)

    def _subscribe(self, symbol, timeframe, asset_class):
        if timeframe == "daily":
            if symbol in self.active_alpaca_daily_symbols:
                return
            self.alpaca_stream.subscribe_daily_bars(self._alpaca_daily_callback, symbol)
            self.active_alpaca_daily_symbols.add(symbol)
            self._ensure_streams_started(asset_class="stock", timeframe="daily")
            self.logger.info("Worker subscribed daily %s", symbol)
            return

        if asset_class == "crypto":
            if symbol in self.active_alpaca_crypto_symbols:
                return
            self.alpaca_crypto_stream.subscribe_bars(self._alpaca_crypto_callback, symbol)
            self.active_alpaca_crypto_symbols.add(symbol)
            self._ensure_streams_started(asset_class="crypto", timeframe="minute")
            self.logger.info("Worker subscribed crypto %s", symbol)
            return

        if symbol in self.active_alpaca_symbols:
            return
        self.alpaca_stream.subscribe_bars(self._alpaca_callback, symbol)
        self.active_alpaca_symbols.add(symbol)
        self._ensure_streams_started(asset_class="stock", timeframe="minute")
        self.logger.info("Worker subscribed stock %s", symbol)

    def _unsubscribe(self, symbol, timeframe, asset_class):
        try:
            if timeframe == "daily":
                if symbol in self.active_alpaca_daily_symbols:
                    self.alpaca_stream.unsubscribe_daily_bars(symbol)
                    self.active_alpaca_daily_symbols.discard(symbol)
                    self.logger.info("Worker unsubscribed daily %s", symbol)
                    if not self.active_alpaca_daily_symbols and not self.active_alpaca_symbols:
                        self._reset_stock_stream()
                return

            if asset_class == "crypto":
                if symbol in self.active_alpaca_crypto_symbols:
                    self.alpaca_crypto_stream.unsubscribe_bars(symbol)
                    self.active_alpaca_crypto_symbols.discard(symbol)
                    self.logger.info("Worker unsubscribed crypto %s", symbol)
                    if not self.active_alpaca_crypto_symbols:
                        self._reset_crypto_stream()
                return

            if symbol in self.active_alpaca_symbols:
                self.alpaca_stream.unsubscribe_bars(symbol)
                self.active_alpaca_symbols.discard(symbol)
                self.logger.info("Worker unsubscribed stock %s", symbol)
                if not self.active_alpaca_symbols and not self.active_alpaca_daily_symbols:
                    self._reset_stock_stream()
        except Exception as exc:
            self.logger.error("Errore unsubscribe %s (%s/%s): %s", symbol, timeframe, asset_class, exc)

    def _handle_command(self, command):
        action = command.get("action")
        symbol = command.get("symbol")
        timeframe = command.get("timeframe", "minute")
        asset_class = command.get("asset_class", "stock")

        if action == "subscribe":
            self._subscribe(symbol=symbol, timeframe=timeframe, asset_class=asset_class)
            return
        if action == "unsubscribe":
            self._unsubscribe(symbol=symbol, timeframe=timeframe, asset_class=asset_class)
            return
        if action == "shutdown":
            self.logger.info("Comando shutdown ricevuto")
            self.running = False
            return
        self.logger.warning("Comando sconosciuto: %s", command)

    def start(self):
        self.logger.info("Worker Alpaca avviato")
        self.running = True
        self.monitor.start()
        while self.running:
            try:
                packed = self.command_socket.recv()
            except zmq.Again:
                continue
            except zmq.ZMQError as exc:
                if self.running:
                    self.logger.error("Errore socket comando: %s", exc, exc_info=True)
                break
            try:
                command = msgpack.unpackb(packed, raw=False)
                self._handle_command(command)
            except Exception as exc:
                self.logger.error("Errore gestione comando worker: %s", exc, exc_info=True)
        self.stop()

    def stop(self):
        if not self.running:
            self.logger.info("Arresto worker in corso...")
        self.running = False

        for symbol in list(self.active_alpaca_symbols):
            self._unsubscribe(symbol=symbol, timeframe="minute", asset_class="stock")
        for symbol in list(self.active_alpaca_crypto_symbols):
            self._unsubscribe(symbol=symbol, timeframe="minute", asset_class="crypto")
        for symbol in list(self.active_alpaca_daily_symbols):
            self._unsubscribe(symbol=symbol, timeframe="daily", asset_class="stock")

        if self.stock_stream_started:
            self._reset_stock_stream()
        if self.crypto_stream_started:
            self._reset_crypto_stream()

        self.monitor.stop()
        self.command_socket.close(0)
        self.event_socket.close(0)
        self.context.term()
        self.logger.info("Worker Alpaca arrestato")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cmd-endpoint", required=True)
    parser.add_argument("--event-endpoint", required=True)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    parser.add_argument("--monitor-seconds", type=float, default=0.0)
    args = parser.parse_args()

    if not API_KEY or not SECRET_KEY:
        raise RuntimeError(
            "Chiavi Alpaca mancanti nel worker: imposta ALPACA_API_KEY/ALPACA_SECRET_KEY "
            "oppure ALPACA_KEY/ALPACA_SECRET"
        )

    worker = AlpacaStreamWorker(
        cmd_endpoint=args.cmd_endpoint,
        event_endpoint=args.event_endpoint,
        log_level=getattr(logging, args.log_level),
        monitor_seconds=args.monitor_seconds,
    )

    def _handle_stop_signal(signum, frame):
        del frame
        sname = signal.Signals(signum).name
        worker.logger.info("Segnale %s ricevuto: arresto worker", sname)
        worker.running = False

    signal.signal(signal.SIGTERM, _handle_stop_signal)
    signal.signal(signal.SIGINT, _handle_stop_signal)

    worker.start()


if __name__ == "__main__":
    main()
