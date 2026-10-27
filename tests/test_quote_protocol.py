import logging
import os
import sys
import unittest
from collections import defaultdict
from pathlib import Path
from unittest.mock import Mock


os.environ.setdefault("ALPACA_API_KEY", "test-key")
os.environ.setdefault("ALPACA_SECRET_KEY", "test-secret")
PACKAGE_DIR = Path(__file__).resolve().parents[1] / "bt_alpaca_zmq"
sys.path.insert(0, str(PACKAGE_DIR))

import alpaca_stream_worker as worker_module  # noqa: E402
import alpaca_zmq_proxy as proxy_module  # noqa: E402
import zmq_logger  # noqa: E402


class QuoteProtocolTest(unittest.TestCase):
    def make_proxy(self):
        proxy = proxy_module.AlpacaSmartProxy.__new__(proxy_module.AlpacaSmartProxy)
        proxy.logger = logging.getLogger("test-proxy")
        proxy.heartbeats = {}
        proxy.client_assets = defaultdict(set)
        proxy.asset_subscribers = defaultdict(set)
        proxy.active_alpaca_symbols = set()
        proxy.active_alpaca_crypto_symbols = set()
        proxy.client_daily_assets = defaultdict(set)
        proxy.daily_asset_subscribers = defaultdict(set)
        proxy.active_alpaca_daily_symbols = set()
        proxy.client_quote_assets = defaultdict(set)
        proxy.quote_asset_subscribers = defaultdict(set)
        proxy.active_alpaca_quote_symbols = set()
        proxy.client_trade_assets = defaultdict(set)
        proxy.trade_asset_subscribers = defaultdict(set)
        proxy.active_alpaca_trade_symbols = set()
        proxy.client_applications = {}
        proxy._send_worker_command = Mock()
        return proxy

    def test_quote_subscription_is_shared_and_released_by_last_client(self):
        proxy = self.make_proxy()
        client_1, client_2 = b"client-1", b"client-2"

        proxy._add_subscription(client_1, "AAPL", timeframe="quote")
        proxy._add_subscription(client_1, "AAPL", timeframe="quote")
        proxy._add_subscription(client_2, "AAPL", timeframe="quote")

        proxy._send_worker_command.assert_called_once_with(
            "subscribe", symbol="AAPL", timeframe="quote", asset_class="stock"
        )
        proxy._safe_remove_client(client_1)
        self.assertEqual(proxy._send_worker_command.call_count, 1)

        proxy._safe_remove_client(client_2)
        self.assertEqual(proxy._send_worker_command.call_count, 2)
        self.assertEqual(proxy._send_worker_command.call_args.args, ("unsubscribe",))
        self.assertEqual(
            proxy._send_worker_command.call_args.kwargs,
            {"symbol": "AAPL", "timeframe": "quote", "asset_class": "stock"},
        )
        self.assertFalse(proxy.active_alpaca_quote_symbols)

    def test_worker_deduplicates_quote_subscription_and_resets_stream(self):
        worker = worker_module.AlpacaStreamWorker.__new__(worker_module.AlpacaStreamWorker)
        worker.logger = logging.getLogger("test-worker")
        worker.alpaca_stream = Mock()
        worker.active_alpaca_quote_symbols = set()
        worker.active_alpaca_trade_symbols = set()
        worker.active_alpaca_symbols = set()
        worker.active_alpaca_daily_symbols = set()
        worker._ensure_streams_started = Mock()
        worker._reset_stock_stream = Mock()

        worker._subscribe("MSFT", "quote", "stock")
        worker._subscribe("MSFT", "quote", "stock")
        worker.alpaca_stream.subscribe_quotes.assert_called_once_with(
            worker._alpaca_quote_callback, "MSFT"
        )

        worker._unsubscribe("MSFT", "quote", "stock")
        worker.alpaca_stream.unsubscribe_quotes.assert_called_once_with("MSFT")
        worker._reset_stock_stream.assert_called_once()

    def test_trade_subscriptions_share_upstream_and_release_last_client(self):
        proxy = self.make_proxy()
        proxy._add_subscription(b"one", "AAPL", timeframe="trade")
        proxy._add_subscription(b"two", "AAPL", timeframe="trade")
        self.assertEqual(proxy._send_worker_command.call_count, 1)
        proxy._safe_remove_client(b"one")
        self.assertEqual(proxy._send_worker_command.call_count, 1)
        proxy._safe_remove_client(b"two")
        self.assertEqual(proxy._send_worker_command.call_args.kwargs["timeframe"], "trade")
        self.assertFalse(proxy.active_alpaca_trade_symbols)

    def test_quote_unsubscribe_keeps_stream_running_for_trades(self):
        worker = worker_module.AlpacaStreamWorker.__new__(worker_module.AlpacaStreamWorker)
        worker.logger = logging.getLogger("test-worker")
        worker.alpaca_stream = Mock()
        worker.active_alpaca_quote_symbols = {"MSFT"}
        worker.active_alpaca_trade_symbols = {"MSFT"}
        worker.active_alpaca_symbols = set()
        worker.active_alpaca_daily_symbols = set()
        worker._reset_stock_stream = Mock()
        worker._unsubscribe("MSFT", "quote", "stock")
        worker._reset_stock_stream.assert_not_called()
        worker._unsubscribe("MSFT", "trade", "stock")
        worker._reset_stock_stream.assert_called_once()

    def test_admin_lists_full_client_identity_and_channels(self):
        from alpaca.data.enums import DataFeed
        proxy = self.make_proxy()
        proxy._data_feed = DataFeed.SIP
        proxy.worker_process = None
        client = b"bt-core:123:AAPL:unique"
        proxy._add_subscription(client, "AAPL", timeframe="trade")
        snapshot = proxy._admin_snapshot()
        self.assertEqual(snapshot["clients"][0]["identity"], client.decode())
        self.assertEqual(snapshot["clients"][0]["application"], "bt-core")
        self.assertEqual(snapshot["clients"][0]["subscriptions"], [{"symbol": "AAPL", "channel": "trade"}])

    def test_trade_callback_and_logger_preserve_real_tick_fields(self):
        import asyncio
        import datetime
        import msgpack
        from types import SimpleNamespace
        from alpaca.data.enums import DataFeed
        worker = worker_module.AlpacaStreamWorker.__new__(worker_module.AlpacaStreamWorker)
        worker.event_socket = Mock()
        worker.data_feed = DataFeed.SIP
        tick = SimpleNamespace(symbol="AAPL", timestamp=datetime.datetime.now(datetime.timezone.utc),
                               price=100.5, size=3, exchange="Q", id=123, conditions=["@"], tape="C")
        asyncio.run(worker._alpaca_trade_callback(tick))
        payload = msgpack.unpackb(worker.event_socket.send_multipart.call_args.args[0][1], raw=False)
        entry = zmq_logger._build_log_entry(payload)
        self.assertEqual(entry["event_type"], "trade")
        self.assertEqual(entry["price"], 100.5)
        self.assertEqual(entry["size"], 3)
        self.assertNotIn("close", entry)

    def test_logger_preserves_quote_execution_fields(self):
        entry = zmq_logger._build_log_entry(
            {
                "type": "quote",
                "symbol": "NVDA",
                "ts": "2026-07-14T12:00:00Z",
                "proxy_ts": 1_789_000_000,
                "bid_price": 100.0,
                "ask_price": 100.1,
                "bid_size": 4,
                "ask_size": 5,
                "bid_exchange": "Q",
                "ask_exchange": "P",
                "conditions": ["R"],
                "tape": "C",
                "feed": "sip",
                "mid": 100.05,
                "spread": 0.1,
                "spread_bps": 9.995,
            }
        )

        self.assertEqual(entry["event_type"], "quote")
        self.assertEqual(entry["timeframe"], "quote")
        self.assertEqual(entry["conditions"], ["R"])
        self.assertEqual(entry["feed"], "sip")
        self.assertEqual(entry["tape"], "C")
        for field in (
            "bid_price",
            "ask_price",
            "bid_size",
            "ask_size",
            "bid_exchange",
            "ask_exchange",
            "mid",
            "spread",
            "spread_bps",
        ):
            self.assertIn(field, entry)


if __name__ == "__main__":
    unittest.main()
