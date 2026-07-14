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
                "mid": 100.05,
                "spread": 0.1,
                "spread_bps": 9.995,
            }
        )

        self.assertEqual(entry["event_type"], "quote")
        self.assertEqual(entry["timeframe"], "quote")
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
