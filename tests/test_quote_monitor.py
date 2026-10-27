import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'bt_alpaca_zmq'))
import quote_monitor as module


class MonitorTest(unittest.TestCase):
    def test_persistent_subscriptions_renew_and_remove_without_other_clients(self):
        with tempfile.TemporaryDirectory() as directory:
            config, status = Path(directory) / 'config.json', Path(directory) / 'status.json'
            config.write_text(json.dumps({'symbols': ['AAPL']}))
            monitor = module.QuoteMonitor(Mock())
            monitor.command = Mock(return_value=[b'CONFIG', b'{"feed":"sip"}'])
            with patch.object(module, 'CONFIG', config), patch.object(module, 'STATUS', status):
                monitor.tick(force=True)
                self.assertEqual([c.args[0] for c in monitor.command.call_args_list], ['REGISTER:{"application":"Registratore persistente L1 / Watchtower"}', 'ADMIN', 'MIN:AAPL', 'QUOTE:AAPL', 'TRADE:AAPL'])
                monitor.command.reset_mock()
                monitor.last_subscription = 0
                monitor.tick(force=True)
                self.assertIn('QUOTE:AAPL', [c.args[0] for c in monitor.command.call_args_list])
                monitor.observe({'symbol': 'AAPL', 'timeframe': 'quote', 'event_type': 'quote', 'bid_price': 100})
                monitor.observe({'symbol': 'AAPL', 'timeframe': 'daily', 'event_type': 'bar'})
                self.assertNotIn('bar', monitor.latest['AAPL'])
                config.write_text(json.dumps({'symbols': ['MSFT']}))
                monitor.command.reset_mock()
                monitor.tick(force=True)
                self.assertEqual(monitor.command.call_args_list[0].args[0], 'DISCONNECT')
                self.assertEqual(json.loads(status.read_text())['symbols'], ['MSFT'])
                self.assertNotIn('AAPL', json.loads(status.read_text())['latest'])

    def test_ack_failure_is_visible_and_forces_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            config, status = Path(directory) / 'config.json', Path(directory) / 'status.json'
            config.write_text('{"symbols": ["AAPL"]}')
            monitor = module.QuoteMonitor(Mock())
            monitor.command = Mock(side_effect=TimeoutError('Proxy timeout'))
            with patch.object(module, 'CONFIG', config), patch.object(module, 'STATUS', status):
                monitor.tick(force=True)
                data = json.loads(status.read_text())
                self.assertFalse(data['proxy_connected'])
                self.assertEqual(data['error'], 'Proxy timeout')
                self.assertEqual(monitor.last_subscription, 0)
