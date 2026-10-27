import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

import msgpack
import zmq

PACKAGE = Path(__file__).resolve().parents[1] / 'bt_alpaca_zmq'
sys.path.insert(0, str(PACKAGE))
from replay_zmq_proxy import ReplayProxy, parse_timestamp


def ports():
    sockets = [socket.socket(), socket.socket()]
    try:
        for item in sockets:
            item.bind(('127.0.0.1', 0))
        return [item.getsockname()[1] for item in sockets]
    finally:
        for item in sockets:
            item.close()


def entries():
    return [
        {'timestamp': '2026-07-13T19:00:00Z', 'close': 9, 'open': 9, 'high': 9, 'low': 9, 'volume': 10},
        {'timestamp': '2026-07-14T13:30:00.000000001Z', 'event_type': 'quote', 'bid_price': 10, 'ask_price': 11},
        {'timestamp': '2026-07-14T13:30:00.000000001Z', 'event_type': 'trade', 'price': 10.5, 'size': 2},
        {'timestamp': '2026-07-14T13:30:00.000000003Z', 'event_type': 'quote', 'bid_price': 10.1, 'ask_price': 11.1},
        {'timestamp': '2026-07-14T13:30:01Z', 'event_type': 'bar', 'close': 10.2, 'open': 10, 'high': 11, 'low': 10, 'volume': 10},
        {'timestamp': '2026-07-14T20:00:00Z', 'event_type': 'trade', 'price': 12, 'size': 1},
        {'timestamp': '2026-07-15T00:00:00Z', 'event_type': 'trade', 'price': 13, 'size': 1},
    ]


class ReplayTest(unittest.TestCase):
    def test_index_keeps_nanoseconds_duplicate_timestamps_and_channels(self):
        from datetime import date
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'ABC.json'
            path.write_text(''.join(json.dumps(row) + '\n' for row in entries()))
            proxy = ReplayProxy(date(2026, 7, 14), 60, folder, *ports(), symbols=['ABC'], session='rth')
            try:
                proxy.load_log_files()
                self.assertEqual(proxy.total_events, 4)
                self.assertEqual(proxy.available['ABC'], {'quote': 2, 'trade': 1, 'minute': 1})
                times = [row[0] for row in proxy.db.execute('SELECT ns FROM events ORDER BY ns,id')]
                self.assertEqual(times[0], times[1])
                self.assertEqual(times[2] - times[1], 2)
                self.assertEqual(proxy._dispatch(b'bad', 'MIN:MISSING'), b'ERR:NO_DATA_FOR_SYMBOL')
                self.assertEqual(proxy.state, 'IDLE')
                self.assertEqual(proxy.clients, {})
                self.assertEqual(proxy._dispatch(b'bad', 'DAY:ABC'), b'ERR:UNSUPPORTED_COMMAND')
                payload = proxy._handle_historical('HISTORICAL|stock|ABC|minute|x|x|10')
                self.assertEqual(payload['df'].iloc[-1]['close'], 9)
            finally:
                proxy.close()

    def test_ah_session_uses_new_york_dst_and_missing_channel_errors(self):
        from datetime import date
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / 'ABC.json').write_text(''.join(json.dumps(row) + '\n' for row in entries()))
            proxy = ReplayProxy(date(2026, 7, 14), 0, folder, *ports(), symbols=['ABC'], session='ah')
            try:
                proxy.load_log_files()
                self.assertEqual(proxy.available['ABC'], {'trade': 1})
                self.assertEqual(proxy._dispatch(b'bad', 'MIN:ABC'), b'ERR:NO_DATA_FOR_CHANNEL')
                self.assertEqual(proxy.state, 'IDLE')
            finally:
                proxy.close()

    def test_winter_ah_includes_next_utc_day(self):
        from datetime import date
        with tempfile.TemporaryDirectory() as folder:
            rows = [{'timestamp': stamp, 'event_type': 'trade', 'price': 10, 'size': 1}
                    for stamp in ['2026-01-15T20:59:59Z', '2026-01-15T21:00:00Z', '2026-01-16T00:30:00Z', '2026-01-16T01:00:00Z']]
            (Path(folder) / 'ABC.json').write_text(''.join(json.dumps(row) + '\n' for row in rows))
            proxy = ReplayProxy(date(2026, 1, 15), 1, folder, *ports(), symbols=['ABC'], session='ah')
            try:
                proxy.load_log_files()
                self.assertEqual(proxy.total_events, 2)
            finally:
                proxy.close()

    def test_clock_scales_recorded_intervals_instead_of_one_minute_per_tick(self):
        from datetime import date
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / 'ABC.json').write_text(''.join(json.dumps(row) + '\n' for row in entries()))
            proxy = ReplayProxy(date(2026, 7, 14), 2, folder, *ports(), symbols=['ABC'], session='rth')
            try:
                proxy.load_log_files()
                proxy._register(b'test', ['ABC'], ['minute', 'quote', 'trade'])
                proxy.wall_origin = 0
                with patch('replay_zmq_proxy.time.monotonic', return_value=0.25):
                    proxy._advance()
                self.assertEqual(proxy.published, 3)
                with patch('replay_zmq_proxy.time.monotonic', return_value=0.5):
                    proxy._advance()
                self.assertEqual(proxy.published, 4)
                self.assertEqual(proxy.state, 'EOD_SIGNALED')
            finally:
                proxy.close()

    def test_real_zmq_replay_client_errors_speed_eod_and_dashboard_subscription(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'ABC.json').write_text(''.join(json.dumps(row) + '\n' for row in entries()))
            req_port, pub_port = ports()
            status = root / 'status.json'
            process = subprocess.Popen([sys.executable, str(PACKAGE / 'replay_zmq_proxy.py'),
                '--date', '2026-07-14', '--session', 'rth', '--speed', '600', '--symbols', 'ABC',
                '--log-dir', folder, '--req-port', str(req_port), '--pub-port', str(pub_port),
                '--status-file', str(status)], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            context = zmq.Context()
            req = context.socket(zmq.REQ)
            req.setsockopt(zmq.LINGER, 0)
            req.setsockopt(zmq.RCVTIMEO, 2000)
            req.connect(f'tcp://127.0.0.1:{req_port}')
            sub = context.socket(zmq.SUB)
            sub.setsockopt(zmq.LINGER, 0)
            sub.setsockopt(zmq.SUBSCRIBE, b'')
            sub.connect(f'tcp://127.0.0.1:{pub_port}')
            def command(value):
                req.send_string(value)
                return req.recv()
            try:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    if status.exists() and json.loads(status.read_text()).get('state') == 'IDLE':
                        break
                    if process.poll() is not None:
                        self.fail(process.stderr.read().decode())
                    time.sleep(0.02)
                self.assertEqual(json.loads(status.read_text())['state'], 'IDLE')
                self.assertEqual(command('MIN:UNKNOWN'), b'ERR:NO_DATA_FOR_SYMBOL')
                self.assertEqual(command('SUBSCRIBE:' + json.dumps({'symbols': ['ABC'], 'channels': ['minute', 'quote', 'trade'], 'application': 'Test replay'})), b'ACK')
                received = []
                start = time.monotonic()
                while sub.poll(2000):
                    payload = msgpack.unpackb(sub.recv_multipart()[1], raw=False)
                    if payload['type'] == 'EOD':
                        break
                    received.append(payload)
                self.assertLess(time.monotonic() - start, 2)
                self.assertEqual([row['type'] for row in received], ['quote', 'trade', 'quote', 'bar'])
                self.assertEqual(received[0]['ts'], '2026-07-14T13:30:00.000000001Z')
                self.assertEqual(received[1]['size'], 2)
                self.assertEqual(command('RESET'), b'ERR:OTHER_CLIENTS_CONNECTED')
                self.assertEqual(command('DISCONNECT'), b'BYE')
                self.assertEqual(command('PREVIEW:START'), b'ACK')
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    data = json.loads(status.read_text())
                    if data['state'] == 'EOD_SIGNALED' and data['preview']['received'] == 4:
                        break
                    time.sleep(0.02)
                self.assertEqual(data['preview']['received'], 4)
                self.assertEqual([row['event_type'] for row in data['preview']['rows']], ['quote', 'trade', 'quote', 'bar'])
                self.assertEqual(data['clients'][0]['application'], 'Dashboard replay')
                self.assertEqual(command('PREVIEW:STOP'), b'ACK')
            finally:
                req.close(0)
                sub.close(0)
                context.term()
                process.terminate()
                process.wait(timeout=5)
                process.stderr.close()

    def test_optional_date_remap_preserves_market_time_and_nanoseconds_across_dst(self):
        from datetime import date, timedelta
        from unittest.mock import Mock
        proxy = ReplayProxy.__new__(ReplayProxy)
        proxy.remap_today = True
        proxy.date_delta = timedelta(days=(date(2026, 7, 14) - date(2026, 1, 15)).days)
        proxy.publisher = Mock()
        proxy.published = 0
        proxy._publish('ABC', 'quote', {'timestamp': '2026-01-15T21:00:00.000000001Z', 'bid_price': 10, 'ask_price': 11})
        payload = msgpack.unpackb(proxy.publisher.send_multipart.call_args.args[0][1], raw=False)
        self.assertEqual(payload['ts'], '2026-07-14T20:00:00.000000001+00:00')
        self.assertEqual(payload['recorded_timestamp'], '2026-01-15T21:00:00.000000001Z')

    def test_timestamp_precision_is_not_limited_to_microseconds(self):
        self.assertEqual(parse_timestamp('2026-07-14T13:30:00.000000009Z')[1] - parse_timestamp('2026-07-14T13:30:00.000000001Z')[1], 8)


if __name__ == '__main__':
    unittest.main()
