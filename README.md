# bt-gateway (bt-alpaca-zmq)

Gateway services for ZMQ market data flow:

- `alpaca_zmq_proxy`: live proxy Alpaca -> ZMQ (REQ/REP + PUB/SUB)
- `zmq_logger`: subscriber logger for market stream dumps
- `replay_zmq_proxy`: replay from dumped JSON logs

## Setup

```bash
../.runtime/python/cpython-3.13-linux-x86_64-gnu/bin/python3.13 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

Keep the gateway virtualenv separate from `bt-core/.venv`. Use the workspace
Python runtime rather than `/usr/bin/python`: a system Python upgrade can leave
the virtualenv pointing at a different minor version and hide installed packages.
The systemd proxy and logger both use `bt-gateway/.venv`.

## Commands

```bash
bt-alpaca-zmq-proxy --help
bt-alpaca-zmq-logger
bt-alpaca-zmq-replay --help
```

## Environment

`bt-alpaca-zmq-proxy` requires:

- `ALPACA_API_KEY` (or `ALPACA_KEY`)
- `ALPACA_SECRET_KEY` (or `ALPACA_SECRET`)
- optional: `ALPACA_DATA_FEED` (`sip` or `iex`, default `sip`)
