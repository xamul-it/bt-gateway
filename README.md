# bt-gateway (bt-alpaca-zmq)

Gateway services for ZMQ market data flow:

- `alpaca_zmq_proxy`: live proxy Alpaca -> ZMQ (REQ/REP + PUB/SUB)
- `zmq_logger`: subscriber logger for market stream dumps
- `replay_zmq_proxy`: replay from dumped JSON logs

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

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
