"""Console entrypoints for bt-alpaca-zmq."""

from __future__ import annotations

import runpy


def alpaca_proxy() -> None:
    runpy.run_module("bt_alpaca_zmq.alpaca_zmq_proxy", run_name="__main__")


def zmq_logger() -> None:
    runpy.run_module("bt_alpaca_zmq.zmq_logger", run_name="__main__")


def replay_proxy() -> None:
    runpy.run_module("bt_alpaca_zmq.replay_zmq_proxy", run_name="__main__")
