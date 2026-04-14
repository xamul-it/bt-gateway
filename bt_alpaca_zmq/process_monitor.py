import logging
import os
import threading
import time


class ProcessMonitor:
    def __init__(self, name, logger, interval_seconds, metrics_callback=None):
        self.name = name
        self.logger = logger
        self.interval_seconds = float(interval_seconds or 0)
        self.metrics_callback = metrics_callback
        self._stop_event = threading.Event()
        self._thread = None
        self._clk_tck = os.sysconf("SC_CLK_TCK")
        self._page_size = os.sysconf("SC_PAGE_SIZE")

    def start(self):
        if self.interval_seconds <= 0 or self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_seconds + 1.0))

    def _snapshot(self):
        with open("/proc/self/stat", "r", encoding="utf-8") as handle:
            fields = handle.read().split()
        cpu_ticks = int(fields[13]) + int(fields[14])
        rss_mb = (int(fields[23]) * self._page_size) / (1024 * 1024)
        num_threads = int(fields[19])
        cpu_core = int(fields[38]) if len(fields) > 38 else -1
        return {
            "cpu_ticks": cpu_ticks,
            "rss_mb": rss_mb,
            "num_threads": num_threads,
            "cpu_core": cpu_core,
        }

    def _run(self):
        try:
            previous = self._snapshot()
            previous_wall = time.time()
        except Exception as exc:
            self.logger.warning("Monitor %s disabilitato: %s", self.name, exc)
            return

        while not self._stop_event.wait(self.interval_seconds):
            try:
                current = self._snapshot()
                current_wall = time.time()
                delta_ticks = current["cpu_ticks"] - previous["cpu_ticks"]
                delta_wall = max(current_wall - previous_wall, 0.001)
                cpu_percent = (delta_ticks / self._clk_tck) / delta_wall * 100.0
                hot = cpu_percent >= 90.0
                extras = {}
                if self.metrics_callback is not None:
                    extras = self.metrics_callback() or {}
                extras_blob = " ".join(f"{key}={value}" for key, value in extras.items())
                self.logger.info(
                    "MONITOR %s pid=%s cpu=%.1f core=%s rss_mb=%.1f threads=%s hot=%s %s",
                    self.name,
                    os.getpid(),
                    cpu_percent,
                    current["cpu_core"],
                    current["rss_mb"],
                    current["num_threads"],
                    str(hot).lower(),
                    extras_blob,
                )
                previous = current
                previous_wall = current_wall
            except Exception as exc:
                self.logger.warning("Monitor %s errore: %s", self.name, exc)
