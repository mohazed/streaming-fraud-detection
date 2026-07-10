import logging

from spark.health import StreamHealthMonitor


class FakeClock:
    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def test_no_warning_while_rows_flow(caplog):
    clock = FakeClock()
    monitor = StreamHealthMonitor(threshold_seconds=60.0, now=clock)
    with caplog.at_level(logging.WARNING):
        for _ in range(20):
            monitor.record("row", 5)
            clock.advance(10.0)
    assert caplog.text == ""


def test_warns_after_threshold_of_zero_rows(caplog):
    clock = FakeClock()
    monitor = StreamHealthMonitor(threshold_seconds=60.0, now=clock)
    monitor.record("velocity", 3)
    with caplog.at_level(logging.WARNING):
        for _ in range(7):
            clock.advance(10.0)
            monitor.record("velocity", 0)
    assert "stream starved" in caplog.text
    assert "velocity" in caplog.text


def test_warns_only_once_per_episode(caplog):
    clock = FakeClock()
    monitor = StreamHealthMonitor(threshold_seconds=60.0, now=clock)
    with caplog.at_level(logging.WARNING):
        for _ in range(20):
            clock.advance(10.0)
            monitor.record("geo", 0)
    assert caplog.text.count("stream starved") == 1


def test_recovers_and_can_warn_again(caplog):
    clock = FakeClock()
    monitor = StreamHealthMonitor(threshold_seconds=60.0, now=clock)
    with caplog.at_level(logging.WARNING):
        for _ in range(7):
            clock.advance(10.0)
            monitor.record("row", 0)
        monitor.record("row", 5)  # recovers
        for _ in range(7):
            clock.advance(10.0)
            monitor.record("row", 0)
    assert caplog.text.count("stream starved") == 2


def test_different_queries_tracked_independently(caplog):
    clock = FakeClock()
    monitor = StreamHealthMonitor(threshold_seconds=60.0, now=clock)
    with caplog.at_level(logging.WARNING):
        for _ in range(7):
            clock.advance(10.0)
            monitor.record("row", 5)
            monitor.record("velocity", 0)
    assert "row" not in caplog.text
    assert "velocity" in caplog.text
