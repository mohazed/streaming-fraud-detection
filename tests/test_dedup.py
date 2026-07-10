from spark.dedup import AlertDeduper


def _clock(start: float = 0.0):
    """Returns (now, advance) where now() reads a mutable box and advance()
    moves it forward by `seconds`."""
    box = {"t": start}

    def now() -> float:
        return box["t"]

    def advance(seconds: float) -> None:
        box["t"] += seconds

    return now, advance


def test_first_alert_passes():
    deduper = AlertDeduper()
    assert deduper.should_emit("velocity", "u1000", "2026-01-01T00:00:00Z") is True


def test_duplicate_key_suppressed():
    deduper = AlertDeduper()
    key = ("velocity", "u1000", "2026-01-01T00:00:00Z")
    assert deduper.should_emit(*key) is True
    assert deduper.should_emit(*key) is False
    assert deduper.should_emit(*key) is False


def test_different_window_passes():
    deduper = AlertDeduper()
    assert deduper.should_emit("velocity", "u1000", "2026-01-01T00:00:00Z") is True
    assert deduper.should_emit("velocity", "u1000", "2026-01-01T00:01:00Z") is True


def test_different_user_passes():
    deduper = AlertDeduper()
    assert deduper.should_emit("velocity", "u1000", "2026-01-01T00:00:00Z") is True
    assert deduper.should_emit("velocity", "u2000", "2026-01-01T00:00:00Z") is True


def test_eviction_after_ten_minutes():
    now, advance = _clock()
    deduper = AlertDeduper(ttl_seconds=600.0, now=now)
    key = ("velocity", "u1000", "2026-01-01T00:00:00Z")
    assert deduper.should_emit(*key) is True
    assert deduper.should_emit(*key) is False

    advance(601.0)
    assert deduper.should_emit(*key) is True


def test_memory_bounded():
    now, advance = _clock()
    deduper = AlertDeduper(ttl_seconds=600.0, now=now)

    n = 100_000
    total_seconds = 3600.0
    step = total_seconds / n
    for i in range(n):
        deduper.should_emit("velocity", f"u{i}", f"window-{i}")
        advance(step)

    assert len(deduper) < n // 2
