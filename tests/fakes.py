"""Test doubles for the Kafka producer, sinks, and Kafka consumer. No network, ever.

Each Fake is structurally (Protocol-) compatible with the real adapter it stands in
for, so it can be injected without a subclass relationship:
  - FakeProducer   ~ confluent_kafka.Producer, used by producer/producer.py
  - FakeSink       ~ a sink callable passed to spark/sinks.py::write_alerts
  - FakeKafkaConsumer ~ confluent_kafka.Consumer, used by api/consumer.py
"""
from __future__ import annotations

from typing import Any, Callable, Optional, Protocol


class ProducerProtocol(Protocol):
    def produce(self, topic: str, key: Any = None, value: Any = None,
                callback: Optional[Callable[[Any, Any], None]] = None) -> None: ...

    def poll(self, timeout: float = 0) -> int: ...

    def flush(self, timeout: Optional[float] = None) -> int: ...


class FakeProducer:
    """Collects every produce() call in .messages; never touches a network."""

    def __init__(self) -> None:
        self.messages: list[dict] = []
        self.flushed = False

    def produce(self, topic, key=None, value=None, callback=None) -> None:
        self.messages.append({"topic": topic, "key": key, "value": value})
        if callback is not None:
            callback(None, None)

    def poll(self, timeout: float = 0) -> int:
        return 0

    def flush(self, timeout: Optional[float] = None) -> int:
        self.flushed = True
        return 0


class SinkProtocol(Protocol):
    def __call__(self, df: Any, batch_id: int) -> None: ...


class FakeSink:
    """Records every (df, batch_id) it is called with."""

    def __init__(self) -> None:
        self.calls: list[tuple[Any, int]] = []

    def __call__(self, df: Any, batch_id: int) -> None:
        self.calls.append((df, batch_id))


class FakeMessage:
    """Mimics confluent_kafka.Message: .value(), .error()."""

    def __init__(self, value: Optional[bytes], error: Any = None) -> None:
        self._value = value
        self._error = error

    def value(self) -> Optional[bytes]:
        return self._value

    def error(self) -> Any:
        return self._error


class KafkaConsumerProtocol(Protocol):
    def poll(self, timeout: float = 1.0) -> Optional[FakeMessage]: ...

    def close(self) -> None: ...


class FakeKafkaConsumer:
    """Yields scripted messages in order, then None forever, like an idle consumer."""

    def __init__(self, messages: list[bytes]) -> None:
        self._messages = list(messages)
        self.closed = False

    def poll(self, timeout: float = 1.0) -> Optional[FakeMessage]:
        if self._messages:
            return FakeMessage(self._messages.pop(0))
        return None

    def close(self) -> None:
        self.closed = True
