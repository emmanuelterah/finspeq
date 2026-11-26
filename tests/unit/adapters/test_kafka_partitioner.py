"""Tests for the Python murmur2 partitioner against Kafka's published test vectors."""

from __future__ import annotations

import pytest

from finspeq.adapters.outbound.kafka_partitioner import key_partition, murmur2


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("21", -973932308),
        ("foobar", -790332482),
        ("a-little-bit-long-string", -985981536),
        ("a-little-bit-longer-string", -1486304829),
        ("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971),
        ("abc", 479470107),
    ],
)
def test_murmur2_matches_kafka(text: str, expected: int) -> None:
    """Return the values asserted in Kafka's UtilsTest for the same inputs."""
    assert murmur2(text.encode("ascii")) == expected


def test_key_partition_is_stable_and_in_range() -> None:
    """Map one key to the same partition every time, inside [0, partitions)."""
    key = b"4000000000000002"
    assert key_partition(key, 4) == key_partition(key, 4)
    assert all(0 <= key_partition(str(n).encode(), 8) < 8 for n in range(1000))
