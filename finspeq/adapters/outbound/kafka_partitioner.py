"""Kafka's default key partitioner (murmur2), computed in Python for checks and experiments."""

from __future__ import annotations

MURMUR2_SEED = 0x9747B28C
MURMUR2_M = 0x5BD1E995
MURMUR2_R = 24
MASK_32 = 0xFFFFFFFF
POSITIVE_MASK = 0x7FFFFFFF
SIGN_BIT = 0x80000000


def murmur2(data: bytes) -> int:
    """Return the signed 32-bit murmur2 hash of data, as Kafka's Utils.murmur2 computes it."""
    length = len(data)
    h = (MURMUR2_SEED ^ length) & MASK_32
    whole = length - length % 4
    for index in range(0, whole, 4):
        k = int.from_bytes(data[index : index + 4], "little")
        k = (k * MURMUR2_M) & MASK_32
        k ^= k >> MURMUR2_R
        k = (k * MURMUR2_M) & MASK_32
        h = (h * MURMUR2_M) & MASK_32
        h ^= k
    tail = data[whole:]
    if tail:
        for shift, byte in reversed(list(enumerate(tail))):
            h ^= byte << (8 * shift)
        h = (h * MURMUR2_M) & MASK_32
    h ^= h >> 13
    h = (h * MURMUR2_M) & MASK_32
    h ^= h >> 15
    return h - (1 << 32) if h & SIGN_BIT else h


def key_partition(key: bytes, partitions: int) -> int:
    """Return the partition Kafka's murmur2 partitioner chooses for key among partitions."""
    return (murmur2(key) & POSITIVE_MASK) % partitions
