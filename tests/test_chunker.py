import pytest
from streamchunk import StreamChunker


def fake_source(n=5000):
    return [{"id": i, "value": i * 1.5} for i in range(n)]


def test_all_rows_consumed():
    data = fake_source(5000)
    chunker = StreamChunker(source=iter(data), initial_chunk_size=500)
    total = sum(len(chunk) for chunk, _ in chunker)
    assert total == 5000


def test_chunk_size_within_bounds():
    chunker = StreamChunker(
        source=iter(fake_source(10000)),
        min_chunk_size=100,
        max_chunk_size=2000,
        initial_chunk_size=500
    )
    for chunk, meta in chunker:
        assert 1 <= meta.chunk_size <= 2000
        chunker.report_latency(meta.chunk_id, 150.0)


def test_latency_feedback_reduces_size():
    chunker = StreamChunker(
        source=iter(fake_source(10000)),
        target_latency_ms=100,
        initial_chunk_size=1000
    )
    sizes = []
    for chunk, meta in chunker:
        sizes.append(meta.chunk_size)
        chunker.report_latency(meta.chunk_id, 800.0)  # way over target
        if len(sizes) > 5:
            break
    assert sizes[-1] < sizes[0], "Chunk size should decrease with high latency"


def test_worker_and_partition_id_in_metadata():
    chunker = StreamChunker(
        source=iter(fake_source(1000)),
        worker_id=3,
        partition_id=3
    )
    for chunk, meta in chunker:
        assert meta.worker_id == 3
        assert meta.partition_id == 3
        break


def test_stats_returns_expected_keys():
    chunker = StreamChunker(source=iter(fake_source(500)), initial_chunk_size=100)
    for chunk, meta in chunker:
        chunker.report_latency(meta.chunk_id, 50.0)
    s = chunker.stats()
    for key in ("total_rows", "total_chunks", "avg_chunk_size", "avg_latency_ms", "memory_pct"):
        assert key in s
    assert s["total_rows"] == 500
