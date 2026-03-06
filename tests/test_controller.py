import pytest
from streamchunk.controller import AdaptiveSizeController


def make_ctrl(
    target_ms=200,
    max_mem=90.0,
    min_size=100,
    max_size=10_000,
    initial=1000
) -> AdaptiveSizeController:
    return AdaptiveSizeController(
        target_latency_ms=target_ms,
        max_memory_pct=max_mem,
        min_chunk_size=min_size,
        max_chunk_size=max_size,
        initial_chunk_size=initial
    )


def test_initial_size_within_bounds():
    ctrl = make_ctrl(initial=1000)
    size = ctrl.next_chunk_size()
    assert 100 <= size <= 10_000


def test_memory_override_reduces_to_min():
    # Set max_memory_pct to 1.0 so it always triggers on any real machine.
    # The override halves current_size each call, so we loop until it bottoms out.
    ctrl = make_ctrl(max_mem=1.0, min_size=100, initial=1000)
    size = ctrl.next_chunk_size()
    while size > 100:
        size = ctrl.next_chunk_size()
    assert size == 100


def test_high_latency_reduces_chunk_size():
    ctrl = make_ctrl(target_ms=100, min_size=10, max_size=10_000, initial=1000)
    ctrl.report_latency(800.0)   # 8x over target
    size = ctrl.next_chunk_size()
    assert size < 1000


def test_low_latency_increases_chunk_size():
    ctrl = make_ctrl(target_ms=500, min_size=10, max_size=10_000, initial=500)
    ctrl.report_latency(10.0)    # well below target
    size = ctrl.next_chunk_size()
    assert size > 500


def test_size_never_exceeds_max():
    ctrl = make_ctrl(target_ms=1000, min_size=10, max_size=2000, initial=1000)
    for _ in range(20):
        ctrl.report_latency(1.0)  # very fast — tries to grow
        size = ctrl.next_chunk_size()
        assert size <= 2000


def test_size_never_falls_below_min():
    ctrl = make_ctrl(target_ms=100, min_size=500, max_size=10_000, initial=1000)
    for _ in range(20):
        ctrl.report_latency(9000.0)  # extremely slow — tries to shrink
        size = ctrl.next_chunk_size()
        assert size >= 500
