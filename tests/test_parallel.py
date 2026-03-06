import pytest
from streamchunk.parallel import ParallelStreamChunker


def noop_processor(chunk, meta):
    pass


def test_thread_mode_processes_all_rows():
    data = list(range(10_000))
    psc = ParallelStreamChunker(data, noop_processor, mode="thread", n_workers=4)
    results = psc.run()
    total = sum(r.get("processed_rows", 0) for r in results)
    assert total == 10_000


def test_process_mode_processes_all_rows():
    data = list(range(10_000))
    psc = ParallelStreamChunker(data, noop_processor, mode="process", n_workers=2)
    results = psc.run()
    total = sum(r.get("processed_rows", 0) for r in results)
    assert total == 10_000


def test_summary_contains_cpu_info():
    data = list(range(1_000))
    psc = ParallelStreamChunker(data, noop_processor, mode="thread", n_workers=2)
    psc.run()
    summary = psc.summary()
    assert "logical_threads" in summary
    assert "physical_cores"  in summary
    assert summary["n_workers"] == 2


def test_summary_total_rows_matches():
    data = list(range(3_000))
    psc = ParallelStreamChunker(data, noop_processor, mode="thread", n_workers=3)
    psc.run()
    assert psc.summary()["total_rows"] == 3_000


def test_invalid_mode_raises():
    with pytest.raises(ValueError):
        ParallelStreamChunker([], noop_processor, mode="invalid")


def test_cpu_info_property():
    psc = ParallelStreamChunker([], noop_processor, mode="thread")
    info = psc.cpu_info
    assert info["logical_threads"] >= 1
    assert info["physical_cores"]  >= 1


def test_worker_results_have_no_errors():
    data = list(range(5_000))
    psc = ParallelStreamChunker(data, noop_processor, mode="thread", n_workers=4)
    results = psc.run()
    for r in results:
        assert "error" not in r


def test_results_sorted_by_worker_id():
    data = list(range(4_000))
    psc = ParallelStreamChunker(data, noop_processor, mode="thread", n_workers=4)
    results = psc.run()
    ids = [r["worker_id"] for r in results]
    assert ids == sorted(ids)


def test_summary_empty_before_run():
    psc = ParallelStreamChunker(list(range(100)), noop_processor, mode="thread", n_workers=2)
    assert psc.summary() == {}
