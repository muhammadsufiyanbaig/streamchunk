"""
ParallelStreamChunker
=====================
Detects available CPU threads, partitions the dataset across them,
and dispatches each partition to a separate worker using either:
  - ProcessPoolExecutor  (CPU-bound transformations)
  - ThreadPoolExecutor   (I/O-bound transformations)

Each worker runs its own StreamChunker with an independent
AdaptiveSizeController so PID tuning is local to that worker's load.
"""

import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import Any, Callable, List, Optional, Sequence

from .chunker import StreamChunker
from .partitioner import detect_cpu_threads, partition_dataset


# ---------------------------------------------------------------------------
# Worker function — must be module-level for ProcessPoolExecutor pickling
# ---------------------------------------------------------------------------

def _partition_worker(
    partition: List[Any],
    processor: Callable,
    worker_id: int,
    chunker_kwargs: dict
) -> dict:
    """
    Runs inside each subprocess / thread.

    Iterates the assigned partition through a StreamChunker and calls
    processor(chunk, meta) for every chunk.

    Returns a stats dict when the partition is fully consumed.
    """
    chunker = StreamChunker(
        source=iter(partition),
        worker_id=worker_id,
        partition_id=worker_id,
        **chunker_kwargs
    )

    processed_rows = 0
    latencies = []

    for chunk, meta in chunker:
        t0 = time.perf_counter()
        processor(chunk, meta)
        latency_ms = (time.perf_counter() - t0) * 1000

        chunker.report_latency(meta.chunk_id, latency_ms)
        latencies.append(latency_ms)
        processed_rows += len(chunk)

    return {
        **chunker.stats(),
        "processed_rows": processed_rows,
        "latencies":      latencies,
    }


# ---------------------------------------------------------------------------
# ParallelStreamChunker
# ---------------------------------------------------------------------------

class ParallelStreamChunker:
    """
    CPU-aware parallel stream chunker.

    Detects logical CPU threads and physical cores at construction,
    partitions `data` across N workers, then dispatches each partition
    to a thread or subprocess that runs its own adaptive StreamChunker.

    Parameters
    ----------
    data : Sequence
        The full dataset to process (must be indexable / sliceable).
    processor : Callable
        Function called for each chunk: processor(chunk, meta).
        Must be picklable when using mode="process".
    mode : str
        "thread"  -- ThreadPoolExecutor (default, best for I/O-bound work)
        "process" -- ProcessPoolExecutor (best for CPU-bound work)
    n_workers : int or None
        Number of parallel workers.
        Defaults to logical_threads for "thread" mode,
        physical_cores for "process" mode.
    **chunker_kwargs
        Extra keyword arguments forwarded to each worker's StreamChunker
        (e.g. target_latency_ms, max_memory_pct, min_chunk_size).
    """

    def __init__(
        self,
        data: Sequence[Any],
        processor: Callable,
        mode: str = "thread",
        n_workers: Optional[int] = None,
        **chunker_kwargs
    ):
        if mode not in ("thread", "process"):
            raise ValueError('mode must be "thread" or "process"')

        self._data = data
        self._processor = processor
        self._mode = mode
        self._chunker_kwargs = chunker_kwargs

        cpu = detect_cpu_threads()
        if n_workers is None:
            self._n_workers = (
                cpu["recommended_io"] if mode == "thread"
                else cpu["recommended_cpu"]
            )
        else:
            self._n_workers = n_workers

        self._cpu_info = cpu
        self._results: List[dict] = []

    @property
    def cpu_info(self) -> dict:
        """CPU topology detected at construction time."""
        return self._cpu_info

    def run(self) -> List[dict]:
        """
        Partition the dataset and dispatch all workers.
        Blocks until every partition is fully processed.

        Returns a list of per-worker stats dicts.
        """
        partitions = partition_dataset(self._data, self._n_workers)

        Executor = (
            ThreadPoolExecutor if self._mode == "thread"
            else ProcessPoolExecutor
        )

        with Executor(max_workers=self._n_workers) as pool:
            futures = {
                pool.submit(
                    _partition_worker,
                    partition,
                    self._processor,
                    worker_id,
                    self._chunker_kwargs
                ): worker_id
                for worker_id, partition in enumerate(partitions)
            }

            results = []
            for future in as_completed(futures):
                worker_id = futures[future]
                try:
                    stats = future.result()
                    results.append(stats)
                except Exception as exc:
                    results.append({"worker_id": worker_id, "error": str(exc)})

        self._results = sorted(results, key=lambda r: r.get("worker_id", 0))
        return self._results

    def summary(self) -> dict:
        """Aggregate stats across all workers after run()."""
        if not self._results:
            return {}

        total_rows    = sum(r.get("processed_rows", 0) for r in self._results)
        total_chunks  = sum(r.get("total_chunks",   0) for r in self._results)
        all_latencies = [l for r in self._results for l in r.get("latencies", [])]

        import numpy as np
        return {
            "mode":            self._mode,
            "n_workers":       self._n_workers,
            "logical_threads": self._cpu_info["logical_threads"],
            "physical_cores":  self._cpu_info["physical_cores"],
            "total_rows":      total_rows,
            "total_chunks":    total_chunks,
            "avg_latency_ms":  float(np.mean(all_latencies)) if all_latencies else 0,
            "p95_latency_ms":  float(np.percentile(all_latencies, 95)) if all_latencies else 0,
            "per_worker":      self._results,
        }
