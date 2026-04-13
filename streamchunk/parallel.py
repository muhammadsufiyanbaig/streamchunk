"""
ParallelStreamChunker / ParallelFileChunker
===========================================
Detects available CPU threads, partitions the dataset (or file) across them,
and dispatches each partition to a separate worker using either:
  - ProcessPoolExecutor  (CPU-bound transformations)
  - ThreadPoolExecutor   (I/O-bound transformations)

Each worker runs its own StreamChunker with an independent
AdaptiveSizeController so PID tuning is local to that worker's load.

New in v2.1.0
-------------
* **Weight-aware partitioning** — ``ParallelStreamChunker`` accepts a
  ``weight_fn`` argument.  When provided, rows are distributed so that every
  worker receives roughly the same *total byte weight*, not just the same row
  count.  This prevents the "heavy partition bottleneck" where one worker
  processes a few very large rows while the others finish instantly.

* **Parallel file processing** — ``ParallelFileChunker`` splits a CSV or
  JSONL file into byte-range segments and gives each worker its own
  independent file handle.  The file is never fully loaded into memory.
"""

import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import Any, Callable, List, Optional, Sequence

from .chunker import StreamChunker
from .partitioner import (
    detect_cpu_threads,
    partition_dataset,
    partition_dataset_weighted,
    partition_file,
)


# ---------------------------------------------------------------------------
# Worker functions — must be module-level for ProcessPoolExecutor pickling
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
    ``processor(chunk, meta)`` for every chunk.

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


def _file_partition_worker(
    path: str,
    start_byte: int,
    end_byte: int,
    format: str,
    headers,
    skip_partial_line: bool,
    processor: Callable,
    worker_id: int,
    chunker_kwargs: dict,
) -> dict:
    """
    Runs inside each subprocess / thread for file-based parallel processing.

    Opens a :class:`~streamchunk.sources.file.RangedFileSource` for the given
    byte range, wraps it in a :class:`~streamchunk.chunker.StreamChunker`, and
    calls ``processor(chunk, meta)`` for every chunk.

    All arguments are plain Python scalars / strings so the function is
    safely picklable by ``ProcessPoolExecutor``.
    """
    from .sources.file import RangedFileSource

    source = RangedFileSource(
        path=path,
        start_byte=start_byte,
        end_byte=end_byte,
        format=format,
        headers=headers,
        skip_partial_line=skip_partial_line,
    )
    chunker = StreamChunker(
        source=source,
        worker_id=worker_id,
        partition_id=worker_id,
        **chunker_kwargs,
    )

    processed_rows = 0
    latencies: List[float] = []

    try:
        for chunk, meta in chunker:
            t0 = time.perf_counter()
            processor(chunk, meta)
            latency_ms = (time.perf_counter() - t0) * 1000

            chunker.report_latency(meta.chunk_id, latency_ms)
            latencies.append(latency_ms)
            processed_rows += len(chunk)
    finally:
        source.close()

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
    partitions ``data`` across N workers, then dispatches each partition
    to a thread or subprocess that runs its own adaptive StreamChunker.

    Parameters
    ----------
    data : Sequence
        The full dataset to process (must be indexable / sliceable).
    processor : Callable
        Function called for each chunk: ``processor(chunk, meta)``.
        Must be picklable when using ``mode="process"``.
    mode : str
        ``"thread"``  — ThreadPoolExecutor (default, best for I/O-bound work).
        ``"process"`` — ProcessPoolExecutor (best for CPU-bound work).
    n_workers : int or None
        Number of parallel workers.
        Defaults to ``logical_threads`` for ``"thread"`` mode and
        ``physical_cores`` for ``"process"`` mode.
    weight_fn : callable(row) -> float, optional
        When provided, rows are distributed so that each worker receives
        approximately equal **total weight** rather than equal row count.
        This solves the "heavy partition bottleneck": if some rows carry
        large attribute values (long strings, nested dicts, binary blobs),
        a purely count-based split leaves one worker finishing long after
        the others.  ``weight_fn`` is called once per row at construction
        time; the result is used only for partitioning.

        Example — balance by serialized JSON length::

            weight_fn=lambda row: len(json.dumps(row))

        Defaults to ``sys.getsizeof(row)`` when ``weight_fn`` is not given
        but weight-aware mode is not active (i.e. rows are split by count).
        Pass any callable to activate weight-aware partitioning.
    **chunker_kwargs
        Extra keyword arguments forwarded to each worker's StreamChunker
        (e.g. ``target_latency_ms``, ``max_memory_pct``, ``min_chunk_size``).
    """

    def __init__(
        self,
        data: Sequence[Any],
        processor: Callable,
        mode: str = "thread",
        n_workers: Optional[int] = None,
        weight_fn: Optional[Callable[[Any], float]] = None,
        **chunker_kwargs
    ):
        if mode not in ("thread", "process"):
            raise ValueError('mode must be "thread" or "process"')

        self._data = data
        self._processor = processor
        self._mode = mode
        self._weight_fn = weight_fn
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

        When a ``weight_fn`` was supplied, partitioning is weight-balanced;
        otherwise rows are split by count (original behaviour).

        Returns a list of per-worker stats dicts.
        """
        if self._weight_fn is not None:
            partitions = partition_dataset_weighted(
                self._data, self._n_workers, self._weight_fn
            )
        else:
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
            "weight_balanced": self._weight_fn is not None,
        }


# ---------------------------------------------------------------------------
# ParallelFileChunker
# ---------------------------------------------------------------------------

class ParallelFileChunker:
    """
    Parallel processor for large CSV or JSONL files.

    Splits the file into N byte-range segments and gives each worker its own
    independent file handle.  **The file is never fully loaded into memory.**

    Each worker runs its own :class:`~streamchunk.chunker.StreamChunker` with
    an independent PID controller, so chunk sizes adapt to each worker's local
    throughput independently.

    Parameters
    ----------
    path : str
        Path to the CSV or JSONL file.
    processor : Callable
        Function called for each chunk: ``processor(chunk, meta)``.
        For CSV files each element of ``chunk`` is a ``dict``
        (column name → value).  For JSONL each element is the parsed object.
        Must be picklable when using ``mode="process"``.
    format : str
        ``"csv"`` or ``"jsonl"``.
    mode : str
        ``"thread"``  — ThreadPoolExecutor (default, best for I/O-bound work).
        ``"process"`` — ProcessPoolExecutor (best for CPU-bound work).
    n_workers : int or None
        Number of parallel workers.  Defaults to CPU topology (logical threads
        for ``"thread"`` mode, physical cores for ``"process"`` mode).
    **chunker_kwargs
        Extra keyword arguments forwarded to each worker's StreamChunker.

    Example
    -------
    ::

        from streamchunk.parallel import ParallelFileChunker

        def process(chunk, meta):
            for row in chunk:
                upload_to_db(row)

        pfc = ParallelFileChunker(
            path="large_data.csv",
            processor=process,
            format="csv",
            mode="thread",
            target_latency_ms=300,
        )
        results = pfc.run()
        print(pfc.summary())

    Notes
    -----
    CSV files with embedded newlines inside quoted fields are not supported
    (the file is split by raw byte offsets).  Standard flat CSV files work
    correctly.
    """

    def __init__(
        self,
        path: str,
        processor: Callable,
        format: str = "csv",
        mode: str = "thread",
        n_workers: Optional[int] = None,
        **chunker_kwargs
    ):
        if format not in ("csv", "jsonl"):
            raise ValueError('format must be "csv" or "jsonl"')
        if mode not in ("thread", "process"):
            raise ValueError('mode must be "thread" or "process"')

        self._path = path
        self._processor = processor
        self._format = format
        self._mode = mode
        self._chunker_kwargs = chunker_kwargs
        self._results: List[dict] = []

        cpu = detect_cpu_threads()
        if n_workers is None:
            self._n_workers = (
                cpu["recommended_io"] if mode == "thread"
                else cpu["recommended_cpu"]
            )
        else:
            self._n_workers = n_workers

        self._cpu_info = cpu

    @property
    def cpu_info(self) -> dict:
        """CPU topology detected at construction time."""
        return self._cpu_info

    def run(self) -> List[dict]:
        """
        Partition the file and dispatch all workers.
        Blocks until every segment is fully processed.

        Returns a list of per-worker stats dicts.
        """
        file_partitions = partition_file(self._path, self._n_workers, self._format)

        Executor = (
            ThreadPoolExecutor if self._mode == "thread"
            else ProcessPoolExecutor
        )

        with Executor(max_workers=self._n_workers) as pool:
            futures = {
                pool.submit(
                    _file_partition_worker,
                    seg["path"],
                    seg["start"],
                    seg["end"],
                    seg["format"],
                    seg["headers"],
                    seg["skip_partial_line"],
                    self._processor,
                    worker_id,
                    self._chunker_kwargs,
                ): worker_id
                for worker_id, seg in enumerate(file_partitions)
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
            "file":            self._path,
            "format":          self._format,
            "total_rows":      total_rows,
            "total_chunks":    total_chunks,
            "avg_latency_ms":  float(np.mean(all_latencies)) if all_latencies else 0,
            "p95_latency_ms":  float(np.percentile(all_latencies, 95)) if all_latencies else 0,
            "per_worker":      self._results,
        }
