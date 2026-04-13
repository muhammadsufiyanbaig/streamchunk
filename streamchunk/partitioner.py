import os
import sys
import psutil
from typing import Any, Callable, List, Optional, Sequence, Tuple


def detect_cpu_threads() -> dict:
    """
    Detect CPU topology of the current machine.

    Returns a dict with:
        logical_threads  -- total logical CPUs (includes hyperthreading)
        physical_cores   -- actual physical cores (no hyperthreading)
        recommended_io   -- suggested workers for I/O-bound tasks (ThreadPoolExecutor)
        recommended_cpu  -- suggested workers for CPU-bound tasks (ProcessPoolExecutor)
    """
    logical  = os.cpu_count() or 1
    physical = psutil.cpu_count(logical=False) or 1

    return {
        "logical_threads":  logical,
        "physical_cores":   physical,
        "recommended_io":   logical,   # ThreadPoolExecutor
        "recommended_cpu":  physical,  # ProcessPoolExecutor
    }


def partition_dataset(data: Sequence[Any], n_partitions: int) -> List[List[Any]]:
    """
    Split `data` into `n_partitions` roughly equal slices.

    The first (total % n_partitions) partitions receive one extra row so
    that every row is accounted for with no overlap or duplication.

    Example:
        partition_dataset(list(range(10)), 3)
        --> [[0,1,2,3], [4,5,6], [7,8,9]]
    """
    if n_partitions <= 0:
        raise ValueError("n_partitions must be >= 1")

    total = len(data)
    base  = total // n_partitions
    extra = total % n_partitions   # first `extra` partitions get one more row

    partitions, start = [], 0
    for i in range(n_partitions):
        end = start + base + (1 if i < extra else 0)
        partitions.append(list(data[start:end]))
        start = end

    return partitions


def partition_dataset_lazy(
    n_partitions: int,
    estimated_total: int
) -> List[Tuple[int, int]]:
    """
    Returns (start, end) index ranges without materialising the dataset in memory.
    Use when the full dataset is too large to hold in RAM at once.

    Example:
        partition_dataset_lazy(4, 100)
        --> [(0, 25), (25, 50), (50, 75), (75, 100)]
    """
    base  = estimated_total // n_partitions
    extra = estimated_total % n_partitions
    ranges, start = [], 0
    for i in range(n_partitions):
        end = start + base + (1 if i < extra else 0)
        ranges.append((start, end))
        start = end
    return ranges


def partition_dataset_weighted(
    data: Sequence[Any],
    n_partitions: int,
    weight_fn: Optional[Callable[[Any], float]] = None,
) -> List[List[Any]]:
    """
    Split ``data`` into ``n_partitions`` partitions balanced by **weight**,
    not row count.  Rows are kept in their original order.

    Use this when rows have variable-size attributes (e.g. long strings,
    nested objects, images) so that each worker receives roughly the same
    total byte load, preventing fast workers from finishing early while one
    slow worker still processes a heavy partition.

    Parameters
    ----------
    data : Sequence
        The full in-memory dataset.
    n_partitions : int
        Number of partitions to produce.
    weight_fn : callable(row) -> float, optional
        Returns the "cost" of a single row.
        Defaults to ``sys.getsizeof(row)``.

    Example
    -------
    Heavy rows are 100× larger than light rows::

        data = [{"x": "a" * 10_000}] + [{"x": "b"} for _ in range(99)]
        # Row-count split: worker 0 gets the 10 KB row + 24 light rows
        # Weight split:    worker 0 gets only the 10 KB row; others share 99 rows

    Returns
    -------
    List of lists, each sub-list is one partition.
    """
    if n_partitions <= 0:
        raise ValueError("n_partitions must be >= 1")
    if not data:
        return [[] for _ in range(n_partitions)]

    if weight_fn is None:
        weight_fn = sys.getsizeof

    weights = [weight_fn(row) for row in data]
    total_weight = sum(weights)
    target_weight = total_weight / n_partitions if n_partitions > 0 else total_weight

    partitions: List[List[Any]] = []
    current: List[Any] = []
    current_weight = 0.0

    for row, w in zip(data, weights):
        current.append(row)
        current_weight += w
        # Flush when we've reached the target and still have partitions to fill
        if current_weight >= target_weight and len(partitions) < n_partitions - 1:
            partitions.append(current)
            current = []
            current_weight = 0.0

    if current:
        partitions.append(current)

    # Pad with empty partitions if the data was too sparse
    while len(partitions) < n_partitions:
        partitions.append([])

    return partitions


def partition_file(
    path: str,
    n_partitions: int,
    format: str = "csv",
) -> List[dict]:
    """
    Split a large CSV or JSONL file into ``n_partitions`` byte-range segments
    **without loading the file into memory**.

    Each returned dict describes one segment and can be passed directly to
    :class:`~streamchunk.sources.file.RangedFileSource`.

    How it works
    ------------
    The file is split at evenly-spaced byte offsets.  Each worker opens the
    file independently and seeks to its start offset.  If the offset falls in
    the middle of a line, the worker skips forward to the next complete line
    boundary, so no rows are duplicated or dropped.

    This is the same technique used by Hadoop ``TextInputFormat`` and Dask's
    ``read_csv`` with ``blocksize``.

    Parameters
    ----------
    path : str
        Absolute or relative path to the file.
    n_partitions : int
        Number of parallel segments to produce.
    format : str
        ``"csv"`` or ``"jsonl"``.

    Returns
    -------
    List of dicts, each with keys:
        ``path``, ``start``, ``end``, ``format``, ``headers`` (CSV only, else None).

    Raises
    ------
    ValueError
        If ``format`` is not ``"csv"`` or ``"jsonl"``.
    FileNotFoundError
        If ``path`` does not exist.
    """
    if format not in ("csv", "jsonl"):
        raise ValueError('format must be "csv" or "jsonl"')
    if not os.path.isfile(path):
        raise FileNotFoundError(f"File not found: {path}")

    file_size = os.path.getsize(path)
    headers: Optional[List[str]] = None
    data_start = 0  # byte offset where actual data rows begin

    if format == "csv":
        with open(path, "rb") as f:
            header_line = f.readline()
            data_start = f.tell()
            # Parse headers respecting basic CSV quoting (no embedded commas in headers assumed)
            headers = header_line.decode("utf-8").strip().split(",")

    data_size = file_size - data_start
    if data_size <= 0 or n_partitions <= 0:
        return [{"path": path, "start": data_start, "end": file_size,
                 "format": format, "headers": headers}]

    base_chunk = data_size // n_partitions

    partitions = []
    for i in range(n_partitions):
        start = data_start + i * base_chunk
        end   = data_start + (i + 1) * base_chunk if i < n_partitions - 1 else file_size
        partitions.append({
            "path":              path,
            "start":             start,
            "end":               end,
            "format":            format,
            "headers":           headers,
            # Partition 0 always starts at an exact line boundary (right after
            # the CSV header or at byte 0 for JSONL).  Partitions 1+ may land
            # mid-line and need to skip the partial line that belongs to the
            # previous worker.
            "skip_partial_line": i > 0,
        })

    return partitions
