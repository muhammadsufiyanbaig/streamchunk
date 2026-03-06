import os
import psutil
from typing import Any, List, Sequence, Tuple


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
