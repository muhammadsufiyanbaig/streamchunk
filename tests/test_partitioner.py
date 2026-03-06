import os
import pytest
from streamchunk.partitioner import detect_cpu_threads, partition_dataset, partition_dataset_lazy


def test_detect_cpu_threads_returns_positive_counts():
    info = detect_cpu_threads()
    assert info["logical_threads"] >= 1
    assert info["physical_cores"]  >= 1
    assert info["recommended_io"]  == info["logical_threads"]
    assert info["recommended_cpu"] == info["physical_cores"]


def test_detect_cpu_threads_matches_os():
    info = detect_cpu_threads()
    assert info["logical_threads"] == (os.cpu_count() or 1)


def test_partition_dataset_total_rows_preserved():
    data = list(range(1000))
    parts = partition_dataset(data, 4)
    assert sum(len(p) for p in parts) == 1000


def test_partition_dataset_count():
    parts = partition_dataset(list(range(100)), 8)
    assert len(parts) == 8


def test_partition_dataset_uneven_split():
    # 10 rows, 3 partitions -> [4, 3, 3]
    parts = partition_dataset(list(range(10)), 3)
    assert len(parts[0]) == 4
    assert len(parts[1]) == 3
    assert len(parts[2]) == 3


def test_partition_dataset_no_overlap():
    data = list(range(100))
    parts = partition_dataset(data, 5)
    flat = [x for p in parts for x in p]
    assert flat == data  # same order, no duplicates, all rows present


def test_partition_dataset_single_partition():
    data = list(range(50))
    parts = partition_dataset(data, 1)
    assert len(parts) == 1
    assert parts[0] == data


def test_partition_dataset_invalid_raises():
    with pytest.raises(ValueError):
        partition_dataset(list(range(10)), 0)


def test_partition_dataset_lazy():
    ranges = partition_dataset_lazy(4, 100)
    assert len(ranges) == 4
    assert ranges[0] == (0, 25)
    assert ranges[-1][1] == 100


def test_partition_dataset_lazy_covers_all():
    ranges = partition_dataset_lazy(3, 10)
    total = sum(end - start for start, end in ranges)
    assert total == 10
