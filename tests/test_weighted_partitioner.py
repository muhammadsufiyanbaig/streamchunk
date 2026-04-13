"""
Tests for partition_dataset_weighted and partition_file.
"""
import csv
import json
import os
import sys
import tempfile

import pytest

from streamchunk.partitioner import partition_dataset_weighted, partition_file


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_csv(rows: int, long_value_at: int = -1, long_len: int = 10_000) -> str:
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    )
    tmp.write("id,value\n")
    for i in range(rows):
        val = "x" * long_len if i == long_value_at else str(i * 1.5)
        tmp.write(f"{i},{val}\n")
    tmp.close()
    return tmp.name


def make_jsonl(rows: int, long_value_at: int = -1, long_len: int = 10_000) -> str:
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    )
    for i in range(rows):
        val = "x" * long_len if i == long_value_at else i * 1.5
        tmp.write(json.dumps({"id": i, "value": val}) + "\n")
    tmp.close()
    return tmp.name


# ---------------------------------------------------------------------------
# partition_dataset_weighted
# ---------------------------------------------------------------------------

class TestWeightedPartitioner:
    def test_total_rows_preserved(self):
        data = list(range(1000))
        parts = partition_dataset_weighted(data, 4)
        assert sum(len(p) for p in parts) == 1000

    def test_returns_n_partitions(self):
        data = list(range(100))
        parts = partition_dataset_weighted(data, 6)
        assert len(parts) == 6

    def test_no_rows_lost_or_duplicated(self):
        data = list(range(200))
        parts = partition_dataset_weighted(data, 4)
        flat = [x for p in parts for x in p]
        assert sorted(flat) == sorted(data)

    def test_order_preserved_within_partitions(self):
        data = list(range(100))
        parts = partition_dataset_weighted(data, 4)
        flat = [x for p in parts for x in p]
        assert flat == data  # rows stay in original order

    def test_empty_data_returns_empty_partitions(self):
        parts = partition_dataset_weighted([], 4)
        assert len(parts) == 4
        assert all(p == [] for p in parts)

    def test_single_partition(self):
        data = list(range(50))
        parts = partition_dataset_weighted(data, 1)
        assert len(parts) == 1
        assert parts[0] == data

    def test_invalid_n_raises(self):
        with pytest.raises(ValueError):
            partition_dataset_weighted(list(range(10)), 0)

    def test_custom_weight_fn(self):
        # Rows are strings of varying length; weight by string length
        data = ["a" * 100, "b" * 1, "c" * 1, "d" * 1]
        parts = partition_dataset_weighted(data, 2, weight_fn=len)
        # First partition should contain the heavy row alone (or close to it)
        assert len(parts) == 2
        assert sum(len(p) for p in parts) == 4

    def test_heavy_row_isolated_from_light_rows(self):
        """
        One very heavy row + 99 tiny rows, 2 partitions.
        With weight-aware splitting, the heavy row gets its own partition;
        with count-based splitting it would be mixed in with 49 others.
        """
        heavy = {"data": "x" * 100_000}
        light = [{"data": "a"} for _ in range(99)]
        data = [heavy] + light

        weight_fn = lambda row: sys.getsizeof(row["data"])
        parts = partition_dataset_weighted(data, 2, weight_fn=weight_fn)

        # Heavy row must be in partition 0 (it appears first)
        assert heavy in parts[0]
        # The light rows should dominate partition 1
        assert len(parts[1]) > len(parts[0])

    def test_heavy_row_not_mixed_with_light_rows(self):
        """
        With 1 very heavy row and 9 light rows split into 2 partitions,
        weight-aware splitting puts the heavy row in its own partition
        (partition 0) and all 9 light rows in the other partition.
        A count-based split would mix 5 rows into each partition, leaving
        the heavy row alongside 4 light rows in partition 0.
        """
        heavy_weight = 1_000_000
        light_weight = 1
        data = [{"w": heavy_weight}] + [{"w": light_weight} for _ in range(9)]
        weight_fn = lambda row: row["w"]

        parts = partition_dataset_weighted(data, 2, weight_fn=weight_fn)

        # The heavy row is alone in partition 0
        assert len(parts[0]) == 1
        assert parts[0][0]["w"] == heavy_weight
        # All 9 light rows are in partition 1
        assert len(parts[1]) == 9
        assert all(r["w"] == light_weight for r in parts[1])


# ---------------------------------------------------------------------------
# partition_file — CSV
# ---------------------------------------------------------------------------

class TestPartitionFileCsv:
    def test_returns_n_partitions(self):
        path = make_csv(200)
        try:
            parts = partition_file(path, 4, "csv")
            assert len(parts) == 4
        finally:
            os.unlink(path)

    def test_headers_extracted(self):
        path = make_csv(10)
        try:
            parts = partition_file(path, 2, "csv")
            for p in parts:
                assert p["headers"] == ["id", "value"]
        finally:
            os.unlink(path)

    def test_start_end_byte_coverage(self):
        path = make_csv(100)
        try:
            file_size = os.path.getsize(path)
            parts = partition_file(path, 4, "csv")
            # Last partition must reach end of file
            assert parts[-1]["end"] == file_size
            # Offsets must be monotonically increasing
            starts = [p["start"] for p in parts]
            assert starts == sorted(starts)
        finally:
            os.unlink(path)

    def test_format_and_path_in_result(self):
        path = make_csv(10)
        try:
            parts = partition_file(path, 2, "csv")
            for p in parts:
                assert p["path"] == path
                assert p["format"] == "csv"
        finally:
            os.unlink(path)

    def test_invalid_format_raises(self):
        path = make_csv(5)
        try:
            with pytest.raises(ValueError):
                partition_file(path, 2, "parquet")
        finally:
            os.unlink(path)

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            partition_file("/nonexistent/path/data.csv", 4, "csv")


# ---------------------------------------------------------------------------
# partition_file — JSONL
# ---------------------------------------------------------------------------

class TestPartitionFileJsonl:
    def test_returns_n_partitions(self):
        path = make_jsonl(200)
        try:
            parts = partition_file(path, 4, "jsonl")
            assert len(parts) == 4
        finally:
            os.unlink(path)

    def test_headers_is_none_for_jsonl(self):
        path = make_jsonl(10)
        try:
            parts = partition_file(path, 2, "jsonl")
            for p in parts:
                assert p["headers"] is None
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# RangedFileSource reads correct data
# ---------------------------------------------------------------------------

class TestRangedFileSource:
    def test_csv_all_rows_read_across_workers(self):
        """Two RangedFileSources covering the whole file must read all rows once."""
        from streamchunk.sources.file import RangedFileSource

        n_rows = 50
        path = make_csv(n_rows)
        try:
            parts = partition_file(path, 2, "csv")
            all_ids = []
            for seg in parts:
                src = RangedFileSource(
                    path=seg["path"],
                    start_byte=seg["start"],
                    end_byte=seg["end"],
                    format="csv",
                    headers=seg["headers"],
                    skip_partial_line=seg["skip_partial_line"],
                )
                while not src.is_exhausted():
                    batch = src.pull(10)
                    if not batch:
                        break
                    all_ids.extend(int(r["id"]) for r in batch)
                src.close()

            assert sorted(all_ids) == list(range(n_rows))
        finally:
            os.unlink(path)

    def test_jsonl_all_rows_read_across_workers(self):
        from streamchunk.sources.file import RangedFileSource

        n_rows = 60
        path = make_jsonl(n_rows)
        try:
            parts = partition_file(path, 3, "jsonl")
            all_ids = []
            for seg in parts:
                src = RangedFileSource(
                    path=seg["path"],
                    start_byte=seg["start"],
                    end_byte=seg["end"],
                    format="jsonl",
                    headers=None,
                    skip_partial_line=seg["skip_partial_line"],
                )
                while not src.is_exhausted():
                    batch = src.pull(10)
                    if not batch:
                        break
                    all_ids.extend(r["id"] for r in batch)
                src.close()

            assert sorted(all_ids) == list(range(n_rows))
        finally:
            os.unlink(path)

    def test_no_row_duplicated(self):
        from streamchunk.sources.file import RangedFileSource

        n_rows = 80
        path = make_csv(n_rows)
        try:
            parts = partition_file(path, 4, "csv")
            all_ids = []
            for seg in parts:
                src = RangedFileSource(
                    path=seg["path"],
                    start_byte=seg["start"],
                    end_byte=seg["end"],
                    format=seg["format"],
                    headers=seg["headers"],
                    skip_partial_line=seg["skip_partial_line"],
                )
                while not src.is_exhausted():
                    batch = src.pull(20)
                    if not batch:
                        break
                    all_ids.extend(int(r["id"]) for r in batch)
                src.close()

            # No duplicates
            assert len(all_ids) == len(set(all_ids))
        finally:
            os.unlink(path)

    def test_unsupported_format_raises(self):
        from streamchunk.sources.file import RangedFileSource
        with pytest.raises(ValueError):
            RangedFileSource("/dev/null", 0, 100, format="parquet")


# ---------------------------------------------------------------------------
# ParallelFileChunker end-to-end
# ---------------------------------------------------------------------------

class TestParallelFileChunker:
    def test_csv_all_rows_processed(self):
        from streamchunk.parallel import ParallelFileChunker

        n_rows = 200
        path = make_csv(n_rows)
        collected = []

        def processor(chunk, meta):
            collected.extend(chunk)

        try:
            pfc = ParallelFileChunker(
                path=path,
                processor=processor,
                format="csv",
                mode="thread",
                n_workers=4,
            )
            pfc.run()
            assert len(collected) == n_rows
        finally:
            os.unlink(path)

    def test_jsonl_all_rows_processed(self):
        from streamchunk.parallel import ParallelFileChunker

        n_rows = 150
        path = make_jsonl(n_rows)
        collected = []

        def processor(chunk, meta):
            collected.extend(chunk)

        try:
            pfc = ParallelFileChunker(
                path=path,
                processor=processor,
                format="jsonl",
                mode="thread",
                n_workers=3,
            )
            pfc.run()
            assert len(collected) == n_rows
        finally:
            os.unlink(path)

    def test_summary_contains_file_info(self):
        from streamchunk.parallel import ParallelFileChunker

        path = make_csv(100)
        try:
            pfc = ParallelFileChunker(
                path=path,
                processor=lambda c, m: None,
                format="csv",
                mode="thread",
                n_workers=2,
            )
            pfc.run()
            s = pfc.summary()
            assert s["file"] == path
            assert s["format"] == "csv"
            assert s["n_workers"] == 2
            assert "total_rows" in s
        finally:
            os.unlink(path)

    def test_invalid_format_raises(self):
        from streamchunk.parallel import ParallelFileChunker
        with pytest.raises(ValueError):
            ParallelFileChunker("/dev/null", lambda c, m: None, format="parquet")

    def test_invalid_mode_raises(self):
        from streamchunk.parallel import ParallelFileChunker
        with pytest.raises(ValueError):
            ParallelFileChunker("/dev/null", lambda c, m: None, mode="invalid")

    def test_summary_empty_before_run(self):
        from streamchunk.parallel import ParallelFileChunker
        path = make_csv(10)
        try:
            pfc = ParallelFileChunker(path=path, processor=lambda c, m: None,
                                      format="csv", mode="thread", n_workers=2)
            assert pfc.summary() == {}
        finally:
            os.unlink(path)

    def test_no_rows_duplicated(self):
        from streamchunk.parallel import ParallelFileChunker

        n_rows = 200
        path = make_csv(n_rows)
        ids = []

        def processor(chunk, meta):
            ids.extend(int(r["id"]) for r in chunk)

        try:
            pfc = ParallelFileChunker(
                path=path,
                processor=processor,
                format="csv",
                mode="thread",
                n_workers=4,
            )
            pfc.run()
            assert len(ids) == len(set(ids)), "Duplicate rows detected"
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# ParallelStreamChunker — weight_fn parameter
# ---------------------------------------------------------------------------

class TestParallelStreamChunkerWeightFn:
    def test_weight_fn_processes_all_rows(self):
        from streamchunk.parallel import ParallelStreamChunker

        data = [{"v": "x" * (i % 100)} for i in range(1000)]
        collected = []

        def processor(chunk, meta):
            collected.extend(chunk)

        psc = ParallelStreamChunker(
            data,
            processor,
            mode="thread",
            n_workers=4,
            weight_fn=lambda row: len(row["v"]),
        )
        psc.run()
        assert len(collected) == 1000

    def test_weight_balanced_flag_in_summary(self):
        from streamchunk.parallel import ParallelStreamChunker

        data = list(range(100))
        psc = ParallelStreamChunker(
            data, lambda c, m: None,
            mode="thread", n_workers=2,
            weight_fn=lambda x: x + 1,
        )
        psc.run()
        assert psc.summary()["weight_balanced"] is True

    def test_no_weight_fn_flag_false(self):
        from streamchunk.parallel import ParallelStreamChunker

        data = list(range(100))
        psc = ParallelStreamChunker(data, lambda c, m: None,
                                    mode="thread", n_workers=2)
        psc.run()
        assert psc.summary()["weight_balanced"] is False
