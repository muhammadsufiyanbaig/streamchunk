import json
import os
import tempfile

import pytest

from streamchunk.sources.file import FileSource
from streamchunk.sources.generator import GeneratorSource


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_csv(rows: int) -> str:
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8")
    tmp.write("id,value\n")
    for i in range(rows):
        tmp.write(f"{i},{i * 1.5}\n")
    tmp.close()
    return tmp.name


def make_jsonl(rows: int) -> str:
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False, encoding="utf-8")
    for i in range(rows):
        tmp.write(json.dumps({"id": i, "value": i * 1.5}) + "\n")
    tmp.close()
    return tmp.name


# ---------------------------------------------------------------------------
# FileSource — CSV
# ---------------------------------------------------------------------------

def test_csv_reads_all_rows():
    path = make_csv(100)
    try:
        src = FileSource(path, format="csv")
        rows = []
        while not src.is_exhausted():
            batch = src.pull(20)
            if not batch:
                break
            rows.extend(batch)
        assert len(rows) == 100
    finally:
        src.close()
        os.unlink(path)


def test_csv_exhausted_flag():
    path = make_csv(10)
    try:
        src = FileSource(path, format="csv")
        src.pull(100)
        assert src.is_exhausted()
    finally:
        src.close()
        os.unlink(path)


def test_csv_partial_pull():
    path = make_csv(50)
    try:
        src = FileSource(path, format="csv")
        batch = src.pull(10)
        assert len(batch) == 10
        assert not src.is_exhausted()
    finally:
        src.close()
        os.unlink(path)


# ---------------------------------------------------------------------------
# FileSource — JSONL
# ---------------------------------------------------------------------------

def test_jsonl_reads_all_rows():
    path = make_jsonl(80)
    try:
        src = FileSource(path, format="jsonl")
        rows = []
        while not src.is_exhausted():
            batch = src.pull(20)
            if not batch:
                break
            rows.extend(batch)
        assert len(rows) == 80
    finally:
        src.close()
        os.unlink(path)


def test_unsupported_format_raises():
    path = make_csv(5)
    try:
        with pytest.raises(ValueError):
            FileSource(path, format="parquet")
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# GeneratorSource
# ---------------------------------------------------------------------------

def test_generator_reads_all():
    data = list(range(500))
    src = GeneratorSource(iter(data))
    collected = []
    while not src.is_exhausted():
        batch = src.pull(50)
        if not batch:
            break
        collected.extend(batch)
    assert collected == data


def test_generator_exhausted_flag():
    src = GeneratorSource(iter(range(5)))
    src.pull(100)
    assert src.is_exhausted()


def test_generator_partial_pull():
    src = GeneratorSource(iter(range(100)))
    batch = src.pull(30)
    assert len(batch) == 30
    assert not src.is_exhausted()


def test_generator_empty_source():
    src = GeneratorSource(iter([]))
    batch = src.pull(10)
    assert batch == []
    assert src.is_exhausted()
