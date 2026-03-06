# streamchunk

**Adaptive data stream chunker with CPU-aware parallel processing for real-time ETL pipelines.**

[![PyPI version](https://img.shields.io/pypi/v/streamchunk)](https://pypi.org/project/streamchunk/)
[![Python](https://img.shields.io/pypi/pyversions/streamchunk)](https://pypi.org/project/streamchunk/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## What is streamchunk?

Most ETL pipelines pick a chunk size once and forget about it. `streamchunk` treats chunk sizing as a **continuous control problem** — it monitors memory pressure and processing latency in real time, adjusting batch sizes automatically using a PID feedback loop.

From **v2.0.0**, it also detects available CPU threads, partitions your dataset across all cores, and dispatches work concurrently using **multiprocessing** (CPU-bound) or **multi-threading** (I/O-bound).

---

## Features

- **Adaptive PID-based chunk sizing** — responds to memory pressure and processing latency
- **CPU topology detection** — reads logical threads and physical cores at runtime
- **Automatic dataset partitioning** — splits data evenly across all CPU workers
- **Parallel processing** — `ProcessPoolExecutor` for CPU-bound, `ThreadPoolExecutor` for I/O-bound
- **Per-worker adaptive control** — each worker has its own independent PID controller
- **Backpressure mode** — automatically slows the pipeline when downstream is overloaded
- **Multiple sources** — File (CSV/JSONL), Kafka, Database (SQLAlchemy), REST API, Generator
- **Async support** — `async for chunk, meta in chunker.aiter()`
- **Prometheus metrics** — optional exporter for Grafana dashboards

---

## Installation

```bash
pip install streamchunk
```

With optional extras:

```bash
pip install "streamchunk[pandas]"    # pandas support
pip install "streamchunk[kafka]"     # Kafka source
pip install "streamchunk[database]"  # SQLAlchemy source
pip install "streamchunk[metrics]"   # Prometheus exporter
pip install "streamchunk[all]"       # everything
```

---

## Quick Start

### Single-threaded

```python
from streamchunk import StreamChunker

data = [{"id": i, "value": i * 1.5} for i in range(100_000)]

chunker = StreamChunker(
    source=iter(data),
    target_latency_ms=200,
    max_memory_pct=75
)

for chunk, meta in chunker:
    process(chunk)
    chunker.report_latency(meta.chunk_id, meta.elapsed_ms)

print(chunker.stats())
```

### Parallel — multi-threaded (I/O-bound)

Best for: writing to databases, S3 uploads, REST API calls, Kafka publishing.

```python
from streamchunk.parallel import ParallelStreamChunker

data = load_your_dataset()

def etl_worker(chunk, meta):
    upload_to_s3(chunk)

psc = ParallelStreamChunker(
    data=data,
    processor=etl_worker,
    mode="thread",          # ThreadPoolExecutor
    target_latency_ms=200,
    max_memory_pct=75
)

results = psc.run()
print(psc.summary())
```

### Parallel — multiprocessing (CPU-bound)

Best for: data transformation, ML inference, compression, encryption.

```python
from streamchunk.parallel import ParallelStreamChunker

def transform_worker(chunk, meta):
    return [heavy_transform(row) for row in chunk]

psc = ParallelStreamChunker(
    data=data,
    processor=transform_worker,
    mode="process",         # ProcessPoolExecutor
    target_latency_ms=500
)

results = psc.run()
print(psc.summary())
```

### Inspect CPU topology

```python
from streamchunk.partitioner import detect_cpu_threads

info = detect_cpu_threads()
# {
#   'logical_threads': 16,
#   'physical_cores':  8,
#   'recommended_io':  16,
#   'recommended_cpu': 8
# }
```

### From a CSV file

```python
from streamchunk import StreamChunker
from streamchunk.sources.file import FileSource

chunker = StreamChunker(
    source=FileSource("data.csv", format="csv"),
    target_latency_ms=300
)

for chunk, meta in chunker:
    process(chunk)
    chunker.report_latency(meta.chunk_id, meta.elapsed_ms)
```

### Async iteration

```python
import asyncio
from streamchunk import StreamChunker

async def run():
    chunker = StreamChunker(source=iter(data))
    async for chunk, meta in chunker.aiter():
        await async_process(chunk)

asyncio.run(run())
```

---

## How It Works

### PID Adaptive Sizing

Before pulling each chunk, the controller checks:

1. **Memory** — if `psutil.virtual_memory().percent >= max_memory_pct`, it halves the chunk size immediately (hard ceiling).
2. **Latency** — a PID controller compares actual processing time against `target_latency_ms` and adjusts chunk size up or down to converge on the target.

```
error      = target_latency_ms - last_latency_ms
adjustment = kp*error + ki*integral + kd*derivative
chunk_size = clamp(chunk_size + adjustment, min, max)
```

### CPU-Aware Parallel Pipeline

```
Dataset
   |
   v
detect_cpu_threads()          <- os.cpu_count() + psutil
   |
   v
partition_dataset()           <- split into N equal slices
   |
   +---> Worker 0  (StreamChunker + PID)
   +---> Worker 1  (StreamChunker + PID)
   ...
   +---> Worker N  (StreamChunker + PID)
   |
   v
as_completed(futures)         <- collect results
   |
   v
summary()                     <- aggregate stats
```

### Choosing Thread vs Process mode

| Scenario | Mode | Why |
|----------|------|-----|
| S3 / GCS uploads | `"thread"` | I/O waits release the GIL |
| Database inserts | `"thread"` | Network I/O dominates |
| REST API calls | `"thread"` | Concurrent HTTP requests |
| Data transformation | `"process"` | Bypasses GIL, true parallelism |
| ML batch inference | `"process"` | CPU-bound across cores |
| Compression / encryption | `"process"` | CPU-bound |

---

## API Reference

### `StreamChunker`

```python
StreamChunker(
    source,
    target_latency_ms: int = 200,
    max_memory_pct: float = 75.0,
    min_chunk_size: int = 100,
    max_chunk_size: int = 100_000,
    initial_chunk_size: int = 1000,
    backpressure: bool = True,
    metrics: bool = False,
    worker_id: int = 0,
    partition_id: int = 0
)
```

| Method | Description |
|--------|-------------|
| `for chunk, meta in chunker` | Iterate chunks with metadata |
| `chunker.report_latency(id, ms)` | Feed actual processing time back to PID |
| `async for chunk, meta in chunker.aiter()` | Async iteration |
| `chunker.stats()` | Per-worker throughput and latency stats |
| `StreamChunker.from_config(path, source)` | Load config from YAML file |

### `ParallelStreamChunker`

```python
ParallelStreamChunker(
    data: Sequence,
    processor: Callable,       # processor(chunk, meta)
    mode: str = "thread",      # "thread" | "process"
    n_workers: int = None,     # defaults to CPU topology
    **chunker_kwargs
)
```

| Method / Property | Description |
|-------------------|-------------|
| `psc.run()` | Partition and dispatch. Blocks until done. Returns per-worker stats. |
| `psc.summary()` | Aggregated stats: total rows, latency p95, CPU info. |
| `psc.cpu_info` | `{logical_threads, physical_cores, recommended_io, recommended_cpu}` |

### `ChunkMetadata`

```python
meta.chunk_id           # str   — unique chunk identifier
meta.chunk_size         # int   — rows in this chunk
meta.chunk_index        # int   — sequential index within this worker
meta.elapsed_ms         # float — ms since chunk was pulled
meta.memory_pct         # float — system memory % at pull time
meta.chunk_size_bytes   # int   — estimated memory footprint
meta.worker_id          # int   — which worker processed this chunk
meta.partition_id       # int   — which dataset partition
```

### Data Sources

| Source | Import | Notes |
|--------|--------|-------|
| Any iterable / generator | built-in | Default wrapping |
| CSV / JSONL file | `streamchunk.sources.file.FileSource` | |
| Kafka topic | `streamchunk.sources.kafka.KafkaSource` | `pip install streamchunk[kafka]` |
| SQL database | `streamchunk.sources.database.DatabaseSource` | `pip install streamchunk[database]` |
| Paginated REST API | `streamchunk.sources.api.APISource` | requires `requests` |

---

## YAML Configuration

Save chunker settings in a YAML file and load at runtime:

```yaml
# config.yaml
target_latency_ms: 300
max_memory_pct: 80.0
min_chunk_size: 200
max_chunk_size: 50000
initial_chunk_size: 2000
backpressure: true
```

```python
chunker = StreamChunker.from_config("config.yaml", source=iter(data))
```

---

## Changelog

### v2.0.0
- CPU thread detection via `os.cpu_count()` and `psutil.cpu_count(logical=False)`
- Dataset partitioning across N workers with no overlap
- `ParallelStreamChunker` with `ProcessPoolExecutor` and `ThreadPoolExecutor`
- Per-worker independent PID controllers
- `worker_id` and `partition_id` added to `ChunkMetadata`
- New modules: `parallel.py`, `partitioner.py`

### v1.0.0
- PID-based adaptive chunk sizing
- Memory pressure override
- File, Kafka, Database, Generator sources
- Async iteration support
- Backpressure mode
- Prometheus metrics exporter

---

## License

MIT — See [LICENSE](https://github.com/muhammadsufiyanbaig/streamchunk/blob/main/LICENSE)

---

## Contributing

Open an issue before submitting a PR. Run `pytest tests/` and ensure `mypy` passes before pushing.
