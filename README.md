# streamchunk

Adaptive data stream chunker with CPU-aware parallel processing for real-time ETL pipelines.

## Install

```bash
pip install streamchunk
```

## Quick start

```python
from streamchunk import StreamChunker

chunker = StreamChunker(source=iter(data), target_latency_ms=200)
for chunk, meta in chunker:
    process(chunk)
    chunker.report_latency(meta.chunk_id, meta.elapsed_ms)
```

## Parallel (multi-thread / multiprocess)

```python
from streamchunk.parallel import ParallelStreamChunker

psc = ParallelStreamChunker(data, processor=my_fn, mode="thread")
psc.run()
print(psc.summary())
```

See `streamchunk.md` for the full reference.
