import sys
import time
from typing import Iterator, Tuple

import psutil

from .controller import AdaptiveSizeController
from .metadata import ChunkMetadata
from .sources.base import BaseSource
from .sources.generator import GeneratorSource


class StreamChunker:
    def __init__(
        self,
        source,
        target_latency_ms: int = 200,
        max_memory_pct: float = 75.0,
        min_chunk_size: int = 100,
        max_chunk_size: int = 100_000,
        initial_chunk_size: int = 1000,
        backpressure: bool = True,
        metrics: bool = False,
        metrics_port: int = 8000,
        worker_id: int = 0,
        partition_id: int = 0
    ):
        # Wrap raw iterables
        if isinstance(source, BaseSource):
            self._source = source
        else:
            self._source = GeneratorSource(source)

        self._controller = AdaptiveSizeController(
            target_latency_ms=target_latency_ms,
            max_memory_pct=max_memory_pct,
            min_chunk_size=min_chunk_size,
            max_chunk_size=max_chunk_size,
            initial_chunk_size=initial_chunk_size
        )

        self.target_latency_ms = target_latency_ms
        self.backpressure = backpressure
        self._chunk_index = 0
        self._total_rows = 0
        self._latency_history = []
        self._worker_id = worker_id
        self._partition_id = partition_id

        if metrics:
            from .metrics import start_metrics_server
            self._metrics = start_metrics_server(metrics_port)
        else:
            self._metrics = None

    def __iter__(self) -> Iterator[Tuple[list, ChunkMetadata]]:
        while not self._source.is_exhausted():
            chunk_size = self._controller.next_chunk_size()
            meta = ChunkMetadata(
                chunk_index=self._chunk_index,
                chunk_size=chunk_size,
                memory_pct=psutil.virtual_memory().percent,
                worker_id=self._worker_id,
                partition_id=self._partition_id
            )
            chunk = self._source.pull(chunk_size)
            if not chunk:
                break

            meta.chunk_size = len(chunk)
            meta.chunk_size_bytes = sys.getsizeof(chunk)

            self._chunk_index += 1
            self._total_rows += len(chunk)

            yield chunk, meta

            # Backpressure: slow down if we're ahead of downstream
            if self.backpressure and meta.elapsed_ms > self.target_latency_ms * 2:
                sleep_s = (meta.elapsed_ms - self.target_latency_ms) / 1000
                time.sleep(min(sleep_s, 2.0))

    def report_latency(self, chunk_id: str, latency_ms: float):
        """Feed actual processing latency back to the controller."""
        self._controller.report_latency(latency_ms)
        self._latency_history.append(latency_ms)

    async def aiter(self):
        """Async generator for non-blocking pipelines."""
        import asyncio
        for chunk, meta in self:
            yield chunk, meta
            await asyncio.sleep(0)

    def stats(self) -> dict:
        import numpy as np
        return {
            "worker_id":          self._worker_id,
            "partition_id":       self._partition_id,
            "total_rows":         self._total_rows,
            "total_chunks":       self._chunk_index,
            "avg_chunk_size":     self._total_rows / max(1, self._chunk_index),
            "avg_latency_ms":     float(np.mean(self._latency_history)) if self._latency_history else 0,
            "current_chunk_size": self._controller.current_size,
            "memory_pct":         psutil.virtual_memory().percent
        }

    @classmethod
    def from_config(cls, path: str, source) -> "StreamChunker":
        import yaml
        with open(path) as f:
            cfg = yaml.safe_load(f)
        return cls(source=source, **cfg)
