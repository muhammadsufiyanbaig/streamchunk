import time
from dataclasses import dataclass, field
from typing import List


@dataclass
class PipelineStats:
    """Tracks aggregate throughput and latency across a pipeline run."""

    start_time: float = field(default_factory=time.time)
    total_rows: int = 0
    total_chunks: int = 0
    latencies_ms: List[float] = field(default_factory=list)

    def record(self, rows: int, latency_ms: float):
        self.total_rows += rows
        self.total_chunks += 1
        self.latencies_ms.append(latency_ms)

    def report(self) -> dict:
        import numpy as np
        elapsed = time.time() - self.start_time
        return {
            "elapsed_s":      round(elapsed, 2),
            "total_rows":     self.total_rows,
            "total_chunks":   self.total_chunks,
            "rows_per_sec":   round(self.total_rows / max(elapsed, 0.001)),
            "avg_latency_ms": round(float(np.mean(self.latencies_ms)), 2) if self.latencies_ms else 0,
            "p95_latency_ms": round(float(np.percentile(self.latencies_ms, 95)), 2) if self.latencies_ms else 0,
        }
