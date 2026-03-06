"""
Prometheus metrics exporter for streamchunk.
Requires: pip install streamchunk[metrics]
"""


def start_metrics_server(port: int = 8000) -> dict:
    """
    Start a Prometheus HTTP metrics server on the given port.

    Returns a dict of metric objects that StreamChunker updates
    as it processes chunks.
    """
    try:
        from prometheus_client import Counter, Gauge, Histogram, start_http_server
    except ImportError:
        raise ImportError(
            "prometheus-client is required. Install with: pip install streamchunk[metrics]"
        )

    start_http_server(port)

    return {
        "rows_processed": Counter(
            "streamchunk_rows_total",
            "Total rows processed",
            ["worker_id"]
        ),
        "chunks_processed": Counter(
            "streamchunk_chunks_total",
            "Total chunks processed",
            ["worker_id"]
        ),
        "chunk_size": Gauge(
            "streamchunk_chunk_size",
            "Current adaptive chunk size",
            ["worker_id"]
        ),
        "latency": Histogram(
            "streamchunk_latency_ms",
            "Chunk processing latency in milliseconds",
            ["worker_id"],
            buckets=[10, 50, 100, 200, 500, 1000, 2000, 5000]
        ),
        "memory_pct": Gauge(
            "streamchunk_memory_pct",
            "System memory usage percent"
        ),
    }
