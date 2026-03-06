from dataclasses import dataclass, field
import time
import uuid


@dataclass
class ChunkMetadata:
    chunk_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    chunk_index: int = 0
    chunk_size: int = 0
    chunk_size_bytes: int = 0
    memory_pct: float = 0.0
    pull_time: float = field(default_factory=time.time)
    worker_id: int = 0        # which CPU thread/process handled this chunk
    partition_id: int = 0     # which dataset partition this chunk belongs to

    @property
    def elapsed_ms(self) -> float:
        return (time.time() - self.pull_time) * 1000
