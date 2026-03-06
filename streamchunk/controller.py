import psutil


class AdaptiveSizeController:
    """
    PID-inspired controller that adjusts chunk size based on
    memory pressure and latency feedback.
    """

    def __init__(
        self,
        target_latency_ms: float,
        max_memory_pct: float,
        min_chunk_size: int,
        max_chunk_size: int,
        initial_chunk_size: int,
        kp: float = 0.3,   # proportional gain
        ki: float = 0.05,  # integral gain
        kd: float = 0.1    # derivative gain
    ):
        self.target_latency_ms = target_latency_ms
        self.max_memory_pct = max_memory_pct
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        self.current_size = initial_chunk_size
        self.kp, self.ki, self.kd = kp, ki, kd

        self._integral = 0.0
        self._prev_error = 0.0
        self._last_latency = target_latency_ms

    def next_chunk_size(self) -> int:
        """Called before pulling each chunk. Returns recommended size."""
        mem = psutil.virtual_memory().percent

        # Memory override: hard ceiling
        if mem >= self.max_memory_pct:
            self.current_size = max(self.min_chunk_size, self.current_size // 2)
            return self.current_size

        # PID step based on last known latency
        error = self.target_latency_ms - self._last_latency
        self._integral += error
        derivative = error - self._prev_error
        self._prev_error = error

        adjustment = int(self.kp * error + self.ki * self._integral + self.kd * derivative)
        self.current_size = max(self.min_chunk_size,
                                min(self.max_chunk_size,
                                    self.current_size + adjustment))
        return self.current_size

    def report_latency(self, latency_ms: float):
        """Called after chunk processing. Feeds actual latency into PID."""
        self._last_latency = latency_ms
