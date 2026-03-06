from typing import Any, Iterable, List

from .base import BaseSource


class GeneratorSource(BaseSource):
    """Wraps any Python iterable or generator as a pull-based source."""

    def __init__(self, iterable: Iterable[Any]):
        self._iter = iter(iterable)
        self._exhausted = False

    def pull(self, n: int) -> List[Any]:
        rows = []
        try:
            for _ in range(n):
                rows.append(next(self._iter))
        except StopIteration:
            self._exhausted = True
        return rows

    def is_exhausted(self) -> bool:
        return self._exhausted
