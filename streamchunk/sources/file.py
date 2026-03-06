import csv
import json
from typing import Any, List

from .base import BaseSource


class FileSource(BaseSource):
    """Reads CSV or JSON Lines files."""

    def __init__(self, path: str, format: str = "csv"):
        self.path = path
        self.format = format
        self._exhausted = False
        self._file = open(path, "r", encoding="utf-8")

        if format == "csv":
            self._reader = csv.DictReader(self._file)
        elif format == "jsonl":
            self._reader = (json.loads(line) for line in self._file)
        else:
            raise ValueError(f"Unsupported format: {format}. Use 'csv' or 'jsonl'.")

    def pull(self, n: int) -> List[Any]:
        rows = []
        try:
            for _ in range(n):
                rows.append(next(self._reader))
        except StopIteration:
            self._exhausted = True
        return rows

    def is_exhausted(self) -> bool:
        return self._exhausted

    def close(self):
        self._file.close()
