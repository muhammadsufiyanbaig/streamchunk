import csv
import io
import json
from typing import Any, List, Optional

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


class RangedFileSource(BaseSource):
    """
    Reads a specific **byte-range** of a CSV or JSONL file.

    Used internally by :class:`~streamchunk.parallel.ParallelFileChunker` to
    give each worker its own independent slice of a large file.  Workers never
    read the same bytes twice and the file is never fully loaded into memory.

    How byte boundaries are handled
    --------------------------------
    Byte offsets almost never align perfectly with line endings.  This class
    uses the same strategy as Hadoop ``TextInputFormat``:

    * **Worker 0** (``start_byte == 0`` or the byte right after the CSV
      header) starts reading from its exact offset — it is guaranteed to be
      at a line boundary.
    * **Workers 1 … N-1** seek to their ``start_byte``, then call one
      ``readline()`` to skip the remainder of the partial line that the
      previous worker owns.  Reading then continues from the next complete
      line.
    * Each worker reads lines whose **start position** is ``< end_byte``.
      Once the file pointer crosses ``end_byte`` the source signals
      exhaustion, so the next worker's skipped partial line is exactly the
      line that would have crossed the boundary.

    Parameters
    ----------
    path : str
        Path to the file.
    start_byte : int
        Byte offset at which this worker begins reading.
    end_byte : int
        Byte offset at which this worker stops (exclusive).  The last byte
        read may be slightly past this value (to finish the current line).
    format : str
        ``"csv"`` or ``"jsonl"``.
    headers : list of str, optional
        Column names for CSV files.  When provided every row is returned as
        a ``dict``; when ``None`` rows are returned as plain lists.
        Ignored for JSONL.

    Notes
    -----
    CSV rows with embedded newlines inside quoted fields are **not** supported
    by this class (the file is read line-by-line in binary mode for accurate
    seeking).  Standard tabular CSV files without multiline fields work
    correctly.
    """

    def __init__(
        self,
        path: str,
        start_byte: int,
        end_byte: int,
        format: str = "csv",
        headers: Optional[List[str]] = None,
        skip_partial_line: bool = True,
    ):
        if format not in ("csv", "jsonl"):
            raise ValueError(f"Unsupported format: {format}. Use 'csv' or 'jsonl'.")

        self._path = path
        self._end_byte = end_byte
        self._format = format
        self._headers = headers
        self._exhausted = False

        # Open in binary mode so seek() gives accurate byte positions.
        self._f = open(path, "rb")
        self._f.seek(start_byte)

        # Workers that start mid-file must skip the partial (broken) line that
        # belongs to the previous partition.  Workers whose start_byte lands
        # exactly on a line boundary (e.g. partition 0 starting right after the
        # CSV header) must NOT skip — hence the ``skip_partial_line`` flag.
        if skip_partial_line:
            self._f.readline()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_raw_line(self) -> Optional[bytes]:
        """Return the next raw line, or None if past end_byte or EOF."""
        if self._f.tell() >= self._end_byte:
            return None
        line = self._f.readline()
        return line if line else None

    # ------------------------------------------------------------------
    # BaseSource interface
    # ------------------------------------------------------------------

    def pull(self, n: int) -> List[Any]:
        rows: List[Any] = []

        if self._format == "csv":
            # Collect raw lines then parse through csv.reader for correct
            # comma / quote handling.
            raw_lines: List[str] = []
            for _ in range(n):
                raw = self._read_raw_line()
                if raw is None:
                    self._exhausted = True
                    break
                decoded = raw.decode("utf-8")
                raw_lines.append(decoded)

            if raw_lines:
                reader = csv.reader(io.StringIO("".join(raw_lines)))
                for row_values in reader:
                    if self._headers:
                        rows.append(dict(zip(self._headers, row_values)))
                    else:
                        rows.append(row_values)

        elif self._format == "jsonl":
            for _ in range(n):
                raw = self._read_raw_line()
                if raw is None:
                    self._exhausted = True
                    break
                line = raw.decode("utf-8").strip()
                if line:
                    rows.append(json.loads(line))

        if not rows and not self._exhausted:
            self._exhausted = True

        return rows

    def is_exhausted(self) -> bool:
        return self._exhausted

    def close(self):
        self._f.close()
