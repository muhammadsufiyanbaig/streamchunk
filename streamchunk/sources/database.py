from typing import Any, List

from .base import BaseSource


class DatabaseSource(BaseSource):
    """
    Streams rows from a SQL query via SQLAlchemy.
    Requires: pip install streamchunk[database]
    """

    def __init__(self, query: str, connection_string: str, **engine_kwargs):
        try:
            from sqlalchemy import create_engine, text
        except ImportError:
            raise ImportError(
                "SQLAlchemy is required. Install with: pip install streamchunk[database]"
            )

        engine = create_engine(connection_string, **engine_kwargs)
        self._conn = engine.connect()
        self._result = self._conn.execute(text(query))
        self._exhausted = False

    def pull(self, n: int) -> List[Any]:
        rows = self._result.fetchmany(n)
        if not rows:
            self._exhausted = True
            return []
        return [dict(r._mapping) for r in rows]

    def is_exhausted(self) -> bool:
        return self._exhausted

    def close(self):
        self._conn.close()
