import time
from typing import Any, Dict, List, Optional

from .base import BaseSource


class APISource(BaseSource):
    """
    Reads from a paginated REST API.
    Each pull() call fetches one page; pagination is handled automatically.

    Requires: pip install requests
    """

    def __init__(
        self,
        url: str,
        page_param: str = "page",
        size_param: str = "page_size",
        data_key: str = "data",
        headers: Optional[Dict[str, str]] = None,
        delay_s: float = 0.0,
    ):
        try:
            import requests
        except ImportError:
            raise ImportError("requests is required. Install with: pip install requests")

        self._url = url
        self._page_param = page_param
        self._size_param = size_param
        self._data_key = data_key
        self._delay_s = delay_s
        self._page = 1
        self._exhausted = False

        import requests as req
        self._session = req.Session()
        self._session.headers.update(headers or {})

    def pull(self, n: int) -> List[Any]:
        if self._exhausted:
            return []

        if self._delay_s:
            time.sleep(self._delay_s)

        params = {self._page_param: self._page, self._size_param: n}
        resp = self._session.get(self._url, params=params)
        resp.raise_for_status()

        body = resp.json()
        rows = body.get(self._data_key, body) if isinstance(body, dict) else body

        if not rows:
            self._exhausted = True
            return []

        self._page += 1
        return rows

    def is_exhausted(self) -> bool:
        return self._exhausted

    def close(self):
        self._session.close()
