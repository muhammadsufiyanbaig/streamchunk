from abc import ABC, abstractmethod
from typing import Any, List


class BaseSource(ABC):
    @abstractmethod
    def pull(self, n: int) -> List[Any]:
        """Pull up to n rows from the source. Returns empty list when exhausted."""
        pass

    @abstractmethod
    def is_exhausted(self) -> bool:
        pass

    def close(self):
        pass
