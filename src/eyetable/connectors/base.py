from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any, ClassVar

from eyetable.models import DatasetInfo

Record = dict[str, Any]


class Connector(ABC):
    """A live connection to one source.

    `config` arrives with `${NAME}` secret references already resolved.
    """

    type: ClassVar[str]

    def __init__(self, config: dict[str, Any]):
        self.config = config

    @abstractmethod
    def test(self) -> None:
        """Raise if the source cannot be reached with this config."""

    @abstractmethod
    def discover(self) -> list[DatasetInfo]:
        """Walk the source's metadata and report every dataset it exposes."""

    @abstractmethod
    def read(self, key: str, batch_size: int = 1000) -> Iterator[list[Record]]:
        """Yield the rows of one dataset in batches."""

    def close(self) -> None:
        pass

    def __enter__(self) -> "Connector":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
