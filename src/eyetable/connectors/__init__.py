from typing import Any

from eyetable.connectors.base import Connector
from eyetable.connectors.database import DatabaseConnector
from eyetable.connectors.postman import PostmanConnector

CONNECTORS: dict[str, type[Connector]] = {
    DatabaseConnector.type: DatabaseConnector,
    PostmanConnector.type: PostmanConnector,
}


def build_connector(type_: str, config: dict[str, Any]) -> Connector:
    try:
        cls = CONNECTORS[type_]
    except KeyError:
        raise ValueError(f"unknown source type {type_!r}; known: {', '.join(sorted(CONNECTORS))}") from None
    return cls(config)


__all__ = ["CONNECTORS", "Connector", "DatabaseConnector", "PostmanConnector", "build_connector"]
