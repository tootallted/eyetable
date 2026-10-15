"""`${NAME}` references in source configs, resolved from the environment at run time.

Source configs are stored in the core database with the references intact, so a
password or API key only ever lives in the environment (or a .env file).
"""

import os
import re
from collections.abc import Mapping
from typing import Any

_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class MissingSecretError(KeyError):
    def __init__(self, names: list[str]):
        self.names = names
        super().__init__(f"environment variable(s) not set: {', '.join(names)}")

    def __str__(self) -> str:
        return self.args[0]


def env_ref(name: str) -> str:
    return "${" + name + "}"


def resolve_env(value: Any, environ: Mapping[str, str] | None = None) -> Any:
    """Return a copy of `value` with every `${NAME}` replaced from `environ`."""
    environ = os.environ if environ is None else environ
    missing: set[str] = set()

    def sub(match: re.Match) -> str:
        name = match.group(1)
        if name in environ:
            return environ[name]
        missing.add(name)
        return match.group(0)

    def walk(v: Any) -> Any:
        if isinstance(v, str):
            return _ENV_REF.sub(sub, v)
        if isinstance(v, dict):
            return {k: walk(x) for k, x in v.items()}
        if isinstance(v, list):
            return [walk(x) for x in v]
        return v

    resolved = walk(value)
    if missing:
        raise MissingSecretError(sorted(missing))
    return resolved
