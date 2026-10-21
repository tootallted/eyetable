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


# -- secrets stored encrypted in a source config ------------------------------
# Values captured from a pasted command (a bearer token, an API key) are kept in
# the source config as "eyetable-enc:<Fernet token>", encrypted with
# EYETABLE_SECRET_KEY, and decrypted only when a connector is opened.

ENC_PREFIX = "eyetable-enc:"


class SecretKeyError(ValueError):
    pass


def _fernet(key: str | None):
    from cryptography.fernet import Fernet

    if not key:
        raise SecretKeyError("EYETABLE_SECRET_KEY is not set (.env), so secrets can't be encrypted or read")
    return Fernet(key.encode())


def encrypt_secret(value: str, key: str | None) -> str:
    return ENC_PREFIX + _fernet(key).encrypt(value.encode()).decode()


def decrypt_secrets(value: Any, key: str | None) -> Any:
    """A copy of `value` with every encrypted string decrypted."""
    from cryptography.fernet import InvalidToken

    fernet = None

    def walk(v: Any) -> Any:
        nonlocal fernet
        if isinstance(v, str) and v.startswith(ENC_PREFIX):
            fernet = fernet or _fernet(key)
            try:
                return fernet.decrypt(v[len(ENC_PREFIX):].encode()).decode()
            except InvalidToken:
                raise SecretKeyError("a stored secret can't be decrypted (EYETABLE_SECRET_KEY changed?)") from None
        if isinstance(v, dict):
            return {k: walk(x) for k, x in v.items()}
        if isinstance(v, list):
            return [walk(x) for x in v]
        return v

    return walk(value)
