"""Client certificates (mutual TLS) for API sources.

Some APIs only answer callers that present a certificate. It is given the
way Postman asks for it: a host (and optional port) it is for, a PFX file or
CRT and KEY files, a passphrase if they have one, and a CA certificate if the
server's own certificate comes from a private authority.

The files are turned into plain PEM, which is stored encrypted with
EYETABLE_SECRET_KEY like any other secret, so the passphrase isn't kept and
reloads don't need the original files. The certificate is only ever presented
to its host: a next-page link or redirect elsewhere doesn't get it.
"""

import datetime as dt
import fnmatch
import os
import re
import ssl
import tempfile
import urllib.parse
from typing import Any

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import serialization as ser
from cryptography.hazmat.primitives.serialization import pkcs12

_PEM_BLOCK = re.compile(rb"-----BEGIN ([A-Z0-9 ]+)-----.*?-----END \1-----", re.S)


class CertificateError(ValueError):
    pass


def _read_pkcs12(data: bytes, password: bytes | None) -> tuple[Any, list[x509.Certificate]]:
    try:
        key, certificate, extra = pkcs12.load_key_and_certificates(data, password)
    except ValueError:
        raise CertificateError(
            "that certificate wouldn't open: check its passphrase (and that it is a .pfx, .p12 or .pem file)"
        ) from None
    return key, ([certificate] if certificate else []) + list(extra)


def _read_key(block: bytes, password: bytes | None) -> Any:
    try:
        return ser.load_pem_private_key(block, password)
    except TypeError:
        if password is None:
            raise CertificateError("that private key needs its passphrase") from None
        return ser.load_pem_private_key(block, None)  # a passphrase was given, but the key has none
    except ValueError:
        raise CertificateError("that private key wouldn't open: check its passphrase") from None


def _read_pem(data: bytes, password: bytes | None) -> tuple[Any, list[x509.Certificate]]:
    key, certificates = None, []
    for match in _PEM_BLOCK.finditer(data):
        kind = match.group(1)
        if kind == b"CERTIFICATE":
            certificates.append(x509.load_pem_x509_certificate(match.group(0)))
        elif kind.endswith(b"PRIVATE KEY"):
            key = _read_key(match.group(0), password)
        elif kind.endswith(b"CERTIFICATE REQUEST") and not certificates:
            raise CertificateError("that file is a certificate request, not a certificate; use the .pfx that came back from it")
    return key, certificates


def _public(item: Any) -> bytes:
    return item.public_key().public_bytes(ser.Encoding.DER, ser.PublicFormat.SubjectPublicKeyInfo)


def to_pem(data: bytes, passphrase: str | None = None) -> str:
    """A certificate file as PEM: the unencrypted private key, its certificate, then the rest of the chain."""
    password = passphrase.encode() if passphrase else None
    key, certificates = (_read_pem if b"-----BEGIN" in data else _read_pkcs12)(data, password)
    if key is None:
        raise CertificateError("that file has no private key; use the .pfx, or choose the certificate and key files together")
    if not certificates:
        raise CertificateError("that file has a private key but no certificate; choose both files together")
    own = [c for c in certificates if _public(c) == _public(key)]
    if not own:
        raise CertificateError("that certificate and private key don't belong together")
    chain = own[:1] + [c for c in certificates if c is not own[0]]
    pem = key.private_bytes(ser.Encoding.PEM, ser.PrivateFormat.PKCS8, ser.NoEncryption())
    return (pem + b"".join(c.public_bytes(ser.Encoding.PEM) for c in chain)).decode()


def describe(pem: str) -> dict[str, Any]:
    """Who a certificate (from `to_pem`) identifies, and until when."""
    certificate = x509.load_pem_x509_certificates(pem.encode())[0]
    names = certificate.subject.get_attributes_for_oid(x509.NameOID.COMMON_NAME)
    return {
        "subject": str(names[0].value) if names else certificate.subject.rfc4514_string(),
        "expires": certificate.not_valid_after_utc,
        "expired": certificate.not_valid_after_utc < dt.datetime.now(dt.timezone.utc),
    }


def _clean_host(host: str | None, port: int | None) -> tuple[str | None, int | None]:
    """'https://api.example.com:8443/x' -> ('api.example.com', 8443); blank means the command's own host."""
    text = re.sub(r"^[a-z]+://", "", (host or "").strip().lower()).split("/")[0]
    name, _, given = text.partition(":")
    if name and not re.fullmatch(r"[a-z0-9*.-]+", name):
        raise CertificateError(f"{host!r} isn't a host name; use something like api.example.com")
    if given and not given.isdigit():
        raise CertificateError(f"{host!r} has an invalid port")
    return name or None, port or (int(given) if given else None)


def _ca_pem(data: bytes) -> str:
    try:
        if b"-----BEGIN" in data:
            return "".join(c.public_bytes(ser.Encoding.PEM).decode() for c in x509.load_pem_x509_certificates(data))
        return x509.load_der_x509_certificate(data).public_bytes(ser.Encoding.PEM).decode()
    except ValueError:
        raise CertificateError("the CA certificate file isn't a certificate") from None


def client_cert(pfx: bytes | None = None, crt: bytes | None = None, key: bytes | None = None, ca: bytes | None = None,
                passphrase: str | None = None, host: str | None = None, port: int | None = None) -> dict[str, Any]:
    """The certificate as a source keeps it: {"pem", "host", "port", "ca"}."""
    if pfx and (crt or key):
        raise CertificateError("choose a PFX file, or CRT and KEY files, not both")
    data = pfx or b"\n".join(part for part in (crt, key) if part)
    if not data:
        raise CertificateError("choose a PFX file, or CRT and KEY files")
    name, port = _clean_host(host, port)
    return {"pem": to_pem(data, passphrase), "host": name, "port": port, "ca": _ca_pem(ca) if ca else None}


def covers(certificate: dict[str, Any], url: str) -> bool:
    """Whether a request to `url` would present the certificate."""
    parts = urllib.parse.urlsplit(url)
    if not fnmatch.fnmatch((parts.hostname or "").lower(), certificate["host"]):
        return False
    return not certificate.get("port") or certificate["port"] == (parts.port or 443)


def mounts(certificate: dict[str, Any]) -> dict[str, httpx.HTTPTransport]:
    """httpx mounts that present the certificate to its host, and nowhere else."""
    address = certificate["host"] + (f":{certificate['port']}" if certificate.get("port") else "")
    return {f"https://{address}": httpx.HTTPTransport(verify=ssl_context(certificate["pem"], certificate.get("ca")))}


def ssl_context(pem: str, ca: str | None = None) -> ssl.SSLContext:
    """An HTTPS context that presents the certificate; the usual server checks still apply."""
    context = httpx.create_ssl_context()
    if ca:
        context.load_verify_locations(cadata=ca)
    handle, path = tempfile.mkstemp(suffix=".pem")  # ssl only loads these from a file; readable only by this user
    try:
        with os.fdopen(handle, "w") as file:
            file.write(pem)
        context.load_cert_chain(path)
    finally:
        os.unlink(path)
    return context
