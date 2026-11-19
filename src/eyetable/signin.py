"""Signing in to the page with a Google or Microsoft (Entra ID) account instead of the API token.

The page signs the person in with Google or Microsoft and sends what it got to
POST /api/session:

  * Google: the short-lived access token from "Sign in with Google". The server
    asks Google's tokeninfo endpoint about it, and accepts it only if it was
    issued to eyetable's own OAuth client and carries a verified email.
  * Microsoft: the ID token from "Sign in with Microsoft". The server checks its
    signature against the tenant's published keys, that it was issued to
    eyetable's app registration (EYETABLE_MS_CLIENT_ID) in the expected tenant
    (EYETABLE_MS_TENANT_ID), and that it hasn't expired; the person is their
    sign-in name, an email address (preferred_username).

The email must be on the allow list, EYETABLE_ALLOWED_USERS: comma-separated
addresses, and "@example.com" for everyone at a domain. With no list, nobody
can sign in this way and the API token is the only way in.

The answer is a session: a token the page then uses in place of the API token,
good for EYETABLE_SESSION_HOURS (12 by default). It names the person and is
signed with a key made from the API token, so it survives restarts and
changing the API token ends every session.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
from dataclasses import dataclass
from typing import Any

import httpx

PREFIX = "ets1."
GOOGLE_TOKENINFO = "https://oauth2.googleapis.com/tokeninfo"
# The OAuth client of the page's "Sign in with Google" (public, like the page's own copy).
DEFAULT_GOOGLE_CLIENT_ID = "946481321542-o8rgrirru07iie5hlg892chjoj5329p7.apps.googleusercontent.com"
MICROSOFT_LOGIN = "https://login.microsoftonline.com"


class SignInError(ValueError):
    def __init__(self, message: str, status: int = 401):
        super().__init__(message)
        self.status = status


@dataclass
class Person:
    email: str
    provider: str  # "google" | "microsoft"
    name: str | None = None


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def parse_allowed(text: str | None) -> list[str]:
    return [x.strip().lower() for x in (text or "").replace(";", ",").split(",") if x.strip()]


class SignIn:
    def __init__(self, api_token: str, allowed: list[str] | str | None = None,
                 google_client_id: str | None = DEFAULT_GOOGLE_CLIENT_ID,
                 ms_client_id: str | None = None, ms_tenant_id: str | None = None,
                 session_hours: float = 12, client: httpx.Client | None = None):
        self.allowed = parse_allowed(allowed) if isinstance(allowed, str) or allowed is None else [a.lower() for a in allowed]
        self.google_client_id = google_client_id or None
        self.ms_client_id = ms_client_id or None
        self.ms_tenant_id = ms_tenant_id or None
        self.session_seconds = int(session_hours * 3600)
        self._key = hashlib.sha256(b"eyetable-session:" + api_token.encode()).digest()
        self._client = client
        self._jwks: Any = None
        self._lock = threading.Lock()

    # -- what the page may offer -------------------------------------------------

    def options(self) -> dict[str, Any]:
        """What the page needs to offer sign-in (none of it secret)."""
        return {
            "enabled": bool(self.allowed),
            "google": {"client_id": self.google_client_id} if self.google_client_id else None,
            "microsoft": {"client_id": self.ms_client_id, "tenant_id": self.ms_tenant_id}
            if self.ms_client_id and self.ms_tenant_id else None,
            "session_hours": self.session_seconds / 3600,
        }

    def is_allowed(self, email: str) -> bool:
        email = email.strip().lower()
        domain = "@" + email.rsplit("@", 1)[-1] if "@" in email else None
        return any(a == email or (a.startswith("@") and a == domain) for a in self.allowed)

    # -- checking what the page sent ------------------------------------------------

    def _http(self) -> httpx.Client:
        return self._client or httpx.Client(timeout=15)

    def verify_google(self, access_token: str) -> Person:
        if not self.google_client_id:
            raise SignInError("Google sign-in isn't set up on this server", 400)
        try:
            r = self._http().get(GOOGLE_TOKENINFO, params={"access_token": access_token})
        except httpx.HTTPError as exc:
            raise SignInError(f"couldn't reach Google to check the sign-in ({type(exc).__name__})", 502) from None
        if r.status_code != 200:
            raise SignInError("Google didn't accept that sign-in; sign in again")
        info = r.json()
        if self.google_client_id not in (info.get("aud"), info.get("azp")):
            raise SignInError("that Google sign-in was made for a different app")
        if str(info.get("email_verified")).lower() != "true" or not info.get("email"):
            raise SignInError("that Google account has no verified email address")
        return Person(info["email"].lower(), "google")

    def _signing_key(self, token: str):
        import jwt

        with self._lock:
            if self._jwks is None:
                self._jwks = jwt.PyJWKClient(f"{MICROSOFT_LOGIN}/{self.ms_tenant_id}/discovery/v2.0/keys",
                                             cache_keys=True, lifespan=3600)
            return self._jwks.get_signing_key_from_jwt(token).key

    def verify_microsoft(self, id_token: str, key: Any = None) -> Person:
        import jwt

        if not (self.ms_client_id and self.ms_tenant_id):
            raise SignInError("Microsoft sign-in isn't set up on this server (EYETABLE_MS_CLIENT_ID and "
                              "EYETABLE_MS_TENANT_ID)", 400)
        try:
            key = key if key is not None else self._signing_key(id_token)
            claims = jwt.decode(id_token, key, algorithms=["RS256"], audience=self.ms_client_id,
                                options={"require": ["exp", "iss", "aud", "tid"]}, leeway=60)
        except jwt.PyJWKClientError as exc:
            raise SignInError(f"couldn't get Microsoft's signing keys ({exc})", 502) from None
        except jwt.ExpiredSignatureError:
            raise SignInError("that Microsoft sign-in has expired; sign in again") from None
        except jwt.InvalidTokenError as exc:
            raise SignInError(f"Microsoft sign-in not accepted ({exc})") from None
        tenant = self.ms_tenant_id.lower()
        if tenant not in ("organizations", "common") and claims["tid"].lower() != tenant:
            raise SignInError("that Microsoft account is from a different organization")
        if claims["iss"] != f"{MICROSOFT_LOGIN}/{claims['tid']}/v2.0":
            raise SignInError("that Microsoft sign-in wasn't issued by Entra ID")
        email = claims.get("preferred_username") or claims.get("email") or claims.get("upn")
        if not email or "@" not in email:
            raise SignInError("that Microsoft account has no email-style sign-in name")
        return Person(email.lower(), "microsoft", claims.get("name"))

    def sign_in(self, provider: str, token: str) -> dict[str, Any]:
        """Check a sign-in and, for someone on the allow list, start a session."""
        if not self.allowed:
            raise SignInError("signing in with an account isn't switched on: no one is on the allow list "
                              "(EYETABLE_ALLOWED_USERS); use the API token", 403)
        if provider == "google":
            person = self.verify_google(token)
        elif provider == "microsoft":
            person = self.verify_microsoft(token)
        else:
            raise SignInError("provider is google or microsoft", 400)
        if not self.is_allowed(person.email):
            raise SignInError(f"{person.email} isn't allowed to use eyetable; ask to be added to EYETABLE_ALLOWED_USERS", 403)
        return self.session_for(person)

    # -- sessions -----------------------------------------------------------------

    def session_for(self, person: Person, now: float | None = None) -> dict[str, Any]:
        expires = int((now or time.time()) + self.session_seconds)
        payload = _b64(json.dumps({"e": person.email, "p": person.provider, "n": person.name, "x": expires},
                                  separators=(",", ":")).encode())
        signature = _b64(hmac.new(self._key, payload.encode(), hashlib.sha256).digest())
        return {"session": f"{PREFIX}{payload}.{signature}", "email": person.email, "provider": person.provider,
                "name": person.name, "expires": expires}

    def check_session(self, token: str, now: float | None = None) -> Person | None:
        """The person a session token stands for, or None (not a session, tampered with, expired,
        or no longer on the allow list)."""
        if not token.startswith(PREFIX):
            return None
        try:
            payload, signature = token[len(PREFIX):].split(".")
            expected = _b64(hmac.new(self._key, payload.encode(), hashlib.sha256).digest())
            if not hmac.compare_digest(signature, expected):
                return None
            data = json.loads(_unb64(payload))
        except (ValueError, TypeError):
            return None
        if data.get("x", 0) < (now or time.time()) or not self.is_allowed(data.get("e", "")):
            return None
        return Person(data["e"], data.get("p", "?"), data.get("n"))
