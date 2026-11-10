"""Microsoft Entra ID sign-in, with MFA, for database connections.

A connection string's Authentication=ActiveDirectoryInteractive makes the ODBC
driver open a sign-in window on the machine it runs on. eyetable runs as a
service, with no screen, and loads on schedules, so it signs in with Microsoft's
device code flow instead: the page shows a short code, you enter it at
https://microsoft.com/devicelogin in your own browser and finish MFA there, and
eyetable keeps the sign-in (MSAL's token cache, encrypted with
EYETABLE_SECRET_KEY) to get fresh access tokens for later loads, until Entra's
own policies ask for a new sign-in.

The access token goes to the ODBC driver as SQL_COPT_SS_ACCESS_TOKEN, for
Azure SQL, Fabric/Synapse and Dataverse's SQL endpoint (*.crm.dynamics.com).

Sign-in uses the public client Microsoft's Azure SDKs sign developers in with;
set EYETABLE_ENTRA_CLIENT_ID to use your own app registration instead (a
public client with "Allow public client flows" on).
"""

import datetime as dt
import re
import secrets
import struct
import threading
from typing import Any, Callable

from cryptography.fernet import Fernet

DEFAULT_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
SQL_COPT_SS_ACCESS_TOKEN = 1256
INTERACTIVE = {"activedirectoryinteractive", "activedirectorydevicecode"}


class EntraSignInNeeded(ValueError):
    """The connection needs a Microsoft sign-in first; `request` says for whom and for what."""

    def __init__(self, message: str, request: dict[str, Any]):
        super().__init__(message)
        self.request = request


def scope_for(server: str) -> str:
    """What a token must be for: the Dataverse environment itself, or Azure SQL (also Fabric and Synapse)."""
    host = re.sub(r"^tcp:", "", server.strip().lower()).split(",")[0].split("\\")[0].split(":")[0]
    if re.search(r"\.crm\d*\.dynamics\.com$", host):
        return f"https://{host}/.default"
    return "https://database.windows.net/.default"


def authority_for(username: str | None) -> str:
    """The sign-in tenant: the user's own domain when known."""
    domain = (username or "").rpartition("@")[2].strip().lower()
    return f"https://login.microsoftonline.com/{domain or 'organizations'}"


def sign_in_request(username: str | None, server: str) -> dict[str, Any]:
    return {"username": username or None, "scope": scope_for(server), "authority": authority_for(username)}


def token_struct(token: str) -> bytes:
    """An access token the way the SQL Server ODBC driver takes it."""
    data = token.encode("utf-16-le")
    return struct.pack(f"<I{len(data)}s", len(data), data)


class EntraTokens:
    """Signs in with the device code flow, and hands out fresh access tokens from the kept sign-in."""

    def __init__(self, store, secret_key: str | None, client_id: str | None = None,
                 app_factory: Callable[..., Any] | None = None):
        self.store = store
        self.client_id = client_id or DEFAULT_CLIENT_ID
        self._fernet = Fernet(secret_key.encode()) if secret_key else None
        self._app_factory = app_factory
        self._lock = threading.Lock()
        self._flows: dict[str, dict[str, Any]] = {}

    def _cache(self):
        import msal

        cache = msal.SerializableTokenCache()
        stored = self.store.get_entra_cache()
        if stored and self._fernet:
            cache.deserialize(self._fernet.decrypt(stored.encode()).decode())
        return cache

    def _keep(self, cache) -> None:
        if cache.has_state_changed and self._fernet:
            self.store.save_entra_cache(self._fernet.encrypt(cache.serialize().encode()).decode())

    def _app(self, authority: str, cache):
        if self._app_factory:
            return self._app_factory(self.client_id, authority, cache)
        import msal

        return msal.PublicClientApplication(self.client_id, authority=authority, token_cache=cache)

    def signed_in(self, username: str | None, authority: str) -> bool:
        """Whether a sign-in is kept for this user (no network)."""
        with self._lock:
            app = self._app(authority, self._cache())
            return bool(app.get_accounts(username=username) if username else app.get_accounts())

    def token(self, username: str | None, scope: str, authority: str) -> str:
        """A fresh access token, from the kept sign-in; EntraSignInNeeded if there's none or it has expired."""
        request = {"username": username, "scope": scope, "authority": authority}
        who = username or "your Microsoft account"
        if not self._fernet:
            raise EntraSignInNeeded("Microsoft sign-in needs EYETABLE_SECRET_KEY set on the eyetable server", request)
        with self._lock:
            cache = self._cache()
            app = self._app(authority, cache)
            accounts = app.get_accounts(username=username) if username else app.get_accounts()
            if not accounts:
                raise EntraSignInNeeded(f"sign in with Microsoft as {who} to connect", request)
            result = app.acquire_token_silent([scope], account=accounts[0])
            self._keep(cache)
        if not result or "access_token" not in result:
            why = (result or {}).get("error_description", "").split("\n")[0].strip()
            raise EntraSignInNeeded(f"the Microsoft sign-in for {who} has expired; sign in again" + (f" ({why})" if why else ""), request)
        return result["access_token"]

    def start(self, username: str | None, scope: str, authority: str) -> dict[str, Any]:
        """Begin a device code sign-in; it finishes in the background when you enter the code."""
        if not self._fernet:
            raise ValueError("Microsoft sign-in needs EYETABLE_SECRET_KEY set on the eyetable server")
        cache = self._cache()
        app = self._app(authority, cache)
        flow = app.initiate_device_flow(scopes=[scope])
        if "user_code" not in flow:
            raise ValueError("Microsoft didn't start a sign-in: " + str(flow.get("error_description") or flow.get("error") or flow))
        flow_id = secrets.token_urlsafe(16)
        expires = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=int(flow.get("expires_in", 900)))
        state: dict[str, Any] = {"status": "pending", "user_code": flow["user_code"], "verification_uri": flow["verification_uri"],
                                 "expires_at": expires, "username": username, "account": None, "error": None}
        self._flows[flow_id] = state

        def finish() -> None:
            try:
                result = app.acquire_token_by_device_flow(flow)  # waits until you finish, or the code expires
            except Exception as exc:  # never let the thread die silently
                result = {"error": type(exc).__name__, "error_description": str(exc)}
            if "access_token" in result:
                with self._lock:
                    self._keep(cache)
                account = (result.get("id_token_claims") or {}).get("preferred_username")
                state.update(status="done", account=account)
                if username and account and account.lower() != username.lower():
                    state["error"] = f"signed in as {account}, but the connection string's user is {username}"
            else:
                why = str(result.get("error_description") or result.get("error") or "the sign-in didn't finish")
                state.update(status="failed", error=why.split("\n")[0].strip())

        threading.Thread(target=finish, name="eyetable-entra-sign-in", daemon=True).start()
        return {"flow_id": flow_id, **{k: state[k] for k in ("user_code", "verification_uri", "expires_at")},
                "message": flow.get("message")}

    def status(self, flow_id: str) -> dict[str, Any]:
        state = self._flows.get(flow_id)
        if state is None:
            raise KeyError("no such sign-in (eyetable may have restarted); start again")
        return {k: state[k] for k in ("status", "account", "error", "expires_at")}


_provider: EntraTokens | None = None


def set_provider(tokens: EntraTokens) -> None:
    global _provider
    _provider = tokens


def provider() -> EntraTokens:
    if _provider is None:
        raise ValueError("Microsoft sign-in isn't available here; connect through the eyetable API")
    return _provider
