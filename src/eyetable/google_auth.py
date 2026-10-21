"""A long-lived Google authorization, so scheduled runs can read private sheets.

The page's "Connect Google for scheduled runs" button runs Google's
authorization-code popup (Google Identity Services, redirect_uri
"postmessage") and sends the one-time code here. The code is exchanged, with
the OAuth client's secret, for a refresh token, which is stored encrypted
(Fernet, key EYETABLE_SECRET_KEY) in the eyetable schema, out of reach of the
query window's read-only role. Whenever a sheet must be read without someone
signed in, a one-hour access token is minted from it (and cached until it is
about to expire). Disconnecting revokes the token at Google and deletes it.

Configuration (.env): GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, EYETABLE_SECRET_KEY.
"""

import datetime as dt
import threading
from typing import Any

import httpx
from cryptography.fernet import Fernet, InvalidToken

from eyetable.store import CoreStore

TOKEN_URL = "https://oauth2.googleapis.com/token"
REVOKE_URL = "https://oauth2.googleapis.com/revoke"
USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"
SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"


class GoogleAuthError(Exception):
    pass


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


class GoogleConnection:
    def __init__(
        self,
        store: CoreStore,
        client_id: str | None,
        client_secret: str | None,
        secret_key: str | None,
        client: httpx.Client | None = None,
    ):
        self.store = store
        self.client_id = client_id or ""
        self.client_secret = client_secret or ""
        self._fernet = Fernet(secret_key.encode()) if secret_key else None
        self._http = client or httpx.Client(timeout=30)
        self._lock = threading.Lock()
        self._access: tuple[str, dt.datetime] | None = None  # cached access token and its expiry

    # -- state ---------------------------------------------------------------

    @property
    def configured(self) -> bool:
        return bool(self.client_id and self.client_secret and self._fernet)

    def missing_settings(self) -> list[str]:
        return [name for name, value in (
            ("GOOGLE_CLIENT_ID", self.client_id),
            ("GOOGLE_CLIENT_SECRET", self.client_secret),
            ("EYETABLE_SECRET_KEY", self._fernet),
        ) if not value]

    def status(self) -> dict[str, Any]:
        row = self.store.get_google_connection()
        out: dict[str, Any] = {"configured": self.configured, "missing": self.missing_settings(), "connected": False}
        if row:
            out.update(connected=row["status"] == "ok", email=row["email"], connected_at=row["connected_at"],
                       last_used_at=row["last_used_at"], status=row["status"], error=row["error"])
        return out

    def is_connected(self) -> bool:
        row = self.store.get_google_connection()
        return bool(self.configured and row and row["status"] == "ok")

    # -- connecting ----------------------------------------------------------

    def _post_token(self, data: dict[str, str]) -> dict[str, Any]:
        try:
            response = self._http.post(TOKEN_URL, data={**data, "client_id": self.client_id, "client_secret": self.client_secret})
        except httpx.HTTPError as exc:
            raise GoogleAuthError(f"couldn't reach Google ({type(exc).__name__})") from None
        body = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        if response.status_code != 200:
            raise GoogleAuthError(body.get("error_description") or body.get("error") or f"Google answered {response.status_code}")
        return body

    def connect(self, code: str) -> dict[str, Any]:
        """Exchange the page's one-time code and keep the refresh token."""
        if not self.configured:
            raise GoogleAuthError("Google access for scheduled runs isn't set up on the server: set "
                                  + ", ".join(self.missing_settings()) + " in .env")
        tokens = self._post_token({"code": code, "grant_type": "authorization_code", "redirect_uri": "postmessage"})
        if SHEETS_SCOPE not in tokens.get("scope", "").split():
            raise GoogleAuthError("Google didn't grant read access to Sheets; connect again and allow it")
        refresh = tokens.get("refresh_token")
        if not refresh:
            raise GoogleAuthError(
                "Google didn't issue a long-lived token, usually because eyetable was already authorized. "
                "Remove eyetable at https://myaccount.google.com/connections and connect again."
            )
        email = self._email(tokens["access_token"])
        old = self.store.get_google_connection()
        self.store.save_google_connection(email, self._fernet.encrypt(refresh.encode()).decode(), tokens.get("scope", ""))
        with self._lock:
            self._access = (tokens["access_token"], _now() + dt.timedelta(seconds=int(tokens.get("expires_in", 3600)) - 120))
        if old and old["refresh_token"]:  # a replaced connection shouldn't stay valid at Google
            self._revoke(old["refresh_token"])
        return self.status()

    def _email(self, access_token: str) -> str:
        try:
            r = self._http.get(USERINFO_URL, headers={"Authorization": f"Bearer {access_token}"})
            return r.json().get("email", "") if r.status_code == 200 else ""
        except (httpx.HTTPError, ValueError):
            return ""

    def _revoke(self, encrypted: str) -> None:
        try:
            token = self._fernet.decrypt(encrypted.encode()).decode()
            self._http.post(REVOKE_URL, data={"token": token})
        except (InvalidToken, httpx.HTTPError, AttributeError):
            pass  # best effort: the stored copy is deleted either way

    def disconnect(self) -> None:
        row = self.store.get_google_connection()
        if row and row["refresh_token"] and self._fernet:
            self._revoke(row["refresh_token"])
        self.store.delete_google_connection()
        with self._lock:
            self._access = None

    # -- using it ------------------------------------------------------------

    def access_token(self) -> str:
        """A valid access token for the connected account, refreshing when needed."""
        with self._lock:
            if self._access and _now() < self._access[1]:
                self.store.touch_google_connection()
                return self._access[0]
            row = self.store.get_google_connection()
            if not self.configured or not row:
                raise GoogleAuthError("no Google account is connected for scheduled runs")
            if row["status"] != "ok":
                raise GoogleAuthError(f"the Google connection needs reconnecting: {row['error']}")
            try:
                refresh = self._fernet.decrypt(row["refresh_token"].encode()).decode()
            except InvalidToken:
                self.store.set_google_connection_status("broken", "the stored token can't be decrypted (EYETABLE_SECRET_KEY changed?)")
                raise GoogleAuthError("the stored Google token can't be decrypted; connect again") from None
            try:
                tokens = self._post_token({"refresh_token": refresh, "grant_type": "refresh_token"})
            except GoogleAuthError as exc:
                if "invalid_grant" in str(exc) or "expired or revoked" in str(exc):
                    self.store.set_google_connection_status("broken", f"Google refused the stored token ({exc})")
                raise GoogleAuthError(f"couldn't renew Google access: {exc}") from None
            self._access = (tokens["access_token"], _now() + dt.timedelta(seconds=int(tokens.get("expires_in", 3600)) - 120))
            self.store.touch_google_connection()
            return self._access[0]
