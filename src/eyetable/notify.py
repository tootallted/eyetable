"""Emailing the result of a group run to the addresses set on the group.

The eyetable server sends the email through an SMTP server you name in .env:

    EYETABLE_SMTP_HOST=smtp.office365.com     (or smtp.gmail.com, your relay, ...)
    EYETABLE_SMTP_PORT=587                    (465 for SSL; 25 for a plain relay)
    EYETABLE_SMTP_USER=reports@example.com    (leave out for a relay that takes no sign-in)
    EYETABLE_SMTP_PASSWORD=...                (an app password, where the provider has them)
    EYETABLE_SMTP_FROM=reports@example.com    (defaults to the user)
    EYETABLE_SMTP_SECURITY=starttls           (starttls, ssl or none; port 465 means ssl)

Without EYETABLE_SMTP_HOST nothing is sent, and the group run records why.
"""

from __future__ import annotations

import datetime as dt
import html
import os
import re
import smtplib
import ssl
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

MAX_RECIPIENTS = 20
_ADDRESS = re.compile(r"^[^@\s,;<>\"]+@[^@\s,;<>\"]+\.[^@\s,;<>\".]{2,}$")
STATUS_WORDS = {"succeeded": "succeeded", "partial": "partly failed", "failed": "failed"}
TRIGGER_WORDS = {"manual": "Run now", "schedule": "On its schedule", "catch-up": "On its schedule (catching up a missed run)"}


class NotifyError(ValueError):
    pass


def parse_recipients(text: str | None) -> list[str]:
    """Addresses from "a@x.com, b@y.com" (commas, semicolons or spaces between); [] for none."""
    out: list[str] = []
    for part in re.split(r"[,;\s]+", (text or "").strip()):
        if not part:
            continue
        if not _ADDRESS.match(part) or len(part) > 254:
            raise NotifyError(f"{part!r} isn't an email address")
        if part.lower() not in (a.lower() for a in out):
            out.append(part)
    if len(out) > MAX_RECIPIENTS:
        raise NotifyError(f"at most {MAX_RECIPIENTS} addresses")
    return out


def clean_recipients(text: str | None) -> str | None:
    """The addresses as they're saved on a group: "a@x.com, b@y.com", or None for none."""
    return ", ".join(parse_recipients(text)) or None


@dataclass
class Mailer:
    host: str
    port: int = 587
    user: str | None = None
    password: str | None = None
    sender: str | None = None
    security: str = "starttls"  # "starttls" | "ssl" | "none"
    timeout: float = 30

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Mailer | None:
        env = os.environ if env is None else env
        host = (env.get("EYETABLE_SMTP_HOST") or "").strip()
        if not host:
            return None
        port = int(env.get("EYETABLE_SMTP_PORT") or 587)
        security = (env.get("EYETABLE_SMTP_SECURITY") or ("ssl" if port == 465 else "starttls")).strip().lower()
        if security not in ("starttls", "ssl", "none"):
            raise NotifyError("EYETABLE_SMTP_SECURITY must be starttls, ssl or none")
        user = (env.get("EYETABLE_SMTP_USER") or "").strip() or None
        sender = (env.get("EYETABLE_SMTP_FROM") or "").strip() or user
        if not sender:
            raise NotifyError("set EYETABLE_SMTP_FROM (or EYETABLE_SMTP_USER) to the address emails come from")
        return cls(host, port, user, env.get("EYETABLE_SMTP_PASSWORD") or None, sender, security)

    def send(self, to: list[str], subject: str, text: str, html_body: str | None = None) -> None:
        msg = EmailMessage()
        msg["From"] = self.sender
        msg["To"] = ", ".join(to)
        msg["Subject"] = subject
        msg["Date"] = formatdate(localtime=True)
        msg["Message-ID"] = make_msgid(domain=self.sender.rpartition("@")[2] or None)
        msg.set_content(text, cte="quoted-printable")  # 7-bit safe through any relay
        if html_body:
            msg.add_alternative(html_body, subtype="html", cte="quoted-printable")
        context = ssl.create_default_context()
        if self.security == "ssl":
            server: smtplib.SMTP = smtplib.SMTP_SSL(self.host, self.port, timeout=self.timeout, context=context)
        else:
            server = smtplib.SMTP(self.host, self.port, timeout=self.timeout)
        with server:
            if self.security == "starttls":
                server.starttls(context=context)
            if self.user and self.password:
                server.login(self.user, self.password)
            server.send_message(msg)


def describe_error(exc: Exception) -> str:
    """A short reason an email wasn't sent, without anything secret."""
    if isinstance(exc, smtplib.SMTPAuthenticationError):
        return "the SMTP server refused the sign-in (check EYETABLE_SMTP_USER and EYETABLE_SMTP_PASSWORD)"
    if isinstance(exc, smtplib.SMTPRecipientsRefused):
        return "the SMTP server refused the address(es): " + ", ".join(exc.recipients)
    if isinstance(exc, smtplib.SMTPSenderRefused):
        return f"the SMTP server refused the sender {exc.sender}"
    if isinstance(exc, smtplib.SMTPException):
        return f"the SMTP server said: {exc}"
    if isinstance(exc, OSError):
        return f"couldn't reach the SMTP server ({exc.strerror or exc})"
    return str(exc) or type(exc).__name__


# -- the message ---------------------------------------------------------------


def _zone(tz: str | None) -> dt.tzinfo | None:
    if tz:
        try:
            return ZoneInfo(tz)
        except (ZoneInfoNotFoundError, ValueError):
            pass
    return None  # astimezone(None): the server's local time


def _when(t: dt.datetime | None, zone: dt.tzinfo | None) -> str:
    if t is None:
        return "—"
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    local = t.astimezone(zone)
    return local.strftime("%Y-%m-%d %H:%M:%S ") + (local.tzname() or "")


def duration_text(seconds: float) -> str:
    seconds = max(0, int(round(seconds)))
    h, rest = divmod(seconds, 3600)
    m, s = divmod(rest, 60)
    if h:
        return f"{h} h {m} min {s} s"
    if m:
        return f"{m} min {s} s"
    return f"{s} s"


def group_run_stats(run: Any) -> dict[str, Any]:
    results = run.results or []
    finished = run.finished_at or dt.datetime.now(dt.timezone.utc)
    return {
        "rows": sum(int(r.get("rows") or 0) for r in results),
        "tables": sum(int(r.get("tables") or 0) for r in results),
        "sources": len(results),
        "succeeded": sum(1 for r in results if r.get("status") == "succeeded"),
        "skipped": sum(1 for r in results if r.get("status") == "skipped"),
        "failed": sum(1 for r in results if r.get("status") == "failed"),
        "seconds": (finished - run.started_at).total_seconds() if run.started_at else 0,
    }


def group_run_message(group_name: str, run: Any, how: str | None = None,
                      timezone: str | None = None) -> tuple[str, str, str]:
    """(subject, plain text, HTML) for a finished group run."""
    zone = _zone(timezone)
    st = group_run_stats(run)
    outcome = STATUS_WORDS.get(run.status, run.status)
    subject = f"eyetable: {group_name} {outcome}"
    counts = f"{st['succeeded']} succeeded" + (f", {st['failed']} failed" if st["failed"] else "") + \
             (f", {st['skipped']} skipped (hidden)" if st["skipped"] else "")
    facts = [
        ("Group", group_name),
        ("Result", outcome[0].upper() + outcome[1:]),
        ("Started", _when(run.started_at, zone)),
        ("Finished", _when(run.finished_at, zone)),
        ("Duration", duration_text(st["seconds"])),
        ("Rows processed", f"{st['rows']:,}"),
        ("Tables populated", f"{st['tables']:,}"),
        ("Sources", f"{st['sources']} ({counts})"),
        ("Started by", how or TRIGGER_WORDS.get(run.trigger, run.trigger)),
        ("Run id", str(run.id)),
    ]
    rows = [(r.get("source", "?"), r.get("status", "?"), int(r.get("tables") or 0), int(r.get("rows") or 0),
             r.get("load") or "", r.get("error") or "") for r in run.results or []]

    width = max(len(k) for k, _ in facts)
    text = [f"{k + ':':<{width + 1}} {v}" for k, v in facts]
    if rows:
        text += ["", "By source:"]
        for source, status, tables, n, load, error in rows:
            line = f"  {source}: {status}, {tables:,} table{'s' if tables != 1 else ''}, {n:,} row{'s' if n != 1 else ''}"
            line += f" ({load})" if load else ""
            text.append(line + (f" — {error}" if error else ""))

    e = html.escape
    color = {"succeeded": "#146c43", "partial": "#9a6700", "failed": "#b42318"}.get(run.status, "#1d2330")
    fact_rows = "".join(
        f'<tr><th style="text-align:left;padding:3px 16px 3px 0;color:#5d6676;font-weight:500">{e(k)}</th>'
        f'<td style="padding:3px 0">{e(v)}</td></tr>' for k, v in facts)
    cell = 'style="text-align:left;padding:4px 12px 4px 0;border-bottom:1px solid #dde1e8;vertical-align:top"'
    source_rows = "".join(
        f"<tr><td {cell}>{e(s)}</td><td {cell}>{e(st_)}</td><td {cell}>{t:,}</td><td {cell}>{n:,}</td>"
        f"<td {cell}>{e(ld)}</td><td {cell}>{e(err)}</td></tr>" for s, st_, t, n, ld, err in rows)
    html_body = (
        '<div style="font:14px/1.5 -apple-system,Segoe UI,sans-serif;color:#1d2330">'
        f'<h2 style="margin:0 0 12px;font-size:18px">{e(group_name)}: '
        f'<span style="color:{color}">{e(outcome)}</span></h2>'
        f'<table style="border-collapse:collapse;margin-bottom:16px">{fact_rows}</table>'
        + (f'<table style="border-collapse:collapse;font-size:13px"><tr>'
           + "".join(f"<th {cell}>{h}</th>" for h in ("Source", "Status", "Tables", "Rows", "Load", "Error"))
           + f"</tr>{source_rows}</table>" if rows else "")
        + '<p style="color:#5d6676;font-size:12px;margin-top:16px">Sent by eyetable after the group run finished. '
          "Change who gets this under Groups &rsaquo; Edit &rsaquo; Email results to.</p></div>"
    )
    return subject, "\n".join(text) + "\n", html_body


def test_message() -> tuple[str, str, str]:
    text = ("This is a test email from eyetable. Group runs will send their results to this address "
            "when it is saved in the group's Email results to box.")
    return "eyetable: test email", text + "\n", f'<p style="font:14px/1.5 sans-serif">{html.escape(text)}</p>'
