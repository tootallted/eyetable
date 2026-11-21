import datetime as dt
import smtplib
from email.message import EmailMessage

import pytest
import sqlalchemy as sa
from fastapi.testclient import TestClient

from eyetable import notify
from eyetable.api import create_app
from eyetable.engine import Engine
from eyetable.groups import GroupRunner
from eyetable.notify import Mailer, NotifyError, clean_recipients, group_run_message, parse_recipients
from eyetable.store import CoreStore, GroupRun

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


class FakeMailer:
    host, port = "smtp.test", 587

    def __init__(self, fail: Exception | None = None):
        self.sent: list[tuple[list[str], str, str, str | None]] = []
        self.fail = fail

    def send(self, to, subject, text, html_body=None):
        if self.fail:
            raise self.fail
        self.sent.append((to, subject, text, html_body))


@pytest.fixture
def loaded(store):
    engine = Engine(store)
    engine.ingest_upload("parts", "parts.csv", b"name\nbolt\nnut\n", create=True)
    engine.ingest_upload("orders", "orders.csv", b"name\no1\n", create=True)
    return engine


# -- addresses ---------------------------------------------------------------

def test_recipients_are_split_checked_and_deduplicated():
    assert parse_recipients("a@x.com, b@y.org;c@z.co  A@X.com") == ["a@x.com", "b@y.org", "c@z.co"]
    assert parse_recipients("") == [] and parse_recipients(None) == [] and parse_recipients(" , ") == []
    assert clean_recipients(" a@x.com ;b@y.org ") == "a@x.com, b@y.org"
    assert clean_recipients("") is None
    for bad in ("nobody", "a@b", "a@@b.com", "a@b.c", "<a@b.com>"):
        with pytest.raises(NotifyError, match="isn't an email address"):
            parse_recipients(bad)
    with pytest.raises(NotifyError, match="at most 20"):
        parse_recipients(",".join(f"u{i}@x.com" for i in range(21)))


def test_mailer_settings_come_from_env():
    assert Mailer.from_env({}) is None
    m = Mailer.from_env({"EYETABLE_SMTP_HOST": "smtp.office365.com", "EYETABLE_SMTP_USER": "r@x.com"})
    assert (m.host, m.port, m.security, m.sender, m.user) == ("smtp.office365.com", 587, "starttls", "r@x.com", "r@x.com")
    m = Mailer.from_env({"EYETABLE_SMTP_HOST": "h", "EYETABLE_SMTP_PORT": "465", "EYETABLE_SMTP_FROM": "f@x.com"})
    assert (m.port, m.security, m.sender, m.user) == (465, "ssl", "f@x.com", None)
    with pytest.raises(NotifyError, match="starttls, ssl or none"):
        Mailer.from_env({"EYETABLE_SMTP_HOST": "h", "EYETABLE_SMTP_FROM": "f@x.com", "EYETABLE_SMTP_SECURITY": "tls"})
    with pytest.raises(NotifyError, match="EYETABLE_SMTP_FROM"):
        Mailer.from_env({"EYETABLE_SMTP_HOST": "h"})


def test_mailer_sends_text_and_html_over_starttls(monkeypatch):
    calls = []

    class FakeSMTP:
        def __init__(self, host, port, timeout):
            calls.append(("connect", host, port))

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            calls.append(("quit",))

        def starttls(self, context):
            calls.append(("starttls",))

        def login(self, user, password):
            calls.append(("login", user))

        def send_message(self, msg: EmailMessage):
            calls.append(("send", msg))

    monkeypatch.setattr(smtplib, "SMTP", FakeSMTP)
    Mailer("smtp.test", 587, "r@x.com", "pw", "r@x.com").send(["a@x.com", "b@y.org"], "Subj", "plain", "<p>html</p>")
    assert [c[0] for c in calls] == ["connect", "starttls", "login", "send", "quit"]
    msg = calls[3][1]
    assert msg["To"] == "a@x.com, b@y.org" and msg["From"] == "r@x.com" and msg["Subject"] == "Subj"
    assert msg.get_body(("plain",)).get_content().strip() == "plain"
    assert "<p>html</p>" in msg.get_body(("html",)).get_content()


def test_errors_are_described_without_secrets():
    assert "refused the sign-in" in notify.describe_error(smtplib.SMTPAuthenticationError(535, b"bad password hunter2"))
    assert "hunter2" not in notify.describe_error(smtplib.SMTPAuthenticationError(535, b"bad password hunter2"))
    assert "couldn't reach" in notify.describe_error(ConnectionRefusedError(10061, "refused"))


# -- the message -------------------------------------------------------------

def test_message_has_the_basic_numbers():
    start = dt.datetime(2026, 11, 20, 14, 0, 0, tzinfo=dt.timezone.utc)
    run = GroupRun(id=7, group_id=1, trigger="schedule", status="partial", started_at=start,
                   finished_at=start + dt.timedelta(minutes=3, seconds=12), results=[
                       {"source": "parts", "status": "succeeded", "rows": 1200, "tables": 2, "load": "full", "error": None},
                       {"source": "orders", "status": "failed", "rows": 34, "tables": 1, "load": "incremental", "error": "timeout"},
                   ])
    subject, text, html_body = group_run_message("Finance <morning>", run, None, "America/Los_Angeles")
    assert subject == "eyetable: Finance <morning> partly failed"
    assert "Group:            Finance <morning>" in text
    assert "Started:          2026-11-20 06:00:00 PST" in text
    assert "Finished:         2026-11-20 06:03:12 PST" in text
    assert "Duration:         3 min 12 s" in text
    assert "Rows processed:   1,234" in text and "Tables populated: 3" in text
    assert "Sources:          2 (1 succeeded, 1 failed)" in text
    assert "Started by:       On its schedule" in text
    assert "orders: failed, 1 table, 34 rows (incremental) — timeout" in text
    assert "Finance &lt;morning&gt;" in html_body and "<morning>" not in html_body
    assert notify.duration_text(3725) == "1 h 2 min 5 s" and notify.duration_text(0.4) == "0 s"


# -- group runs ----------------------------------------------------------------

def test_a_finished_run_is_emailed_to_the_group(loaded, store):
    runner = GroupRunner(loaded)
    runner.mailer = FakeMailer()
    group = runner.create("Morning", ["parts", "orders"], notify_to="ops@x.com; me@y.org")
    assert group.notify == "ops@x.com, me@y.org"
    run_id = runner.run(group.id, wait=True)
    [(to, subject, text, html_body)] = runner.mailer.sent
    assert to == ["ops@x.com", "me@y.org"] and subject == "eyetable: Morning succeeded"
    assert "Rows processed:   3" in text and "Tables populated: 2" in text and "Group:            Morning" in text
    assert "Started by:       Run now\n" in text
    run = store.get_group_run(run_id)
    assert run.email == "sent to ops@x.com, me@y.org"
    assert [r["tables"] for r in run.results] == [1, 1]


def test_failures_are_emailed_too(loaded, store):
    runner = GroupRunner(loaded)
    runner.mailer = FakeMailer()
    group = runner.create("Mixed", ["parts", "orders"], notify_to="ops@x.com")
    store.remove_source("parts")
    store.remove_source("orders")
    runner.run(group.id, wait=True)
    [(_, subject, text, _)] = runner.mailer.sent
    assert subject == "eyetable: Mixed failed" and "parts: failed" in text and "no longer exists" in text


def test_the_run_notes_why_no_email_went(loaded, store):
    runner = GroupRunner(loaded)
    group = runner.create("Morning", ["parts"], notify_to="ops@x.com")
    run = store.get_group_run(runner.run(group.id, wait=True))
    assert run.status == "succeeded" and run.email.startswith("not sent: email isn't set up")

    runner.mailer = FakeMailer(fail=smtplib.SMTPAuthenticationError(535, b"nope"))
    run = store.get_group_run(runner.run(group.id, wait=True))
    assert run.status == "succeeded" and "refused the sign-in" in run.email

    quiet = runner.create("Quiet", ["parts"])
    runner.mailer = FakeMailer()
    run = store.get_group_run(runner.run(quiet.id, wait=True))
    assert run.email is None and runner.mailer.sent == []


def test_tag_runs_send_nothing(loaded, store):
    store.set_source_tags("parts", ["fin"])
    runner = GroupRunner(loaded)
    runner.mailer = FakeMailer()
    run = store.get_group_run(runner.run_tag("fin", wait=True))
    assert run.status == "succeeded" and run.email is None and runner.mailer.sent == []


def test_editing_keeps_or_changes_addresses(loaded):
    runner = GroupRunner(loaded)
    group = runner.create("Morning", ["parts"], notify_to="ops@x.com")
    assert runner.update(group.id, "Morning", ["parts", "orders"], None, True).notify == "ops@x.com"
    assert runner.update(group.id, "Morning", ["parts"], None, True, "a@x.com,b@x.com").notify == "a@x.com, b@x.com"
    assert runner.update(group.id, "Morning", ["parts"], None, True, "").notify is None
    with pytest.raises(ValueError, match="isn't an email address"):
        runner.update(group.id, "Morning", ["parts"], None, True, "ops")
    with pytest.raises(ValueError, match="isn't an email address"):
        runner.create("Other", ["parts"], notify_to="nope")
    assert [g.name for g in loaded.store.list_groups()] == ["Morning"]


def test_api(loaded):
    app = create_app(loaded, TOKEN)
    client = TestClient(app)
    r = client.post("/api/groups", headers=AUTH, json={"name": "Morning", "members": ["parts"], "notify": "ops@x.com"})
    assert r.status_code == 200, r.text
    g = r.json()
    assert g["notify"] == "ops@x.com" and g["email_ready"] is False
    assert client.post("/api/groups", headers=AUTH, json={"name": "B", "members": ["parts"], "notify": "x"}).status_code == 400

    # a page that doesn't send "notify" (an older copy) leaves the addresses alone
    r = client.put(f"/api/groups/{g['id']}", headers=AUTH, json={"name": "Morning", "members": ["parts", "orders"]})
    assert r.json()["notify"] == "ops@x.com"
    r = client.put(f"/api/groups/{g['id']}", headers=AUTH, json={"name": "Morning", "members": ["parts"], "notify": ""})
    assert r.json()["notify"] is None

    r = client.post("/api/notify/test", headers=AUTH, json={"to": "ops@x.com"})
    assert r.status_code == 409 and "EYETABLE_SMTP_HOST" in r.json()["detail"]
    app.state.group_runner.mailer = FakeMailer()
    assert client.post("/api/notify/test", headers=AUTH, json={"to": "ops@x.com, me@y.org"}).json() == {"sent_to": ["ops@x.com", "me@y.org"]}
    assert app.state.group_runner.mailer.sent[0][1] == "eyetable: test email"
    assert client.get("/api/groups", headers=AUTH).json()[0]["email_ready"] is True
    assert client.post("/api/notify/test", headers=AUTH, json={"to": "ops"}).status_code == 400
    app.state.group_runner.mailer = FakeMailer(fail=ConnectionRefusedError(10061, "refused"))
    r = client.post("/api/notify/test", headers=AUTH, json={"to": "ops@x.com"})
    assert r.status_code == 502 and "couldn't reach" in r.json()["detail"]
    assert client.post("/api/notify/test", json={"to": "ops@x.com"}).status_code == 401


def test_older_databases_get_the_new_columns(tmp_path):
    url = f"sqlite:///{(tmp_path / 'old.db').as_posix()}"
    db = sa.create_engine(url)
    with db.begin() as conn:
        conn.execute(sa.text("CREATE TABLE groups (id INTEGER PRIMARY KEY, name VARCHAR(80) NOT NULL UNIQUE, "
                             "members JSON NOT NULL, schedule JSON, enabled BOOLEAN NOT NULL, next_run_at DATETIME, "
                             "created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL)"))
        conn.execute(sa.text("CREATE TABLE group_runs (id INTEGER PRIMARY KEY, group_id INTEGER NOT NULL, "
                             "trigger VARCHAR(16) NOT NULL, status VARCHAR(16) NOT NULL, started_at DATETIME NOT NULL, "
                             "finished_at DATETIME, results JSON NOT NULL)"))
    store = CoreStore(url)
    store.init()
    store.init()  # again: nothing left to add
    cols = {t: {c["name"] for c in sa.inspect(store.db).get_columns(t)} for t in ("groups", "group_runs")}
    assert "notify" in cols["groups"] and "email" in cols["group_runs"]
    group = store.create_group("G", ["x"], None, True, None)
    assert store.update_group(group.id, notify="a@x.com").notify == "a@x.com"
