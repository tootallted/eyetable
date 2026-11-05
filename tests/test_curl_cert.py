"""curl sources against a real HTTPS server that refuses callers without a client certificate."""

import base64
import datetime as dt
import http.server
import json
import ssl
import threading
import time

import httpx
import pytest
from cryptography import x509
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization as ser
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import NameOID
from fastapi.testclient import TestClient

from eyetable import tls
from eyetable.api import create_app
from eyetable.curl import CurlError, parse_curl, plan
from eyetable.engine import Engine

TOKEN = "test-token-0123456789"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
KEY = Fernet.generate_key().decode()
PASSPHRASE = "correct horse"


def issue(name, issuer=None, issuer_key=None, days=30, sans=None):
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    now = dt.datetime.now(dt.timezone.utc)
    builder = (x509.CertificateBuilder().subject_name(subject).issuer_name(issuer.subject if issuer else subject)
               .public_key(key.public_key()).serial_number(x509.random_serial_number())
               .not_valid_before(now - dt.timedelta(days=2)).not_valid_after(now + dt.timedelta(days=days))
               .add_extension(x509.BasicConstraints(ca=issuer is None, path_length=None), critical=True)
               .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
               .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key((issuer_key or key).public_key()), critical=False)
               .add_extension(x509.KeyUsage(digital_signature=True, key_cert_sign=issuer is None, crl_sign=issuer is None,
                                            content_commitment=False, key_encipherment=False, data_encipherment=False,
                                            key_agreement=False, encipher_only=False, decipher_only=False), critical=True))
    if sans:
        builder = builder.add_extension(x509.SubjectAlternativeName(sans), critical=False)
    return key, builder.sign(issuer_key or key, hashes.SHA256())


def pem_of(certificate):
    return certificate.public_bytes(ser.Encoding.PEM)


def key_pem(key, passphrase=None):
    encryption = ser.BestAvailableEncryption(passphrase.encode()) if passphrase else ser.NoEncryption()
    return key.private_bytes(ser.Encoding.PEM, ser.PrivateFormat.PKCS8, encryption)


CA_KEY, CA = issue("test ca")
CLIENT_KEY, CLIENT = issue("apitest.example", CA, CA_KEY)
PFX = pkcs12.serialize_key_and_certificates(
    b"client", CLIENT_KEY, CLIENT, [CA], ser.BestAvailableEncryption(PASSPHRASE.encode()))


class Invoices(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.server.seen.append(self.path)
        who = self.connection.getpeercert()["subject"][0][0][1]
        body = json.dumps({"invoices": [{"id": 1, "seenAs": who}, {"id": 2, "seenAs": who}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def server(tmp_path, monkeypatch):
    key, certificate = issue("localhost", CA, CA_KEY, sans=[x509.DNSName("localhost")])
    (tmp_path / "server.pem").write_bytes(key_pem(key) + pem_of(certificate))
    (tmp_path / "ca.pem").write_bytes(pem_of(CA))
    monkeypatch.setenv("SSL_CERT_FILE", str(tmp_path / "ca.pem"))  # trust the test CA as a client

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tmp_path / "server.pem")
    context.load_verify_locations(tmp_path / "ca.pem")
    context.verify_mode = ssl.CERT_REQUIRED
    httpd = http.server.ThreadingHTTPServer(("localhost", 0), Invoices)
    httpd.seen = []
    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield httpd
    httpd.shutdown()
    httpd.server_close()
    thread.join()


def command(server, query=""):
    return f"curl 'https://localhost:{server.server_address[1]}/invoices{query}'"


# -- reading certificate files -----------------------------------------------------

def test_a_pfx_becomes_pem_without_its_passphrase():
    pem = tls.to_pem(PFX, PASSPHRASE)
    assert pem.startswith("-----BEGIN PRIVATE KEY-----") and "ENCRYPTED" not in pem
    assert [c.subject for c in x509.load_pem_x509_certificates(pem.encode())] == [CLIENT.subject, CA.subject]
    info = tls.describe(pem)
    assert info["subject"] == "apitest.example" and info["expired"] is False


@pytest.mark.parametrize("data, passphrase", [
    (key_pem(CLIENT_KEY) + pem_of(CLIENT), None),
    (pem_of(CA) + pem_of(CLIENT) + b"\n" + key_pem(CLIENT_KEY), None),  # any order; two files joined
    (pem_of(CLIENT) + key_pem(CLIENT_KEY, PASSPHRASE), PASSPHRASE),
    (key_pem(CLIENT_KEY) + pem_of(CLIENT), "not needed"),
])
def test_pem_files(data, passphrase):
    pem = tls.to_pem(data, passphrase)
    assert x509.load_pem_x509_certificates(pem.encode())[0].subject == CLIENT.subject


CSR = (x509.CertificateSigningRequestBuilder().subject_name(CLIENT.subject).sign(CLIENT_KEY, hashes.SHA256())
       .public_bytes(ser.Encoding.PEM).replace(b"CERTIFICATE REQUEST", b"NEW CERTIFICATE REQUEST"))


@pytest.mark.parametrize("data, passphrase, message", [
    (PFX, "wrong", "check its passphrase"),
    (PFX, None, "check its passphrase"),
    (b"not a certificate", None, "wouldn't open"),
    (pem_of(CLIENT), None, "no private key"),
    (key_pem(CLIENT_KEY), None, "no certificate"),
    (key_pem(CA_KEY) + pem_of(CLIENT), None, "don't belong together"),
    (pem_of(CLIENT) + key_pem(CLIENT_KEY, PASSPHRASE), None, "needs its passphrase"),
    (pem_of(CLIENT) + key_pem(CLIENT_KEY, PASSPHRASE), "wrong", "check its passphrase"),
    (CSR, None, "certificate request"),
])
def test_unusable_files_are_explained(data, passphrase, message):
    with pytest.raises(tls.CertificateError, match=message):
        tls.to_pem(data, passphrase)


def test_the_fields_of_a_certificate():
    assert tls.client_cert(pfx=PFX, passphrase=PASSPHRASE, host=" HTTPS://Api.Example.com:8443/v1 ")["host"] == "api.example.com"
    assert tls.client_cert(pfx=PFX, passphrase=PASSPHRASE, host="api.example.com:8443")["port"] == 8443
    assert tls.client_cert(crt=pem_of(CLIENT), key=key_pem(CLIENT_KEY))["host"] is None
    assert tls.client_cert(crt=pem_of(CLIENT), key=key_pem(CLIENT_KEY), ca=CA.public_bytes(ser.Encoding.DER))["ca"].startswith("-----BEGIN")
    with pytest.raises(tls.CertificateError, match="not both"):
        tls.client_cert(pfx=PFX, crt=pem_of(CLIENT))
    with pytest.raises(tls.CertificateError, match="choose a PFX"):
        tls.client_cert(host="api.example.com")
    with pytest.raises(tls.CertificateError, match="isn't a host name"):
        tls.client_cert(pfx=PFX, passphrase=PASSPHRASE, host="not a host")
    with pytest.raises(tls.CertificateError, match="isn't a certificate"):
        tls.client_cert(pfx=PFX, passphrase=PASSPHRASE, ca=b"nonsense")


@pytest.mark.parametrize("host, port, url, expected", [
    ("api.example.com", None, "https://api.example.com/x", True),
    ("api.example.com", None, "https://API.example.com:8443/x", True),
    ("api.example.com", 443, "https://api.example.com/x", True),
    ("api.example.com", 8443, "https://api.example.com/x", False),
    ("api.example.com", None, "https://other.example.com/x", False),
    ("*.example.com", None, "https://api.example.com/x", True),
])
def test_which_requests_a_certificate_covers(host, port, url, expected):
    assert tls.covers({"host": host, "port": port}, url) is expected


def test_only_its_host_gets_the_certificate():
    transports = tls.mounts({"pem": tls.to_pem(PFX, PASSPHRASE), "host": "api.example.com", "port": None, "ca": None})
    assert list(transports) == ["https://api.example.com"]
    with httpx.Client(mounts=transports) as client:
        assert client._transport_for_url(httpx.URL("https://api.example.com/x")) is transports["https://api.example.com"]
        assert client._transport_for_url(httpx.URL("https://elsewhere.example/x")) is client._transport


# -- against a server that requires one ---------------------------------------------

def test_without_a_certificate_the_server_refuses(server):
    with pytest.raises(CurlError):
        plan(command(server), KEY)
    assert server.seen == []


def test_the_refusal_says_what_to_do():
    def refuse(request):
        raise httpx.ReadError("[SSL: TLSV13_ALERT_CERTIFICATE_REQUIRED] tlsv13 alert certificate required (_ssl.c:2648)")

    with pytest.raises(CurlError, match="requires a client certificate; add it under Client certificate"):
        plan("curl https://a.test/x", KEY, client=httpx.Client(transport=httpx.MockTransport(refuse)))


def test_check_load_and_reload_with_a_certificate(store, server):
    p, _ = plan(command(server), KEY, certificate=tls.client_cert(pfx=PFX, passphrase=PASSPHRASE))
    assert p.rows == 2 and p.sample[0]["seenAs"] == "apitest.example"
    assert p.certificate["subject"] == "apitest.example" and p.certificate["host"] == "localhost" and not p.warnings

    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    [result] = engine.load_curl(command(server), "invoices", certificate=tls.client_cert(pfx=PFX, passphrase=PASSPHRASE))
    assert (result.status, result.rows) == ("succeeded", 2), result.error
    config = store.get_source("invoices").config
    assert config["client_cert"]["pem"].startswith("eyetable-enc:") and "PRIVATE KEY" not in str(config)
    assert config["client_cert"]["host"] == "localhost"
    assert [r.rows for r in engine.reload_source("invoices")] == [2]  # no file or passphrase needed


def test_a_private_server_authority_can_be_given(store, server, monkeypatch):
    monkeypatch.delenv("SSL_CERT_FILE")
    with pytest.raises(CurlError, match="CERTIFICATE_VERIFY_FAILED"):  # the usual checks still apply
        plan(command(server), KEY, certificate=tls.client_cert(pfx=PFX, passphrase=PASSPHRASE))
    certificate = tls.client_cert(pfx=PFX, passphrase=PASSPHRASE, ca=pem_of(CA))
    assert plan(command(server), KEY, certificate=certificate)[0].rows == 2
    engine = Engine(store, environ={"EYETABLE_SECRET_KEY": KEY})
    [result] = engine.load_curl(command(server), "invoices", certificate=certificate)
    assert result.rows == 2, result.error
    assert store.get_source("invoices").config["client_cert"]["ca"].startswith("eyetable-enc:")
    assert [r.rows for r in engine.reload_source("invoices")] == [2]


def test_an_expired_certificate_is_pointed_out(server):
    key, certificate = issue("old", CA, CA_KEY, days=-1)
    p = None
    try:
        p, _ = plan(command(server), KEY, certificate=tls.client_cert(crt=pem_of(certificate), key=key_pem(key)))
    except CurlError:
        pass  # this server refuses it; a real one might not
    assert p is None or any("expired" in w for w in p.warnings)
    assert tls.describe(tls.to_pem(key_pem(key) + pem_of(certificate)))["expired"] is True


def test_endpoints(store, server):
    web = TestClient(create_app(Engine(store, environ={"EYETABLE_SECRET_KEY": KEY}), TOKEN))
    file = base64.b64encode(PFX).decode()
    certificate = {"host": "localhost", "pfx": file, "passphrase": PASSPHRASE}
    body = {"curl": command(server), "certificate": certificate}

    assert web.post("/api/curl/inspect", headers=AUTH, json={"curl": command(server)}).status_code == 400
    wrong = {**body, "certificate": {**certificate, "passphrase": "wrong"}}
    r = web.post("/api/curl/inspect", headers=AUTH, json=wrong)
    assert r.status_code == 400 and "passphrase" in r.json()["detail"]
    r = web.post("/api/curl/inspect", headers=AUTH, json={**body, "certificate": {**certificate, "pfx": "%%%"}})
    assert r.status_code == 400
    r = web.post("/api/curl/inspect", headers=AUTH, json={**body, "certificate": {**certificate, "host": "other.example"}})
    assert r.status_code == 400 and "set for other.example" in r.json()["detail"]
    r = web.post("/api/curl/inspect", headers=AUTH, json={**body, "certificate": {"host": "localhost"}})
    assert r.status_code == 400 and "choose a PFX" in r.json()["detail"]

    p = web.post("/api/curl/inspect", headers=AUTH, json=body).json()
    assert p["rows"] == 2 and p["certificate"]["subject"] == "apitest.example"
    assert "PRIVATE" not in str(p) and PASSPHRASE not in str(p)

    assert web.post("/api/curl", headers=AUTH, json={**wrong, "name": "x"}).status_code == 400
    job = web.post("/api/curl", headers=AUTH, json={**body, "name": "invoices"}).json()["job_id"]
    for _ in range(200):
        state = web.get(f"/api/jobs/{job}", headers=AUTH).json()
        if state["status"] != "running":
            break
        time.sleep(0.05)
    assert state["status"] == "succeeded" and state["result"]["rows"] == 2, state["error"]
    assert PASSPHRASE not in web.get("/api/sources/invoices", headers=AUTH).text
    assert "PRIVATE" not in web.get("/api/sources/invoices", headers=AUTH).text


# -- the command itself --------------------------------------------------------------

def test_spaces_in_a_filter_stay_percent_encoded(server):
    query = "?%24filter=status%20eq%20%27received%2C%20paid%27%20and%20buyerDUNS%20eq%20108203241"
    plan(command(server, query), KEY, certificate=tls.client_cert(pfx=PFX, passphrase=PASSPHRASE))
    assert server.seen == ["/invoices" + query]


def test_certificate_options_in_a_command_get_a_note():
    req = parse_curl(r"curl https://a.test/x --cert C:\client.pfx:secret --cert-type P12 --key k.pem")
    assert req.url == "https://a.test/x"
    assert [w for w in req.warnings if "Client certificate" in w] and len(req.warnings) == 1
