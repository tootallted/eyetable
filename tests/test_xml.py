"""XML API responses become tables of rows and columns."""

import datetime as dt

import httpx
import pytest
from cryptography.fernet import Fernet

from eyetable.connectors import build_connector
from eyetable.connectors.curl_source import CurlConnector
from eyetable.connectors.postman import extract_records, parse_response, records_path
from eyetable.curl import plan
from eyetable.engine import Engine
from eyetable.xmldata import XmlDocument, parse_xml

KEY = Fernet.generate_key().decode()

INVOICES = b"""<?xml version="1.0" encoding="UTF-8"?>
<ns:InvoiceResponse xmlns:ns="urn:example:invoices" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xsi:schemaLocation="urn:example:invoices invoices.xsd">
  <ns:Header><ns:RequestId>abc-1</ns:RequestId><ns:Count>2</ns:Count></ns:Header>
  <ns:Invoices>
    <ns:Invoice id="1001" status="paid">
      <ns:Number>INV-1</ns:Number>
      <ns:BuyerDUNS>008203241</ns:BuyerDUNS>
      <ns:Date>2026-11-04</ns:Date>
      <ns:Total currency="USD">1250.50</ns:Total>
      <ns:Supplier><ns:Name>Acme &amp; Sons</ns:Name><ns:City>Tulsa</ns:City></ns:Supplier>
      <ns:Line><ns:Sku>A</ns:Sku></ns:Line>
      <ns:Line><ns:Sku>B</ns:Sku></ns:Line>
      <ns:PaidOn xsi:nil="true"/>
    </ns:Invoice>
    <ns:Invoice id="1002" status="disputed">
      <ns:Number>INV-2</ns:Number>
      <ns:BuyerDUNS>108203241</ns:BuyerDUNS>
      <ns:Date>2026-11-03</ns:Date>
      <ns:Total currency="USD">99</ns:Total>
      <ns:Supplier><ns:Name>Globex</ns:Name><ns:City>Reno</ns:City></ns:Supplier>
      <ns:Line><ns:Sku>C</ns:Sku></ns:Line>
    </ns:Invoice>
  </ns:Invoices>
</ns:InvoiceResponse>"""


def rows(xml: bytes, path=None):
    return extract_records(parse_xml(xml), path)


def test_rows_columns_and_types():
    doc = parse_xml(INVOICES)
    assert isinstance(doc, XmlDocument) and records_path(doc) == "Invoices.Invoice"
    first, second = extract_records(doc)
    assert first["id"] == 1001 and first["status"] == "paid" and first["Number"] == "INV-1"
    assert first["Date"] == dt.date(2026, 11, 4)
    assert first["Total__value"] == 1250.5 and second["Total__value"] == 99 and first["Total__currency"] == "USD"
    assert first["Supplier__Name"] == "Acme & Sons" and second["Supplier__City"] == "Reno"
    assert first["BuyerDUNS"] == "008203241"  # a code, not a number: the leading zero stays
    assert first["Line"] == [{"Sku": "A"}, {"Sku": "B"}]  # lists in a row stay in one column
    assert first["PaidOn"] is None


def test_one_row_has_the_same_columns_as_many():
    many = rows(b"<Orders><Order><Id>1</Id></Order><Order><Id>2</Id></Order></Orders>")
    one = rows(b"<Orders><Order><Id>1</Id></Order></Orders>")
    assert many == [{"Id": 1}, {"Id": 2}] and one == [{"Id": 1}]
    assert rows(b"<r><Companies><Company><Id>1</Id></Company></Companies></r>") == [{"Id": 1}]
    assert rows(b"<r><ItemList><Item><Id>1</Id></Item></ItemList></r>") == [{"Id": 1}]


def test_a_repeated_element_under_any_wrapper_is_the_rows():
    soap = b"""<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body>
        <GetJobsResponse xmlns="urn:x"><Result><Job><Id>1</Id></Job><Job><Id>2</Id></Job></Result></GetJobsResponse>
        </soap:Body></soap:Envelope>"""
    assert rows(soap) == [{"Id": 1}, {"Id": 2}]


def test_the_longest_list_wins_and_rows_are_not_searched():
    xml = b"""<r><Errors><Error><Code>1</Code></Error><Error><Code>2</Code></Error></Errors>
        <Data><Row><Lines><L>1</L><L>2</L><L>3</L><L>4</L></Lines></Row><Row/><Row/></Data></r>"""
    assert records_path(parse_xml(xml)) == "Data.Row"


def test_a_single_record_is_one_row():
    assert rows(b"<Invoice><Id>7</Id><Vendor><Name>Acme</Name></Vendor></Invoice>") == [{"Id": 7, "Vendor__Name": "Acme"}]
    assert rows(b"<Message>hello</Message>") == [{"Message": "hello"}]


@pytest.mark.parametrize("xml", [b"<Invoices/>", b"<Invoices></Invoices>", b'<?xml version="1.0"?><r>  </r>'])
def test_an_empty_response_has_no_rows(xml):
    assert rows(xml) == []


def test_a_rows_field_copes_with_days_that_have_none():
    assert rows(b"<r><Invoices><Invoice><Id>1</Id></Invoice></Invoices></r>", "Invoices.Invoice") == [{"Id": 1}]
    assert rows(b"<r><Status>ok</Status><Invoices/></r>", "Invoices.Invoice") == []
    assert rows(b"<r><Status>ok</Status></r>", "Invoices.Invoice") == []
    assert rows(b"<r><Invoice><Id>1</Id></Invoice></r>", "Invoice") == [{"Id": 1}]  # one, not under a plural


def test_values_that_dont_all_fit_stay_text():
    assert rows(b"<r><i><v>1</v></i><i><v>x</v></i></r>") == [{"v": "1"}, {"v": "x"}]
    assert rows(b"<r><i><v>2026-02-30</v></i><i><v>2026-02-28</v></i></r>")[0]["v"] == "2026-02-30"
    assert rows(b"<r><i><v>true</v></i><i><v>false</v></i></r>") == [{"v": True}, {"v": False}]
    assert rows(b"<r><i><v>1</v></i><i/></r>") == [{"v": 1}]  # an empty element is no row


def test_entities_are_refused():
    bomb = b'<!DOCTYPE lolz [<!ENTITY a "aaaaaaaaaa"><!ENTITY b "&a;&a;&a;&a;&a;">]><r>&b;</r>'
    with pytest.raises(ValueError):
        parse_xml(bomb)
    assert parse_xml(b'<!DOCTYPE cXML SYSTEM "http://x.test/cXML.dtd"><cXML><A>1</A></cXML>') == {"A": "1"}


def response(body: bytes, content_type: str) -> httpx.Response:
    return httpx.Response(200, content=body, headers={"content-type": content_type})


@pytest.mark.parametrize("content_type", ["application/xml", "text/xml; charset=utf-8", "application/soap+xml", "text/plain"])
def test_xml_is_recognized(content_type):
    assert isinstance(parse_response(response(b'<?xml version="1.0"?><r><A>1</A></r>', content_type)), XmlDocument)


def test_other_replies_are_left_alone():
    assert parse_response(response(b'{"a": 1}', "application/xml")) == {"a": 1}  # a mislabeled JSON reply
    assert parse_response(response(b"<r><unclosed>", "application/xml")) == "<r><unclosed>"
    assert parse_response(response(b"<html><body>hi</body></html>", "text/html")) == "<html><body>hi</body></html>"


def test_other_encodings():
    body = '<?xml version="1.0" encoding="UTF-16"?><r><Name>Åsa</Name></r>'.encode("utf-16")
    assert parse_response(response(body, "application/xml")) == {"Name": "Åsa"}


# -- from a curl command into a table ------------------------------------------------

class XmlApi:
    def __init__(self, body=INVOICES):
        self.body = body

    def __call__(self, request):
        if request.url.path == "/paged":
            start = int(request.url.params.get("start", 0))
            items = "".join(f"<Job><Id>{i}</Id></Job>" for i in range(start + 1, min(start + 3, 6)))
            more = f"<NextLink>https://xml.test/paged?start={start + 2}</NextLink>" if start + 2 < 5 else ""
            return httpx.Response(200, text=f"<Result>{more}<Jobs>{items}</Jobs></Result>", headers={"content-type": "application/xml"})
        return httpx.Response(200, content=self.body, headers={"content-type": "application/xml"})


def engine_for(store, api):
    def factory(type_, config):
        if type_ == "curl":
            return CurlConnector(config, client=httpx.Client(transport=httpx.MockTransport(api)))
        return build_connector(type_, config)
    return Engine(store, factory, environ={"EYETABLE_SECRET_KEY": KEY})


def http(api):
    return httpx.Client(transport=httpx.MockTransport(api))


def test_a_command_shows_and_loads_a_table(store):
    api = XmlApi()
    p, _ = plan("curl https://xml.test/invoices", KEY, client=http(api))
    assert p.rows == 2 and p.records_field == "Invoices.Invoice"
    assert {"name": "Total__value", "type": "double"} in p.columns and {"name": "Date", "type": "date"} in p.columns
    assert {"field": "Invoices.Invoice", "rows": 2} in p.list_fields

    engine = engine_for(store, api)
    [result] = engine.load_curl("curl https://xml.test/invoices", "invoices", client=http(api))
    assert (result.status, result.rows) == ("succeeded", 2), result.error
    table = store.read_table("invoices__data")
    assert [r["number"] for r in table] == ["INV-1", "INV-2"] and [r["ET_id"] for r in table] == [1, 2]
    assert table[0]["total_value"] == 1250.5 and table[0]["buyerduns"] == "008203241"


def test_a_day_with_nothing_loads_no_rows(store):
    api = XmlApi()
    engine = engine_for(store, api)
    engine.load_curl("curl https://xml.test/invoices", "invoices", client=http(api), records_field="Invoices.Invoice")
    api.body = b'<?xml version="1.0"?><InvoiceResponse><Header><Count>0</Count></Header><Invoices/></InvoiceResponse>'
    [again] = engine.reload_source("invoices")
    assert (again.status, again.rows) == ("succeeded", 0), again.error

    p, _ = plan("curl https://xml.test/invoices", KEY, client=http(api), records_field="Invoices.Invoice")
    assert p.rows == 0
    api.body = b"<InvoiceResponse><Header/></InvoiceResponse>"
    p, _ = plan("curl https://xml.test/invoices", KEY, client=http(api), records_field="Invoices.Invoice")
    assert p.rows == 0 and any("no Invoices.Invoice" in w for w in p.warnings)


def test_a_first_load_with_no_rows_can_grow_later(store):
    api = XmlApi(b"<Invoices/>")
    engine = engine_for(store, api)
    [first] = engine.load_curl("curl https://xml.test/invoices", "invoices", client=http(api))
    assert (first.status, first.rows) == ("succeeded", 0), first.error
    api.body = INVOICES
    [again] = engine.reload_source("invoices")
    assert (again.status, again.rows) == ("succeeded", 2), again.error


def test_a_column_with_one_value_in_some_rows_and_several_in_others(store):
    api = XmlApi(b"<r><i><Tag>a</Tag></i><i><Tag>b</Tag><Tag>c</Tag></i></r>")
    engine = engine_for(store, api)
    [result] = engine.load_curl("curl https://xml.test/x", "tagged", client=http(api))
    assert (result.status, result.rows) == ("succeeded", 2), result.error


def test_next_links_in_xml_are_followed(store):
    api = XmlApi()
    p, _ = plan("curl https://xml.test/paged", KEY, client=http(api), page_size=2)
    assert p.next_field == "NextLink" and p.records_field == "Jobs.Job" and p.rows == 2
    engine = engine_for(store, api)
    [result] = engine.load_curl("curl https://xml.test/paged", "jobs", client=http(api), page_size=2)
    assert result.rows == 5, result.error
    assert [r["id"] for r in store.read_table("jobs__data")] == [1, 2, 3, 4, 5]
