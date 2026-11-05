"""XML API responses as rows and columns.

An XML document becomes the same nested structure a JSON response has, so
rows, next-page links and paging work the same way:

  * the root element is dropped and namespaces are ignored
  * an element's children and attributes become keys; an element that repeats
    becomes a list; text next to attributes is kept under "value"
  * a lone child of a plural parent (<Invoices><Invoice>) is still a list, so
    a response with one row has the same columns as one with many

The rows are the longest list found without going inside another list. Lists
inside a row (line items) stay as JSON in one column. XML has no types, so each
column's text is converted the way a CSV column is: numbers, dates and true/false
where every value fits, and text otherwise (codes with leading zeros stay text).

DTD entities are refused, so a document can't expand itself to exhaust memory.
"""

from typing import Any
from xml.etree import ElementTree
from xml.parsers import expat

from eyetable.connectors.file import column_converter

_SEPARATOR = "}"
_XSI = "http://www.w3.org/2001/XMLSchema-instance" + _SEPARATOR


class XmlDocument(dict):
    """A parsed XML response: the root element's content, marked so rows are found the XML way."""

    def rows_path(self) -> str | None:
        best: tuple[tuple[bool, int, int], str] | None = None

        def walk(node: dict, prefix: str, depth: int) -> None:
            nonlocal best
            for key, value in node.items():
                path = prefix + str(key)
                if isinstance(value, list):
                    rank = (all(v is None or isinstance(v, dict) for v in value), len(value), -depth)
                    if best is None or rank > best[0]:
                        best = rank, path
                elif isinstance(value, dict):
                    walk(value, path + ".", depth + 1)

        walk(self, "", 0)
        return best[1] if best else None


def _local(name: str) -> str:
    return name.rsplit(_SEPARATOR, 1)[-1]


def _refuse_entities(*_: Any) -> None:
    raise ValueError("XML entity declarations aren't allowed")


def _plural_of(parent: str, child: str) -> bool:
    parent, child = parent.lower(), child.lower()
    forms = {child + "s", child + "es", child + "list", child + "_list", child + "collection", child + "array"}
    if child.endswith("y"):
        forms.add(child[:-1] + "ies")
    return parent in forms


def _value(element: ElementTree.Element) -> Any:
    if element.get(_XSI + "nil") in ("true", "1"):
        return None
    out: dict[str, Any] = {_local(k): v for k, v in element.attrib.items() if not k.startswith(_XSI)}
    children: dict[str, list[Any]] = {}
    for child in element:
        children.setdefault(_local(child.tag), []).append(_value(child))
    text = (element.text or "").strip()
    if not out and not children:
        return text or None
    if text:
        out["value"] = text
    for name, values in children.items():
        out[name] = values if len(values) > 1 or _plural_of(_local(element.tag), name) else values[0]
    return out


def parse_xml(data: bytes) -> XmlDocument:
    """An XML document as nested dicts and lists; ValueError if it isn't one."""
    builder = ElementTree.TreeBuilder()
    parser = expat.ParserCreate(namespace_separator=_SEPARATOR)
    parser.buffer_text = True
    parser.EntityDeclHandler = _refuse_entities
    parser.StartElementHandler = builder.start
    parser.EndElementHandler = builder.end
    parser.CharacterDataHandler = builder.data
    try:
        parser.Parse(data, True)
        root = builder.close()
        value = _value(root)
    except (expat.ExpatError, AssertionError, RecursionError) as exc:
        raise ValueError(f"not readable XML ({exc})") from None
    if isinstance(value, dict):
        return XmlDocument(value)
    return XmlDocument({} if value is None else {_local(root.tag): value})


def type_columns(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert each column's text the way a CSV column is, where every value in it fits."""
    for name in dict.fromkeys(key for record in records for key in record):
        convert = column_converter([r[name] for r in records if isinstance(r.get(name), str)])
        if convert:
            for record in records:
                if isinstance(record.get(name), str):
                    record[name] = convert(record[name])
    return records
