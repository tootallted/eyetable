import dataclasses
import time
from collections.abc import Iterator

import httpx

from eyetable.connectors.base import Record
from eyetable.connectors.postman import PaginationLimitError, PostmanConnector, raw_url
from eyetable.statements import count, expand, incremental_window, rewrite, sets_paging
from eyetable.store import STATEMENT_COLUMN


class CurlConnector(PostmanConnector):
    """A source made from a pasted curl command: a one-request Postman collection
    (see eyetable.curl, which builds its config). Its "statements" config runs the
    request as a set, every statement paging as usual (see eyetable.statements).
    A statement that fails stops the run, so a replace keeps the table as it was.

    Rows of a set carry their statement's label in ET_statement. With `window` set, only the
    set's incremental window runs (see statements.incremental_window); `ran` lists the labels
    of the statements that finished."""

    type = "curl"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.window = False
        self.ran: list[str] = []

    def pages(self, key: str, resume: dict | None = None) -> Iterator[list[Record]]:
        """The request's pages, or each statement's in turn; `resume` carries on from a statement
        (and a page within it) where a failed load stopped (see eyetable.resume)."""
        from eyetable import resume as resuming

        plan = self.config.get("statements")
        if plan and self.window:
            plan = incremental_window(plan) or plan
        if not plan:
            yield from super().pages(key, resume)
            return
        request = self._readable(key)
        paging = self.pagination.get(key)
        if sets_paging(plan, paging):
            paging = None
        total = count(plan)
        start = (resume or {}).get("statement", 1)
        for number, statement in enumerate(expand(plan), 1):
            if number < start:
                continue  # in already, from the load being retried
            if number > start and plan.get("pause_seconds"):
                time.sleep(plan["pause_seconds"])
            url, body = rewrite(raw_url(request.url), request.body, statement.changes)
            rows = 0
            inner = (resume or {}).get("page") if number == start else None
            try:
                for records in self._pages(dataclasses.replace(request, url=url, body=body), paging, inner,
                                           {"statement": number, "statements": total}):
                    rows += len(records)
                    yield [{**r, STATEMENT_COLUMN: statement.label} for r in records]
            except httpx.HTTPStatusError as exc:
                reply = f"the API answered {exc.response.status_code} {exc.response.reason_phrase}"
                raise RuntimeError(f"statement {number} of {total} ({statement.label}): {reply}") from None
            except (httpx.HTTPError, PaginationLimitError) as exc:
                raise RuntimeError(f"statement {number} of {total} ({statement.label}): {exc}") from None
            resuming.mark({"statement": number + 1, "statements": total})  # this statement is in
            self.ran.append(statement.label)
            if not rows and plan.get("stop_when_empty"):
                return
