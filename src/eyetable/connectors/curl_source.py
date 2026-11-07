import dataclasses
import time
from collections.abc import Iterator

import httpx

from eyetable.connectors.base import Record
from eyetable.connectors.postman import PaginationLimitError, PostmanConnector, raw_url
from eyetable.statements import count, expand, rewrite, sets_paging


class CurlConnector(PostmanConnector):
    """A source made from a pasted curl command: a one-request Postman collection
    (see eyetable.curl, which builds its config). Its "statements" config runs the
    request as a set, every statement paging as usual (see eyetable.statements).
    A statement that fails stops the run, so a replace keeps the table as it was."""

    type = "curl"

    def pages(self, key: str) -> Iterator[list[Record]]:
        plan = self.config.get("statements")
        if not plan:
            yield from super().pages(key)
            return
        request = self._readable(key)
        paging = self.pagination.get(key)
        if sets_paging(plan, paging):
            paging = None
        total = count(plan)
        for number, statement in enumerate(expand(plan), 1):
            if number > 1 and plan.get("pause_seconds"):
                time.sleep(plan["pause_seconds"])
            url, body = rewrite(raw_url(request.url), request.body, statement.changes)
            rows = 0
            try:
                for records in self._pages(dataclasses.replace(request, url=url, body=body), paging):
                    rows += len(records)
                    yield records
            except httpx.HTTPStatusError as exc:
                reply = f"the API answered {exc.response.status_code} {exc.response.reason_phrase}"
                raise RuntimeError(f"statement {number} of {total} ({statement.label}): {reply}") from None
            except (httpx.HTTPError, PaginationLimitError) as exc:
                raise RuntimeError(f"statement {number} of {total} ({statement.label}): {exc}") from None
            if not rows and plan.get("stop_when_empty"):
                return
