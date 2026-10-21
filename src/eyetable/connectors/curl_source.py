from eyetable.connectors.postman import PostmanConnector


class CurlConnector(PostmanConnector):
    """A source made from a pasted curl command: a one-request Postman collection
    (see eyetable.curl, which builds its config)."""

    type = "curl"
