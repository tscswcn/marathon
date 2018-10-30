import json
import logging
import pkg_resources
import requests

from six.moves import urllib

from ..clients.authentication import dcos_acs_token, DCOSAcsAuth

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 5


def create_client(url, timeout=DEFAULT_TIMEOUT, auth_token=None):
    return RpcClient(url, timeout, auth_token)


def load_error_json_schema():
    """Reads and parses Marathon error response JSON schema from file

    :returns: the parsed JSON schema
    :rtype: dict
    """
    schema_path = 'data/marathon/error.schema.json'
    schema_bytes = pkg_resources.resource_string('dcos', schema_path)
    return json.loads(schema_bytes.decode('utf-8'))


class RpcClient(object):
    """Convenience class for making requests against a common RPC API.

    For example, it ensures the same base URL is used for all requests. This
    class is also useful as a target for mocks in unit tests, because it
    presents a minimal, application-focused interface.

    :param base_url: the URL prefix to use for all requests
    :type base_url: str
    :param timeout: number of seconds to wait for a response
    :type timeout: float
    :param auth_token: the DC/OS authentication token.
    :type auth_token: str
    """

    def __init__(self, base_url, timeout=None, auth_token=None):
        self.session = BaseUrlSession(base_url)
        self.session.auth = DCOSAcsAuth(auth_token or dcos_acs_token())
        self.session.timeout = timeout or DEFAULT_TIMEOUT


class BaseUrlSession(requests.Session):
    """A Session with a URL that all requests will use as a base.

    This is a fork of https://github.com/requests/toolbelt/blob/master/requests_toolbelt/sessions.py.
    """
    base_url = None

    def __init__(self, base_url=None):
        if base_url:
            self.base_url = base_url
        super(BaseUrlSession, self).__init__()

    def request(self, method, url, *args, **kwargs):
        """Send the request after generating the complete URL."""
        url = self.create_url(url)
        return super(BaseUrlSession, self).request(
            method, url, *args, **kwargs
        )

    def create_url(self, url):
        """Create the URL based off this partial path."""
        return urllib.parse.urljoin(self.base_url, url)