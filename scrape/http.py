"""HTTPS sessions for legislature sites that require the OS certificate store."""
import ssl
import time

import requests
import truststore
from urllib3.util.retry import Retry


class SystemTrustAdapter(requests.adapters.HTTPAdapter):
    """Keep certificate verification enabled with OS intermediate-chain support."""
    def __init__(self):
        self.context = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        super().__init__(max_retries=Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504]))

    def build_connection_pool_key_attributes(self, request, verify, cert=None):
        host, kwargs = super().build_connection_pool_key_attributes(request, verify, cert)
        kwargs['ssl_context'] = self.context
        return host, kwargs


class ScrapeSession(requests.Session):
    """Retry interrupted GET response bodies, which adapter retries don't cover."""

    def get(self, url, **kwargs):
        for attempt in range(3):
            try:
                return super().get(url, **kwargs)
            except (requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout):
                if attempt == 2:
                    raise
                time.sleep(2 ** (attempt + 1))


def make_session():
    http = ScrapeSession()
    http.mount('https://', SystemTrustAdapter())
    return http
