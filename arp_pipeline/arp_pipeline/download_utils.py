import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class NotAZipFileException(RuntimeError):
    pass


def download_file(url: str, retry_count: int = 5) -> requests.Response:
    retry_strategy = Retry(
        total=retry_count,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    resp = http.get(url)
    resp.raise_for_status()
    return resp


def download_zip(url: str, retry_count: int = 5) -> bytes:
    """Download a zip from URL with retries and confirmation of content type"""
    resp = download_file(url, retry_count)
    if resp.headers["Content-Type"] != "application/zip":
        raise NotAZipFileException(
            f"Request returned {resp.headers['Content-Type']}, not a zip."
        )
    return resp.content
