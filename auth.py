# soot_tool/auth.py
from __future__ import annotations

import os
import tempfile
from http.cookiejar import MozillaCookieJar
from pathlib import Path
from typing import Optional

import requests


BASE_URL = "https://asdc.larc.nasa.gov/soot-api"
AUTH_URL = f"{BASE_URL}/Authenticate/user"


def session_from_cookiejar_bytes(cookie_bytes: bytes) -> requests.Session:
    """
    Create a requests.Session from an uploaded Netscape-format cookie jar (.urs_cookies).
    Streamlit gives you bytes; MozillaCookieJar expects a filename, so we use a temp file.
    """
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(cookie_bytes)
        tmp_path = tmp.name

    try:
        cj = MozillaCookieJar(tmp_path)
        cj.load(ignore_expires=True)
        s = requests.Session()
        s.cookies.update(cj)
        return s
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def assert_authorized(session: requests.Session, *, timeout: int = 60) -> None:
    r = session.get(AUTH_URL, allow_redirects=True, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(
            f"Authorization failed (HTTP {r.status_code}). "
            "Your .urs_cookies may be expired or not in Netscape format."
        )