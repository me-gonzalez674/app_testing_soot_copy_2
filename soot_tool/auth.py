# soot_tool/auth.py
from __future__ import annotations

import os
import tempfile
from http.cookiejar import MozillaCookieJar

import requests


BASE_URL = "https://asdc.larc.nasa.gov/soot-api"


def session_from_token(user_token: str) -> requests.Session:
    """
    Create a requests.Session authenticated with a NASA Earthdata Bearer token.
    The token is attached as a persistent Authorization header on every request,
    including file downloads.

    Users generate their token at: https://urs.earthdata.nasa.gov
    Tokens are valid for 60 days. Users can hold a maximum of 2 active tokens.
    """
    user_token = user_token.strip()
    if not user_token:
        raise ValueError("Token cannot be empty.")

    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {user_token}"})
    return s


def session_from_cookiejar_bytes(cookie_bytes: bytes) -> requests.Session:
    """
    Legacy fallback: create a session from an uploaded .urs_cookies file.
    Kept in case users still have a valid cookie file they prefer to use.
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
    """
    Verify the session can reach the SOOT API.
    Uses the /campaigns endpoint which accepts Bearer token auth directly.
    """
    r = session.get(
        f"{BASE_URL}/campaigns",
        allow_redirects=True,
        timeout=timeout,
        headers={"Accept": "application/json"},
    )

    if r.status_code == 401:
        raise RuntimeError(
            "Authorization failed (HTTP 401). "
            "Your token may be invalid or expired. "
            "Generate a new one at https://urs.earthdata.nasa.gov"
        )
    if r.status_code != 200:
        raise RuntimeError(
            f"Authorization failed (HTTP {r.status_code}). "
            "Please check your token and try again."
        )
