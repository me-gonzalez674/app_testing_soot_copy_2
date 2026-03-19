# soot_tool/auth.py
from __future__ import annotations

import os
import tempfile
from http.cookiejar import MozillaCookieJar

import requests


BASE_URL = "https://asdc.larc.nasa.gov/soot-api"
AUTH_URL = f"{BASE_URL}/Authenticate/user"


def session_from_token(user_token: str) -> requests.Session:
    """
    Create a requests.Session using the user's NASA Earthdata Bearer token.
    Users generate this at: https://urs.earthdata.nasa.gov
    Token is valid for 60 days. The user can hold a max of 2 active tokens.
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
    Works for both token-based and cookie-based sessions.
    """
    r = session.get(AUTH_URL, allow_redirects=True, timeout=timeout)

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
