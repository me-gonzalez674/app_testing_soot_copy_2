# soot_tool/auth.py
from __future__ import annotations

import os
import tempfile
from http.cookiejar import MozillaCookieJar

import earthaccess
import requests


BASE_URL = "https://asdc.larc.nasa.gov/soot-api"
AUTH_URL = f"{BASE_URL}/Authenticate/user"


def session_from_token(user_token: str) -> requests.Session:
    """
    Create a fully authenticated requests.Session using the user's NASA
    Earthdata Bearer token.

    earthaccess handles the OAuth session establishment internally,
    which satisfies both the metadata endpoints (Bearer header check)
    and the download endpoint (ASDC session cookie check).

    Users generate their token at: https://urs.earthdata.nasa.gov
    Tokens are valid for 60 days.
    """
    user_token = user_token.strip()
    if not user_token:
        raise ValueError("Token cannot be empty.")

    # Inject the token into the environment so earthaccess can pick it up
    os.environ["EARTHDATA_TOKEN"] = user_token

    try:
        auth = earthaccess.login(strategy="environment")
    except Exception as e:
        raise RuntimeError(
            f"earthaccess login failed: {e}. "
            "Your token may be invalid or expired. "
            "Generate a new one at https://urs.earthdata.nasa.gov"
        ) from e

    # Get the fully authenticated session from earthaccess
    session = earthaccess.get_requests_https_session()
    return session


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
    Works for both earthaccess-based and cookie-based sessions.
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
