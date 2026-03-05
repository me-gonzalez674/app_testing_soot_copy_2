# soot_tool/soot_api.py
from __future__ import annotations

import pandas as pd
import requests

BASE_URL = "https://asdc.larc.nasa.gov/soot-api"


def _get_df(session: requests.Session, url: str) -> pd.DataFrame:
    r = session.get(
        url,
        allow_redirects=True,
        timeout=60,
        headers={"Accept": "application/json"},
    )

    ctype = (r.headers.get("content-type") or "").lower()

    if r.status_code != 200:
        snippet = (r.text or "")[:800]
        raise RuntimeError(
            f"GET {url} failed (HTTP {r.status_code}). Content-Type={ctype}\n"
            f"Response starts with:\n{snippet}"
        )

    if "application/json" not in ctype:
        snippet = (r.text or "")[:800]
        raise RuntimeError(
            f"GET {url} returned non-JSON. Content-Type={ctype}\n"
            f"Response starts with:\n{snippet}"
        )

    return pd.DataFrame(r.json())


# ---- Campaign lookup chain (matches NASA docs) ----
def get_campaigns(session: requests.Session) -> pd.DataFrame:
    # GET /campaigns
    return _get_df(session, f"{BASE_URL}/campaigns")


def get_years(session: requests.Session, acronym: str) -> pd.DataFrame:
    # GET /campaigns/years/{acronym}
    return _get_df(session, f"{BASE_URL}/campaigns/years/{acronym}")


def get_platforms(session: requests.Session, acronym: str, year: str) -> pd.DataFrame:
    # GET /campaigns/years/{acronym}/{year}
    return _get_df(session, f"{BASE_URL}/campaigns/years/{acronym}/{year}")


def get_pis(session: requests.Session, acronym: str, year: str, platform: str) -> pd.DataFrame:
    # GET /campaigns/years/{acronym}/{year}/{platform}
    return _get_df(session, f"{BASE_URL}/campaigns/years/{acronym}/{year}/{platform}")


def get_filenames(session: requests.Session, acronym: str, year: str, platform: str, pi_lastname: str) -> pd.DataFrame:
    # GET /campaigns/years/{acronym}/{year}/{platform}/{pilast}
    return _get_df(session, f"{BASE_URL}/campaigns/years/{acronym}/{year}/{platform}/{pi_lastname}")