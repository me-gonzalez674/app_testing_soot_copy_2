# soot_tool/pipeline.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
from zipfile import ZipFile

from .icartt import ICARTTReader


DOWNLOAD_FILES_URL = "https://asdc.larc.nasa.gov/soot-api/data_files/downloadFiles"
AUTH_URL           = "https://asdc.larc.nasa.gov/soot-api/Authenticate/user"
LOGIN_URL          = "https://asdc.larc.nasa.gov/soot-api/login"


@dataclass
class RunResult:
    df: pd.DataFrame
    ict_files: List[Path]
    rows: int
    cols: int


def _extract_bearer_token(session: requests.Session) -> str:
    """Pull the Bearer token back out of the session headers for direct use."""
    auth_header = session.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[len("Bearer "):]
    return ""


def _try_direct_login(session: requests.Session, timeout: int = 60) -> None:
    """
    Approach B: attempt to hit /soot-api/login directly with the Bearer token
    to trick ASDC into setting its session cookie without the browser OAuth dance.

    We try three strategies in order:
      1. GET /login with Authorization: Bearer header
      2. POST /login with token in the request body
      3. GET /login?token=<token> as a query param
    """
    token = _extract_bearer_token(session)

    diag_lines = ["── Approach B: Direct Login Diagnostic ──", ""]

    # ── Strategy 1: GET with Bearer header ───────────────────────────────
    r1 = session.get(LOGIN_URL, allow_redirects=True, timeout=timeout)
    redirect_history_1 = [f"  [{h.status_code}] {h.url}" for h in r1.history] or ["  (none)"]
    cookies_after_1 = [f"  {c.name} (domain={c.domain}): {c.value[:40]}" for c in session.cookies] or ["  (none)"]

    diag_lines += [
        "── Strategy 1: GET /login with Bearer header ──",
        f"  Final URL:   {r1.url}",
        f"  Status:      {r1.status_code}",
        f"  Body:        {(r1.text or '')[:200]}",
        "  Redirects:   " + ", ".join(redirect_history_1),
        "  Cookies now: " + ", ".join(cookies_after_1),
        "",
    ]

    # Check if strategy 1 set an ASDC session cookie
    asdc_cookies_1 = [c for c in session.cookies if "asdc" in c.domain.lower() or "larc" in c.domain.lower()]
    if asdc_cookies_1 and r1.status_code == 200 and "urs.earthdata" not in r1.url:
        diag_lines.append("✅ Strategy 1 appears to have worked — ASDC cookie set!")
        raise RuntimeError("\n".join(diag_lines))

    # ── Strategy 2: POST /login with token in body ────────────────────────
    r2 = requests.post(
        LOGIN_URL,
        json={"token": token},
        headers={"Authorization": f"Bearer {token}"},
        allow_redirects=True,
        timeout=timeout,
    )
    redirect_history_2 = [f"  [{h.status_code}] {h.url}" for h in r2.history] or ["  (none)"]

    diag_lines += [
        "── Strategy 2: POST /login with token in body ──",
        f"  Final URL:   {r2.url}",
        f"  Status:      {r2.status_code}",
        f"  Body:        {(r2.text or '')[:200]}",
        "  Redirects:   " + ", ".join(redirect_history_2),
        "",
    ]

    # Update session cookies from response 2
    session.cookies.update(r2.cookies)
    asdc_cookies_2 = [c for c in session.cookies if "asdc" in c.domain.lower() or "larc" in c.domain.lower()]
    if asdc_cookies_2 and r2.status_code == 200 and "urs.earthdata" not in r2.url:
        diag_lines.append("✅ Strategy 2 appears to have worked — ASDC cookie set!")
        raise RuntimeError("\n".join(diag_lines))

    # ── Strategy 3: GET /login?token=<token> as query param ──────────────
    r3 = requests.get(
        LOGIN_URL,
        params={"token": token},
        headers={"Authorization": f"Bearer {token}"},
        allow_redirects=True,
        timeout=timeout,
    )
    redirect_history_3 = [f"  [{h.status_code}] {h.url}" for h in r3.history] or ["  (none)"]

    diag_lines += [
        "── Strategy 3: GET /login?token=<token> as query param ──",
        f"  Final URL:   {r3.url}",
        f"  Status:      {r3.status_code}",
        f"  Body:        {(r3.text or '')[:200]}",
        "  Redirects:   " + ", ".join(redirect_history_3),
        "",
    ]

    session.cookies.update(r3.cookies)
    asdc_cookies_3 = [c for c in session.cookies if "asdc" in c.domain.lower() or "larc" in c.domain.lower()]
    if asdc_cookies_3 and r3.status_code == 200 and "urs.earthdata" not in r3.url:
        diag_lines.append("✅ Strategy 3 appears to have worked — ASDC cookie set!")
        raise RuntimeError("\n".join(diag_lines))

    # ── All strategies exhausted ──────────────────────────────────────────
    diag_lines += [
        "── Final session cookies ──",
        *[f"  {c.name} (domain={c.domain}): {c.value[:40]}" for c in session.cookies],
        "",
        "❌ All strategies failed to set an ASDC session cookie.",
        "   Approach B is not viable — OAuth2 app registration (Approach A) will be required.",
    ]
    raise RuntimeError("\n".join(diag_lines))


def download_and_extract_ict_files(
    session: requests.Session,
    filenames: List[str],
    out_dir: Path,
) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)

    _try_direct_login(session)

    for fn in filenames:
        fn = str(fn).strip()
        zip_base = fn.split(".ict")[0]
        zip_path = out_dir / f"{zip_base}.zip"

        resp = session.get(
            DOWNLOAD_FILES_URL,
            params={"filenames": fn},
            allow_redirects=True,
            timeout=180,
        )

        if resp.status_code != 200:
            redirect_lines = [f"  [{h.status_code}] {h.url}" for h in resp.history] or ["  (none)"]
            cookie_lines = [f"  {c.name} (domain={c.domain}): {c.value[:40]}" for c in session.cookies]
            header_lines = [f"  {k}: {v}" for k, v in resp.headers.items()]

            diag_parts = (
                [
                    f"Download failed for: {fn}",
                    f"HTTP status:         {resp.status_code}",
                    "",
                    "── Response body (first 1000 chars) ──",
                    (resp.text or "")[:1000],
                    "",
                    "── Response headers ──",
                ]
                + header_lines
                + ["", "── Session cookies at time of request ──"]
                + cookie_lines
                + ["", "── Redirect history ──"]
                + redirect_lines
            )
            raise RuntimeError("\n".join(diag_parts))

        zip_path.write_bytes(resp.content)

        with ZipFile(zip_path, "r") as z:
            z.extractall(out_dir)

        zip_path.unlink(missing_ok=True)

    ict_files = list(out_dir.rglob("*.ict")) + list(out_dir.rglob("*.ICT"))
    return ict_files


def _add_datetime_columns(df: pd.DataFrame, meta: dict) -> pd.DataFrame:
    fmt = "%Y,%m,%d"

    date_info = meta.get("date_info")
    seconds = meta.get("seconds")

    if not date_info or not seconds:
        return df

    s = ",".join([x.strip() for x in date_info.split(",")[:3]])
    start_date = datetime.strptime(s, fmt)
    start_time = timedelta(seconds=int(seconds))
    start_datetime = start_date + start_time

    time_columns = [col for col in df.columns if "UTC" in str(col).upper()]
    for col in time_columns:
        new_col_name = str(col).replace("UTC", "Datetime")
        df[new_col_name] = start_datetime + pd.to_timedelta(df[col], unit="s")

    if len(time_columns) == 0:
        time_columns = [col for col in df.columns if "TIME" in str(col).upper()]
        for col in time_columns:
            column = str(col).title()
            new_col_name = column.replace("Time", "Datetime")
            df[new_col_name] = start_datetime + pd.to_timedelta(df[col], unit="s")

    return df


def parse_ict_files_to_df(ict_files: List[Path]) -> pd.DataFrame:
    combined = pd.DataFrame()

    for p in ict_files:
        r = ICARTTReader(p)
        df = r.read_table()
        meta = r.read_metadata()

        df = _add_datetime_columns(df, meta)
        combined = pd.concat([combined, df], ignore_index=True)

    return combined


def run_download_convert(
    session: requests.Session,
    filenames: List[str],
    working_dir: Path,
    *,
    cleanup_ict: bool = True,
) -> RunResult:
    ict_files = download_and_extract_ict_files(session, filenames, working_dir)
    df = parse_ict_files_to_df(ict_files)

    if cleanup_ict:
        for p in ict_files:
            try:
                p.unlink()
            except OSError:
                pass

    return RunResult(df=df, ict_files=ict_files, rows=len(df), cols=len(df.columns))
