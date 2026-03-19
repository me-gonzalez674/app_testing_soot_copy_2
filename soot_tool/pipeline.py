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


@dataclass
class RunResult:
    df: pd.DataFrame
    ict_files: List[Path]
    rows: int
    cols: int


def _establish_download_session(session: requests.Session, timeout: int = 60) -> None:
    """
    Hit /Authenticate/user so the server sets any required session cookie
    before the download loop begins.
    """
    r = session.get(AUTH_URL, allow_redirects=True, timeout=timeout)

    # ── Diagnostic: log everything about the auth call ────────────────────
    diag_lines = [
        "── _establish_download_session diagnostic ──",
        f"Final URL:       {r.url}",
        f"Status code:     {r.status_code}",
        "",
        "── Auth response body (first 500 chars) ──",
        (r.text or "")[:500],
        "",
        "── Auth response headers ──",
        *[f"  {k}: {v}" for k, v in r.headers.items()],
        "",
        "── Redirect history ──",
        *[f"  [{h.status_code}] {h.url}" for h in r.history] or ["  (none)"],
        "",
        "── Cookies set on session AFTER auth call ──",
        *[f"  {c.name} (domain={c.domain}): {c.value[:40]}..." for c in session.cookies] or ["  (none)"],
    ]
    raise RuntimeError("\n".join(diag_lines))
    # ── End diagnostic ────────────────────────────────────────────────────


def download_and_extract_ict_files(
    session: requests.Session,
    filenames: List[str],
    out_dir: Path,
) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Establish session cookie required by the download endpoint
    _establish_download_session(session)

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
            diag_lines = [
                f"Download failed for: {fn}",
                f"HTTP status:         {resp.status_code}",
                "",
                "── Response body (first 1000 chars) ──",
                (resp.text or "")[:1000],
                "",
                "── Response headers ──",
                *[f"  {k}: {v}" for k, v in resp.headers.items()],
                "",
                "── Session cookies at time of request ──",
                *[f"  {c.name} (domain={c.domain}): {c.value[:40]}..." for c in session.cookies],
                "",
                "── Redirect history ──",
                *[f"  [{r.status_code}] {r.url}" for r in resp.history] or ["  (none)"],
            ]
            raise RuntimeError("\n".join(diag_lines))

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
