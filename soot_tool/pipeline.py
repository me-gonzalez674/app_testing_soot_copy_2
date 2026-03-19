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


@dataclass
class RunResult:
    df: pd.DataFrame
    ict_files: List[Path]
    rows: int
    cols: int


def find_url_column(df: pd.DataFrame) -> Optional[str]:
    """Check if the filenames dataframe contains a direct URL column."""
    candidates = ["url", "download_url", "downloadUrl", "download_link",
                  "link", "href", "file_url", "fileUrl", "path", "file_path"]
    cols_lower = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in cols_lower:
            return cols_lower[candidate.lower()]
    for col in df.columns:
        if "url" in col.lower() or "link" in col.lower():
            return col
    return None


def download_and_extract_ict_files(
    session: requests.Session,
    filenames_df: pd.DataFrame,
    out_dir: Path,
) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)

    filename_col = "filename" if "filename" in filenames_df.columns else filenames_df.columns[0]
    filenames = filenames_df[filename_col].dropna().astype(str).tolist()

    # ── Diagnostic: check redirect URL on the first file only ────────────
    if filenames:
        fn = filenames[0].strip()

        # Step 1: check what URL the endpoint redirects to WITHOUT following
        r_no_follow = session.get(
            DOWNLOAD_FILES_URL,
            params={"filenames": fn},
            allow_redirects=False,  # catch the redirect
            timeout=30,
        )

        diag = [
            f"── Redirect diagnostic for: {fn} ──",
            f"Status (no follow): {r_no_follow.status_code}",
            f"Location header:    {r_no_follow.headers.get('Location', '(none)')}",
            f"All headers: {dict(r_no_follow.headers)}",
            "",
        ]

        # Step 2: if there's a redirect, try hitting that URL directly with Bearer
        location = r_no_follow.headers.get("Location")
        if location:
            r_direct = session.get(location, allow_redirects=True, timeout=30)
            diag += [
                f"── Direct URL attempt ──",
                f"URL:    {location}",
                f"Status: {r_direct.status_code}",
                f"Body:   {(r_direct.text or '')[:300]}",
            ]
        else:
            diag.append("No Location header — endpoint does not redirect.")
            # Also try following redirects normally to see the final URL
            r_follow = session.get(
                DOWNLOAD_FILES_URL,
                params={"filenames": fn},
                allow_redirects=True,
                timeout=30,
            )
            diag += [
                f"── Follow redirect result ──",
                f"Final URL: {r_follow.url}",
                f"Status:    {r_follow.status_code}",
                f"Body:      {(r_follow.text or '')[:300]}",
                f"History:   {[(h.status_code, str(h.url)) for h in r_follow.history]}",
            ]

        raise RuntimeError("\n".join(diag))
    # ── End diagnostic ────────────────────────────────────────────────────

    return []


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
    filenames_df: pd.DataFrame,
    working_dir: Path,
    *,
    cleanup_ict: bool = True,
) -> RunResult:
    ict_files = download_and_extract_ict_files(session, filenames_df, working_dir)
    df = parse_ict_files_to_df(ict_files)
    if cleanup_ict:
        for p in ict_files:
            try:
                p.unlink()
            except OSError:
                pass
    return RunResult(df=df, ict_files=ict_files, rows=len(df), cols=len(df.columns))
