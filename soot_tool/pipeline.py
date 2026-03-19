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


# Candidate column names to look for direct download URLs in the filenames response.
# The first match found will be used.
_URL_COLUMN_CANDIDATES = [
    "url", "download_url", "downloadUrl", "download_link",
    "link", "href", "file_url", "fileUrl", "path", "file_path",
]


@dataclass
class RunResult:
    df: pd.DataFrame
    ict_files: List[Path]
    rows: int
    cols: int


def find_url_column(df: pd.DataFrame) -> Optional[str]:
    """
    Find the first column in df that looks like it contains direct download URLs.
    Returns None if no match is found.
    """
    cols_lower = {c.lower(): c for c in df.columns}

    for candidate in _URL_COLUMN_CANDIDATES:
        if candidate.lower() in cols_lower:
            return cols_lower[candidate.lower()]

    # Secondary pass: any column whose name contains 'url' or 'link'
    for col in df.columns:
        if "url" in col.lower() or "link" in col.lower():
            return col

    return None


def download_and_extract_ict_files(
    session: requests.Session,
    filenames_df: pd.DataFrame,
    out_dir: Path,
) -> List[Path]:
    """
    Download .ict files using direct URLs from filenames_df with Bearer token auth.

    This mirrors the wget approach:
        wget --header "Authorization: Bearer $TOKEN" $URL

    The Bearer token is already attached to all session requests as a header,
    so no additional auth steps are needed — direct URL + header = download.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Find the URL column
    url_col = find_url_column(filenames_df)
    if url_col is None:
        available = filenames_df.columns.tolist()
        raise RuntimeError(
            f"Could not find a URL column in the filenames response.\n"
            f"Available columns: {available}\n"
            f"Please check the column names and update _URL_COLUMN_CANDIDATES in pipeline.py."
        )

    filename_col = "filename" if "filename" in filenames_df.columns else filenames_df.columns[0]
    urls = filenames_df[url_col].dropna().astype(str).tolist()
    filenames = filenames_df[filename_col].dropna().astype(str).tolist()

    for url, fn in zip(urls, filenames):
        url = url.strip()
        fn = fn.strip()
        zip_base = fn.split(".ict")[0]
        zip_path = out_dir / f"{zip_base}.zip"

        resp = session.get(
            url,
            allow_redirects=True,
            timeout=180,
        )

        if resp.status_code != 200:
            raise RuntimeError(
                f"Download failed for {fn} (HTTP {resp.status_code}).\n"
                f"URL: {url}\n"
                f"Response: {(resp.text or '')[:300]}"
            )

        # Handle zip or raw .ict response
        content_type = resp.headers.get("content-type", "").lower()
        if "zip" in content_type or url.endswith(".zip"):
            zip_path.write_bytes(resp.content)
            with ZipFile(zip_path, "r") as z:
                z.extractall(out_dir)
            zip_path.unlink(missing_ok=True)
        else:
            # Raw .ict file returned directly
            ict_path = out_dir / fn
            ict_path.write_bytes(resp.content)

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
