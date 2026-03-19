# soot_tool/pipeline.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import pandas as pd
import requests
from zipfile import ZipFile

from .icartt import ICARTTReader


DOWNLOAD_FILES_URL = "https://asdc.larc.nasa.gov/soot-api/data_files/downloadFiles"
LOGIN_URL          = "https://asdc.larc.nasa.gov/soot-api/login"


@dataclass
class RunResult:
    df: pd.DataFrame
    ict_files: List[Path]
    rows: int
    cols: int


def _establish_asdc_session(session: requests.Session, timeout: int = 60) -> None:
    """
    Hit /soot-api/login to establish the ASDC session cookie required
    by the download endpoint.

    The Bearer token on the session header is accepted directly by this
    endpoint, which then sets an asdc.larc.nasa.gov session cookie that
    authorizes subsequent file downloads.
    """
    r = session.get(LOGIN_URL, allow_redirects=True, timeout=timeout)

    if r.status_code == 401:
        raise RuntimeError(
            "Authentication failed (HTTP 401). "
            "Your token may be invalid or expired. "
            "Generate a new one at https://urs.earthdata.nasa.gov"
        )

    if "urs.earthdata.nasa.gov" in r.url:
        raise RuntimeError(
            "Authentication failed — redirected back to Earthdata Login. "
            "Your token may be invalid or expired. "
            "Generate a new one at https://urs.earthdata.nasa.gov"
        )

    asdc_cookies = [
        c for c in session.cookies
        if "asdc" in c.domain.lower() or "larc" in c.domain.lower()
    ]
    if not asdc_cookies:
        raise RuntimeError(
            "Authentication completed but no ASDC session cookie was set. "
            "Please try regenerating your token at https://urs.earthdata.nasa.gov"
        )


def download_and_extract_ict_files(
    session: requests.Session,
    filenames: List[str],
    out_dir: Path,
) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)

    _establish_asdc_session(session)

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
            raise RuntimeError(
                f"Download failed for {fn} (HTTP {resp.status_code}). "
                f"Response: {(resp.text or '')[:300]}"
            )

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
