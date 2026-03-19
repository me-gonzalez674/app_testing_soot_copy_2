# app.py
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from soot_tool.auth import session_from_token, assert_authorized
from soot_tool.soot_api import (
    get_campaigns,
    get_years,
    get_platforms,
    get_pis,
    get_filenames,
)
from soot_tool.pipeline import run_download_convert


st.set_page_config(page_title="NASA SOOT ICARTT Converter", layout="wide")
st.title("NASA SOOT — ICARTT Downloader + CSV Converter")

st.write(
    "Enter your NASA Earthdata Bearer Token to authorize downloads. "
    "To generate a token:"
)
st.markdown(
    "1. Log in at [urs.earthdata.nasa.gov](https://urs.earthdata.nasa.gov)\n"
    "2. Click **Generate Token** from the top-right menu\n"
    "3. Click **Show Token**, copy it, and paste it below\n\n"
    "_Tokens are valid for 60 days and can be revoked at any time._"
)

user_token = st.text_input(
    "Earthdata Bearer Token",
    type="password",
    placeholder="Paste your token here...",
)

if not user_token:
    st.stop()

# Cache the session so re-runs don't re-authenticate on every interaction
@st.cache_resource(show_spinner="Authenticating with NASA Earthdata...")
def get_session(token: str):
    session = session_from_token(token)
    assert_authorized(session)
    return session

try:
    session = get_session(user_token)
    st.success("Authorized ✅")
except Exception as e:
    st.error(str(e))
    st.stop()

# ---- Load selection tables ----
with st.spinner("Loading campaigns..."):
    campaigns_df = get_campaigns(session)

campaign_col = "acronym" if "acronym" in campaigns_df.columns else campaigns_df.columns[0]
campaign = st.selectbox("Campaign", sorted(campaigns_df[campaign_col].astype(str).unique()))

with st.spinner("Loading years..."):
    years_df = get_years(session, campaign)

year_col = "year" if "year" in years_df.columns else years_df.columns[0]
year = st.selectbox("Year", sorted(years_df[year_col].astype(str).unique()))

with st.spinner("Loading platforms..."):
    platforms_df = get_platforms(session, campaign, year)

platform_col = "name" if "name" in platforms_df.columns else platforms_df.columns[0]
platform = st.selectbox("Platform", sorted(platforms_df[platform_col].astype(str).unique()))

with st.spinner("Loading PIs..."):
    pis_df = get_pis(session, campaign, year, platform)

pi_col = "lastname" if "lastname" in pis_df.columns else pis_df.columns[0]
pi_lastname = st.selectbox("PI Last Name", sorted(pis_df[pi_col].astype(str).unique()))

# ---- Fetch filenames preview ----
with st.spinner("Fetching filenames..."):
    fn_df = get_filenames(session, campaign, year, platform, pi_lastname)

if "filename" not in fn_df.columns:
    st.error("Filename response missing 'filename' column.")
    st.stop()

filenames = fn_df["filename"].dropna().astype(str).tolist()
st.write(f"Files available: **{len(filenames)}**")
st.dataframe(fn_df.head(200), use_container_width=True)

# ---- Run pipeline ----
if st.button("Download + Convert", type="primary"):
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)

        with st.spinner("Downloading, extracting, parsing..."):
            result = run_download_convert(session, filenames, workdir, cleanup_ict=True)

        st.success(f"Done. Rows: {result.rows:,} | Columns: {result.cols:,}")
        st.dataframe(result.df.head(200), use_container_width=True)

        csv_bytes = result.df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name=f"{campaign}_{year}_{platform}_{pi_lastname}.csv",
            mime="text/csv",
        )
