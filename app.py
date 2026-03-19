# app.py
import tempfile
from pathlib import Path

import streamlit as st

from soot_tool.auth import session_from_credentials, assert_authorized
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

st.markdown(
    "Enter your [NASA Earthdata Login](https://urs.earthdata.nasa.gov) credentials "
    "to access and download SOOT data."
)

# ── Privacy notice ────────────────────────────────────────────────────────────
with st.expander("ℹ️ How your credentials are used", expanded=False):
    st.markdown(
        """
        Your username and password are used **only** to authenticate with NASA's
        Earthdata Login (urs.earthdata.nasa.gov) on your behalf. Specifically:

        - Credentials are submitted directly to NASA's OAuth2 login endpoint over HTTPS
        - They are **never stored**, logged, or written to disk
        - They are discarded from memory immediately after your session is established
        - Only the resulting session cookie is retained for the duration of your visit
        - Closing or refreshing the app ends the session entirely

        This is the same authentication method used by NASA's own
        [earthaccess](https://github.com/nsidc/earthaccess) Python library.
        """
    )

# ── Credential inputs ─────────────────────────────────────────────────────────
col1, col2 = st.columns(2)
with col1:
    username = st.text_input(
        "Earthdata Username",
        placeholder="Your Earthdata Login username",
    )
with col2:
    password = st.text_input(
        "Earthdata Password",
        type="password",
        placeholder="Your Earthdata Login password",
    )

if not username or not password:
    st.stop()

# ── Session creation ──────────────────────────────────────────────────────────
# Cache by username only — password never stored in cache key or session state.
# If a new login is needed, the user refreshes the page.
@st.cache_resource(show_spinner="Authenticating with NASA Earthdata...")
def get_session(uname: str, _password: str):
    """
    _password is prefixed with _ so Streamlit does not include it in the
    cache key hash — it is used only inside this function and discarded.
    """
    session = session_from_credentials(uname, _password)
    assert_authorized(session)
    return session

try:
    session = get_session(username, password)
    st.success("Authorized ✅")
except Exception as e:
    st.error(str(e))
    st.stop()

# ── Campaign selection ────────────────────────────────────────────────────────
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

# ── Filename preview ──────────────────────────────────────────────────────────
with st.spinner("Fetching filenames..."):
    fn_df = get_filenames(session, campaign, year, platform, pi_lastname)

if "filename" not in fn_df.columns:
    st.error("Filename response missing 'filename' column.")
    st.stop()

filenames = fn_df["filename"].dropna().astype(str).tolist()
st.write(f"Files available: **{len(filenames)}**")
st.dataframe(fn_df.head(200), use_container_width=True)

# ── Download + convert ────────────────────────────────────────────────────────
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
