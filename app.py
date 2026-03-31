import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from soot_tool.graphing import build_figure
from soot_tool.auth import session_from_credentials, assert_authorized
from soot_tool.soot_api import (
    get_campaigns,
    get_years,
    get_platforms,
    get_pis,
    get_filenames,
)
from soot_tool.pipeline import run_download_convert

@st.cache_resource(show_spinner="Authenticating with NASA Earthdata...")
def get_session(uname: str, _password: str):
    session = session_from_credentials(uname, _password)
    assert_authorized(session)
    return session


# ------------------------------------------------------------
# Session state defaults
# ------------------------------------------------------------
defaults = {
    "page": "download",
    "download_complete": False,
    "download_csv_bytes": None,
    "download_filename": None,
    "download_preview_df": None,
    "download_summary": None,
    "saved_username": "",
    "saved_password": "",
    "selected_campaign": None,
    "selected_year": None,
    "selected_platform": None,
    "selected_pi_lastname": None,
}

for key, value in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = value

# ------------------------------------------------------------
# Download page
# ------------------------------------------------------------
st.title("NASA SOOT — ICARTT Downloader + CSV Converter")

st.markdown(
    "Enter your [NASA Earthdata Login](https://urs.earthdata.nasa.gov) credentials "
    "to access and download SOOT data."
)

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

# ------------------------------------------------------------
# Credential inputs
# ------------------------------------------------------------
col1, col2 = st.columns(2)
with col1:
    username = st.text_input(
        "Earthdata Username",
        value=st.session_state["saved_username"],
        placeholder="Your Earthdata Login username",
    )
with col2:
    password = st.text_input(
        "Earthdata Password",
        value=st.session_state["saved_password"],
        type="password",
        placeholder="Your Earthdata Login password",
    )

st.session_state["saved_username"] = username
st.session_state["saved_password"] = password

if not username or not password:
    st.stop()

# ------------------------------------------------------------
# Authenticate
# ------------------------------------------------------------
try:
    session = get_session(username, password)
    st.success("Authorized ✅")
except Exception as e:
    st.error(str(e))
    st.stop()

# ------------------------------------------------------------
# Campaign selection
# ------------------------------------------------------------
with st.spinner("Loading campaigns..."):
    campaigns_df = get_campaigns(session)

campaign_col = "acronym" if "acronym" in campaigns_df.columns else campaigns_df.columns[0]
campaign_options = sorted(campaigns_df[campaign_col].astype(str).unique())

if st.session_state["selected_campaign"] not in campaign_options:
    st.session_state["selected_campaign"] = campaign_options[0]

campaign = st.selectbox(
    "Campaign",
    campaign_options,
    index=campaign_options.index(st.session_state["selected_campaign"]),
)
st.session_state["selected_campaign"] = campaign

with st.spinner("Loading years..."):
    years_df = get_years(session, campaign)

year_col = "year" if "year" in years_df.columns else years_df.columns[0]
year_options = sorted(years_df[year_col].astype(str).unique())

if st.session_state["selected_year"] not in year_options:
    st.session_state["selected_year"] = year_options[0]

year = st.selectbox(
    "Year",
    year_options,
    index=year_options.index(st.session_state["selected_year"]),
)
st.session_state["selected_year"] = year

with st.spinner("Loading platforms..."):
    platforms_df = get_platforms(session, campaign, year)

platform_col = "name" if "name" in platforms_df.columns else platforms_df.columns[0]
platform_options = sorted(platforms_df[platform_col].astype(str).unique())

if st.session_state["selected_platform"] not in platform_options:
    st.session_state["selected_platform"] = platform_options[0]

platform = st.selectbox(
    "Platform",
    platform_options,
    index=platform_options.index(st.session_state["selected_platform"]),
)
st.session_state["selected_platform"] = platform

with st.spinner("Loading PIs..."):
    pis_df = get_pis(session, campaign, year, platform)

pi_col = "lastname" if "lastname" in pis_df.columns else pis_df.columns[0]
pi_options = sorted(pis_df[pi_col].astype(str).unique())

if st.session_state["selected_pi_lastname"] not in pi_options:
    st.session_state["selected_pi_lastname"] = pi_options[0]

pi_lastname = st.selectbox(
    "PI Last Name",
    pi_options,
    index=pi_options.index(st.session_state["selected_pi_lastname"]),
)
st.session_state["selected_pi_lastname"] = pi_lastname

# ------------------------------------------------------------
# Filename preview
# ------------------------------------------------------------
with st.spinner("Fetching filenames..."):
    fn_df = get_filenames(session, campaign, year, platform, pi_lastname)

if "filename" not in fn_df.columns:
    st.error("Filename response missing 'filename' column.")
    st.stop()

filenames = fn_df["filename"].dropna().astype(str).tolist()
st.write(f"Files available: **{len(filenames)}**")
st.dataframe(fn_df.head(200), use_container_width=True)

# ------------------------------------------------------------
# Download + convert
# ------------------------------------------------------------
if st.button("Download + Convert", type="primary"):
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)

        with st.spinner("Downloading, extracting, parsing..."):
            result = run_download_convert(session, filenames, workdir, cleanup_ict=True)

        st.session_state["download_complete"] = True
        st.session_state["download_csv_bytes"] = result.df.to_csv(index=False).encode("utf-8")
        st.session_state["download_filename"] = (
            f"{campaign}_{year}_{platform}_{pi_lastname}.csv"
        )
        st.session_state["download_preview_df"] = result.df.head(200)
        st.session_state["download_full_df"] = result.df
        st.session_state["download_summary"] = (
            f"Done. Rows: {result.rows:,} | Columns: {result.cols:,}"
        )

# ------------------------------------------------------------
# Show download results if available
# ------------------------------------------------------------
if st.session_state["download_complete"]:
    st.success(st.session_state["download_summary"])
    st.dataframe(st.session_state["download_preview_df"], use_container_width=True)

    st.download_button(
        "Download CSV",
        data=st.session_state["download_csv_bytes"],
        file_name=st.session_state["download_filename"],
        mime="text/csv",
    )

    if st.button("Show Graph"):
        st.session_state["page"] = "graph"
        st.rerun()

# ------------------------------------------------------------
# Graphing capabilities
# ------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_graph_df(df: pd.DataFrame) -> pd.DataFrame:
    return df

graph_df = load_graph_df(st.session_state["download_full_df"])

graph_cols = sorted(graph_df.columns.astype(str).unique())

y_axis = st.selectbox(
    "Y Axis Variable",
    graph_cols,
)
x_axis = st.selectbox(
    "X Axis Variable",
    graph_cols,
)

def render_graph_page() -> None:
    st.title("Graph")
    st.write(f"This graph is generated from {st.session_state['download_filename']}.")

    if st.button("← Back to Download Page"):
        st.session_state["page"] = "download"
        st.rerun()
    
    if ("download_full_df" not in st.session_state or st.session_state["download_full_df"] is None):
        st.error("No data loaded. Please try again.")
        st.stop()

    st.sidebar.header("Graph Controls")

    show_raw = st.sidebar.checkbox(
        "Show raw scatter",
        value=True,
        key="graph_show_raw",
    )

    # bin_m = st.sidebar.slider(
    #     "Altitude bin size (m)",
    #     min_value=10,
    #     max_value=500,
    #     value=50,
    #     step=10,
    #     key="graph_bin_m",
    # )

    # window = st.sidebar.slider(
    #     "Rolling window (bins)",
    #     min_value=3,
    #     max_value=51,
    #     value=11,
    #     step=2,
    #     key="graph_window",
    # )

    # show_ci = st.sidebar.checkbox(
    #     "Show ~95% CI band (SEM)",
    #     value=True,
    #     key="graph_show_ci",
    # )

    try:
        st.caption(
            f"Using {len(graph_df):,} rows from {st.session_state['download_filename']} "
            f"| Columns: {', '.join(graph_df.columns.astype(str))}"
        )

        fig = build_figure(
            graph_df,
            y_col=y_axis,
            x_col=x_axis,
            show_raw=show_raw,
            title=f"{x_axis} vs {y_axis} (From {st.session_state['download_filename']})",
        )

        st.pyplot(fig)

    except Exception as e:
        st.warning(f"Could not build graph from {st.session_state["download_filename"]}: {e}")


st.set_page_config(page_title="NASA SOOT ICARTT Converter", layout="wide")


# ------------------------------------------------------------
# Graph page
# ------------------------------------------------------------
if st.session_state["page"] == "graph":
    render_graph_page()
    st.stop()
