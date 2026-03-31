from __future__ import annotations

import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


FILL_VALUES = {-9999, -9999.0, -8888, -8888.0, -7777, -7777.0}


def _validate_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required column(s): {missing}. "
            f"Available columns: {list(df.columns)}"
        )


def _replace_fill_values(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace(list(FILL_VALUES), np.nan)


def clean_altitude_ozone(
    df: pd.DataFrame,
    *,
    alt_col: str = "Altitude_m_MSL",
    ozone_col: str = "Ozone_ppbv",
    altitude_min: float | None = None,
    altitude_max: float | None = None,
) -> pd.DataFrame:
    _validate_columns(df, [alt_col, ozone_col])

    x = _replace_fill_values(df.copy())

    x[alt_col] = pd.to_numeric(x[alt_col], errors="coerce")
    x[ozone_col] = pd.to_numeric(x[ozone_col], errors="coerce")

    x.loc[x[ozone_col] <= 0, ozone_col] = np.nan

    x = x.dropna(subset=[alt_col, ozone_col]).copy()

    if altitude_min is not None:
        x = x[x[alt_col] >= altitude_min]
    if altitude_max is not None:
        x = x[x[alt_col] <= altitude_max]

    if x.empty:
        raise ValueError("No valid rows remain after altitude/ozone cleaning.")

    return x


def build_altitude_profile(
    df: pd.DataFrame,
    *,
    alt_col: str = "Altitude_m_MSL",
    ozone_col: str = "Ozone_ppbv",
    bin_m: int = 50,
    window: int = 11,
    min_periods: int = 3,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if bin_m <= 0:
        raise ValueError("bin_m must be > 0.")
    if window <= 0:
        raise ValueError("window must be > 0.")

    cleaned = clean_altitude_ozone(df, alt_col=alt_col, ozone_col=ozone_col)

    cleaned = cleaned.copy()
    cleaned["alt_bin"] = (cleaned[alt_col] / bin_m).round() * bin_m

    profile = (
        cleaned.groupby("alt_bin")[ozone_col]
        .agg(mean="mean", median="median", n="size", std="std")
        .reset_index()
        .sort_values("alt_bin")
    )

    profile["sem"] = profile["std"] / np.sqrt(profile["n"])
    profile.loc[profile["n"] < 5, "sem"] = np.nan

    profile["mean_smooth"] = (
        profile["mean"]
        .rolling(window=window, center=True, min_periods=min_periods)
        .mean()
    )

    return cleaned, profile


def make_altitude_profile_plot(
    cleaned: pd.DataFrame,
    profile: pd.DataFrame,
    *,
    alt_col: str = "Altitude_m_MSL",
    ozone_col: str = "Ozone_ppbv",
    bin_m: int = 50,
    window: int = 11,
    show_raw: bool = True,
    show_ci: bool = True,
    title: str = "NASA SOOT — Ozone vs Altitude",
) -> matplotlib.figure.Figure:
    fig = matplotlib.figure.Figure(figsize=(8, 7), dpi=150)
    ax = fig.add_subplot(111)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    if show_raw:
        ax.scatter(
            cleaned[ozone_col],
            cleaned[alt_col],
            s=7,
            alpha=0.10,
            linewidths=0,
            color="#9aa0a6",
            label="Raw",
        )

    ax.plot(
        profile["mean"],
        profile["alt_bin"],
        linewidth=1.4,
        alpha=0.75,
        color="#1f77b4",
        label=f"Binned mean ({bin_m} m)",
    )

    ax.plot(
        profile["mean_smooth"],
        profile["alt_bin"],
        linewidth=2.6,
        color="#d62728",
        label=f"Smoothed (rolling {window} bins)",
    )

    if show_ci:
        mask = profile["sem"].notna() & profile["mean_smooth"].notna()
        if mask.any():
            lower = profile.loc[mask, "mean_smooth"] - 1.96 * profile.loc[mask, "sem"]
            upper = profile.loc[mask, "mean_smooth"] + 1.96 * profile.loc[mask, "sem"]

            ax.fill_betweenx(
                y=profile.loc[mask, "alt_bin"],
                x1=lower,
                x2=upper,
                alpha=0.18,
                color="#ff7f0e",
                label="~95% CI (SEM)",
            )

    ax.set_title(title)
    ax.set_xlabel("Ozone (ppbv)")
    ax.set_ylabel("Altitude (m MSL)")
    ax.grid(True, alpha=0.22)
    ax.legend(frameon=False, loc="best")

    # ------------------------------------------------------------
    # FIXED AXIS LIMITS (independent of plotting order)
    # ------------------------------------------------------------
    x_min = cleaned[ozone_col].min()
    x_max = cleaned[ozone_col].max()

    y_min = cleaned[alt_col].min()
    y_max = cleaned[alt_col].max()

    # Optional padding so points don’t sit on edges
    x_pad = 0.05 * (x_max - x_min)
    y_pad = 0.05 * (y_max - y_min)

    ax.set_xlim(x_min - x_pad, x_max + x_pad)
    ax.set_ylim(y_min - y_pad, y_max + y_pad)

    fig.tight_layout()
    return fig


def build_altitude_profile_figure(
    df: pd.DataFrame,
    *,
    alt_col: str = "Altitude_m_MSL",
    ozone_col: str = "Ozone_ppbv",
    bin_m: int = 50,
    window: int = 11,
    show_raw: bool = True,
    show_ci: bool = True,
    title: str = "NASA SOOT — Ozone vs Altitude",
) -> matplotlib.figure.Figure:
    cleaned, profile = build_altitude_profile(
        df,
        alt_col=alt_col,
        ozone_col=ozone_col,
        bin_m=bin_m,
        window=window,
    )

    return make_altitude_profile_plot(
        cleaned,
        profile,
        alt_col=alt_col,
        ozone_col=ozone_col,
        bin_m=bin_m,
        window=window,
        show_raw=show_raw,
        show_ci=show_ci,
        title=title,
    )


def clean_time_ozone(
    df: pd.DataFrame,
    *,
    time_col: str = "Datetime_Mid",
    ozone_col: str = "Ozone_ppbv",
) -> pd.DataFrame:
    _validate_columns(df, [time_col, ozone_col])

    x = _replace_fill_values(df.copy())

    x[time_col] = pd.to_datetime(x[time_col], errors="coerce")
    x[ozone_col] = pd.to_numeric(x[ozone_col], errors="coerce")

    x.loc[x[ozone_col] < 0, ozone_col] = np.nan

    x = x.dropna(subset=[time_col, ozone_col]).sort_values(time_col).copy()

    if x.empty:
        raise ValueError("No valid rows remain after time/ozone cleaning.")

    return x


def build_time_series(
    df: pd.DataFrame,
    *,
    time_col: str = "Datetime_Mid",
    ozone_col: str = "Ozone_ppbv",
    grid: str = "10s",
    smooth_window: int = 21,
) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    if smooth_window <= 0:
        raise ValueError("smooth_window must be > 0.")

    cleaned = clean_time_ozone(df, time_col=time_col, ozone_col=ozone_col)

    ts = cleaned.set_index(time_col)[ozone_col]
    ts_reg = ts.resample(grid).mean().interpolate("time")
    smooth = ts_reg.rolling(window=smooth_window, center=True, min_periods=1).mean()

    return cleaned, ts_reg, smooth


def make_time_series_plot(
    ts_reg: pd.Series,
    smooth: pd.Series,
    *,
    grid: str = "10s",
    title: str = "NASA SOOT — Ozone vs Time",
) -> matplotlib.figure.Figure:
    fig = matplotlib.figure.Figure(figsize=(11, 5.8), dpi=150)
    ax = fig.add_subplot(111)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    ax.plot(
        ts_reg.index,
        ts_reg.values,
        linewidth=1.0,
        alpha=0.35,
        label=f"Ozone ({grid} grid)",
    )

    ax.plot(
        smooth.index,
        smooth.values,
        linewidth=2.2,
        label="Smoothed",
    )

    ax.set_title(title)
    ax.set_xlabel("Time")
    ax.set_ylabel("Ozone (ppbv)")
    ax.grid(True, alpha=0.25)

    loc = mdates.AutoDateLocator(minticks=6, maxticks=10)
    ax.xaxis.set_major_locator(loc)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(loc))
    ax.legend(frameon=False)

    fig.tight_layout()
    return fig