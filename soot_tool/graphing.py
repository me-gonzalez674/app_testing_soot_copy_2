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


def clean_data(
    df: pd.DataFrame,
    *,
    y_col: str = "Altitude_m_MSL",
    x_col: str = "Ozone_ppbv",
    y_min: float | None = None,
    y_max: float | None = None,
) -> pd.DataFrame:
    _validate_columns(df, [y_col, x_col])

    x = _replace_fill_values(df.copy())

    x[y_col] = pd.to_numeric(x[y_col], errors="coerce")
    x[x_col] = pd.to_numeric(x[x_col], errors="coerce")

    x.loc[x[x_col] <= 0, x_col] = np.nan

    x = x.dropna(subset=[y_col, x_col]).copy()

    if y_min is not None:
        x = x[x[y_col] >= y_min]
    if y_max is not None:
        x = x[x[y_col] <= y_max]

    if x.empty:
        raise ValueError("No valid rows remain after cleaning.")

    return x


def build_profile(
    df: pd.DataFrame,
    *,
    y_col: str = "Altitude_m_MSL",
    x_col: str = "Ozone_ppbv",
    min_periods: int = 3,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cleaned = clean_data(df, y_col=y_col, x_col=x_col)
    return cleaned


def make_plot(
    cleaned: pd.DataFrame,
    *,
    y_col: str = "Altitude_m_MSL",
    x_col: str = "Ozone_ppbv",
    show_raw: bool = True,
    title: str = f"NASA SOOT Visualization",
) -> matplotlib.figure.Figure:
    fig = matplotlib.figure.Figure(figsize=(8, 7), dpi=150)
    ax = fig.add_subplot(111)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    if show_raw:
        ax.scatter(
            cleaned[x_col],
            cleaned[y_col],
            s=7,
            alpha=0.10,
            linewidths=0,
            color="#9aa0a6",
            label="Raw",
        )

    ax.set_title(title)
    ax.set_xlabel(f"{x_col}")
    ax.set_ylabel(f"{y_cl}")
    ax.grid(True, alpha=0.22)
    ax.legend(frameon=False, loc="best")

    # ------------------------------------------------------------
    # FIXED AXIS LIMITS (independent of plotting order)
    # ------------------------------------------------------------
    x_min = cleaned[x_col].min()
    x_max = cleaned[x_col].max()

    y_min = cleaned[y_col].min()
    y_max = cleaned[y_col].max()

    # Optional padding so points don’t sit on edges
    x_pad = 0.05 * (x_max - x_min)
    y_pad = 0.05 * (y_max - y_min)

    ax.set_xlim(x_min - x_pad, x_max + x_pad)
    ax.set_ylim(y_min - y_pad, y_max + y_pad)

    fig.tight_layout()
    return fig


def build_figure(
    df: pd.DataFrame,
    *,
    y_col: str = "Altitude_m_MSL",
    x_col: str = "Ozone_ppbv",
    show_raw: bool = True,
    title: str = "NASA SOOT — Ozone vs Altitude",
) -> matplotlib.figure.Figure:
    cleaned, profile = build_profile(
        df,
        y_col=y_col,
        x_col=x_col,
    )

    return make_plot(
        cleaned,
        y_col=y_col,
        x_col=x_col,
        show_raw=show_raw,
        title=title,
    )
