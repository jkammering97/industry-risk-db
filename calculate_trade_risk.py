from fetch_data_products import main
import pandas as pd
from typing import Tuple


def get_exports(country=None, flow_code: str = "X") -> pd.DataFrame:
    """Load exports DataFrame using `fetch_data_products.main`.

    If `country` is provided, attempt to pass it through to `main`. If the
    underlying `main` does not accept a `country` argument, fall back to
    calling it with only `flow_code`.

    Args:
        country: optional country identifier passed to the data loader
        flow_code: flow code to pass to the data loader (default: "X").

    Returns:
        DataFrame of exports.
    """
    if country is None:
        return main(flow_code=flow_code)
    try:
        return main(country=country, flow_code=flow_code)
    except TypeError:
        # underlying API doesn't accept `country`
        return main(flow_code=flow_code)


def filter_exports(exports_df: pd.DataFrame, exclude_partner: str = "World") -> pd.DataFrame:
    """Return a copy of exports with the given partner excluded.

    Args:
        exports_df: original exports DataFrame
        exclude_partner: partnerText value to exclude (default: "World")

    Returns:
        Filtered DataFrame copy.
    """
    # Handle None input from the data loader
    if exports_df is None:
        return pd.DataFrame()

    # If `partnerText` exists, filter by it; otherwise return the original DataFrame copy.
    if "partnerText" in exports_df.columns:
        mask = exports_df["partnerText"].fillna("") != exclude_partner
        return exports_df[mask].copy()
    return exports_df.copy()


def compute_hhi(df: pd.DataFrame, value_col: str = "tradeValueUSD", weight_col: str = "netWeightKg") -> Tuple[float, float, pd.DataFrame]:
    """Compute HHI (value and weight) and return DataFrame with shares.

    Returns (hhi_value, hhi_weight, df_with_shares).
    """
    # Handle None/empty input
    if df is None or df.empty:
        empty = pd.DataFrame()
        empty["value_share"] = pd.Series(dtype=float)
        empty["weight_share"] = pd.Series(dtype=float)
        return 0.0, 0.0, empty

    df_out = df.copy()
    value_series = pd.to_numeric(df_out[value_col], errors="coerce").fillna(0)
    weight_series = pd.to_numeric(df_out[weight_col], errors="coerce").fillna(0)

    total_value = value_series.sum()
    total_weight = weight_series.sum()

    # Compute value and weight concentrations independently so one zero total
    # does not erase the other metric for small-country edge cases.
    if total_value > 0:
        df_out["value_share"] = (value_series / total_value) ** 2
    else:
        df_out["value_share"] = 0.0

    if total_weight > 0:
        df_out["weight_share"] = (weight_series / total_weight) ** 2
    else:
        df_out["weight_share"] = 0.0

    hhi_value = df_out["value_share"].sum()
    hhi_weight = df_out["weight_share"].sum()

    return hhi_value, hhi_weight, df_out


def get_trade_risk(country=None, flow_code: str = "X", exclude_partner: str = "World", drop_cols: bool = True) -> Tuple[float, float, pd.DataFrame]:
    """High-level helper: load data, filter, compute HHI and return sorted DataFrame.

    Args:
        flow_code: passed to data loader
        exclude_partner: partnerText to exclude
        drop_cols: whether to drop helper columns like `refYear` and `partnerCode` if present

    Returns:
        (hhi_value, hhi_weight, sorted_filtered_df)
    """
    exports = get_exports(country=country, flow_code=flow_code)
    if exports is None or (isinstance(exports, pd.DataFrame) and exports.empty):
        # Return safe defaults when no data is available
        return 0.0, 0.0, pd.DataFrame()

    filtered = filter_exports(exports, exclude_partner)
    if filtered is None or filtered.empty:
        return 0.0, 0.0, pd.DataFrame()

    hhi_value, hhi_weight, df_with_shares = compute_hhi(filtered)

    if drop_cols:
        drop_list = [c for c in ["refYear", "partnerCode"] if c in df_with_shares.columns]
        df_out = df_with_shares.drop(columns=drop_list)
    else:
        df_out = df_with_shares

    sorted_df = df_out.sort_values(by=["value_share", "weight_share"], ascending=False)
    return hhi_value, hhi_weight, sorted_df


__all__ = [
    "get_exports",
    "filter_exports",
    "compute_hhi",
    "get_trade_risk",
]
