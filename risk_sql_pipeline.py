from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List
from urllib.parse import quote_plus

from dotenv import load_dotenv
import pandas as pd

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
else:
    Engine = object

from fetch_data_products import (
    enrich_dataframe_with_partner_text,
    fetch_trade_data,
    load_environment,
    load_partner_areas,
    process_trade_dataframe,
)


def normalize_country_code(country_code: str) -> str:
    code = str(country_code).strip()
    if code.isdigit():
        return code.zfill(3)
    return code


def to_comtrade_reporter_code(country_code: str) -> str:
    code = normalize_country_code(country_code)
    if code.isdigit():
        return str(int(code))
    return code


def get_sql_engine() -> Engine:
    try:
        from sqlalchemy import create_engine
    except ImportError as exc:
        raise ImportError("sqlalchemy is required. Install with `pip install -r requirements-sql.txt`.") from exc

    dotenv_file = Path(__file__).parent / ".env"
    load_dotenv(dotenv_path=dotenv_file, override=True)
    server = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DATABASE")
    user = os.getenv("AZURE_SQL_USER")
    password = os.getenv("AZURE_SQL_PASSWORD")
    driver = os.getenv("AZURE_SQL_DRIVER")
    if not driver:
        try:
            import pyodbc

            available = set(pyodbc.drivers())
            if "ODBC Driver 18 for SQL Server" in available:
                driver = "ODBC Driver 18 for SQL Server"
            elif "ODBC Driver 17 for SQL Server" in available:
                driver = "ODBC Driver 17 for SQL Server"
            else:
                driver = "ODBC Driver 18 for SQL Server"
        except Exception:
            driver = "ODBC Driver 18 for SQL Server"

    missing = [key for key, value in {
        "AZURE_SQL_SERVER": server,
        "AZURE_SQL_DATABASE": database,
        "AZURE_SQL_USER": user,
        "AZURE_SQL_PASSWORD": password,
    }.items() if not value]
    if missing:
        raise ValueError(f"Missing SQL env vars: {', '.join(missing)}")

    safe_driver = quote_plus(driver)
    safe_user = quote_plus(user)
    safe_password = quote_plus(password)
    safe_database = quote_plus(database)
    safe_server = quote_plus(server)
    url = (
        f"mssql+pyodbc://{safe_user}:{safe_password}@{safe_server}:1433/{safe_database}"
        f"?driver={safe_driver}&Encrypt=yes&TrustServerCertificate=no&Connection+Timeout=30"
    )
    return create_engine(url, future=True)


def _stable_bucket(value: str) -> int:
    return sum(ord(char) for char in str(value)) % 100


def _to_csv_list(items: Iterable[str]) -> str:
    return ",".join(str(item) for item in items)


def _exclude_world_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    world_code_mask = pd.Series(False, index=out.index)
    world_text_mask = pd.Series(False, index=out.index)

    if "partner_code" in out.columns:
        partner_codes = out["partner_code"].astype(str).str.strip()
        world_code_mask = partner_codes.isin({"0", "00", "000"})

    if "partner_text" in out.columns:
        partner_text = out["partner_text"].fillna("").astype(str).str.strip().str.lower()
        world_text_mask = partner_text.eq("world")

    return out.loc[~(world_code_mask | world_text_mask)].copy()


def fetch_comtrade_rows(
    reporter_code: str,
    period: str,
    cmd_codes: List[str],
    flow_codes: List[str],
) -> pd.DataFrame:
    api_key = load_environment()
    partner_file = Path(__file__).parent / "partnerAreas.json"
    partner_map = load_partner_areas(str(partner_file))
    collected: List[pd.DataFrame] = []

    for cmd_code in cmd_codes:
        for flow_code in flow_codes:
            payload = fetch_trade_data(
                api_key=api_key,
                reporter_code=reporter_code,
                period=period,
                cmd_code=cmd_code,
                flow_code=flow_code,
            )
            df = process_trade_dataframe(payload)
            if df is None or df.empty:
                continue

            if partner_map:
                df = enrich_dataframe_with_partner_text(df, partner_map)

            df = df.rename(columns={
                "flowCode": "flow_code",
                "partnerCode": "partner_code",
                "partnerText": "partner_text",
                "cmdCode": "cmd_code",
                "refYear": "ref_year",
                "tradeValueUSD": "trade_value_usd",
                "netWeightKg": "net_weight_kg",
            })
            df = _exclude_world_rows(df)
            if df.empty:
                continue
            df["reporter_code"] = reporter_code
            df["source_period"] = period
            df["source_system"] = "UN_COMTRADE"
            collected.append(df)

    if not collected:
        return pd.DataFrame()

    out = pd.concat(collected, ignore_index=True, sort=False)
    return out


def build_logistics_signals(trade_df: pd.DataFrame, ingest_id: str, ingested_at: datetime) -> pd.DataFrame:
    if trade_df.empty:
        return pd.DataFrame()

    grouped = (
        trade_df.groupby(["reporter_code", "partner_code", "partner_text"], as_index=False, dropna=False)[
            ["trade_value_usd"]
        ].sum()
        .rename(columns={"trade_value_usd": "total_trade_value"})
    )
    total = grouped["total_trade_value"].fillna(0).sum()
    grouped["trade_share"] = grouped["total_trade_value"].fillna(0) / total if total > 0 else 0.0

    rows = []
    for _, row in grouped.iterrows():
        code = str(row.get("partner_code", "UNK"))
        bucket = _stable_bucket(code)
        lead_time_days = 5 + bucket * 0.45
        lead_time_stddev_days = 1 + (bucket % 12) * 0.45
        freight_index = 0.85 + (bucket % 60) / 100
        disruption_index = min(1.0, float(row["trade_share"]) * 1.6 + (bucket % 20) / 100)

        risk_score = min(
            1.0,
            0.35 * (lead_time_days / 50.0)
            + 0.30 * ((freight_index - 0.85) / 0.60)
            + 0.35 * disruption_index,
        )

        rows.append(
            {
                "ingest_id": ingest_id,
                "ingested_at": ingested_at,
                "reporter_code": row["reporter_code"],
                "supplier_country_code": code,
                "supplier_country": row.get("partner_text"),
                "route_name": f"{code}-AT",
                "lead_time_days": lead_time_days,
                "lead_time_stddev_days": lead_time_stddev_days,
                "freight_index": freight_index,
                "disruption_index": disruption_index,
                "risk_score": risk_score,
                "source_system": "HEURISTIC_V1",
            }
        )

    return pd.DataFrame(rows)


def build_policy_signals(trade_df: pd.DataFrame, ingest_id: str, ingested_at: datetime) -> pd.DataFrame:
    if trade_df.empty:
        return pd.DataFrame()

    grouped = (
        trade_df.groupby(["reporter_code", "partner_code", "partner_text", "cmd_code"], as_index=False, dropna=False)[
            ["trade_value_usd"]
        ].sum()
        .rename(columns={"trade_value_usd": "total_trade_value"})
    )

    reporter_total = grouped["total_trade_value"].fillna(0).sum()
    grouped["trade_share"] = grouped["total_trade_value"].fillna(0) / reporter_total if reporter_total > 0 else 0.0

    rows = []
    for _, row in grouped.iterrows():
        code = str(row.get("partner_code", "UNK"))
        bucket = _stable_bucket(code)
        tariff_pct = (bucket % 45) / 10.0
        sanctions_flag = 1 if bucket >= 98 else 0
        export_control_flag = 1 if bucket % 17 == 0 else 0
        policy_volatility = min(1.0, (bucket % 70) / 100 + float(row["trade_share"]) * 0.5)

        risk_score = min(
            1.0,
            0.35 * (tariff_pct / 4.5)
            + 0.30 * sanctions_flag
            + 0.10 * export_control_flag
            + 0.25 * policy_volatility,
        )

        rows.append(
            {
                "ingest_id": ingest_id,
                "ingested_at": ingested_at,
                "reporter_code": row["reporter_code"],
                "supplier_country_code": code,
                "supplier_country": row.get("partner_text"),
                "hs_code": str(row.get("cmd_code") or ""),
                "tariff_pct": tariff_pct,
                "sanctions_flag": sanctions_flag,
                "export_control_flag": export_control_flag,
                "policy_volatility": policy_volatility,
                "risk_score": risk_score,
                "source_system": "HEURISTIC_V1",
            }
        )

    return pd.DataFrame(rows)


def append_raw_tables(
    engine: Engine,
    comtrade_df: pd.DataFrame,
    logistics_df: pd.DataFrame,
    policy_df: pd.DataFrame,
) -> None:
    # SQL Server supports at most 2100 parameters per statement.
    # pandas.to_sql(method="multi") binds rows*columns parameters in one INSERT.
    # Keep headroom below the limit for stable inserts across large datasets.
    def _safe_chunksize(df: pd.DataFrame, max_params: int = 2000, default: int = 2000) -> int:
        cols = max(1, len(df.columns))
        return max(1, min(default, max_params // cols))

    if not comtrade_df.empty:
        comtrade_chunksize = _safe_chunksize(comtrade_df)
        comtrade_df.to_sql(
            name="comtrade_trade",
            schema="raw",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=comtrade_chunksize,
        )

    if not logistics_df.empty:
        logistics_chunksize = _safe_chunksize(logistics_df)
        logistics_df.to_sql(
            name="logistics_signals",
            schema="raw",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=logistics_chunksize,
        )

    if not policy_df.empty:
        policy_chunksize = _safe_chunksize(policy_df)
        policy_df.to_sql(
            name="policy_signals",
            schema="raw",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=policy_chunksize,
        )


def run_pipeline(country_code: str, period: str, cmd_codes: List[str], flow_codes: List[str]) -> None:
    normalized_country = normalize_country_code(country_code)
    reporter_code = to_comtrade_reporter_code(normalized_country)
    engine = get_sql_engine()

    comtrade = fetch_comtrade_rows(
        reporter_code=reporter_code,
        period=period,
        cmd_codes=cmd_codes,
        flow_codes=flow_codes,
    )
    if comtrade.empty:
        print("No comtrade rows fetched.")
        return

    ingest_time = datetime.now(timezone.utc).replace(tzinfo=None)
    ingest_id = f"{normalized_country}-{period}-{ingest_time.strftime('%Y%m%d%H%M%S')}"
    comtrade["ingest_id"] = ingest_id
    comtrade["ingested_at"] = ingest_time
    comtrade["reporter_code"] = normalized_country

    logistics = build_logistics_signals(comtrade, ingest_id=ingest_id, ingested_at=ingest_time)
    policy = build_policy_signals(comtrade, ingest_id=ingest_id, ingested_at=ingest_time)

    append_raw_tables(engine=engine, comtrade_df=comtrade, logistics_df=logistics, policy_df=policy)

    print("Pipeline write complete:")
    print(f"  raw.comtrade_trade: {len(comtrade)} rows")
    print(f"  raw.logistics_signals: {len(logistics)} rows")
    print(f"  raw.policy_signals: {len(policy)} rows")
    print(f"  ingest_id: {ingest_id}")
    print(f"  reporter_code: {normalized_country}")
    print(f"  cmd_codes: {_to_csv_list(cmd_codes)}")
    print(f"  flow_codes: {_to_csv_list(flow_codes)}")


def _parse_cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and load risk raw data into Azure SQL.")
    parser.add_argument("--country", default="040", help="Country code (e.g., 040 for Austria)")
    parser.add_argument("--period", default="2024", help="Comtrade period/year")
    parser.add_argument("--cmd-codes", default="7208", help="Comma-separated HS codes")
    parser.add_argument("--flows", default="M,X", help="Comma-separated flow codes")
    return parser.parse_args()


def main() -> None:
    args = _parse_cli()
    cmd_codes = [item.strip() for item in args.cmd_codes.split(",") if item.strip()]
    flow_codes = [item.strip() for item in args.flows.split(",") if item.strip()]
    run_pipeline(
        country_code=args.country,
        period=str(args.period),
        cmd_codes=cmd_codes,
        flow_codes=flow_codes,
    )


if __name__ == "__main__":
    main()
