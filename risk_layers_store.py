from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv

try:
    from azure.data.tables import TableServiceClient, UpdateMode
except ImportError:  # pragma: no cover - optional dependency in local bootstrap
    TableServiceClient = None
    UpdateMode = None


RISK_TABLES = {
    "hhi": "hhirisk",
    "logistics": "logisticsrisk",
    "policy": "policyrisk",
}

RISK_SCORE_COLUMNS = {
    "hhi": "riskScore",
    "logistics": "riskScore",
    "policy": "riskScore",
}


SAMPLE_LAYER_DATA = {
    "hhi": [
        {"supplierCountry": "China", "hsCode": "7208", "year": 2024, "hhiValue": 0.34, "hhiWeight": 0.29, "riskScore": 0.82},
        {"supplierCountry": "Germany", "hsCode": "7208", "year": 2024, "hhiValue": 0.21, "hhiWeight": 0.19, "riskScore": 0.36},
        {"supplierCountry": "Turkey", "hsCode": "7208", "year": 2024, "hhiValue": 0.17, "hhiWeight": 0.16, "riskScore": 0.44},
        {"supplierCountry": "India", "hsCode": "7208", "year": 2024, "hhiValue": 0.14, "hhiWeight": 0.12, "riskScore": 0.51},
        {"supplierCountry": "Viet Nam", "hsCode": "7208", "year": 2024, "hhiValue": 0.09, "hhiWeight": 0.08, "riskScore": 0.40},
    ],
    "logistics": [
        {"supplierCountry": "China", "route": "CN-EU Rail/Sea", "leadTimeDays": 44, "leadTimeStdDays": 9.4, "freightIndex": 1.31, "riskScore": 0.74},
        {"supplierCountry": "Germany", "route": "DE-AT Road", "leadTimeDays": 4, "leadTimeStdDays": 1.1, "freightIndex": 1.02, "riskScore": 0.19},
        {"supplierCountry": "Turkey", "route": "TR-AT Road/Sea", "leadTimeDays": 11, "leadTimeStdDays": 2.7, "freightIndex": 1.11, "riskScore": 0.41},
        {"supplierCountry": "India", "route": "IN-EU Sea", "leadTimeDays": 32, "leadTimeStdDays": 7.1, "freightIndex": 1.22, "riskScore": 0.59},
        {"supplierCountry": "Viet Nam", "route": "VN-EU Sea", "leadTimeDays": 38, "leadTimeStdDays": 8.5, "freightIndex": 1.27, "riskScore": 0.67},
    ],
    "policy": [
        {"supplierCountry": "China", "hsCode": "7208", "tariffPct": 2.8, "sanctionsFlag": 0, "policyVolatility": 0.43, "riskScore": 0.61},
        {"supplierCountry": "Germany", "hsCode": "7208", "tariffPct": 0.0, "sanctionsFlag": 0, "policyVolatility": 0.12, "riskScore": 0.12},
        {"supplierCountry": "Turkey", "hsCode": "7208", "tariffPct": 1.9, "sanctionsFlag": 0, "policyVolatility": 0.33, "riskScore": 0.39},
        {"supplierCountry": "India", "hsCode": "7208", "tariffPct": 2.5, "sanctionsFlag": 0, "policyVolatility": 0.47, "riskScore": 0.53},
        {"supplierCountry": "Viet Nam", "hsCode": "7208", "tariffPct": 2.1, "sanctionsFlag": 0, "policyVolatility": 0.28, "riskScore": 0.34},
    ],
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_country_code(country_code: str) -> str:
    code = str(country_code).strip()
    if code.isdigit():
        return code.zfill(3)
    return code


class RiskLayerStore:
    def __init__(self, connection_string: Optional[str] = None) -> None:
        load_dotenv()
        self.connection_string = connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self._service_client = None

        if self.connection_string and TableServiceClient is not None:
            self._service_client = TableServiceClient.from_connection_string(self.connection_string)

    @property
    def azure_enabled(self) -> bool:
        return self._service_client is not None

    def ensure_tables(self) -> bool:
        if not self.azure_enabled:
            return False

        for table_name in RISK_TABLES.values():
            self._service_client.create_table_if_not_exists(table_name=table_name)
        return True

    def upsert_layer_records(self, layer: str, records: List[dict], country_code: str = "040") -> int:
        if layer not in RISK_TABLES:
            raise ValueError(f"Unknown layer: {layer}")

        if not self.azure_enabled:
            return 0

        normalized_code = normalize_country_code(country_code)
        table_name = RISK_TABLES[layer]
        table_client = self._service_client.get_table_client(table_name=table_name)
        inserted = 0

        for idx, record in enumerate(records):
            row_key = str(record.get("rowKey", f"{normalized_code}-{idx+1:03d}"))
            payload = {
                "PartitionKey": str(record.get("PartitionKey", normalized_code)),
                "RowKey": row_key,
                "layer": layer,
                "updatedAt": _utc_now_iso(),
            }
            payload.update(record)
            payload.pop("rowKey", None)
            if UpdateMode is not None:
                table_client.upsert_entity(mode=UpdateMode.REPLACE, entity=payload)
            else:  # pragma: no cover
                table_client.upsert_entity(entity=payload)
            inserted += 1

        return inserted

    def seed_sample_data(self, country_code: str = "040") -> Dict[str, int]:
        results: Dict[str, int] = {}
        if not self.azure_enabled:
            return results

        normalized_code = normalize_country_code(country_code)
        for layer, rows in SAMPLE_LAYER_DATA.items():
            layer_rows = []
            for idx, row in enumerate(rows):
                current = row.copy()
                current["PartitionKey"] = normalized_code
                current["rowKey"] = f"{normalized_code}-{layer}-{idx+1:03d}"
                layer_rows.append(current)
            results[layer] = self.upsert_layer_records(layer, layer_rows, country_code=normalized_code)

        return results

    def fetch_layer(self, layer: str, country_code: str = "040") -> pd.DataFrame:
        if layer not in RISK_TABLES:
            raise ValueError(f"Unknown layer: {layer}")

        normalized_code = normalize_country_code(country_code)
        if self.azure_enabled:
            table_name = RISK_TABLES[layer]
            table_client = self._service_client.get_table_client(table_name=table_name)
            filter_query = f"PartitionKey eq '{normalized_code}'"
            entities = list(table_client.query_entities(query_filter=filter_query))
            if entities:
                return pd.DataFrame(entities)
            return pd.DataFrame()

        local_rows = []
        for idx, row in enumerate(SAMPLE_LAYER_DATA[layer]):
            current = row.copy()
            current["PartitionKey"] = normalized_code
            current["RowKey"] = f"{normalized_code}-{layer}-{idx+1:03d}"
            current["layer"] = layer
            current["updatedAt"] = _utc_now_iso()
            local_rows.append(current)
        return pd.DataFrame(local_rows)


def build_combined_supplier_risk(
    hhi_df: pd.DataFrame,
    logistics_df: pd.DataFrame,
    policy_df: pd.DataFrame,
    weights: Optional[Dict[str, float]] = None,
) -> pd.DataFrame:
    active_weights = weights or {"hhi": 0.5, "logistics": 0.3, "policy": 0.2}

    if hhi_df.empty and logistics_df.empty and policy_df.empty:
        return pd.DataFrame(columns=["supplierCountry", "hhi_risk", "logistics_risk", "policy_risk", "overall_risk"])

    def agg_layer(df: pd.DataFrame, score_col: str, target_col: str) -> pd.DataFrame:
        if df.empty or "supplierCountry" not in df.columns or score_col not in df.columns:
            return pd.DataFrame(columns=["supplierCountry", target_col])
        out = (
            df[["supplierCountry", score_col]]
            .dropna(subset=["supplierCountry"])
            .groupby("supplierCountry", as_index=False)[score_col]
            .mean()
            .rename(columns={score_col: target_col})
        )
        return out

    out = agg_layer(hhi_df, RISK_SCORE_COLUMNS["hhi"], "hhi_risk")
    out = out.merge(agg_layer(logistics_df, RISK_SCORE_COLUMNS["logistics"], "logistics_risk"), on="supplierCountry", how="outer")
    out = out.merge(agg_layer(policy_df, RISK_SCORE_COLUMNS["policy"], "policy_risk"), on="supplierCountry", how="outer")
    out = out.fillna(0.0)
    out["overall_risk"] = (
        out["hhi_risk"] * active_weights["hhi"]
        + out["logistics_risk"] * active_weights["logistics"]
        + out["policy_risk"] * active_weights["policy"]
    )
    out = out.sort_values("overall_risk", ascending=False).reset_index(drop=True)
    return out
