from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from fetch_data_products import load_partner_areas
from risk_layers_store import RiskLayerStore, build_combined_supplier_risk, normalize_country_code

st.set_page_config(page_title="SME Import Risk Observer", layout="wide")
st.title("SME Import Risk Observer")
st.caption("Layered risk view for Austrian sourcing decisions: HHI, logistics, policy")

script_dir = Path(__file__).parent
partner_areas_file = script_dir / "partnerAreas.json"
partner_map = load_partner_areas(str(partner_areas_file))

country_names = sorted(partner_map.values()) if partner_map else ["Austria"]
selected_country_name = st.sidebar.selectbox("Country at risk", country_names, index=country_names.index("Austria") if "Austria" in country_names else 0)

country_code = None
for code, name in partner_map.items():
    if name == selected_country_name:
        country_code = code
        break
if country_code is None:
    country_code = "040"
country_code = normalize_country_code(country_code)

store = RiskLayerStore()
if store.azure_enabled:
    st.sidebar.success("Data source: Azure Table Storage")
    store.ensure_tables()
else:
    st.sidebar.warning("Data source: Local sample (set AZURE_STORAGE_CONNECTION_STRING to switch to Azure)")

if st.sidebar.button("Seed Sample Data"):
    if store.azure_enabled:
        seeded = store.seed_sample_data(country_code=country_code)
        st.sidebar.success(f"Seeded Azure rows: {seeded}")
    else:
        st.sidebar.info("Local sample mode does not require seeding.")

hhi_df = store.fetch_layer("hhi", country_code=country_code)
logistics_df = store.fetch_layer("logistics", country_code=country_code)
policy_df = store.fetch_layer("policy", country_code=country_code)

combined = build_combined_supplier_risk(hhi_df, logistics_df, policy_df)

st.subheader(f"Selected Country: {selected_country_name} ({country_code})")

metric_cols = st.columns(4)

def avg_or_zero(df: pd.DataFrame, col: str = "riskScore") -> float:
    if df.empty or col not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0).mean())

metric_cols[0].metric("HHI Risk (avg)", f"{avg_or_zero(hhi_df):.2f}")
metric_cols[1].metric("Logistics Risk (avg)", f"{avg_or_zero(logistics_df):.2f}")
metric_cols[2].metric("Policy Risk (avg)", f"{avg_or_zero(policy_df):.2f}")
metric_cols[3].metric("Overall Supplier Risk", f"{avg_or_zero(combined, 'overall_risk'):.2f}")

if combined.empty:
    st.info("No risk records found for this country. Seed sample data or load real layer data.")
else:
    top_chart = px.bar(
        combined.head(15),
        x="supplierCountry",
        y="overall_risk",
        title="Top Supplier Risk (weighted: HHI 50%, Logistics 30%, Policy 20%)",
        labels={"supplierCountry": "Supplier Country", "overall_risk": "Risk Score"},
    )
    st.plotly_chart(top_chart, width="stretch")

tab_hhi, tab_logistics, tab_policy, tab_combined = st.tabs(
    ["HHI Layer", "Logistics Layer", "Policy Layer", "Combined View"]
)

with tab_hhi:
    st.write("Import concentration risk by supplier/HS code.")
    if hhi_df.empty:
        st.info("No HHI records available.")
    else:
        st.dataframe(hhi_df, width="stretch")

with tab_logistics:
    st.write("Lead-time and transport disruption risk.")
    if logistics_df.empty:
        st.info("No logistics records available.")
    else:
        st.dataframe(logistics_df, width="stretch")

with tab_policy:
    st.write("Tariff/sanctions/regulatory volatility risk.")
    if policy_df.empty:
        st.info("No policy records available.")
    else:
        st.dataframe(policy_df, width="stretch")

with tab_combined:
    st.write("Supplier-level risk blend used for procurement prioritization.")
    if combined.empty:
        st.info("No combined records available.")
    else:
        st.dataframe(combined, width="stretch")
