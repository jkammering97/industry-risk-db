from risk_layers_store import RISK_TABLES, RiskLayerStore, normalize_country_code


def main() -> None:
    store = RiskLayerStore()
    if not store.azure_enabled:
        raise SystemExit(
            "AZURE_STORAGE_CONNECTION_STRING is not set or azure-data-tables is missing.\n"
            "Set the connection string in .env and install requirements first."
        )

    store.ensure_tables()
    country_code = normalize_country_code("040")
    inserted = store.seed_sample_data(country_code=country_code)

    print("Seed complete.")
    print(f"  country_code: {country_code}")
    for layer in RISK_TABLES:
        print(f"  {layer}: {inserted.get(layer, 0)} rows upserted")


if __name__ == "__main__":
    main()
