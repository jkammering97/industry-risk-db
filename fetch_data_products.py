#%%
import pandas as pd
import requests
import os
import json
from dotenv import load_dotenv
from pathlib import Path


def load_environment():
    """Load environment variables from .env file."""
    load_dotenv()
    api_key = os.getenv('comtrade_subscription_key')
    if not api_key:
        raise ValueError("API key not loaded. Check your .env file and variable name.")
    print(f"✓ API key loaded: {api_key[:6]}...")
    return api_key


def load_partner_areas(filepath: str) -> dict:
    """
    Load partner areas from JSON file and create a mapping of PartnerCode to text.
    
    Args:
        filepath: Path to partnerAreas.json
        
    Returns:
        Dictionary mapping PartnerCode to text description
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        partner_map = {item['PartnerCode']: item['text'] for item in data.get('results', [])}
        print(f"✓ Loaded {len(partner_map)} partner areas from {filepath}")
        return partner_map
    except FileNotFoundError:
        print(f"✗ Partner areas file not found: {filepath}")
        return {}
    except json.JSONDecodeError:
        print(f"✗ Invalid JSON in partner areas file: {filepath}")
        return {}


def fetch_trade_data(api_key: str, reporter_code: str = "40", period: str = "2024", 
                     cmd_code: str = "7208", flow_code: str = "M,X") -> dict:
    """
    Fetch trade data from UN Comtrade API.
    
    Args:
        api_key: Comtrade API subscription key
        reporter_code: Country/region code (default: 40 = Austria)
        period: Year or period (default: 2024)
        cmd_code: Commodity code (default: 7208)
        flow_code: Flow type - M (imports), X (exports) (default: M,X)
        
    Returns:
        Parsed JSON response from API
    """
    url = "https://comtradeapi.un.org/data/v1/get/C/A/HS"
    
    params = {
        "reporterCode": reporter_code,
        "period": period,
        "partner": "everything",
        "cmdCode": cmd_code,
        "flowCode": flow_code
    }
    
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    
    print(f"\n📡 Fetching trade data from Comtrade API...")
    print(f"   Reporter: {reporter_code}, Period: {period}, Commodity: {cmd_code}")
    
    response = requests.get(url, params=params, headers=headers)
    
    if response.status_code != 200:
        print(f"✗ API Error {response.status_code}")
        return None
    
    try:
        data = response.json()
        print(f"✓ API response received")
        return data
    except json.JSONDecodeError:
        print("✗ Invalid JSON response from API")
        return None


def process_trade_dataframe(data: dict) -> pd.DataFrame:
    """
    Convert API response to processed DataFrame with selected columns.
    
    Args:
        data: Parsed JSON response from API
        
    Returns:
        Processed DataFrame with renamed columns
    """
    if not data or "data" not in data:
        print("✗ No 'data' field found in API response")
        return None
    
    # Normalize JSON to DataFrame
    df = pd.json_normalize(data["data"])
    print(f"✓ Created DataFrame with shape {df.shape}")
    
    # Select relevant columns
    selected_columns = [
        "refYear",
        # "reporterISO",
        "flowCode",
        "partnerCode",
        "cmdCode",
        "netWgt",
        "primaryValue"
    ]
    
    # Keep only columns that exist in the data
    selected_columns = [col for col in selected_columns if col in df.columns]
    df = df[selected_columns]
    
    # Rename columns for clarity
    df = df.rename(columns={
        "primaryValue": "tradeValueUSD",
        "netWgt": "netWeightKg",
    })

    # Ensure expected columns exist (Comtrade responses can vary by query)
    if "netWeightKg" not in df.columns:
        # try common alternates
        if "netWgt" in df.columns:
            df["netWeightKg"] = df["netWgt"]
        elif "netWgtKg" in df.columns:
            df["netWeightKg"] = df["netWgtKg"]
        else:
            df["netWeightKg"] = pd.NA

    if "tradeValueUSD" not in df.columns:
        # try alternates (depending on API settings)
        if "primaryValue" in df.columns:
            df["tradeValueUSD"] = df["primaryValue"]
        elif "fobvalue" in df.columns:
            df["tradeValueUSD"] = df["fobvalue"]
        elif "cifvalue" in df.columns:
            df["tradeValueUSD"] = df["cifvalue"]
        else:
            df["tradeValueUSD"] = pd.NA

    # Format numeric columns for readability
    df["netWeightKg"] = pd.to_numeric(df["netWeightKg"], errors="coerce").round(0).astype("Int64")
    df["tradeValueUSD"] = pd.to_numeric(df["tradeValueUSD"], errors="coerce").round(0).astype("Int64")
    
    print(f"✓ Processed DataFrame shape: {df.shape}")
    return df


def enrich_dataframe_with_partner_text(df: pd.DataFrame, partner_map: dict) -> pd.DataFrame:
    """Add partner text description to DataFrame using partnerCode lookup.

    Args:
        df: Trade data DataFrame
        partner_map: Dictionary mapping PartnerCode to text

    Returns:
        DataFrame with new 'partnerText' column
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    # Prefer mapping from partnerCode if present.
    if 'partnerCode' in df.columns:
        df['partnerText'] = df['partnerCode'].map(partner_map)
    else:
        # Fallbacks if partnerCode was not included in the selected columns
        if 'partnerDesc' in df.columns:
            df['partnerText'] = df['partnerDesc']
        elif 'partnerISO' in df.columns:
            df['partnerText'] = df['partnerISO']
        else:
            df['partnerText'] = pd.NA

    # Show stats (guard if column is entirely NA)
    matched = df['partnerText'].notna().sum() if 'partnerText' in df.columns else 0
    total = len(df)
    unmatched = total - matched

    print(f"✓ Enriched DataFrame with partner text")
    print(f"   Matched: {matched}/{total} ({100*matched/total:.1f}%)")
    if unmatched > 0:
        print(f"   ⚠ Unmatched partner codes: {unmatched}")

    return df


def main(country=None, flow_code="M"):
    """Main execution function."""
    print("=" * 60)
    print("Trade Data Fetch & Processing")
    print("=" * 60)
    
    # Setup
    api_key = load_environment()
    
    # Get file paths
    script_dir = Path(__file__).parent
    partner_areas_file = script_dir / "partnerAreas.json"
    
    # Load partner areas mapping
    partner_map = load_partner_areas(str(partner_areas_file))
    
    # Determine reporter code: use provided `country` if available
    reporter = str(country) if country is not None else "40"

    # Fetch trade data
    data = fetch_trade_data(api_key, reporter_code=reporter, flow_code=flow_code)
    if not data:
        print("✗ Failed to fetch trade data. Exiting.")
        return
    
    # Process DataFrame
    df = process_trade_dataframe(data)
    if df is None:
        print("✗ Failed to process DataFrame. Exiting.")
        return
    
    # Enrich with partner text
    if partner_map:
        df = enrich_dataframe_with_partner_text(df, partner_map)
    
    # Display results
    print(f"\n{'=' * 60}")
    print("Sample Data:")
    print("=" * 60)
    print(df.head(10))
    
    print(f"\n{'=' * 60}")
    print("DataFrame Summary:")
    print("=" * 60)
    print(df.info())
    
    return df

#%%
if __name__ == "__main__":
    #%%
    df = main()
#%%