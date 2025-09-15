#
# src/01_fetch_comtrade_data.py
# Final corrected version with flowCode argument
#
import os
import pandas as pd
import argparse
from dotenv import load_dotenv
from pathlib import Path
import comtradeapicall

# --- CONFIGURATION ---
load_dotenv()
PROJECT_ROOT = Path(__file__).resolve().parents[1]
COUNTRY_CODES = {
    "India": 699, "Maldives": 462, "USA": 842, "China": 156, "Japan": 392,
}

# --- FUNCTIONS ---
def get_comtrade_data(reporter: str, partner: str, year: int, flow_code: str, api_key: str) -> pd.DataFrame:
    """
    Fetches trade data from UN Comtrade using the official comtradeapicall package.
    """
    if not api_key:
        raise ValueError("API key not found. Please set COMTRADE_API_KEY in your .env file.")
    
    print(f"Fetching {flow_code} data using the official comtradeapicall package...")
    
    df = comtradeapicall.getFinalData(
        subscription_key=api_key,
        typeCode='C',
        freqCode='A',
        clCode='HS',
        period=str(year),
        reporterCode=COUNTRY_CODES[reporter],
        partnerCode=COUNTRY_CODES[partner],
        cmdCode='TOTAL',
        flowCode=flow_code, # <-- FIX: Use the argument passed to the function
        partner2Code=None,
        customsCode=None,
        motCode=None,
        maxRecords=250000 
    )
    
    return df

def process_trade_data(df: pd.DataFrame) -> pd.DataFrame:
    """Processes the DataFrame from the API into a cleaner format."""
    if df is None or df.empty:
        print("Warning: API returned no data for the selected parameters.")
        return pd.DataFrame()

    relevant_cols = {
        'year': 'Year', 'reporterDesc': 'Reporter', 'partnerDesc': 'Partner',
        'flowDesc': 'Trade Flow', 'cmdCode': 'HS Code', 'cmdDesc': 'Commodity',
        'primaryValue': 'Trade Value (US$)', 'netWgt': 'Net Weight (kg)'
    }
    
    cols_to_keep = {k: v for k, v in relevant_cols.items() if k in df.columns}
    
    processed_df = df[list(cols_to_keep.keys())].copy()
    processed_df.rename(columns=cols_to_keep, inplace=True)
    
    return processed_df

# --- SCRIPT EXECUTION ---
def main():
    parser = argparse.ArgumentParser(description="Fetch UN Comtrade data and save it to data/raw.")
    parser.add_argument("reporter", type=str, choices=COUNTRY_CODES.keys(), help="The reporting country.")
    parser.add_argument("partner", type=str, choices=COUNTRY_CODES.keys(), help="The partner country.")
    parser.add_argument("year", type=int, help="The year of the data.")
    # FIX: Add a new argument for trade flow
    parser.add_argument("flow", type=str, choices=['M', 'X'], help="The trade flow ('M' for Imports, 'X' for Exports).")
    args = parser.parse_args()

    API_KEY = os.getenv("COMTRADE_API_KEY")

    try:
        raw_df = get_comtrade_data(args.reporter, args.partner, args.year, args.flow, API_KEY)
        processed_df = process_trade_data(raw_df)

        if not processed_df.empty:
            output_dir = PROJECT_ROOT / "data" / "raw"
            output_dir.mkdir(parents=True, exist_ok=True)
            # FIX: Update the filename to be more descriptive
            flow_name = "Imports" if args.flow == "M" else "Exports"
            csv_filename = f"{args.reporter}_{args.partner}_{flow_name}_{args.year}.csv"
            output_path = output_dir / csv_filename
            processed_df.to_csv(output_path, index=False)
            print(f"✅ Data successfully saved to: {output_path}")
        else:
            print(f"❌ No data was returned for {args.reporter}-{args.partner} ({args.flow}) in {args.year}.")

    except Exception as e:
        print(f"❌ An error occurred: {str(e)}")

if __name__ == "__main__":
    main()