#
# src/01b_fetch_monthly_data.py
# FINAL version with fixes for maxRecords and find_latest_month
#
import os
import pandas as pd
import argparse
from dotenv import load_dotenv
from pathlib import Path
import comtradeapicall
from datetime import datetime
from dateutil.relativedelta import relativedelta

# --- CONFIGURATION ---
load_dotenv()
PROJECT_ROOT = Path(__file__).resolve().parents[1]
COUNTRY_CODES = {
    "India": 699, "Maldives": 462, "USA": 842, "China": 156, "Japan": 392,
}

# --- HELPER FUNCTION (NEW ROBUST VERSION) ---
def find_latest_available_month(reporter_code: int, partner_code: int, api_key: str) -> str:
    """
    Finds the latest month with data by trying to download data, starting from the current month and going backwards.
    """
    print(f"Finding latest available month for reporter {reporter_code} by testing recent periods...")
    current_date = datetime.now()
    
    # Check the last 24 months
    for i in range(24):
        test_date = current_date - relativedelta(months=i)
        period_str = test_date.strftime('%Y%m')
        print(f"  ...checking {period_str}")
        try:
            # Try to fetch a tiny amount of data (1 record) to see if it exists
            df = comtradeapicall.getFinalData(
                subscription_key=api_key, typeCode='C', freqCode='M', clCode='HS',
                period=period_str, reporterCode=reporter_code, partnerCode=partner_code,
                cmdCode='TOTAL', flowCode='M', partner2Code=None, customsCode=None, motCode=None,
                maxRecords=1 # We only need to know if it's not empty
            )
            if df is not None and not df.empty:
                print(f"✅ Found latest available month: {period_str[:4]}-{period_str[4:]}")
                return period_str
        except Exception:
            # Ignore errors and try the previous month
            continue
            
    raise RuntimeError(f"Could not find any available monthly data for reporter {reporter_code} in the last two years.")

# --- CORE FUNCTIONS ---
def get_monthly_comtrade_data(reporter: str, partner: str, period_str: str, flow_code: str, api_key: str) -> pd.DataFrame:
    if not api_key:
        raise ValueError("API key not found.")
    
    print(f"Fetching {flow_code} data for periods: {period_str[:50]}...")
    
    df = comtradeapicall.getFinalData(
        subscription_key=api_key, typeCode='C', freqCode='M', clCode='HS',
        period=period_str, reporterCode=COUNTRY_CODES[reporter],
        partnerCode=COUNTRY_CODES[partner], cmdCode='TOTAL', flowCode=flow_code,
        partner2Code=None, customsCode=None, motCode=None
        # <-- FIX: The maxRecords parameter has been removed
    )
    return df

def process_trade_data(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    relevant_cols = {
        'period': 'Period', 'reporterDesc': 'Reporter', 'partnerDesc': 'Partner',
        'flowDesc': 'Trade Flow', 'cmdDesc': 'Commodity',
        'primaryValue': 'Trade Value (US$)', 'netWgt': 'Net Weight (kg)'
    }
    cols_to_keep = {k: v for k, v in relevant_cols.items() if k in df.columns}
    processed_df = df[list(cols_to_keep.keys())].copy()
    processed_df.rename(columns=cols_to_keep, inplace=True)
    return processed_df

# --- SCRIPT EXECUTION ---
def main():
    parser = argparse.ArgumentParser(description="Fetch a range of monthly UN Comtrade data.")
    parser.add_argument("reporter", type=str, choices=COUNTRY_CODES.keys(), help="Reporting country.")
    parser.add_argument("partner", type=str, choices=COUNTRY_CODES.keys(), help="Partner country.")
    parser.add_argument("flow", type=str, choices=['M', 'X'], help="Trade flow ('M' for Imports, 'X' for Exports).")
    parser.add_argument("start_date", type=str, help="Start date in YYYY-MM format (e.g., '2021-01').")
    parser.add_argument("end_date", type=str, help="End date in YYYY-MM format, or the keyword 'latest'.")
    args = parser.parse_args()

    API_KEY = os.getenv("COMTRADE_API_KEY")

    try:
        if args.end_date.lower() == 'latest':
            end_period = find_latest_available_month(COUNTRY_CODES[args.reporter], COUNTRY_CODES[args.partner], API_KEY)
        else:
            end_period = args.end_date.replace('-', '')

        start_period = args.start_date.replace('-', '')
        
        date_range = pd.date_range(start=f"{start_period[:4]}-{start_period[4:]}",
                                   end=f"{end_period[:4]}-{end_period[4:]}", freq='MS')
        
        period_list_str = ",".join(date.strftime('%Y%m') for date in date_range)

        raw_df = get_monthly_comtrade_data(args.reporter, args.partner, period_list_str, args.flow, API_KEY)
        processed_df = process_trade_data(raw_df)

        if not processed_df.empty:
            output_dir = PROJECT_ROOT / "data" / "raw"
            output_dir.mkdir(parents=True, exist_ok=True)
            flow_name = "Imports" if args.flow == "M" else "Exports"
            csv_filename = f"{args.reporter}_{args.partner}_{flow_name}_{start_period}-{end_period}.csv"
            output_path = output_dir / csv_filename
            processed_df.to_csv(output_path, index=False)
            print(f"✅ Data for {len(date_range)} months successfully saved to: {output_path}")
        else:
            print(f"❌ No data was returned for the specified period.")

    except Exception as e:
        print(f"❌ An error occurred: {str(e)}")

if __name__ == "__main__":
    main()