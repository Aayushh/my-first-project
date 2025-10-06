#
# src/01d_fetch_usitc_data.py
# FINAL version with corrected argparse and a clean JSON payload.
#
import os
import pandas as pd
import argparse
import requests
import json
from dotenv import load_dotenv
from pathlib import Path

# --- CONFIGURATION ---
load_dotenv()
PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_URL = 'https://datawebws.usitc.gov/dataweb'
requests.packages.urllib3.disable_warnings() 

# Add a dictionary to map partner names to their official Dataweb codes
PARTNER_CODES = {
    "World": "0",
    "India": "357"
    # You can find more codes by inspecting the 'value' in the website's HTML
}

# --- HELPER FUNCTIONS ---
def get_columns(column_groups, prev_cols=None):
    columns = prev_cols if prev_cols is not None else []
    for group in column_groups:
        if isinstance(group, dict) and 'columns' in group.keys(): get_columns(group['columns'], columns)
        elif isinstance(group, dict) and 'label' in group.keys(): columns.append(group['label'])
        elif isinstance(group, list): get_columns(group, columns)
    return columns

def get_data(data_groups):
    data = []
    for row in data_groups:
        rowData = [field['value'] for field in row['rowEntries']]
        data.append(rowData)
    return data

# --- MAIN SCRIPT EXECUTION ---
def main():
    # FIX #1: Re-add the 'partner' argument
    parser = argparse.ArgumentParser(description="Fetch monthly USITC Dataweb trade and tariff data.")
    parser.add_argument("trade_flow", type=str, choices=['Import', 'Export'], help="Trade flow from the US perspective.")
    parser.add_argument("partner", type=str, choices=PARTNER_CODES.keys(), help="Partner country name.")
    parser.add_argument("start_date", type=str, help="Start date in YYYY-MM format (e.g., '2023-01').")
    parser.add_argument("end_date", type=str, help="End date in YYYY-MM format (e.g., '2023-12').")
    args = parser.parse_args()

    API_KEY = os.getenv("USITC_API_KEY")
    if not API_KEY:
        raise ValueError("API key not found. Please set USITC_API_KEY in your .env file.")

    headers = {"Authorization": "Bearer " + API_KEY}
    
    # FIX #2: Use a clean, minimal, multi-line JSON template
    request_payload_template_str = """
    {
        "reportOptions": {"tradeType": "Import", "classificationSystem": "HTS"},
        "searchOptions": {
            "componentSettings": {
                "dataToReport": ["CONS_CUSTOMS_VALUE", "CALC_DUTY", "GEN_DUTY_RATE"],
                "timeframeSelectType": "specificDateRange",
                "startDate": "01/2023",
                "endDate": "02/2023",
                "yearsTimeline": "Monthly"
            },
            "countries": {
                "countries": ["0"], 
                "countriesExpanded": [{"name": "World Total", "value": "0"}], 
                "countriesSelectType": "list"
            },
            "commodities": {"commoditySelectType": "all", "aggregation": "Aggregate Commodities"},
            "districts": {"districtsSelectType": "all"},
            "provisionCodes": {"provisionCodesSelectType": "all"}
        }
    }
    """
    payload = json.loads(request_payload_template_str)

    # Dynamically modify the payload with our arguments
    payload['reportOptions']['tradeType'] = args.trade_flow
    
    start_dt = pd.to_datetime(args.start_date)
    end_dt = pd.to_datetime(args.end_date)
    payload['searchOptions']['componentSettings']['startDate'] = start_dt.strftime('%m/%Y')
    payload['searchOptions']['componentSettings']['endDate'] = end_dt.strftime('%m/%Y')
    
    # Dynamically set the partner country
    partner_code = PARTNER_CODES[args.partner]
    payload['searchOptions']['countries']['countries'] = [partner_code]
    payload['searchOptions']['countries']['countriesExpanded'] = [{"name": args.partner, "value": partner_code}]
    
    print(f"Submitting query for {args.trade_flow}s with '{args.partner}' for {args.start_date} to {args.end_date}...")
    
    try:
        response = requests.post(f"{BASE_URL}/api/v2/report2/runReport", headers=headers, json=payload, verify=False)
        response.raise_for_status()
        json_response = response.json()
        
        if json_response.get('dto') is None:
            print("❌ Query successful, but the server returned an empty data object (dto is null).")
            return

        if not json_response['dto']['tables'][0]['row_groups']:
            print("❌ Query successful, but no data rows were returned.")
            return

        columns = get_columns(json_response['dto']['tables'][0]['column_groups'])
        data = get_data(json_response['dto']['tables'][0]['row_groups'][0]['rowsNew'])
        df = pd.DataFrame(data, columns=columns)

        output_dir = PROJECT_ROOT / "data" / "raw"
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = f"USITC_data_{args.trade_flow}_{args.partner}_{args.start_date}_to_{args.end_date}.csv"
        output_path = output_dir / filename
        
        df.to_csv(output_path, index=False)
        print(f"✅ Data successfully saved to: {output_path}")
        print(df.head())

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        if 'response' in locals():
            print(f"Server response: {response.text}")

if __name__ == "__main__":
    main()