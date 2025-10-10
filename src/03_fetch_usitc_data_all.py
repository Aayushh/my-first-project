import pandas as pd
import requests
import json
import time
import copy
import os
from dotenv import load_dotenv

# --- 1. Initial Setup ---
# Load environment variables from a .env file (for your API key)
load_dotenv()

# --- Configuration ---
token = os.getenv("USITC_API_KEY")
if not token:
    raise ValueError("API Key not found. Please create a .env file with USITC_API_KEY='your_key'")

baseUrl = 'https://datawebws.usitc.gov/dataweb'
headers = {
    "Content-Type": "application/json; charset=utf-8",
    "Authorization": "Bearer " + token
}
requests.packages.urllib3.disable_warnings()

output_filename = "usitc_imports_final.csv"
API_RECORD_LIMIT = 20000

# --- Base Query Template ---
base_query = {
    "savedQueryType": "", "isOwner": True, "unitConversion": "0", "manualConversions": [],
    "reportOptions": {"tradeType": "Import", "classificationSystem": "HTS"},
    "searchOptions": {
        "MiscGroup": {
            "districts": {"aggregation": "Aggregate District", "districtsExpanded": [{"name": "All Districts", "value": "all"}], "districtsSelectType": "all"},
            "importPrograms": {"programsSelectType": "all"},
            "extImportPrograms": {"aggregation": "Aggregate CSC", "programsSelectType": "all"},
            "provisionCodes": {"aggregation": "Aggregate RPCODE", "provisionCodesSelectType": "all"},
        },
        "commodities": {
            "aggregation": "Break Out Commodities", "codeDisplayFormat": "YES", "commodities": [],
            "commoditiesManual": "", "commoditySelectType": "all", "granularity": "10",
        },
        "componentSettings": {
            "dataToReport": ["CONS_FIR_UNIT_QUANT"], "scale": "1", "timeframeSelectType": "specificDateRange",
            "startDate": "01/2024", "endDate": "07/2025", "yearsTimeline": "Monthly",
        },
        "countries": { "aggregation": "Aggregate Countries", "countries": [], "countriesExpanded": [], "countriesSelectType": "list" },
    },
    "sortingAndDataFormat": {
        "DataSort": {"columnOrder": ["COUNTRY", "HTS10 & DESCRIPTION"]},
        "reportCustomizations": {"totalRecords": str(API_RECORD_LIMIT), "exportRawData": False},
    },
}

# --- 2. Helper Functions ---

def get_columns(column_groups, prev_cols=None):
    """Recursively extracts column headers from the API response."""
    columns = prev_cols if prev_cols is not None else []
    for group in column_groups:
        if isinstance(group, dict) and 'columns' in group.keys():
            get_columns(group['columns'], columns)
        elif isinstance(group, dict) and 'label' in group.keys():
            columns.append(group['label'])
        elif isinstance(group, list):
            get_columns(group, columns)
    return columns

def make_api_request(query, timeout, attempt_description):
    """
    Makes an API request with a built-in exponential backoff retry mechanism.
    Returns the response object on success, or None on failure.
    """
    max_retries = 4
    base_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            response = requests.post(
                f"{baseUrl}/api/v2/report2/runReport",
                headers=headers, json=query, verify=False, timeout=timeout
            )

            # If we get a 429, raise an exception to trigger the retry logic
            if response.status_code == 429:
                raise requests.exceptions.RequestException(f"Rate limit hit (429)")
            
            # For any other non-200 code, it's an unrecoverable error
            if response.status_code != 200:
                print(f"    🚨 Unrecoverable error for {attempt_description}: Status {response.status_code}")
                return None # Give up on this request

            return response # ✅ Success!

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = base_delay * (2 ** attempt) # Exponential backoff
                print(f"    🚦 {e}. Retrying {attempt_description} in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"    ❌ Max retries exceeded for {attempt_description}. Giving up. Error: {e}")
                return None # All retries failed
    return None

# --- 3. Main Script Logic ---

print("🚀 Starting data collection script...")

# Get list of all countries to process
try:
    response = requests.get(f"{baseUrl}/api/v2/country/getAllCountries", headers=headers, verify=False)
    response.raise_for_status()
    country_list = response.json()['options']
    print(f"🌍 Found {len(country_list)} countries.")
except Exception as e:
    print(f"❌ Critical error: Failed to get country list. {e}")
    country_list = []

# Checkpoint/Resume: Find countries we've already processed
countries_already_processed = set()
if os.path.exists(output_filename):
    print(f"📄 Found existing file: '{output_filename}'. Resuming download.")
    try:
        df_existing = pd.read_csv(output_filename)
        if 'Country' in df_existing.columns:
            countries_already_processed = set(df_existing['Country'].unique())
            print(f"👍 Already processed {len(countries_already_processed)} countries.")
    except Exception as e:
        print(f"🚨 Could not read existing file: {e}. Starting fresh.")

column_headers = None

# Main loop through each country
for i, country in enumerate(country_list):
    country_name = country['name']
    country_code = country['value']

    if country_name in countries_already_processed:
        continue # Skip this country entirely

    print(f"\n--- ({i+1}/{len(country_list)}) Processing: {country_name} ({country_code}) ---")
    
    country_specific_data = []
    fast_query_succeeded = False

    # Attempt 1: The Fast, "All Commodities" Query
    print("  -> Attempting fast query...")
    fast_query = copy.deepcopy(base_query)
    fast_query['searchOptions']['countries']['countries'] = [country_code]
    fast_query['searchOptions']['countries']['countriesExpanded'] = [country]
    fast_query['searchOptions']['commodities']['commoditySelectType'] = 'all'

    response = make_api_request(fast_query, timeout=60, attempt_description=f"fast query for {country_name}")

    if response:
        response_json = response.json()
        if response_json and response_json.get('dto') and response_json['dto'].get('tables'):
            table = response_json['dto']['tables'][0]
            rows = table.get('row_groups', [{}])[0].get('rowsNew', [])
            
            if len(rows) < API_RECORD_LIMIT:
                print(f"  ✅ Fast query successful with {len(rows)} records.")
                if column_headers is None:
                    column_headers = ['Country'] + get_columns(table['column_groups'])
                for row in rows:
                    row_values = [entry['value'] for entry in row['rowEntries']]
                    country_specific_data.append([country_name] + row_values)
                fast_query_succeeded = True
            else:
                print("  ⚠️ Fast query hit record limit. Switching to robust method.")
        else:
            print(f"  ✅ Fast query successful but returned no data.")
            fast_query_succeeded = True

    # Attempt 2: The Robust, Chapter-by-Chapter Query (Fallback)
    if not fast_query_succeeded:
        print("  -> Switching to robust, chapter-by-chapter query...")
        hts_chapters = [str(n).zfill(2) for n in range(1, 100)]
        
        for chapter in hts_chapters:
            robust_query = copy.deepcopy(base_query)
            robust_query['searchOptions']['countries']['countries'] = [country_code]
            robust_query['searchOptions']['countries']['countriesExpanded'] = [country]
            robust_query['searchOptions']['commodities']['commoditySelectType'] = 'manual'
            robust_query['searchOptions']['commodities']['commoditiesManual'] = chapter
            
            response = make_api_request(robust_query, timeout=90, attempt_description=f"Chapter {chapter} for {country_name}")
            
            if response:
                response_json = response.json()
                if response_json and response_json.get('dto') and response_json['dto'].get('tables'):
                    table = response_json['dto']['tables'][0]
                    if column_headers is None:
                        column_headers = ['Country'] + get_columns(table['column_groups'])
                    rows = table.get('row_groups', [{}])[0].get('rowsNew', [])
                    for row in rows:
                        row_values = [entry['value'] for entry in row['rowEntries']]
                        country_specific_data.append([country_name] + row_values)
            
            time.sleep(1)

    # Save data for the completed country
    if country_specific_data and column_headers:
        df_country = pd.DataFrame(country_specific_data, columns=column_headers)
        df_country.to_csv(
            output_filename, mode='a', index=False, 
            header=not os.path.exists(output_filename) or os.path.getsize(output_filename) == 0
        )
        print(f"💾 Saved {len(df_country)} rows for {country_name}.")
    else:
        print(f"ℹ️ No data collected for {country_name}. Marked as complete.")
        # We create an empty placeholder to mark it as processed and prevent rerunning
        if not os.path.exists(output_filename):
            with open(output_filename, 'w') as f:
                if column_headers:
                    f.write(','.join(column_headers) + '\n')
                f.write(f'"{country_name}"' + ',' * (len(column_headers or []) - 1) + '\n')


print("\n🎉 All countries processed. Script finished.")