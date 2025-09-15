# utils/check_availability.py
import os
import argparse
from dotenv import load_dotenv
import comtradeapicall

load_dotenv()

def check_data_availability(year: int, api_key: str):
    """Checks which countries have reported annual data for a given year."""
    print(f"Checking data availability for the year {year}...")
    try:
        df = comtradeapicall.getFinalDataAvailability(
            subscription_key=api_key,
            typeCode='C',
            freqCode='A',
            clCode='HS',
            period=str(year),
            reporterCode=None
        )
        if df is not None and not df.empty:
            print("✅ Data is available for the following reporters (and more):")
            print(df[['reporterDesc', 'firstReleased', 'lastReleased']].head(10))
        else:
            print(f"❌ No annual data seems to be available for {year} yet.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check UN Comtrade annual data availability.")
    parser.add_argument("year", type=int, help="The year to check.")
    args = parser.parse_args()
    API_KEY = os.getenv("COMTRADE_API_KEY")
    if not API_KEY:
        print("API key not found. Please set COMTRADE_API_KEY in your .env file.")
    else:
        check_data_availability(args.year, API_KEY)