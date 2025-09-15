#
# utils/test_bulk_downloads.py
# A simple tool to download and compare Final vs. Tariffline bulk files.
#
import os
import argparse
from dotenv import load_dotenv
from pathlib import Path
import comtradeapicall

# --- Standard Project Configuration ---
load_dotenv()
PROJECT_ROOT = Path(__file__).resolve().parents[1]
COUNTRY_CODES = {
    "India": 699, "Maldives": 462, "USA": 842, "China": 156, "Japan": 392,
}

# --- Main Script Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test Comtrade bulk downloads.")
    parser.add_argument("file_type", type=str, choices=['final', 'tariffline'], help="The type of bulk file to download ('final' or 'tariffline').")
    parser.add_argument("reporter", type=str, choices=COUNTRY_CODES.keys(), help="The reporting country.")
    parser.add_argument("period", type=str, help="The month to fetch, in YYYY-MM format (e.g., '2023-01').")
    args = parser.parse_args()

    API_KEY = os.getenv("COMTRADE_API_KEY")
    if not API_KEY:
        raise ValueError("API key not found in .env file.")

    # We will save these large files to a new directory to keep things organized
    output_dir = PROJECT_ROOT / "outputs" / "bulk_downloads"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get parameters from command line
    reporter_code = COUNTRY_CODES[args.reporter]
    period_str = args.period.replace('-', '')

    print(f"\n--- Attempting to download '{args.file_type.title()}' file ---")
    print(f"Reporter: {args.reporter} ({reporter_code})")
    print(f"Period:   {period_str}")
    print(f"Saving to: {output_dir}")
    print("--------------------------------------------------")

    try:
        if args.file_type == 'final':
            # Download the standard, HS 6-digit data
            comtradeapicall.bulkDownloadFinalFile(
                subscription_key=API_KEY,
                directory=str(output_dir),
                typeCode='C', freqCode='M', clCode='HS',
                period=period_str,
                reporterCode=reporter_code,
                decompress=True
            )
        elif args.file_type == 'tariffline':
            # Download the hyper-detailed, >6-digit data
            comtradeapicall.bulkDownloadTarifflineFile(
                subscription_key=API_KEY,
                directory=str(output_dir),
                typeCode='C', freqCode='M', clCode='HS',
                period=period_str,
                reporterCode=reporter_code,
                decompress=True
            )
        
        print(f"\n✅ Download complete! Check the files in the '{output_dir}' directory.")

    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
        print("NOTE: Bulk downloads often require a premium subscription key.")