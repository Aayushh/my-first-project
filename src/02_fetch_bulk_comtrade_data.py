# made to bulk download data for multiple months at once
# src/02_fetch_bulk_comtrade_data.py

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
comtradeapicall.bulkDownloadTarifflineFile(subscription_key, directory, typeCode='C', freqCode='M', clCode='HS',
                                           period='200001,200002,200003', reporterCode=504, decompress=True)