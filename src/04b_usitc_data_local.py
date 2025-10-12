import pandas as pd
import os
import glob
from functools import reduce
from multiprocessing import Pool, cpu_count
import warnings

# Suppress a common, non-critical warning for a cleaner output
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- 1. CONFIGURATION ---
BASE_PATH = r'C:\Users\aayus\Downloads\Trade data' 
KEY_COLUMNS = ['Country', 'HTS Number']
VARIABLES = [
    'Customs value 24-25',
    'Calculated duties 24-25',
    'quantity 24-25'
]
ANCHOR_COLUMN = 'HTS Number'

# --- NEW: Validation Configuration ---
# Set the expected date range for your files. The script will error if any are missing.
EXPECTED_START_DATE = '2024-01-01'
EXPECTED_END_DATE = '2025-07-01'


# --- 2. HELPER FUNCTIONS ---

def show_df_preview(df, message="DataFrame Preview"):
    """A helper function to print the shape and a sample of a DataFrame."""
    if df is not None and not df.empty:
        print(f"\n--- {message} ---")
        print(f"Shape: {df.shape} (rows, columns)")
        print("First 5 rows:")
        print(df.head())
        print("\nLast 5 rows:")
        print(df.tail())
        print("-" * (len(message) + 8))
    else:
        print(f"\n--- {message}: No data to display ---")

def validate_monthly_files(file_paths: list, folder_name: str):
    """
    Checks for missing or duplicate months in a list of files based on their metadata.
    Raises a ValueError if validation fails.
    """
    print(f"      -> Validating {len(file_paths)} files in '{folder_name}'...")
    found_months = set()
    for file_path in file_paths:
        meta_df = pd.read_excel(file_path, sheet_name=0, header=None, index_col=0, engine='calamine')
        metadata = meta_df[1].to_dict()
        year = int(metadata.get('Years', 0))
        month_num = int(metadata.get('Start Month', 0))
        if year != 0 and month_num != 0:
            found_months.add((year, month_num))

    if len(found_months) != len(file_paths):
        raise ValueError(f"VALIDATION FAILED: Duplicate months found in folder '{folder_name}'. Expected {len(file_paths)} unique months, but found {len(found_months)}.")

    expected_dates = pd.date_range(start=EXPECTED_START_DATE, end=EXPECTED_END_DATE, freq='MS')
    expected_months = set([(date.year, date.month) for date in expected_dates])

    missing_months = expected_months - found_months
    if missing_months:
        raise ValueError(f"VALIDATION FAILED: Missing months in folder '{folder_name}'. Missing: {sorted(list(missing_months))}")
    
    print(f"      -> Validation successful for '{folder_name}'.")

def process_single_file(file_path: str) -> pd.DataFrame:
    """
    Reads metadata from Sheet 1 to decide which columns to load from Sheet 2,
    making the process more memory-efficient and faster.
    """
    try:
        # Step 1: Read Metadata
        meta_df = pd.read_excel(file_path, sheet_name=0, header=None, index_col=0, engine='calamine')
        metadata = meta_df[1].to_dict()
        
        variable_name = metadata.get('Data To Report', 'Unknown Variable').strip()
        year = int(metadata.get('Years', 0))
        month_num = int(metadata.get('Start Month', 0))
        month_map = {1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr', 5:'May', 6:'Jun', 7:'Jul', 8:'Aug', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec'}
        month_name = month_map.get(month_num, 'Unk')
        
        # Step 2: Selectively Load Data
        columns_to_skip = None
        clean_variable_name = variable_name.split(' ')[-1]

        if 'quantity' in clean_variable_name.lower():
            columns_to_skip = [4] # Skip column E

        df = pd.read_excel(
            file_path, sheet_name=1, header=None, engine='calamine',
            usecols=lambda x: x not in columns_to_skip if columns_to_skip else True
        )
        
        # Step 3: Find Header and Clean
        header_row_index = -1
        for i, row in df.head().iterrows():
            if any(ANCHOR_COLUMN in str(cell).strip() for cell in row.values):
                header_row_index = i
                break
        if header_row_index == -1: 
            print(f"      -> WARNING: Header '{ANCHOR_COLUMN}' not found in {os.path.basename(file_path)}. Skipping file.")
            return pd.DataFrame()

        df.columns = df.iloc[header_row_index]
        df = df.drop(range(header_row_index + 1)).reset_index(drop=True)
        df.columns = df.columns.str.strip()

        # Step 4: Rename Value Column
        new_col_name = f"{clean_variable_name}_{month_name}_{year}"

        if 'quantity' in clean_variable_name.lower():
            suppressed_cols = [col for col in df.columns if str(col).endswith('_Suppressed')]
            df = df.drop(columns=suppressed_cols, errors='ignore')
            value_col_name = next((col for col in df.columns if '_to_' in str(col)), None)
            if value_col_name:
                df = df.rename(columns={value_col_name: new_col_name})
        else:
            month_col_name = df.columns[-1]
            df = df.rename(columns={month_col_name: new_col_name})
        
        # Step 5: Final Cleanup
        columns_to_keep = KEY_COLUMNS + [new_col_name]
        df = df[[col for col in columns_to_keep if col in df.columns]]
        
        return df
    except Exception as e:
        print(f"  -> Critical Error processing {os.path.basename(file_path)}: {e}")
        return pd.DataFrame()

def generate_summary_file(df: pd.DataFrame, output_path: str):
    """
    Creates a summary report with country totals for each variable and month.
    """
    print("\n--- Generating Country Summary Report ---")
    # Melt the wide DataFrame into a long format
    long_df = pd.melt(df, id_vars=KEY_COLUMNS, var_name='Metric', value_name='Value')
    
    # Extract Variable, Month, and Year from the 'Metric' column
    # Example: 'Value_Jan_2024' -> 'Value', 'Jan', '2024'
    long_df[['Variable', 'Month', 'Year']] = long_df['Metric'].str.extract(r'(\w+)_(\w+)_(\d{4})')
    
    # Group by Country, Variable, Year, and Month to get totals
    summary = long_df.groupby(['Country', 'Variable', 'Year', 'Month'])['Value'].sum().reset_index()
    
    # Save to an Excel file with a separate sheet for each variable
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for var_name, data in summary.groupby('Variable'):
            data.pivot_table(index=['Country', 'Year'], columns='Month', values='Value').to_excel(writer, sheet_name=var_name)
    
    print(f"✅ Country summary report saved to: {output_path}")

# --- 3. MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    final_variable_dfs = {}

    for var in VARIABLES:
        print(f"\n\n========================================================")
        print(f"--- Processing Variable Folder: {var} ---")
        print(f"========================================================")
        sort_order_dfs = []
        
        for sort_order in ['ascending', 'descending']:
            folder_path = os.path.join(BASE_PATH, f'{var} {sort_order}')
            if not os.path.isdir(folder_path): 
                print(f"\n  -> INFO: Folder not found, skipping: {folder_path}")
                continue

            monthly_files = glob.glob(os.path.join(folder_path, '*.xlsx'))
            if not monthly_files: 
                print(f"\n  -> INFO: No Excel files found in folder: {folder_path}")
                continue
            
            # --- NEW: Perform validation before processing ---
            try:
                validate_monthly_files(monthly_files, f"{var} {sort_order}")
            except ValueError as e:
                print(f"\n❌ {e}")
                exit() # Stop the script if validation fails

            with Pool(processes=max(1, cpu_count() - 1)) as pool:
                print(f"\n  -> Reading {len(monthly_files)} files from '{sort_order}' folder in parallel...")
                list_of_monthly_dfs = pool.map(process_single_file, monthly_files)

            list_of_monthly_dfs = [df for df in list_of_monthly_dfs if not df.empty]
            
            if not list_of_monthly_dfs:
                print(f"  -> WARNING: No valid data could be extracted from any file in the '{sort_order}' folder.")
                continue

            if list_of_monthly_dfs:
                merged_months_df = reduce(lambda left, right: pd.merge(left, right, on=KEY_COLUMNS, how='outer'), list_of_monthly_dfs)
                show_df_preview(merged_months_df, f"1. Merged Months for '{var} {sort_order}'")
                sort_order_dfs.append(merged_months_df)
        
        print(f"\n  -> Found {len(sort_order_dfs)} DataFrame(s) to consolidate for '{var}'.")
        if len(sort_order_dfs) > 0:
            print(f"  -> Consolidating ascending/descending data for {var}...")
            stacked_df = pd.concat(sort_order_dfs, ignore_index=True)
            consolidated_df = stacked_df.groupby(KEY_COLUMNS, as_index=False).first()
            show_df_preview(consolidated_df, f"2. Consolidated Data for '{var}'")
            final_variable_dfs[var] = consolidated_df
            
    print("\n\n========================================================")
    print("--- Final Step: Merging all variables into master file ---")
    print("========================================================")
    dataframes_to_merge = list(final_variable_dfs.values())

    if len(dataframes_to_merge) > 1:
        final_df = dataframes_to_merge[0]
        for i in range(1, len(dataframes_to_merge)):
            right_df = dataframes_to_merge[i]
            final_df = pd.merge(final_df, right_df, on=KEY_COLUMNS, how='outer')

        final_df = final_df.fillna(0).drop_duplicates()

        show_df_preview(final_df, "3. Final Merged DataFrame")
        print("\nFinal Column List (Sorted):")
        print(sorted(list(final_df.columns)))

        print("\n\n--- Saving final files ---")
        print("This may take a few moments for large files...")
        try:
            # --- Generate the summary file ---
            summary_output_path = os.path.join(BASE_PATH, 'country_summary_report.xlsx')
            generate_summary_file(final_df, summary_output_path)
            
            # --- Saving to Parquet (fastest, for future analysis) ---
            output_path_parquet = os.path.join(BASE_PATH, 'master_trade_data_final.parquet')
            print(f"\n -> Writing to Parquet file: {output_path_parquet} (fastest)...")
            final_df.to_parquet(output_path_parquet, index=False)
            print(f"✅ Master data file saved to efficient Parquet format.")
            print("\nRECOMMENDATION: For your next analysis, load the '.parquet' file. It will be much faster.")

            # --- Saving to Excel (slower, for human readability) ---
            # Using the 'xlsxwriter' engine can be faster than the default.
            # You may need to install it first: pip install xlsxwriter
            output_path_xlsx = os.path.join(BASE_PATH, 'master_trade_data.xlsx')
            print(f"\n -> Writing to Excel file: {output_path_xlsx} (this is often the slowest step)...")
            final_df.to_excel(output_path_xlsx, index=False, engine='xlsxwriter')
            print(f"✅ Master data file saved to Excel.")

            # --- Saving to CSV (faster alternative to Excel) ---
            output_path_csv = os.path.join(BASE_PATH, 'master_trade_data.csv')
            print(f"\n -> Writing to CSV file: {output_path_csv} (faster)...")
            final_df.to_csv(output_path_csv, index=False)
            print(f"✅ Master data file saved to CSV.")

            # --- Saving to Parquet (fastest, for future analysis) ---
            output_path_parquet = os.path.join(BASE_PATH, 'master_trade_data_final.parquet')
            print(f"\n -> Writing to Parquet file: {output_path_parquet} (fastest)...")
            final_df.to_parquet(output_path_parquet, index=False)
            print(f"✅ Master data file saved to efficient Parquet format.")
            print("\nRECOMMENDATION: For your next analysis, load the '.parquet' file. It will be much faster.")

        except PermissionError:
            print("\n❌ ERROR: Could not save a file. Please ensure it is not open elsewhere and try again.")
        except ImportError:
            print("\n❌ ERROR: The 'xlsxwriter' engine is not installed. Please run: pip install xlsxwriter")
    else:
        print("Processing complete, but less than two variables had data to merge.")

