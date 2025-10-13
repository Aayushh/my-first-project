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

# --- Validation Configuration ---
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

# --- NEW: Helper function for the "Non-Zero First" logic ---
def resolve_quantity_conflicts(group, value_col_name):
    """
    Applies the "Non-Zero First, Unit Priority Second" logic to a group of conflicting rows.
    """
    # If there's only one row, no conflict to resolve.
    if len(group) == 1:
        return group

    # Step 1: Separate non-zero rows from zero rows
    non_zero_rows = group[group[value_col_name] > 0]
    
    pool_to_search = non_zero_rows
    if non_zero_rows.empty:
        # Fallback: if all values are zero, use the original group
        pool_to_search = group

    # Step 2: Apply unit priority to the selected pool of rows
    chosen_row = None
    
    # Priority 1: 'number'
    number_row = pool_to_search[pool_to_search['Quantity Description'] == 'number']
    if not number_row.empty:
        chosen_row = number_row.iloc[0:1]
    
    # Priority 2: 'kilograms'
    if chosen_row is None:
        kg_row = pool_to_search[pool_to_search['Quantity Description'] == 'kilograms']
        if not kg_row.empty:
            chosen_row = kg_row.iloc[0:1]
    
    # Priority 3 (Fallback): First available row in the pool
    if chosen_row is None:
        chosen_row = pool_to_search.iloc[0:1]

    # --- Logging the decision ---
    discarded_rows = group.drop(chosen_row.index)
    
    # Safely get values for logging
    country = chosen_row['Country'].iloc[0]
    hts_number = chosen_row['HTS Number'].iloc[0]
    chosen_unit = chosen_row['Quantity Description'].iloc[0]
    chosen_value = chosen_row[value_col_name].iloc[0]
    
    discarded_info = [f"'{row['Quantity Description']}' ({row[value_col_name]})" for _, row in discarded_rows.iterrows()]
    
    print(
        f"      [Conflict Resolved] HTS: {hts_number} | Country: {country} -> "
        f"Chose '{chosen_unit}' ({chosen_value}) over [{', '.join(discarded_info)}]"
    )
    
    return chosen_row

def process_single_file(file_path: str) -> pd.DataFrame:
    """
    Reads a single Excel file. For 'quantity' files, it applies the new conflict
    resolution logic. For all other files, it uses the original logic.
    """
    try:
        # --- Step 1: Read Metadata (Identical to original) ---
        meta_df = pd.read_excel(file_path, sheet_name=0, header=None, index_col=0, engine='calamine')
        metadata = meta_df[1].to_dict()
        
        variable_name = metadata.get('Data To Report', 'Unknown Variable').strip()
        year = int(metadata.get('Years', 0))
        month_num = int(metadata.get('Start Month', 0))
        month_map = {1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr', 5:'May', 6:'Jun', 7:'Jul', 8:'Aug', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec'}
        month_name = month_map.get(month_num, 'Unk')
        
        # --- MODIFIED: More robust variable name cleaning ---
        # This now correctly handles all variables
        is_quantity_file = 'quantity' in variable_name.lower()
        if is_quantity_file:
            clean_variable_name = 'quantity'
        else:
            # Use the first word for other variables (e.g., 'Customs' or 'Calculated')
            clean_variable_name = variable_name.split(' ')[0]

        # --- Step 2: Load Data and Find Header (Identical to original) ---
        df = pd.read_excel(file_path, sheet_name=1, header=None, engine='calamine')
        
        header_row_index = -1
        for i, row in df.head(10).iterrows(): # Scan more rows for safety
            if any(ANCHOR_COLUMN in str(cell).strip() for cell in row.values):
                header_row_index = i
                break
        if header_row_index == -1: 
            print(f"      -> WARNING: Header '{ANCHOR_COLUMN}' not found in {os.path.basename(file_path)}. Skipping file.")
            return pd.DataFrame()

        df.columns = df.iloc[header_row_index]
        df = df.drop(range(header_row_index + 1)).reset_index(drop=True)
        df.columns = [str(col).strip() for col in df.columns]

        # --- REFACTORED LOGIC ---
        if is_quantity_file:
            # --- Step 3a: Process QUANTITY files with new logic ---
            value_col_name = next((col for col in df.columns if '_to_' in str(col)), df.columns[-1])
            df.rename(columns={'Quantity Description': 'Quantity Description'}, inplace=True)
            
            # Ensure value column is numeric for comparison
            df[value_col_name] = pd.to_numeric(df[value_col_name], errors='coerce').fillna(0)

            # Identify groups with multiple different units
            unit_counts = df.groupby(KEY_COLUMNS)['Quantity Description'].nunique()
            conflict_groups_index = unit_counts[unit_counts > 1].index
            
            is_conflict = df.set_index(KEY_COLUMNS).index.isin(conflict_groups_index)
            df_conflicts = df[is_conflict].copy()
            df_no_conflicts = df[~is_conflict].copy()

            df_resolved = pd.DataFrame()
            if not df_conflicts.empty:
                df_resolved = df_conflicts.groupby(KEY_COLUMNS, as_index=False).apply(
                    resolve_quantity_conflicts, value_col_name=value_col_name
                ).reset_index(drop=True)
            
            df_final_monthly = pd.concat([df_no_conflicts, df_resolved], ignore_index=True)

            # Create new _Value and _Unit columns
            value_col_final_name = f"{clean_variable_name}_Value_{month_name}_{year}"
            unit_col_final_name = f"{clean_variable_name}_Unit_{month_name}_{year}"
            
            df_final_monthly = df_final_monthly.rename(columns={value_col_name: value_col_final_name})
            df_final_monthly[unit_col_final_name] = df_final_monthly['Quantity Description']
            
            columns_to_keep = KEY_COLUMNS + [value_col_final_name, unit_col_final_name]
            return df_final_monthly[[col for col in columns_to_keep if col in df_final_monthly.columns]]
        
        else:
            # --- Step 3b: Process ALL OTHER files using original logic ---
            # This block is now identical in function to your original script
            new_col_name = f"{clean_variable_name}_{month_name}_{year}"
            month_col_name = df.columns[-1]
            df = df.rename(columns={month_col_name: new_col_name})
            
            columns_to_keep = KEY_COLUMNS + [new_col_name]
            return df[[col for col in columns_to_keep if col in df.columns]]

    except Exception as e:
        print(f"      -> Critical Error processing {os.path.basename(file_path)}: {e}")
        return pd.DataFrame()

def generate_summary_file(df: pd.DataFrame, output_path: str):
    """
    Creates a summary report. MODIFIED to be compatible with new quantity columns.
    """
    print("\n--- Generating Country Summary Report ---")
    
    # --- MODIFIED: Select only numeric value columns for melting ---
    value_cols = [col for col in df.columns if any(v in col for v in ['Customs_', 'Calculated_', '_Value_'])]
    if not value_cols:
        print("WARNING: No numeric value columns found to generate a summary. Skipping report.")
        return
        
    long_df = pd.melt(df, id_vars=KEY_COLUMNS, value_vars=value_cols, var_name='Metric', value_name='Value')
    
    long_df['Value'] = pd.to_numeric(long_df['Value'], errors='coerce').fillna(0)
    
    # --- MODIFIED: Regex now handles both 'Customs_Jan_2024' and 'quantity_Value_Jan_2024' ---
    long_df[['Variable', 'Month', 'Year']] = long_df['Metric'].str.extract(r'(\w+?)(?:_Value)?_(\w+)_(\d{4})')
    
    summary = long_df.groupby(['Country', 'Variable', 'Year', 'Month'])['Value'].sum().reset_index()
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for var_name, data in summary.groupby('Variable'):
            month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            pivot_data = data.pivot_table(index=['Country', 'Year'], columns='Month', values='Value')
            pivot_data = pivot_data.reindex(columns=[m for m in month_order if m in pivot_data.columns])
            pivot_data.to_excel(writer, sheet_name=var_name)
    
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
                print(f"\n   -> INFO: Folder not found, skipping: {folder_path}")
                continue

            monthly_files = glob.glob(os.path.join(folder_path, '*.xlsx'))
            if not monthly_files: 
                print(f"\n   -> INFO: No Excel files found in folder: {folder_path}")
                continue
            
            try:
                validate_monthly_files(monthly_files, f"{var} {sort_order}")
            except ValueError as e:
                print(f"\n❌ {e}")
                exit()

            with Pool(processes=max(1, cpu_count() - 1)) as pool:
                print(f"\n   -> Reading {len(monthly_files)} files from '{sort_order}' folder in parallel...")
                list_of_monthly_dfs = pool.map(process_single_file, monthly_files)

            list_of_monthly_dfs = [df for df in list_of_monthly_dfs if not df.empty]
            
            if not list_of_monthly_dfs:
                print(f"   -> WARNING: No valid data could be extracted from any file in the '{sort_order}' folder.")
                continue

            if list_of_monthly_dfs:
                merged_months_df = reduce(lambda left, right: pd.merge(left, right, on=KEY_COLUMNS, how='outer'), list_of_monthly_dfs)
                show_df_preview(merged_months_df, f"1. Merged Months for '{var} {sort_order}'")
                sort_order_dfs.append(merged_months_df)
        
    
        if len(sort_order_dfs) > 0:
            print(f"\n   -> Consolidating ascending/descending data for {var}...")
            stacked_df = pd.concat(sort_order_dfs, ignore_index=True)
            consolidated_df = stacked_df.groupby(KEY_COLUMNS, as_index=False).first()
            show_df_preview(consolidated_df, f"2. Consolidated Data for '{var}'")
            final_variable_dfs[var] = consolidated_df
            
    print("\n\n========================================================")
    print("--- Final Step: Merging all variables into master file ---")
    print("========================================================")
    dataframes_to_merge = list(final_variable_dfs.values())

    if len(dataframes_to_merge) > 1:
        final_df = reduce(lambda left, right: pd.merge(left, right, on=KEY_COLUMNS, how='outer'), dataframes_to_merge)
        
        # --- MODIFIED: Safely fill NaNs only in numeric columns ---
        numeric_cols = final_df.select_dtypes(include='number').columns
        final_df[numeric_cols] = final_df[numeric_cols].fillna(0)
        
        final_df = final_df.drop_duplicates()

        show_df_preview(final_df, "3. Final Merged DataFrame")
        print("\nFinal Column List (Sorted):")
        print(sorted(list(final_df.columns)))

        print("\n\n--- Saving final files ---")
        print("This may take a few moments for large files...")
        try:
            summary_output_path = os.path.join(BASE_PATH, 'country_summary_report.xlsx')
            generate_summary_file(final_df, summary_output_path)
            
            output_path_parquet = os.path.join(BASE_PATH, 'master_trade_data_final.parquet')
            print(f"\n -> Writing to Parquet file: {output_path_parquet} (fastest)...")
            final_df.to_parquet(output_path_parquet, index=False)
            print(f"✅ Master data file saved to efficient Parquet format.")
            print("\nRECOMMENDATION: For your next analysis, load the '.parquet' file. It will be much faster.")
            
            output_path_xlsx = os.path.join(BASE_PATH, 'master_trade_data.xlsx')
            print(f"\n -> Writing to Excel file: {output_path_xlsx} (this is often the slowest step)...")
            final_df.to_excel(output_path_xlsx, index=False, engine='xlsxwriter')
            print(f"✅ Master data file saved to Excel.")
            

        except PermissionError:
            print("\n❌ ERROR: Could not save a file. Please ensure it is not open elsewhere and try again.")
        except ImportError:
            print("\n❌ ERROR: The 'xlsxwriter' or 'openpyxl' engine is not installed. Please install it.")
    else:
        print("Processing complete, but less than two variables had data to merge.")