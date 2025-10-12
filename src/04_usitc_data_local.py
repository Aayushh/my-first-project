import pandas as pd
import os
import glob
from functools import reduce

# --- 1. CONFIGURATION ---
BASE_PATH = r'C:\Users\aayus\Downloads\Trade data'
KEY_COLUMNS = ['HTS Number']
VARIABLES = [
    'Customs value 24-25',
    'Calculated duties 24-25',
    'quantity 24-25'
]
# A known column name to help find the real header row
ANCHOR_COLUMN = 'HTS Number' 

# --- 2. HELPER FUNCTION (NEW AND IMPROVED) ---
def read_and_clean_excel(file_path: str) -> pd.DataFrame:
    """
    Reads the second sheet of an Excel file, automatically finds the header row,
    and cleans the column names.
    """
    try:
        # Step 1: Read the SECOND sheet (index 1) without assuming a header.
        df = pd.read_excel(file_path, sheet_name=1, header=None, engine='openpyxl')

        # Step 2: Find the actual header row by looking for our anchor column.
        header_row_index = -1
        for i, row in df.head().iterrows(): # Check first 5 rows
            # See if the anchor column name (with or without spaces) is in this row's values
            if any(ANCHOR_COLUMN in str(cell).strip() for cell in row.values):
                header_row_index = i
                break
        
        if header_row_index == -1:
            print(f"  -> Warning: Header '{ANCHOR_COLUMN}' not found in {os.path.basename(file_path)}. Skipping file.")
            return pd.DataFrame() # Return an empty DataFrame

        # Step 3: Promote the found header row to be the actual column headers.
        df.columns = df.iloc[header_row_index]
        
        # Step 4: Drop all junk rows above and including the header row.
        df = df.drop(range(header_row_index + 1)).reset_index(drop=True)
        
        # Step 5: Clean the column names by stripping whitespace.
        df.columns = df.columns.str.strip()
        
        return df

    except Exception as e:
        print(f"  -> Error reading {os.path.basename(file_path)}: {e}. Skipping file.")
        return pd.DataFrame() # Return empty DataFrame on error


# --- 3. MAIN LOGIC ---
if __name__ == "__main__":
    
    # === PART 1: CONSOLIDATE EACH VARIABLE ===
    print("--- Starting Part 1: Consolidating files for each variable ---")
    consolidated_dfs = {}

    for var in VARIABLES:
        print(f"\nProcessing variable: '{var}'...")
        file_pattern = os.path.join(BASE_PATH, f'{var} *', '*.xlsx')
        files_to_process = glob.glob(file_pattern)

        if not files_to_process:
            print(f"No Excel files found for '{var}'. Skipping.")
            continue

        list_of_dfs = [read_and_clean_excel(f) for f in files_to_process]
        
        # Filter out any empty DataFrames that may have been returned due to errors
        list_of_dfs = [df for df in list_of_dfs if not df.empty]

        if not list_of_dfs:
            print(f"No valid data found for '{var}' after processing files. Skipping.")
            continue

        combined_df = pd.concat(list_of_dfs, ignore_index=True).drop_duplicates()
        consolidated_dfs[var] = combined_df
        
        print(f"✅ Success! Combined {len(files_to_process)} files for '{var}' into {len(combined_df)} unique rows.")

    # === PART 2 & 3 (No changes needed here) ===
    print("\n--- Starting Part 2: Merging all variables together ---")
    dataframes_to_merge = [df for var, df in consolidated_dfs.items() if var in VARIABLES]

    if len(dataframes_to_merge) < 2:
        print("Not enough data to merge. Exiting.")
        final_df = dataframes_to_merge[0] if dataframes_to_merge else pd.DataFrame()
    else:
        final_df = reduce(lambda left, right: pd.merge(left, right, on=KEY_COLUMNS, how='outer'), dataframes_to_merge)
        print("\n✅ All variables merged successfully using an outer join!")

    if not final_df.empty:
        print("Final DataFrame preview:")
        print(final_df.head())
        print(f"\nThe final master DataFrame has {final_df.shape[0]} rows and {final_df.shape[1]} columns.")
        
        output_path = os.path.join(BASE_PATH, 'master_trade_data.xlsx')
        final_df.to_excel(output_path, index=False)
        print(f"\n✅ Final data saved to: {output_path}")
    else:
        print("\nNo data was processed to create a final file.")