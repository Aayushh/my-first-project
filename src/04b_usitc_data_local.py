import pandas as pd
import os
import glob
from functools import reduce
from multiprocessing import Pool, cpu_count

# --- 1. CONFIGURATION ---
BASE_PATH = r'C:\Users\aayus\Downloads\Trade data' 
KEY_COLUMNS = ['Country', 'HTS Number']
VARIABLES = [
    'Customs value 24-25',
    'Calculated duties 24-25',
    'quantity 24-25'
]
ANCHOR_COLUMN = 'HTS Number'

# --- 2. CORE PROCESSING FUNCTION (OPTIMIZED) ---
def process_single_file(file_path: str) -> pd.DataFrame:
    """
    Reads metadata from Sheet 1 first to decide which columns to load from Sheet 2,
    making the process more memory-efficient and faster.
    """
    try:
        # --- Step 1: Read Metadata from Sheet 1 ---
        meta_df = pd.read_excel(file_path, sheet_name=0, header=None, index_col=0, engine='calamine')
        metadata = meta_df[1].to_dict()
        
        variable_name = metadata.get('Data To Report', '').strip()
        year = int(metadata.get('Years', 0))
        month_num = int(metadata.get('Start Month', 0))
        month_map = {1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr', 5:'May', 6:'Jun', 7:'Jul', 8:'Aug', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec'}
        month_name = month_map.get(month_num, 'Unk')
        
        # --- Step 2: Selectively Load Data from Sheet 2 based on Metadata ---
        columns_to_skip = None
        clean_variable_name = variable_name.split(' ')[0]

        if 'quantity' in clean_variable_name.lower():
            # If it's a quantity file, skip column E (index 4)
            columns_to_skip = [4] 

        df = pd.read_excel(
            file_path,
            sheet_name=1,
            header=None,
            engine='calamine',
            usecols=lambda x: x not in columns_to_skip if columns_to_skip else True
        )
        
        # --- Step 3: Find Header and Clean Data (same as before) ---
        header_row_index = -1
        for i, row in df.head().iterrows():
            if any(ANCHOR_COLUMN in str(cell).strip() for cell in row.values):
                header_row_index = i
                break
        if header_row_index == -1: return pd.DataFrame()

        df.columns = df.iloc[header_row_index]
        df = df.drop(range(header_row_index + 1)).reset_index(drop=True)
        df.columns = df.columns.str.strip()

        # --- Step 4: Rename Value Column (same as before) ---
        new_col_name = f"{clean_variable_name}_{month_name}{year}"

        if 'quantity' in clean_variable_name.lower():
            suppressed_cols = [col for col in df.columns if str(col).endswith('_Suppressed')]
            df = df.drop(columns=suppressed_cols)
            value_col_name = next((col for col in df.columns if '_to_' in str(col)), None)
            if value_col_name:
                df = df.rename(columns={value_col_name: new_col_name})
        else:
            month_col_name = df.columns[-1]
            df = df.rename(columns={month_col_name: new_col_name})
        
        # --- Step 5: Final Cleanup (same as before) ---
        columns_to_keep = KEY_COLUMNS + [new_col_name]
        df = df[[col for col in columns_to_keep if col in df.columns]]
        
        return df
    except Exception as e:
        print(f"  -> Critical Error processing {os.path.basename(file_path)}: {e}")
        return pd.DataFrame()

# --- 3. MAIN EXECUTION BLOCK (UPDATED) ---
if __name__ == "__main__":
    final_variable_dfs = {}

    for var in VARIABLES:
        print(f"\n--- Processing Variable Folder: {var} ---")
        sort_order_dfs = []
        
        for sort_order in ['ascending', 'descending']:
            folder_path = os.path.join(BASE_PATH, f'{var} {sort_order}')
            if not os.path.isdir(folder_path): continue

            monthly_files = glob.glob(os.path.join(folder_path, '*.xlsx'))
            if not monthly_files: continue
            
            # The function now gets all info from the file, so we use pool.map
            with Pool(processes=max(1, cpu_count() - 1)) as pool:
                print(f"  -> Reading {len(monthly_files)} files from '{sort_order}' folder in parallel...")
                list_of_monthly_dfs = pool.map(process_single_file, monthly_files)

            list_of_monthly_dfs = [df for df in list_of_monthly_dfs if not df.empty]

            if list_of_monthly_dfs:
                merged_months_df = reduce(lambda left, right: pd.merge(left, right, on=KEY_COLUMNS, how='outer'), list_of_monthly_dfs)
                sort_order_dfs.append(merged_months_df)

        if len(sort_order_dfs) > 0:
            print(f"  -> Consolidating data for {var}...")
            stacked_df = pd.concat(sort_order_dfs, ignore_index=True)
            consolidated_df = stacked_df.groupby(KEY_COLUMNS).first().reset_index()
            final_variable_dfs[var] = consolidated_df
            
    print("\n--- Merging all variables into master file ---")
    dataframes_to_merge = list(final_variable_dfs.values())

    if len(dataframes_to_merge) > 1:
        # Note: The merge key here might need to be adjusted if column names differ post-processing
        # We find the common columns between all tables to merge on, which should include KEY_COLUMNS
        common_keys = list(set.intersection(*(set(df.columns) for df in dataframes_to_merge)))
        merge_on_keys = [key for key in KEY_COLUMNS if key in common_keys]
        
        final_df = reduce(lambda left, right: pd.merge(left, right, on=merge_on_keys, how='outer'), dataframes_to_merge)
        final_df = final_df.fillna(0).drop_duplicates()

        print("\n✅ Final merge complete!")
        print("Final DataFrame preview:")
        print(final_df.head())
        
        
        output_path = os.path.join(BASE_PATH, 'master_trade_data.xlsx')
        final_df.to_excel(output_path, index=False)
        print(f"\n✅ Final data saved to: {output_path}")


        output_path = os.path.join(BASE_PATH, 'master_trade_data_final.parquet')
        final_df.to_parquet(output_path, index=False)
        print(f"\n✅ Final data saved to efficient Parquet format: {output_path}")
    else:
        print("Processing complete, but less than two variables had data to merge.")