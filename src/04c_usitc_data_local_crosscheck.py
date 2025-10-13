import pandas as pd
import os
from datetime import datetime

# --- 1. CONFIGURATION ---
BASE_PATH = r'C:\Users\aayus\Downloads\Trade data'
YOUR_MASTER_PARQUET_FILE = 'master_trade_data_final.parquet' 
OFFICIAL_DATA_FILE = 'official_summary_data.xlsx'
OUTPUT_FILE = 'validation_report.xlsx'
YOUR_SUMMARY_CACHE_FILE = 'your_summary_cache.parquet'
OFFICIAL_SUMMARY_PROCESSED_FILE = 'official_summary_processed.xlsx'

# Tolerance for comparison (adjust as needed)
TOLERANCE = 1.0  # Allow $1 difference due to rounding

# --- 2. LOAD YOUR MASTER DATA ---

def load_your_data(file_path, cache_path, force_rebuild=False):
    """
    Load and aggregate your HTS-level master data to country-month-variable level.
    """
    print(f"\n{'='*70}")
    print("LOADING YOUR MASTER DATA")
    print(f"{'='*70}")
    
    # Try to load from cache
    if os.path.exists(cache_path) and not force_rebuild:
        print(f"  → Loading from cache: {os.path.basename(cache_path)}")
        try:
            df = pd.read_parquet(cache_path)
            print(f"  ✓ Loaded {len(df):,} aggregated records from cache")
            print(f"  ✓ Date range: {df['Year'].min()}-{df['Year'].max()}")
            print(f"  ✓ Variables: {sorted(df['Variable'].unique())}")
            return df
        except Exception as e:
            print(f"  ⚠ Cache load failed: {e}. Rebuilding...")
    
    # Load raw parquet file
    print(f"  → Loading raw data: {os.path.basename(file_path)}")
    try:
        df_raw = pd.read_parquet(file_path)
        print(f"  ✓ Loaded {len(df_raw):,} HTS codes × {len(df_raw.columns)} columns")
    except FileNotFoundError:
        print(f"  ✗ ERROR: File not found: {file_path}")
        return None
    except Exception as e:
        print(f"  ✗ ERROR: Failed to load file: {e}")
        return None
    
    # Melt to long format
    print(f"  → Reshaping data from wide to long format...")
    df_long = pd.melt(
        df_raw, 
        id_vars=['Country', 'HTS Number'], 
        var_name='Metric', 
        value_name='Value'
    )
    print(f"  ✓ Created {len(df_long):,} records")
    
    # Parse column names (format: Variable_Month_Year)
    print(f"  → Parsing column names (format: Variable_Month_Year)...")
    split_cols = df_long['Metric'].str.split('_')
    df_long['Year'] = split_cols.str[-1]
    df_long['Month'] = split_cols.str[-2]
    df_long['Variable'] = split_cols.str[:-2].str.join('_').str.lower().str.strip()

    
    # Clean and standardize
    print(f"  → Standardizing fields...")
    df_long['Country'] = df_long['Country'].str.strip().str.title()
    df_long['Year'] = pd.to_numeric(df_long['Year'], errors='coerce').astype('Int64')
    df_long['Value'] = pd.to_numeric(df_long['Value'], errors='coerce').fillna(0)
    
    # Drop rows with missing keys
    before_drop = len(df_long)
    df_long = df_long.dropna(subset=['Country', 'Year', 'Month', 'Variable'])
    after_drop = len(df_long)
    if before_drop > after_drop:
        print(f"  ⚠ Dropped {before_drop - after_drop:,} records with missing key fields")
    
    # Aggregate from HTS level to Country level
    print(f"  → Aggregating from HTS-level to Country-level...")
    df_agg = df_long.groupby(
        ['Country', 'Year', 'Month', 'Variable'], 
        as_index=False,
        dropna=False
    )['Value'].sum()
    
    print(f"  ✓ Aggregated to {len(df_agg):,} country-level records")
    print(f"  ✓ Countries: {len(df_agg['Country'].unique())}")
    print(f"  ✓ Variables: {sorted(df_agg['Variable'].unique())}")
    
    # Save to cache
    try:
        df_agg.to_parquet(cache_path, index=False)
        print(f"  ✓ Saved cache: {os.path.basename(cache_path)}")
    except Exception as e:
        print(f"  ⚠ Could not save cache: {e}")
    
    return df_agg


# --- 3. LOAD OFFICIAL DATA ---

def load_official_data(file_path):
    """
    Load official multi-sheet Excel data and transform to long format.
    """
    print(f"\n{'='*70}")
    print("LOADING OFFICIAL DATA")
    print(f"{'='*70}")
    
    print(f"  → Loading Excel file: {os.path.basename(file_path)}")
    
    try:
        # Read metadata sheet to get sheet names
        meta_df = pd.read_excel(file_path, sheet_name=0, header=None, index_col=0, engine='openpyxl')
        sheet_names = [name.strip() for name in meta_df.loc['Data To Report', 1].split(',')]
        print(f"  ✓ Found {len(sheet_names)} data sheets: {', '.join(sheet_names)}")
        
        # Load all data sheets
        data_sheets = pd.read_excel(file_path, sheet_name=sheet_names, engine='openpyxl', header=2)
        
        # Process each sheet
        long_dfs = []
        for sheet_name, sheet_df in data_sheets.items():
            print(f"  → Processing sheet: {sheet_name}")
            
            # Drop unnecessary columns
            cols_to_drop = ['Quantity Description', 'Data Type']
            sheet_df = sheet_df.drop(columns=[c for c in cols_to_drop if c in sheet_df.columns])
            
            # Identify month columns (everything except Country and Year)
            id_cols = ['Country', 'Year']
            month_cols = [col for col in sheet_df.columns if col not in id_cols]
            
            # Melt to long format
            melted = sheet_df.melt(
                id_vars=id_cols,
                value_vars=month_cols,
                var_name='Month',
                value_name='Value'
            )
            melted['Source_Sheet'] = sheet_name
            
            long_dfs.append(melted)
            print(f"    ✓ {len(melted):,} records")
        
        # Combine all sheets
        df_long = pd.concat(long_dfs, ignore_index=True)
        print(f"  ✓ Combined {len(df_long):,} total records")
        
    except Exception as e:
        print(f"  ✗ ERROR: Failed to load official data: {e}")
        return None
    
    # Standardize fields
    print(f"  → Standardizing fields...")
    
    # Map sheet names to variable names
    var_map = {
        'Customs Value': 'customs',
        'Calculated Duties': 'calculated',
        'First Unit of Quantity': 'quantity_value'
    }
    df_long['Variable'] = df_long['Source_Sheet'].map(var_map).fillna('unknown').str.lower().str.strip()
    
    # Standardize country names
    df_long['Country'] = df_long['Country'].str.strip().str.title()
    
    # Standardize month names (full name → 3-letter abbreviation)
    month_map = {
        'January': 'Jan', 'February': 'Feb', 'March': 'Mar', 'April': 'Apr',
        'May': 'May', 'June': 'Jun', 'July': 'Jul', 'August': 'Aug',
        'September': 'Sep', 'October': 'Oct', 'November': 'Nov', 'December': 'Dec'
    }
    # Handle both full names and abbreviations
    df_long['Month'] = df_long['Month'].str.strip()
    df_long['Month'] = df_long['Month'].replace(month_map)
    
    # Standardize year and value
    df_long['Year'] = pd.to_numeric(df_long['Year'], errors='coerce').astype('Int64')
    df_long['Value'] = pd.to_numeric(df_long['Value'], errors='coerce').fillna(0)
    
    # Drop rows with missing keys
    before_drop = len(df_long)
    df_long = df_long.dropna(subset=['Country', 'Year', 'Month', 'Variable'])
    after_drop = len(df_long)
    if before_drop > after_drop:
        print(f"  ⚠ Dropped {before_drop - after_drop:,} records with missing key fields")
    
    # Aggregate to final format
    print(f"  → Aggregating to country-level...")
    df_agg = df_long.groupby(
        ['Country', 'Year', 'Month', 'Variable'],
        as_index=False,
        dropna=False
    )['Value'].sum()
    
    print(f"  ✓ Aggregated to {len(df_agg):,} records")
    print(f"  ✓ Countries: {len(df_agg['Country'].unique())}")
    print(f"  ✓ Variables: {sorted(df_agg['Variable'].unique())}")
    
    # Save processed file for review
    try:
        output_path = os.path.join(BASE_PATH, OFFICIAL_SUMMARY_PROCESSED_FILE)
        df_agg.to_excel(output_path, index=False)
        print(f"  ✓ Saved processed file: {os.path.basename(output_path)}")
    except Exception as e:
        print(f"  ⚠ Could not save processed file: {e}")
    
    return df_agg


# --- 4. COMPARE DATASETS ---

def compare_datasets(your_df, official_df):
    """
    Compare your aggregated data with official data and generate validation report.
    """
    print(f"\n{'='*70}")
    print("COMPARING DATASETS")
    print(f"{'='*70}")
    
    # Pre-merge diagnostics
    print(f"\n  Your Data:")
    print(f"    Records: {len(your_df):,}")
    print(f"    Countries: {len(your_df['Country'].unique())}")
    print(f"    Variables: {sorted(your_df['Variable'].unique())}")
    print(f"    Sample countries: {sorted(your_df['Country'].unique())[:5]}")
    
    print(f"\n  Official Data:")
    print(f"    Records: {len(official_df):,}")
    print(f"    Countries: {len(official_df['Country'].unique())}")
    print(f"    Variables: {sorted(official_df['Variable'].unique())}")
    print(f"    Sample countries: {sorted(official_df['Country'].unique())[:5]}")
    
    # Rename value columns for clarity
    your_df = your_df.rename(columns={'Value': 'Your_Value'})
    official_df = official_df.rename(columns={'Value': 'Official_Value'})
    
    # Merge datasets
    print(f"\n  → Merging on [Country, Year, Month, Variable]...")
    merge_keys = ['Country', 'Year', 'Month', 'Variable']
    
    comparison_df = pd.merge(
        official_df,
        your_df,
        on=merge_keys,
        how='outer',
        indicator=True
    )
    
    # Fill NaN with 0
    comparison_df['Your_Value'] = comparison_df['Your_Value'].fillna(0)
    comparison_df['Official_Value'] = comparison_df['Official_Value'].fillna(0)
    
    # Calculate differences
    comparison_df['Difference'] = comparison_df['Your_Value'] - comparison_df['Official_Value']
    comparison_df['Abs_Difference'] = abs(comparison_df['Difference'])
    comparison_df['Percent_Diff'] = (
        comparison_df['Difference'] / comparison_df['Official_Value'].replace(0, float('nan'))
    ) * 100
    
    # Merge statistics
    merge_stats = comparison_df['_merge'].value_counts()
    print(f"\n  Merge Results:")
    print(f"    In both datasets: {merge_stats.get('both', 0):,}")
    print(f"    Only in official: {merge_stats.get('left_only', 0):,}")
    print(f"    Only in yours: {merge_stats.get('right_only', 0):,}")
    
    print(f"  ✓ Comparison complete")
    
    return comparison_df


# --- 5. GENERATE REPORT ---

def generate_report(comparison_df, output_path):
    """
    Analyze comparison results and generate detailed validation report.
    """
    print(f"\n{'='*70}")
    print("GENERATING VALIDATION REPORT")
    print(f"{'='*70}")
    
    # Identify inconsistencies
    inconsistencies = comparison_df[abs(comparison_df['Difference']) > TOLERANCE].copy()
    
    if inconsistencies.empty:
        print(f"\n  ✓✓✓ SUCCESS! No inconsistencies found (tolerance: ${TOLERANCE:,.2f})")
        print(f"  ✓✓✓ Your data matches official data perfectly!")
        return
    
    # Sort by absolute difference
    inconsistencies = inconsistencies.sort_values('Abs_Difference', ascending=False)
    
    print(f"\n  Found {len(inconsistencies):,} inconsistencies (>{TOLERANCE} difference)")
    
    # Create human-readable summaries
    summaries = []
    for _, row in inconsistencies.iterrows():
        if row['_merge'] == 'left_only':
            summary = f"[MISSING] {row['Country']} {row['Month']} {row['Year']} {row['Variable']}: Official has {row['Official_Value']:,.2f} but your data is missing"
        elif row['_merge'] == 'right_only':
            summary = f"[EXTRA] {row['Country']} {row['Month']} {row['Year']} {row['Variable']}: You have {row['Your_Value']:,.2f} but official data is missing"
        else:
            summary = f"{row['Country']} {row['Month']} {row['Year']} {row['Variable']}: Official={row['Official_Value']:,.2f}, Yours={row['Your_Value']:,.2f}, Diff={row['Difference']:,.2f}"
        summaries.append(summary)
    
    inconsistencies['Summary'] = summaries
    
    # Display top mismatches
    print(f"\n  --- Top 25 Largest Inconsistencies ---")
    for i, summary in enumerate(inconsistencies['Summary'].head(25), 1):
        print(f"  {i:2d}. {summary}")
    
    # Statistics
    print(f"\n  --- Overall Statistics ---")
    print(f"  Total mismatches: {len(inconsistencies):,}")
    print(f"  Average absolute difference: ${inconsistencies['Abs_Difference'].mean():,.2f}")
    print(f"  Median absolute difference: ${inconsistencies['Abs_Difference'].median():,.2f}")
    print(f"  Maximum difference: ${inconsistencies['Abs_Difference'].max():,.2f}")
    
    # Breakdown by variable
    print(f"\n  --- Mismatches by Variable ---")
    by_var = inconsistencies.groupby('Variable').agg({
        'Difference': ['count', 'sum', 'mean'],
        'Abs_Difference': ['mean', 'max']
    }).round(2)
    print(by_var.to_string())
    
    # Save Excel report
    print(f"\n  → Saving detailed report to: {os.path.basename(output_path)}")
    
    try:
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Format for currency
            currency_fmt = workbook.add_format({'num_format': '#,##0.00'})
            percent_fmt = workbook.add_format({'num_format': '0.00%'})
            
            # Summary sheet
            summary_data = pd.DataFrame({
                'Metric': [
                    'Total Records Compared',
                    'Perfect Matches',
                    'Mismatches (> $' + str(TOLERANCE) + ')',
                    'Records Only in Official',
                    'Records Only in Yours',
                    '',
                    'Average Abs Difference',
                    'Median Abs Difference',
                    'Maximum Difference'
                ],
                'Value': [
                    len(comparison_df),
                    len(comparison_df) - len(inconsistencies),
                    len(inconsistencies),
                    len(comparison_df[comparison_df['_merge'] == 'left_only']),
                    len(comparison_df[comparison_df['_merge'] == 'right_only']),
                    '',
                    f"${inconsistencies['Abs_Difference'].mean():,.2f}",
                    f"${inconsistencies['Abs_Difference'].median():,.2f}",
                    f"${inconsistencies['Abs_Difference'].max():,.2f}"
                ]
            })
            summary_data.to_excel(writer, sheet_name='Summary', index=False)
            
            # All mismatches
            output_cols = [
                'Summary', 'Country', 'Year', 'Month', 'Variable',
                'Official_Value', 'Your_Value', 'Difference', 'Percent_Diff', '_merge'
            ]
            inconsistencies[output_cols].to_excel(writer, sheet_name='All Mismatches', index=False)
            
            # By variable
            by_var_reset = by_var.reset_index()
            by_var_reset.to_excel(writer, sheet_name='By Variable', index=False)
            
            # By country
            by_country = inconsistencies.groupby('Country').agg({
                'Difference': ['count', 'sum'],
                'Abs_Difference': 'mean'
            }).round(2).reset_index()
            by_country.columns = ['Country', 'Mismatch_Count', 'Total_Difference', 'Avg_Abs_Difference']
            by_country = by_country.sort_values('Mismatch_Count', ascending=False)
            by_country.to_excel(writer, sheet_name='By Country', index=False)
        
        print(f"  ✓ Report saved successfully")
        
    except PermissionError:
        print(f"  ✗ ERROR: Cannot save file (may be open). Close {os.path.basename(output_path)} and try again.")
    except ImportError:
        print(f"  ✗ ERROR: xlsxwriter not installed. Run: pip install xlsxwriter")
    except Exception as e:
        print(f"  ✗ ERROR: {e}")


# --- 6. MAIN EXECUTION ---

def main():
    """
    Main validation workflow.
    """
    start_time = datetime.now()
    
    print("\n" + "="*70)
    print(" "*15 + "TRADE DATA VALIDATION")
    print(" "*20 + f"Run: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Load your data
    your_df = load_your_data(
        os.path.join(BASE_PATH, YOUR_MASTER_PARQUET_FILE),
        os.path.join(BASE_PATH, YOUR_SUMMARY_CACHE_FILE),
        force_rebuild=False  # Set to True to rebuild cache
    )
    
    if your_df is None:
        print("\n  ✗ Failed to load your data. Exiting.")
        return
    
    # Load official data
    official_df = load_official_data(
        os.path.join(BASE_PATH, OFFICIAL_DATA_FILE)
    )
    
    if official_df is None:
        print("\n  ✗ Failed to load official data. Exiting.")
        return
    
    # Compare
    comparison_df = compare_datasets(your_df, official_df)
    
    # Generate report
    generate_report(
        comparison_df,
        os.path.join(BASE_PATH, OUTPUT_FILE)
    )
    
    # Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n{'='*70}")
    print(f" "*20 + "VALIDATION COMPLETE")
    print(f" "*23 + f"Time: {elapsed:.1f}s")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()