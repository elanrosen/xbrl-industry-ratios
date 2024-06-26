# calculate_industry_ratios_and_export.py is a Python script that reads data from a SQLite database and a JSON file, calculates industry ratios, and exports the results to a CSV file. The script performs the following steps:
import sqlite3
import pandas as pd
import json
import sys

def load_data_from_sqlite(db_path):
    conn = sqlite3.connect(db_path)
    query = "SELECT * FROM reports"
    df = pd.read_sql(query, conn, index_col='report_id')
    return df

# Define a function to calculate total debt
def calculate_total_debt(row):
    total_debt = 0
    
    # Check if DebtNoncurrent is not zero
    if row['DebtNoncurrent'] != 0:
        total_debt += row['DebtNoncurrent']
    
    # If DebtNoncurrent is zero, check if LongTermDebtNoncurrent is not zero
    elif row['LongTermDebtNoncurrent'] != 0:
        total_debt += row['LongTermDebtNoncurrent']
    
    # If both DebtNoncurrent and LongTermDebtNoncurrent are zero, calculate the total debt from other sources
    else:
        total_debt += row['CapitalLeaseObligationsNoncurrent']
        total_debt += row['FinanceLeaseLiabilityNoncurrent']
    
    # Check if DebtCurrent is zero
    if row['DebtCurrent'] == 0:
        total_debt += row['CapitalLeaseObligationsCurrent']
        total_debt += row['LinesOfCreditCurrent']
        total_debt += row['FinanceLeaseLiabilityCurrent']
    
    # Add DebtCurrent to the total debt
    total_debt += row['DebtCurrent']
    
    return total_debt

def apply_total_debt_calculation(df):
    df['total_debt'] = df.apply(calculate_total_debt, axis=1)
    return df

def load_and_map_report_data(json_path, df):
    with open(json_path, 'r') as f:
        report_data = json.load(f)

    report_id_to_key = {report['report_id']: key for key, reports in report_data.items() for report in reports}
    df['key'] = df.index.map(report_id_to_key)
    return df

def filter_and_calculate_ratios(df):
    df_filtered_common = df[(df['Assets'] != 0) & (df['StockholdersEquity'] != 0)]
    df_all, df_exclude_zero_debt = calculate_ratios(df_filtered_common)
    return df_all, df_exclude_zero_debt

def calculate_ratios(df_filtered_common):
    df_all = df_filtered_common.copy()
    df_all['debt_to_assets_all'] = df_all.apply(lambda x: x['total_debt'] / x['Assets'] if x['total_debt'] != 0 else 0, axis=1)
    df_all['debt_to_equity_all'] = df_all.apply(lambda x: x['total_debt'] / x['StockholdersEquity'] if x['total_debt'] != 0 else 0, axis=1)
    
    df_exclude_zero_debt = df_filtered_common[df_filtered_common['total_debt'] != 0].copy()
    df_exclude_zero_debt['debt_to_assets_exclude'] = df_exclude_zero_debt['total_debt'] / df_exclude_zero_debt['Assets']
    df_exclude_zero_debt['debt_to_equity_exclude'] = df_exclude_zero_debt['total_debt'] / df_exclude_zero_debt['StockholdersEquity']
    
    return df_all, df_exclude_zero_debt

def group_and_merge(df_all, df_exclude_zero_debt):
    grouped_all = df_all.groupby('key').agg(
        debt_to_assets_median_all=('debt_to_assets_all', 'median'),
        debt_to_equity_median_all=('debt_to_equity_all', 'median'),
        count_all=('debt_to_assets_all', 'size')
    )
    
    grouped_exclude_zero_debt = df_exclude_zero_debt.groupby('key').agg(
        debt_to_assets_median_exclude=('debt_to_assets_exclude', 'median'),
        debt_to_equity_median_exclude=('debt_to_equity_exclude', 'median'),
        count_exclude=('debt_to_assets_exclude', 'size')
    )
    
    median_ratios_combined = pd.merge(grouped_all, grouped_exclude_zero_debt, on='key', how='outer')
    return median_ratios_combined

def extract_and_sort(median_ratios_combined):
    pattern = r'(\d+)_([1-4]Q)(\d{4})'
    median_ratios_combined[['SIC', 'Quarter', 'Year']] = median_ratios_combined.index.to_series().str.extract(pattern)
    median_ratios_combined['Year'] = pd.to_datetime(median_ratios_combined['Year'],format='%Y').dt.year
    median_ratios_combined['Quarter'] = median_ratios_combined['Quarter'].str.replace('Q', '').astype(int)
    median_ratios_combined = median_ratios_combined.sort_values(by=['Year', 'Quarter'], ascending=[False, False]).reset_index(drop=True)
    return median_ratios_combined

def main():
    if len(sys.argv) != 3:
        print("Usage: python aggregator.py <db_path> <json_path>")
        sys.exit(1)

    db_path = sys.argv[1]
    json_path = sys.argv[2]
    
    # Load data from SQLite
    df = load_data_from_sqlite(db_path)

    # Calculate total debt and add as a new column
    df = apply_total_debt_calculation(df)

    # Load JSON data and map report IDs to keys
    df = load_and_map_report_data(json_path, df)

    # Filter DataFrame and calculate ratios
    df_all, df_exclude_zero_debt = filter_and_calculate_ratios(df)

    # Group and merge data based on keys
    median_ratios_combined = group_and_merge(df_all, df_exclude_zero_debt)

    # Extract and sort information
    median_ratios_combined = extract_and_sort(median_ratios_combined)

    print(median_ratios_combined)

if __name__ == "__main__":
    main()
