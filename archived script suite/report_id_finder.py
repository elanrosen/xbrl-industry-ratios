import os
import psycopg2
import pandas as pd
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_fiscal_quarter_dates(date):
    date = pd.Timestamp(date)
    quarter_start_date = pd.Timestamp(year=date.year, month=3 * (date.quarter - 1) + 1, day=1)
    quarter_end_date = (quarter_start_date + pd.offsets.QuarterEnd()).date()
    return quarter_start_date.strftime('%Y-%m-%d'), quarter_end_date.strftime('%Y-%m-%d')

# Assuming sic_code_output.json and tickers.csv are properly formatted and available
# Load the spinoff ticker to SIC code mapping
with open('sic_code_output.json', 'r') as file:
    sic_code_mapping = json.load(file)

# Load tickers CSV
tickers_df = pd.read_csv('tickers.csv', parse_dates=['Date'])

# Initialize a dictionary to store results
report_data = {}

# Database connection parameters
params = {
    'host': 'public.xbrl.us',
    'port': '5432',
    'database': 'edgar_db',
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Iterate over each row in the tickers DataFrame
for index, row in tickers_df.iterrows():
    spinoff_ticker = row['Spinoff']
    sic_code_4digit = sic_code_mapping.get(spinoff_ticker, 'N/A')
    if sic_code_4digit == 'N/A':
        continue  # Skip this ticker and continue to the next
    sic_code_3digit = str(sic_code_4digit)[:3]
    # Convert date to fiscal quarter
    start_date, end_date = get_fiscal_quarter_dates(row['Date'])
    
    # SQL query with dynamic SIC code and date
    sql_query = f"""
    SELECT report.report_id, report.entity_name, report.reporting_period_end_date, report.properties->>'document_type' AS document_type
    FROM report
    WHERE report.reporting_period_end_date >= '{start_date}'
    AND report.reporting_period_end_date <= '{end_date}'
    AND (report.properties->>'standard_industrial_classification')::text LIKE '{sic_code_3digit}%'
    AND report.source_id = (SELECT source_id FROM source WHERE source_name = 'SEC')
    AND report.properties->>'document_type' IN ('10-Q', '10-K');
    """
    
    # Execute the query and store results
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                rows = cursor.fetchall()
                # Key for storing in the dictionary
                key = f"{sic_code_3digit}_{pd.Timestamp(row['Date']).quarter}Q{pd.Timestamp(row['Date']).year}"
                # Update this line to include document type
                report_data[key] = [{'report_id': report_id, 'document_type': document_type} for report_id, _, _, document_type in rows]
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# Save the results to a JSON file
with open('report_data_v2.json', 'w') as json_file:
    json.dump(report_data, json_file, indent=4)  # Use indent for better readability

print("Operation completed, data saved to 'report_data_v2.json'.")
