import csv
from datetime import datetime
import os
import requests
import json
from dotenv import load_dotenv
import xbrl_api

# Load environment variables from .env file
load_dotenv()

def process_csv_with_xbrl_api():
    # Retrieve credentials from environment variables
    email = os.getenv('XBRL_EMAIL')
    password = os.getenv('XBRL_PASSWORD')
    clientid = os.getenv('XBRL_CLIENT_ID')
    secret = os.getenv('XBRL_SECRET')

    # Authenticate and get access token
    access_token, refresh_token = xbrl_api.authenticate(email, password, clientid, secret)
    if access_token:
        print('Access token:', access_token)
        # Now, process the CSV file
        file_path = 'tickers.csv'
        sic_code_dict = {}
        with open(file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Access the "Spinoff" and "Date" columns
                spinoff_ticker = row['Spinoff']
                date = row['Date']
                # Clean up ticker according to specifications
                ticker = spinoff_ticker[:4].rstrip('Q').upper().replace('.', '')
                # Call XBRL API to retrieve SIC code
                sic_code = retrieve_sic_code(ticker, date, access_token)
                # Store in dictionary
                sic_code_dict[spinoff_ticker] = sic_code if sic_code else "N/A"
                # Print for demonstration
                print(f"Spinoff: {spinoff_ticker}, Date: {date}, SIC Code: {sic_code}")

        # Return the dictionary
        return sic_code_dict

def retrieve_sic_code(ticker, date, access_token):
    date = datetime.strptime(date, '%m/%d/%y')  # Convert date string to datetime object
    report_search_url = 'https://api.xbrl.us/api/v1/report/search'
    params = {
        'entity.ticker': ticker,
        'fields': 'report.sic-code,report.period-end',
    }
    response = requests.get(report_search_url, params=params, headers={'Authorization': 'Bearer {}'.format(access_token)})
    if response.status_code == 200:
        sic_code_data = response.json()
        if 'data' in sic_code_data and sic_code_data['data']:
            closest_report = None
            closest_difference = float('inf')  # Initialize with a large value
            for entry in sic_code_data['data']:
                report_date = entry['report.period-end']
                report_date = datetime.strptime(report_date, '%Y-%m-%d')  # Convert report date string to datetime object
                difference = abs((report_date - date).days)
                if difference < closest_difference:
                    closest_difference = difference
                    closest_report = entry
            if closest_report:
                sic_code = closest_report['report.sic-code']  # Retrieve SIC code from the closest report
                return sic_code
        else:
            print(f"Error: Report for {ticker} on or after {date} likely not found.")
    return None


def main():
    # Process CSV with XBRL API
    sic_code_dict = process_csv_with_xbrl_api()
    print("SIC Code Dictionary:")
    print(sic_code_dict)

    # Save the dictionary as JSON
    with open('sic_code_output.json', 'w') as json_file:
        json.dump(sic_code_dict, json_file)

if __name__ == "__main__":
    main()