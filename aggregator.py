import sqlite3
import json

# Load data from the SQL database
conn = sqlite3.connect('reports.db')
cursor = conn.cursor()
cursor.execute("SELECT * FROM reports")
reports_data = cursor.fetchall()
conn.close()

# Load data from JSON files
with open('report_data_v2.json', 'r') as f:
    report_data_v2 = json.load(f)

with open('sic_code_output.json', 'r') as f:
    sic_code_output = json.load(f)
# Preprocess and map reports to SIC codes and dates
reports_mapping = {}  # Key: report_id, Value: (SIC, date)

for sic_date, reports in report_data_v2.items():
    sic, date = sic_date.split('_')
    for report in reports:
        report_id = report["report_id"]
        # Assuming SIC code is stored as an integer in sic_code_output
        ticker = [ticker for ticker, sic_code in sic_code_output.items() if sic_code == int(sic)]
        if ticker:  # If a matching ticker is found
            reports_mapping[report_id] = (ticker[0], date)

def calculate_book_leverage(report):
    total_debt = report[5] if report[5] is not None else 0
    total_assets = report[4]
    if total_assets is None or total_assets == 0:
        return None  # Indicate invalid or incomplete data for this calculation

    return total_debt / total_assets

def calculate_debt_to_equity(report):
    total_debt = report[5] if report[5] is not None else 0
    total_equity = report[1]
    if total_equity is None or total_equity == 0:
        return None  # Indicate invalid or incomplete data for this calculation

    return total_debt / total_equity

def calculate_market_leverage(report):
    # Placeholder for market leverage calculation
    # This example assumes you need additional data for market value of total assets
    pass

from statistics import median

# Assuming reports_data is a list of dictionaries
leverage_ratios = {}  # Key: (SIC, date), Value: list of leverage ratios

for report in reports_data:
    report_id = report[0]  # Adjusting to access report_id by index
    if report_id in reports_mapping:
        ticker, date = reports_mapping[report_id]
        book_leverage = calculate_book_leverage(report)
        # Add leverage ratio to the list for the corresponding SIC-Date pair
        leverage_ratios.setdefault((ticker, date), []).append(book_leverage)

# Calculate median leverage ratios for each SIC-Date pair
median_leverages = {k: median([x for x in v if x is not None]) for k, v in leverage_ratios.items()}


# Calculate median leverage ratios for each SIC-Date pair
median_leverages = {k: median([x for x in v if x is not None]) for k, v in leverage_ratios.items()}

# Assuming you're creating a new SQLite database
conn = sqlite3.connect('new_leverage_ratios_v2.db')
cursor = conn.cursor()

# Create table
cursor.execute('''CREATE TABLE median_leverages
             (ticker_date TEXT PRIMARY KEY, median_book_leverage REAL)''')

# Insert data
for (ticker, date), median_leverage in median_leverages.items():
    cursor.execute("INSERT INTO median_leverages VALUES (?, ?)", (f"{ticker}_{date}", median_leverage))

conn.commit()
conn.close()