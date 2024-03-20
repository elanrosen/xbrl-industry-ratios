import json
from sqlalchemy import Float, create_engine, Table, Column, Integer, String, MetaData
from xbrl_helpers import find_concepts, filter_for_correct_values_10Q, filter_for_correct_values_10K
import xbrl_api
from datetime import datetime, timedelta
import os

# Define the path for the progress file
progress_file_path = 'progress.json'

# Function to check for existing progress and load it
def load_progress():
    if os.path.exists(progress_file_path):
        with open(progress_file_path, 'r') as file:
            return json.load(file)
    else:
        return {"start_index": 0, "keys_seen": {}}
    
# Function to save progress
def save_progress(start_index, keys_seen):
    progress_data = {"start_index": start_index, "keys_seen": keys_seen}
    with open(progress_file_path, 'w') as file:
        json.dump(progress_data, file)

# Assuming you have your JSON data in a variable named report_data_json
with open('report_data_v2.json', 'r') as file:
    report_data = json.load(file)

# Initialize the database engine; replace 'sqlite:///reports.db' with your actual database connection string
engine = create_engine('sqlite:///reports.db')  # Use your database URL
metadata = MetaData()

# Define the table structure with a column for each concept
reports_table = Table('reports', metadata,
                      Column('report_id', Integer, primary_key=True),
                      Column('AssetsCurrent', Float),
                      Column('StockholdersEquity', Float),
                      Column('LongTermDebtCurrent', Float),
                      Column('Assets', Float),
                      Column('Liabilities', Float),
                        Column('LongTermDebtAndCapitalLeaseObligations', Float),
                      )

# Create the table in the database
metadata.create_all(engine)


access_token = xbrl_api.get_access_token()
last_refreshed = datetime.now()

progress = load_progress()
start_index = progress['start_index']
keys_seen = progress['keys_seen']

total_reports = sum(len(reports) for reports in report_data.values())
report_count = 0

report_count += sum(len(reports) for reports in list(report_data.values())[:start_index])  # Increase the report count by the number of reports in the skipped keys
for idx, (key, reports) in enumerate(list(report_data.items())[start_index:]):
    # Check if the access token needs to be refreshed
    if datetime.now() - last_refreshed >= timedelta(minutes=55):
        access_token = xbrl_api.get_access_token()
        last_refreshed = datetime.now()
    if key in keys_seen:
        print(f"Skipping {key} because it was already processed")
        report_count += len(reports)  # Increase the report count by the number of reports in the skipped key
        continue
    keys_seen[key] = True 
    for report in reports:
        report_count += 1
        report_id = report['report_id']
        document_type = report['document_type']
        
        # Placeholder for obtaining the access token
        
        # Find concepts
        concepts = find_concepts(access_token, report_id, document_type)
        # concepts is a dict

        # Build the SQL statement to insert or update records
        # Build the SQL statement to insert or update records
        insert_values = {
            "report_id": report_id,
            "AssetsCurrent": float(concepts.get('AssetsCurrent', 0.0) or 0.0),
            "StockholdersEquity": float(concepts.get('StockholdersEquity', 0.0) or 0.0),
            "LongTermDebtCurrent": float(concepts.get('LongTermDebtCurrent', 0.0) or 0.0),
            "Assets": float(concepts.get('Assets', 0.0) or 0.0),
            "Liabilities": float(concepts.get('Liabilities', 0.0) or 0.0),
            "LongTermDebtAndCapitalLeaseObligations": float(concepts.get('LongTermDebtAndCapitalLeaseObligations', 0.0) or 0.0)
        }

        # Execute the SQL statement
        with engine.connect() as connection:
            # Try to insert the record; if it fails due to a primary key violation, update the existing record
            try:
                connection.execute(reports_table.insert().values(**insert_values))
                if report_count % 50 == 0:
                    print(f"Inserted report {report_count} of {total_reports}")
                connection.commit()  # Commit the transaction
            except Exception as e:
                # If the primary key constraint fails, update the existing record
                print(f"Failed to insert report {report_count} of {total_reports}. Attempting to update the existing record.")
                update_values = {k: v for k, v in insert_values.items() if v != ''}
                update_stmt = reports_table.update().where(reports_table.c.report_id == report_id).values(**update_values)
                connection.execute(update_stmt)
                connection.commit()  # Commit the transaction
        save_progress(idx + 1, keys_seen)  # Save progress after each key is processed

