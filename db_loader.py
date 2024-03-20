import json
import os
from datetime import datetime, timedelta
from sqlalchemy import Date, String, create_engine, Table, Column, Integer, Float, MetaData
from xbrl_helpers import find_concepts
import xbrl_api
from concepts import concept_names  # Importing concept_names from concepts.py

# Define the path for the progress file
PROGRESS_FILE_PATH = 'progress.json'

# Function to check for existing progress and load it
def load_progress():
    if os.path.exists(PROGRESS_FILE_PATH):
        with open(PROGRESS_FILE_PATH, 'r') as file:
            return json.load(file)
    else:
        return {"start_index": 0, "keys_seen": {}}

# Function to save progress
def save_progress(start_index, keys_seen):
    progress_data = {"start_index": start_index, "keys_seen": keys_seen}
    with open(PROGRESS_FILE_PATH, 'w') as file:
        json.dump(progress_data, file)

# Assuming you have your JSON data in a variable named report_data_json
with open('report_data_v2.json', 'r') as file:
    report_data = json.load(file)

# Initialize the database engine; replace 'sqlite:///reports.db' with your actual database connection string
engine = create_engine('sqlite:///reports_v5.db')  # Use your database URL
metadata = MetaData()

# Define the table structure with a column for each concept
columns = [
    Column('report_id', Integer, primary_key=True),
    Column('sec_url', String),  # Add column for entity name
] + [Column(name, Float) for name in concept_names]

reports_table = Table('reports', metadata, *columns)

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

        # Find concepts (assuming this is done within your loop)
        concepts = find_concepts(access_token, report_id, document_type)
        
        # Build the insert_values dict dynamically based on concept_names
        insert_values = {"report_id": report_id}
        insert_values["sec_url"] =  concepts.get('sec-url', '')

        insert_values.update({name: float(concepts.get(name, 0.0) or 0.0) for name in concept_names})
        progress_percentage = (report_count / total_reports) * 100
        # Execute the SQL statement
        with engine.connect() as connection:
            try:
                connection.execute(reports_table.insert().values(**insert_values))
                if report_count % 100 == 0:
                    print(f"Progress: {progress_percentage:.2f}%")
                connection.commit()  # Commit the transaction
            except Exception as e:
                # Specific error handling for primary key violation
                print(f"Failed to insert report {report_count} of {total_reports}. Attempting to update the existing record.")
                update_values = {k: v for k, v in insert_values.items() if v != ''}
                update_stmt = reports_table.update().where(reports_table.c.report_id == report_id).values(**update_values)
                connection.execute(update_stmt)
                connection.commit()  # Commit the transaction

        save_progress(idx + 1, keys_seen)  # Save progress after each key is processed
