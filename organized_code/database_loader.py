# Import necessary libraries
import json
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData
from xbrl_helpers import find_concepts
import xbrl_api
from concepts import concept_names  # Importing concept_names from concepts.py

# Constants
PROGRESS_FILE_PATH = 'progress.json'
DATABASE_URL = 'sqlite:///reports_v5.db'  # Modify as necessary

# Load and save progress functions
def load_progress():
    """Loads progress from a JSON file."""
    if os.path.exists(PROGRESS_FILE_PATH):
        with open(PROGRESS_FILE_PATH, 'r') as file:
            return json.load(file)
    else:
        return {"start_index": 0, "keys_seen": {}}

def save_progress(start_index, keys_seen):
    """Saves progress to a JSON file."""
    progress_data = {"start_index": start_index, "keys_seen": keys_seen}
    with open(PROGRESS_FILE_PATH, 'w') as file:
        json.dump(progress_data, file)

# Database initialization
def initialize_database(concept_names, database_url=DATABASE_URL):
    """Initializes the database with the specified structure."""
    engine = create_engine(database_url)
    metadata = MetaData()

    columns = [
        Column('report_id', Integer, primary_key=True),
        Column('sec_url', String),
    ] + [Column(name, Float) for name in concept_names]

    reports_table = Table('reports', metadata, *columns)
    metadata.create_all(engine)
    return engine, reports_table

# Process reports
def process_reports(report_data, concept_names):
    """Processes the reports, updating the database accordingly."""
    engine, reports_table = initialize_database(concept_names)
    access_token = xbrl_api.get_access_token()
    last_refreshed = datetime.now()

    progress = load_progress()
    start_index = progress['start_index']
    keys_seen = progress['keys_seen']

    total_reports = sum(len(reports) for reports in report_data.values())
    report_count = sum(len(reports) for reports in list(report_data.values())[:start_index])

    for idx, (key, reports) in enumerate(list(report_data.items())[start_index:]):
        if datetime.now() - last_refreshed >= timedelta(minutes=55):
            access_token = xbrl_api.get_access_token()
            last_refreshed = datetime.now()

        if key in keys_seen:
            print(f"Skipping {key} because it was already processed")
            report_count += len(reports)
            continue

        keys_seen[key] = True

        for report in reports:
            report_count += 1
            concepts = find_concepts(access_token, report['report_id'], report['document_type'])

            insert_values = {"report_id": report['report_id'], "sec_url": concepts.get('sec-url', '')}
            insert_values.update({name: float(concepts.get(name, 0.0) or 0.0) for name in concept_names})

            with engine.connect() as connection:
                try:
                    connection.execute(reports_table.insert().values(**insert_values))
                    if report_count % 100 == 0:
                        print(f"Progress: {(report_count / total_reports) * 100:.2f}%")
                    connection.commit()
                except Exception as e:
                    print(f"Failed to insert report {report_count} of {total_reports}. Attempting to update the existing record.")
                    update_values = {k: v for k, v in insert_values.items() if v != ''}
                    update_stmt = reports_table.update().where(reports_table.c.report_id == report['report_id']).values(**update_values)
                    connection.execute(update_stmt)
                    connection.commit()

            save_progress(idx + 1, keys_seen)

if __name__ == "__main__":
    # Load your JSON data into report_data
    with open('report_data_v2.json', 'r') as file:
        report_data = json.load(file)

    process_reports(report_data, concept_names)
