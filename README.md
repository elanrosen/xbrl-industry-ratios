# Industry Analysis Automation Project (XBRL)

## Overview

The Industry Analysis Automation Project streamlines the collection, processing, and analysis of financial reports across various industries using Standard Industrial Classification (SIC) codes. The automated pipeline includes scripts for retrieving SIC codes for listed companies, finding related financial reports by SIC code and date, populating a database with these reports, computing industry ratios, and exporting the data to Excel for analysis.

## Detailed Components and Process

### Scripts Overview

#### 1. `populate_sic_codes_from_tickers.py`

- **Purpose:** Maps each ticker in `tickers.csv` to its corresponding SIC code.
- **Output:** `sic_code_output.json`, containing ticker-SIC code mappings.

#### 2. `find_reports_by_sic_and_date.py`

- **Purpose:** Identifies financial report IDs and their types based on SIC codes and specific dates, utilizing tickers for date reference and `sic_code_output.json` for SIC codes.
- **Functionality:** The script:
  - Reads from a CSV containing ticker information and dates.
  - Connects to a PostgreSQL database using credentials stored in a `.env` file to execute queries.
  - Finds reports within the fiscal quarter of each ticker's specified date, filtered by the SIC code.
- **Output:** `report_data_v2.json`, with mappings of SIC code-date pairs to corresponding reports, including report ID and document type.

#### 3. `load_reports_into_database.py`

- **Purpose:** Loads financial report data into a SQL database from the identified reports.
- **Functionality:** This script:
  - Initializes a database with tables structured to hold report IDs, SEC URLs, and financial concept values.
  - Utilizes a JSON progress tracking mechanism to handle partial execution and resumes where left off upon rerun.
  - Processes reports by calling the XBRL API to find relevant financial concepts for each report, inserting or updating records in the database.
- **Output:** A SQL database, `reports_v5.db`, containing financial report data structured for analysis.

#### 4. `calculate_industry_ratios_and_export.py`

- **Purpose:** Calculates financial ratios for industries based on the collected report data.
- **Output:** `industry_ratios_analysis.xlsx`, containing computed industry ratios for analysis.

#### 5. `export_ticker_sic_to_excel.py`

- **Purpose:** Exports the ticker-SIC code mappings to an Excel file for reference.
- **Output:** `ticker_sic_reference.xlsx`, mapping each ticker to its SIC code.

### Running the Automation

1. Ensure `tickers.csv` is prepared with the required ticker and date information.
2. Execute `main_runner.py` to start the automation process, which sequentially runs each script according to the defined steps, handling data collection, processing, and analysis seamlessly.

### Additional Script Details

#### `load_reports_into_database.py`

- **Key Components:**
  - Error handling for insert/update operations to manage duplicates or data inconsistencies.
  - Progress tracking to facilitate interrupted runs by saving and loading progress from `progress.json`.
  - Dynamic database initialization based on financial concepts required for analysis.

#### `find_reports_by_sic_and_date.py`

- **Key Components:**
  - Dynamic SQL query generation based on SIC codes and dates to precisely identify relevant financial reports.
  - Efficient database connection handling to ensure queries are executed and closed properly.
  - Fiscal quarter calculation to match reports with the correct time periods for analysis.

## Contribution and Collaboration

Contributions to enhance or extend the project's capabilities are welcome. For issues, suggestions, or contributions, please use the project's GitHub issues and pull requests features.