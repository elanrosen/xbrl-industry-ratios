1) Use sic_json_populator.py to find the SIC code of each ticker in tickers.csv. This outputs sic_code_output.json, which has keys of each ticker and value of its SIC code.
2) Use report_id_finder.py to find the report_id's (and the report type for said reports) for each SIC code and date combo. It references both tickers.csv to find dates, and sic_code_output to find the SIC codes. It outputs report_data_v2.json, which has a key of each SIC code - date pair (ex. 140_2Q2023), and then it has all the corresponding reports in that SIC code and quarter.
3) Use database_loader.py to populate a sql database called reports_v5.db for each report_id listed in report_data_v2.json.
4) Use aggregator.py to calculate the desired industry ratios based off the database and return an excel file with the ratios
5) use sic_code_to_excel to generate to the ticker_sic ref sheet for the combined.xlsx
