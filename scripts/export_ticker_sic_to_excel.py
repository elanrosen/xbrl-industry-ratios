import pandas as pd
import json
import sys

def sic_code_to_excel(input_json_file, output_excel_file):
    # Load JSON data from file
    with open(input_json_file, 'r') as f:
        data = json.load(f)

    # Convert data to DataFrame
    df = pd.DataFrame(list(data.items()), columns=['Ticker', 'SIC'])

    # Extract first 3 digits of SIC code
    df['SIC'] = df['SIC'].astype(str).str[:3]

    # Save DataFrame to Excel file
    df.to_excel(output_excel_file, index=False)

    print(f"Excel file '{output_excel_file}' has been created successfully.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python sic_code_to_excel.py <input_json_file> <output_excel_file>")
        sys.exit(1)

    input_json_file = sys.argv[1]
    output_excel_file = sys.argv[2]

    sic_code_to_excel(input_json_file, output_excel_file)
