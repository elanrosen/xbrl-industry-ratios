import subprocess
import os

# Define the steps as a list of dictionaries for clarity and easy updates.
steps = [
    {"script": "populate_sic_codes_from_tickers.py", "args": ["data/input/tickers.csv", "data/output/ticker_to_sic_code_mapping.json"]},
    {"script": "find_reports_by_sic_and_date.py", "args": ["data/input/tickers.csv", "data/output/ticker_to_sic_code_mapping.json", "data/output/reports_by_sic_and_date_mapping.json"]},
    {"script": "load_reports_into_database.py", "args": ["data/output/reports_by_sic_and_date_mapping.json", "data/output/industry_reports_database.db"]},
    {"script": "calculate_industry_ratios_and_export.py", "args": ["data/output/industry_reports_database.db", "data/output/industry_ratios_analysis.xlsx"]},
    {"script": "export_ticker_sic_to_excel.py", "args": ["data/output/ticker_to_sic_code_mapping.json", "data/output/reference/ticker_sic_reference.xlsx"]}
]

progress_file = "progress/script_progress.txt"

def run_script(script_name, args):
    """
    Runs a Python script with the given arguments.

    :param script_name: Name of the script to run.
    :param args: A list of command-line arguments to pass to the script.
    """
    subprocess.run(["python", script_name] + args, check=True)

def save_progress(step_index):
    """
    Saves the current step index to a file.

    :param step_index: The index of the current step.
    """
    with open(progress_file, "w") as file:
        file.write(str(step_index))

def load_progress():
    """
    Loads the current step index from a file.

    :return: The index of the last completed step, or -1 if the progress file does not exist.
    """
    try:
        with open(progress_file, "r") as file:
            return int(file.read())
    except FileNotFoundError:
        return -1

def main():
    # Load progress, if any
    last_completed_step = load_progress()

    for i, step in enumerate(steps):
        if i <= last_completed_step:
            # Skip this step as it has already been completed
            continue
        
        print(f"Running step {i+1}/{len(steps)}: {step['script']}")
        run_script(os.path.join("scripts", step['script']), step['args'])
        
        # Save progress after successful completion of the step
        save_progress(i)

    # Cleanup progress file after all steps are completed
    try:
        os.remove(progress_file)
    except OSError:
        pass

if __name__ == "__main__":
    main()
