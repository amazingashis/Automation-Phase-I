# header_infer.py
# Agent to infer column names for a CSV file with no header using LLM and sample data.

import pandas as pd
import os
import json

def infer_header_from_samples(csv_path, llm=None, nrows=5):
    df = pd.read_csv(csv_path, nrows=nrows, header=None)
    sample_rows = df.values.tolist()
    # Ask user for a brief description of the data type/content
    print("No headers detected. Please provide a brief description of the data type or content in this file (e.g., 'healthcare enrollment', 'employee payroll', 'customer orders', etc.):")
    data_type_desc = input("Data type/description: ")
    prompt = f"""
You are a data expert. The user says this file contains: {data_type_desc}
Given the following sample rows from a CSV file with NO header, infer the most likely column names for each column. Return a Python list of column names, in order, that best matches the described data type.
Sample rows:
{sample_rows}
Respond with only a Python list of column names.
"""
    if llm is not None:
        col_names_str = llm(prompt)
        print("\n[LLM raw output for header inference]:\n", col_names_str)
        # Remove code block markers if present
        if col_names_str.strip().startswith('```'):
            col_names_str = col_names_str.strip().lstrip('`')
            # Remove language if present (e.g., python)
            if col_names_str.lower().startswith('python'):
                col_names_str = col_names_str[6:].lstrip('\n')
            # Remove trailing code block
            if '```' in col_names_str:
                col_names_str = col_names_str.split('```')[0]
        try:
            col_names = eval(col_names_str)
            if not isinstance(col_names, list):
                raise ValueError("LLM did not return a list of column names.")
        except Exception as e:
            print("\n[Warning] LLM did not return a valid list of column names.")
            print("LLM output:", col_names_str)
            print("Error:", e)
            print("\nPlease enter the column names manually, separated by commas (e.g. id,first_name,last_name):")
            user_input = input("Column names: ")
            col_names = [col.strip() for col in user_input.split(",") if col.strip()]
        return col_names
    else:
        raise RuntimeError("LLM function must be provided to infer headers.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Infer header from sample data using LLM.")
    parser.add_argument("csv_path", help="Path to the CSV file.")
    parser.add_argument("--nrows", type=int, default=10, help="Number of rows to sample.")
    args = parser.parse_args()
    llm = None
    headers = infer_header_from_samples(args.csv_path, llm=llm, nrows=args.nrows)
    print(headers)
