# agent_header_check.py
# Agent to check if a CSV file has headers using the top 5 rows.
# If header is present, pass to agent1. If not, use LLM to infer column names and pass to agent1.

import pandas as pd
import os
import json

def check_header_and_infer(csv_path, llm=None, output_json_path=None):
    # Read top 8 rows and ask LLM if it has headers
    df_sample = pd.read_csv(csv_path, nrows=8, header=None)
    sample_rows = df_sample.values.tolist()
    prompt = f"""
You are a data expert in healthcare enrollment data. Here are the first 8 rows of a CSV file (each row is a list of values):
{sample_rows}
Does this file have a header row? Respond with only 'header' if the first row is a header, or 'no_header' if it does not.
"""
    if llm is not None:
        header_decision = llm(prompt).strip().lower()
    else:
        header_decision = 'header'  # fallback
    if header_decision == 'header':
        print("[Agent: Header Check] LLM decided: File HAS header row. Proceeding to schema inference without user approval.")
        from agents.infer_schema import infer_schema_from_csv
        return infer_schema_from_csv(csv_path, output_json_path=output_json_path, llm=llm), False
    else:
        print("[Agent: Header Check] LLM decided: File has NO header row. Inferring column names...")
        # Use header_infer agent to infer column names
        from agents.header_infer import infer_header_from_samples
        col_names = infer_header_from_samples(csv_path, llm=llm, nrows=10)
        df_no_header = pd.read_csv(csv_path, header=None)
        df_no_header.columns = col_names
        temp_path = csv_path + ".headered.csv"
        df_no_header.to_csv(temp_path, index=False)
        from agents.infer_schema import infer_schema_from_csv
        return infer_schema_from_csv(temp_path, output_json_path=output_json_path, llm=llm), True

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Check if CSV has header and infer schema.")
    parser.add_argument("csv_path", help="Path to the CSV file.")
    parser.add_argument("--output", help="Path to output JSON schema file.")
    args = parser.parse_args()
    # Example: import and initialize LLM here if needed
    llm = None
    schema = check_header_and_infer(args.csv_path, llm=llm, output_json_path=args.output)
    print(json.dumps(schema, indent=4))
