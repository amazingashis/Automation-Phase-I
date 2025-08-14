# infer_schema.py
# (Renamed from agent1.py)

import pandas as pd
import json
import os

def infer_dtype_llm(col_name, values, llm=None):
    if llm is None:
        dtype = pd.Series(values).dtype
        if pd.api.types.is_integer_dtype(dtype):
            return "integer"
        elif pd.api.types.is_float_dtype(dtype):
            return "float"
        elif pd.api.types.is_bool_dtype(dtype):
            return "boolean"
        else:
            return "string"
    prompt = f"""
Given the following column name and sample values, infer the most likely data type (integer, float, boolean, string, date, etc.).\nColumn: {col_name}\nSample values: {values}\nRespond with only the data type name.
"""
    result = llm(prompt)
    return result.strip().lower()

def infer_schema_from_csv(csv_path, output_json_path=None, llm=None):
    df = pd.read_csv(csv_path, nrows=10)
    fields = []
    for col in df.columns:
        values = df[col].dropna().tolist()
        sample_values = values[:10] if len(values) > 10 else values
        dtype = infer_dtype_llm(col, sample_values, llm=llm)
        nullable = df[col].isnull().any()
        fields.append({
            "name": col,
            "type": dtype,
            "nullable": bool(nullable),
            "metadata": {}
        })
    schema = {
        "type": "struct",
        "fields": fields
    }
    if output_json_path:
        with open(output_json_path, 'w') as f:
            json.dump(schema, f, indent=4)
    return schema

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Infer schema from CSV file.")
    parser.add_argument("csv_path", help="Path to the CSV file.")
    parser.add_argument("--output", help="Path to output JSON schema file.")
    args = parser.parse_args()
    schema = infer_schema_from_csv(args.csv_path, args.output)
    print(json.dumps(schema, indent=4))
