# Agent 1: CSV Schema Inference
# Reads a CSV file, infers headers and data types, and exports schema as JSON.

import pandas as pd
import json
import os


# Use LLM to infer data type from sample values
def infer_dtype_llm(col_name, values, llm=None):
    """
    Infers the data type of a column using an LLM, given the column name and sample values.
    """
    if llm is None:
        # fallback: use pandas dtype as before
        import pandas as pd
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
Given the following column name and sample values, infer the most likely data type (integer, float, boolean, string, date, etc.).
Column: {col_name}
Sample values: {values}
Respond with only the data type name.
"""
    result = llm(prompt)
    return result.strip().lower()


def infer_schema_from_csv(csv_path, output_json_path=None, llm=None):
    df = pd.read_csv(csv_path, nrows=10)  # Only top 10 rows for LLM
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
    parser = argparse.ArgumentParser(description="Infer schema from CSV file using LLM for data types.")
    parser.add_argument("csv_path", help="Path to the CSV file.")
    parser.add_argument("--output", help="Path to output JSON schema file.")
    args = parser.parse_args()

    # Use LM Studio's OpenAI-compatible API for Gemma model inference
    import requests

    def lmstudio_llm(prompt):
        url = "http://127.0.0.1:1234/v1/chat/completions"
        headers = {"Content-Type": "application/json"}
        data = {
            "model": "gemma-3n-E4B-it-Q4_K_M",
            "messages": [
                {"role": "system", "content": "You are a helpful AI assistant for data engineering."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 16,
            "temperature": 0
        }
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]

    schema = infer_schema_from_csv(args.csv_path, args.output, llm=lmstudio_llm)
    print(json.dumps(schema, indent=4))
