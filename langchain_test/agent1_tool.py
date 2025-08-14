# agent1_tool.py
# LangChain Tool for inferring schema from CSV using LLM (LM Studio)

import pandas as pd
import requests
from langchain.tools import tool

@tool
def infer_schema_from_csv(csv_path: str) -> str:
    """
    Reads the top 10 rows of a CSV and infers schema using an LLM hosted at LM Studio.
    Returns the inferred schema as a JSON string.
    """
    df = pd.read_csv(csv_path, nrows=10)
    fields = []
    for col in df.columns:
        values = df[col].dropna().tolist()[:10]
        prompt = f"""
Given the following column name and sample values, infer the most likely data type (integer, float, boolean, string, date, etc.).\nColumn: {col}\nSample values: {values}\nRespond with only the data type name.
"""
        url = "http://127.0.0.1:1234/v1/chat/completions"
        headers = {"Content-Type": "application/json"}
        data = {
            "model": "llama-3.2-3b-instruct",
            "messages": [
                {"role": "system", "content": "You are a helpful AI assistant for data engineering."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 16,
            "temperature": 0
        }
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        dtype = response.json()["choices"][0]["message"]["content"].strip().lower()
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
    import json
    return json.dumps(schema, indent=4)
