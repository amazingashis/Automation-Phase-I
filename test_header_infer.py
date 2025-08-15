# test_header_infer.py
# Test script to test header inference function separately

import pandas as pd
import requests

def lmstudio_llm(prompt):
    """LLM function for LM Studio API"""
    url = "http://127.0.0.1:1234/v1/chat/completions"
    headers = {"Content-Type": "application/json"}
    data = {
        "model": "google/gemma-3n-e4b",
        "messages": [
            {"role": "system", "content": "You are a helpful AI assistant for data engineering and you are expert in US Health care domain."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 100,
        "temperature": 0
    }
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]

def test_header_inference(csv_path, nrows=10):
    """
    Simple test function to infer headers from top 10 rows using LLM
    """
    # Read top 10 rows without header
    df = pd.read_csv(csv_path, nrows=nrows, header=None)
    sample_rows = df.values.tolist()
    
    print(f"Sample data (top {nrows} rows):")
    for i, row in enumerate(sample_rows):
        print(f"Row {i+1}: {row}")
    
    # Get user input about data type
    print("\nPlease provide a brief description of what type of data this is:")
    print("(e.g., 'healthcare enrollment', 'employee payroll', 'customer orders', 'sales data', etc.)")
    data_type_desc = input("Data type/description: ")
    
    # Simple prompt to LLM with user context
    prompt = f"""
Given the following {nrows} rows of data from a CSV file containing {data_type_desc}, infer the most likely column headers.
Data rows:
{sample_rows}

Return only a comma-separated list of column names that best describe each column based on the data type: {data_type_desc}
"""
    
    print("\n[Sending to LLM...]")
    print("Prompt:", prompt)
    
    try:
        llm_response = lmstudio_llm(prompt)
        print("\n[LLM Response]:")
        print(llm_response)
        
        # Parse the response
        cleaned = llm_response.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.lstrip("`")
            if "```" in cleaned:
                cleaned = cleaned.split("```")[0]
        
        # Take first line and split by comma
        cleaned = cleaned.splitlines()[0]
        headers = [col.strip() for col in cleaned.split(",") if col.strip()]
        
        print("\n[Parsed Headers]:")
        print(headers)
        
        return headers
        
    except Exception as e:
        print(f"\n[Error]: {e}")
        return None

if __name__ == "__main__":
    csv_path = input("Enter path to CSV file: ")
    headers = test_header_inference(csv_path, nrows=10)
    if headers:
        print(f"\n[Final Result]: {headers}")
    else:
        print("\n[Failed to infer headers]")
