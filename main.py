# Agentic AI Phase I - Llama 3, LangChain Orchestration

# This is a high-level orchestrator for the agentic workflow using LangChain and Llama 3.
# It coordinates Agent 1 (schema inference) and Agent 2 (import script generation),
# and interacts with the user for schema approval.


# Agentic AI Phase I - Llama 3, LM Studio Orchestration
# This script coordinates Agent 1 (schema inference) and Agent 2 (import script generation),
# and interacts with the user for schema approval, using LM Studio's OpenAI-compatible API.

import os
import subprocess
import requests

def infer_schema_tool(csv_path: str, output_json_path: str = "schema.json"):
    result = subprocess.run([
        "python", os.path.join("agents", "agent1.py"), csv_path, "--output", output_json_path
    ], capture_output=True, text=True)
    return result.stdout

def generate_import_script_tool(csv_path: str, schema_json_path: str, output_script_path: str = "import_script.py"):
    result = subprocess.run([
        "python", os.path.join("agents", "agent2.py"), csv_path, schema_json_path, "--output", output_script_path
    ], capture_output=True, text=True)
    return result.stdout

# If you want to call the LLM directly from main.py, use this function:
def lmstudio_llm(prompt):
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
    return response.json()["choices"][0]["message"]["content"]

if __name__ == "__main__":
    csv_path = input("Enter path to CSV file: ")
    print("Inferring schema...")
    schema_json = infer_schema_tool(csv_path)
    print("\nInferred Schema:\n", schema_json)
    approve = input("Approve schema? (y/n): ")
    if approve.lower() == 'y':
        print("Generating Databricks import script...")
        script = generate_import_script_tool(csv_path, "schema.json")
        print("\nGenerated PySpark Script:\n", script)
    else:
        print("Schema not approved. Exiting.")
