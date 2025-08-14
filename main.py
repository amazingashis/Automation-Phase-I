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
import json

def infer_schema_tool(csv_path: str, output_json_path: str = "schema.json"):
    result = subprocess.run([
        "python", os.path.join("agents", "infer_schema.py"), csv_path, "--output", output_json_path
    ], capture_output=True, text=True)
    return result.stdout

def generate_import_script_tool(csv_path: str, schema_json_path: str, output_script_path: str = "import_script.py"):
    result = subprocess.run([
        "python", os.path.join("agents", "import_schema_generator.py"), csv_path, schema_json_path, "--output", output_script_path
    ], capture_output=True, text=True)
    return result.stdout

# If you want to call the LLM directly from main.py, use this function:
def lmstudio_llm(prompt):
    url = "http://127.0.0.1:1234/v1/chat/completions"
    headers = {"Content-Type": "application/json"}
    data = {
        "model": "google/gemma-3n-e4b",
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

    print("[Agent: Header Check] Checking if file has header using LLM...")
    # Call agent_header_check and checkpoint with user for header validation
    import importlib.util
    agent_header_check_path = os.path.join("agents", "agent_header_check.py")
    spec = importlib.util.spec_from_file_location("agent_header_check", agent_header_check_path)
    agent_header_check = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(agent_header_check)

    # Use lmstudio_llm as the LLM function
    def llm_func(prompt):
        return lmstudio_llm(prompt)

    print("[Agent: Header Check] Running header check...")

    schema = None
    header_inferred = False
    # Patch agent_header_check to print update when header_infer is called
    import builtins
    orig_import = builtins.__import__
    def custom_import(name, *args, **kwargs):
        if name == "agents.header_infer":
            print("[Agent: Header Infer] Inferring column names using LLM...")
        return orig_import(name, *args, **kwargs)
    builtins.__import__ = custom_import
    try:
        # Modify agent_header_check to return (schema, header_inferred) tuple
        result = agent_header_check.check_header_and_infer(csv_path, llm=llm_func)
        if isinstance(result, tuple):
            schema, header_inferred = result
        else:
            schema = result
            header_inferred = False
    finally:
        builtins.__import__ = orig_import

    headers = [field['name'] for field in schema['fields']]
    if header_inferred:
        print("\n[Checkpoint] Inferred Headers:", headers)
        approve_headers = input("Are these headers correct? (y/n): ")
        if approve_headers.lower() != 'y':
            print("Header validation failed. Exiting.")
            exit(0)

    print("[Agent 1] Inferring schema...")
    # Save schema to JSON for downstream use
    output_json_path = "schema.json"
    with open(output_json_path, 'w') as f:
        json.dump(schema, f, indent=4)
    print("\nInferred Schema:\n", json.dumps(schema, indent=4))
    approve = input("Approve schema? (y/n): ")
    if approve.lower() == 'y':
        print("[Agent 2] Generating Databricks import script...")
        script = generate_import_script_tool(csv_path, output_json_path)
        print("\nGenerated PySpark Script:\n", script)
    else:
        print("Schema not approved. Exiting.")
