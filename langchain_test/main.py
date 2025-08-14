# main.py
# LangChain agentic workflow using tools for schema inference and PySpark script generation

from langchain.agents import initialize_agent, Tool
from langchain.agents import AgentType
from langchain_openai import ChatOpenAI
from agent1_tool import infer_schema_from_csv
from agent2_tool import generate_pyspark_import

if __name__ == "__main__":
    csv_path = input("Enter path to CSV file: ")
    # Define tools
    tools = [
        Tool(
            name="Infer CSV Schema",
            func=infer_schema_from_csv,
            description="Infers schema from a CSV file and exports as JSON string."
        ),
        Tool(
            name="Generate Databricks Import Script",
            func=generate_pyspark_import,
            description="Generates a PySpark import script for Databricks using the approved schema."
        )
    ]

    # Connect to LM Studio LLM (OpenAI-compatible endpoint)
    llm = ChatOpenAI(
        openai_api_base="http://127.0.0.1:1234/v1",
        openai_api_key="lm-studio",  # Any string works for LM Studio
        model="meta-llama-3.1-8b-instruct"
    )

    # Initialize agent (function-calling agent)
    agent = initialize_agent(
        tools,
        llm=llm,
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True
    )

    # Step 1: Check if CSV has headers
    import pandas as pd
    try:
        df_sample = pd.read_csv(csv_path, nrows=10)
        has_header = True
    except pd.errors.ParserError:
        # Try reading without header
        df_sample = pd.read_csv(csv_path, nrows=10, header=None)
        has_header = False
    except Exception as e:
        print(f"Error reading CSV: {e}")
        exit(1)

    if has_header:
        # Data has headers: infer schema and ask user to validate
        schema_json = infer_schema_from_csv.run(csv_path)
        print("\nInferred Schema:\n", schema_json)
        approve = input("Approve schema? (y/n): ")
        if approve.lower() == 'y':
            # Step 3: Generate PySpark script
            script = generate_pyspark_import.run({"csv_path": csv_path, "schema_json": schema_json})
            print("\nGenerated PySpark Script:\n", script)
        else:
            print("Schema not approved. Exiting.")
    else:
        # Data has no headers: infer headers and types, ask user to validate
        data_sample = df_sample.to_csv(index=False, header=False)
        header_type_prompt = f"""
You are a data engineering assistant. The following is a sample of a CSV file without headers. Infer the most likely column headers and their data types. Return your answer as a JSON array of objects with 'name' and 'type' fields, e.g. [{{"name": "col1", "type": "integer"}}, ...].

Sample Data (CSV):
{data_sample}
"""
        header_type_response = llm.invoke(header_type_prompt)
        print("\nInferred Headers and Types:")
        print(header_type_response.content)
        approve = input("Approve inferred headers and types? (y/n): ")
        if approve.lower() == 'y':
            # Use inferred headers for schema inference
            inferred_headers = [col['name'] for col in eval(header_type_response.content)]
            df_sample.columns = inferred_headers
            temp_csv_path = "temp_with_headers.csv"
            df_sample.to_csv(temp_csv_path, index=False)
            schema_json = infer_schema_from_csv.run(temp_csv_path)
            print("\nInferred Schema:\n", schema_json)
            approve_schema = input("Approve schema? (y/n): ")
            if approve_schema.lower() == 'y':
                script = generate_pyspark_import.run({"csv_path": temp_csv_path, "schema_json": schema_json})
                print("\nGenerated PySpark Script:\n", script)
            else:
                print("Schema not approved. Exiting.")
        else:
            print("Headers and types not approved. Exiting.")
