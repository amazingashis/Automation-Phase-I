# main.py
# LangChain agentic workflow using tools for schema inference and PySpark script generation

from langchain.agents import initialize_agent, Tool
from langchain.agents import AgentType
from langchain.tools import load_tools
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

    # Initialize agent (function-calling agent)
    agent = initialize_agent(
        tools,
        llm=None,  # No LLM for orchestration, tools use LLM internally
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True
    )

    # Step 1: Infer schema
    schema_json = infer_schema_from_csv.run(csv_path)
    print("\nInferred Schema:\n", schema_json)
    approve = input("Approve schema? (y/n): ")
    if approve.lower() == 'y':
        # Step 2: Generate PySpark script
        script = generate_pyspark_import.run(csv_path, schema_json)
        print("\nGenerated PySpark Script:\n", script)
    else:
        print("Schema not approved. Exiting.")
