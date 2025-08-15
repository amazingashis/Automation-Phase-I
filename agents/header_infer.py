import pandas as pd

def infer_header_from_samples(csv_path, llm=None, nrows=10):
    """
    Infers column headers for a CSV file with no header using LLM and user-provided data type description.
    Prints the LLM's feedback for transparency.
    """
    # Read top nrows without header
    df = pd.read_csv(csv_path, nrows=nrows, header=None)
    sample_rows = df.values.tolist()

    print(f"Sample data (top {nrows} rows):")
    for i, row in enumerate(sample_rows):
        print(f"Row {i+1}: {row}")

    # Ask user for a brief description of the data type/content
    print("\nPlease provide a brief description of what type of data this is:")
    print("(e.g., 'healthcare enrollment', 'employee payroll', 'customer orders', 'sales data', etc.)")
    data_type_desc = input("Data type/description: ")

    # Prepare prompt for LLM
    num_columns = df.shape[1]
    prompt = f"""
Given the following {nrows} rows of data from a CSV file containing {data_type_desc}, infer the most likely column headers.
Data rows:
{sample_rows}

This file has exactly {num_columns} columns. Return exactly {num_columns} comma-separated column names that best describe each column based on the data type: {data_type_desc}
"""

    if llm is not None:
        llm_response = llm(prompt)
        print("\n[LLM feedback for header inference]:\n", llm_response)
        # Attempt to extract a comma-separated list from the LLM response
        try:
            cleaned = llm_response.strip()
            # Remove code block markers if present
            if cleaned.startswith("```"):
                cleaned = cleaned.lstrip("`")
                if cleaned.lower().startswith("python"):
                    cleaned = cleaned[6:].lstrip('\n')
                if "```" in cleaned:
                    cleaned = cleaned.split("```")[0]
            # Take only the first line if LLM returns extra text
            cleaned = cleaned.splitlines()[0]
            col_names = [col.strip() for col in cleaned.split(",") if col.strip()]
            if not col_names:
                raise ValueError("LLM did not return any column names.")
            if len(col_names) != df.shape[1]:
                raise ValueError(f"LLM returned {len(col_names)} column names, but file has {df.shape[1]} columns.")
        except Exception as e:
            print("\n[Warning] LLM did not return a valid comma-separated list of column names.")
            print("LLM output:", llm_response)
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
    llm = None  # Replace with your LLM function if running standalone
    headers = infer_header_from_samples(args.csv_path, llm=llm, nrows=args.nrows)
    print(headers)