# import_schema_generator.py
# (Renamed from agent2.py)

import json
import os

def generate_pyspark_import(csv_path, schema_json_path, output_script_path=None):
    with open(schema_json_path, 'r') as f:
        schema = json.load(f)
    fields = schema['fields']
    spark_schema_fields = []
    for field in fields:
        name = field['name']
        dtype = field['type']
        nullable = field['nullable']
        spark_type = {
            'integer': 'IntegerType()',
            'float': 'FloatType()',
            'boolean': 'BooleanType()',
            'string': 'StringType()',
            'date': 'DateType()'
        }.get(dtype, 'StringType()')
        spark_schema_fields.append(f"StructField('{name}', {spark_type}, {str(nullable)})")
    spark_schema = ",\n    ".join(spark_schema_fields)
    script = f"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, BooleanType, StringType, DateType

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    {spark_schema}
])
df = spark.read.csv('{csv_path}', header=True, schema=schema)
df.show(5)
"""
    if output_script_path:
        with open(output_script_path, 'w') as f:
            f.write(script)
    return script

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate PySpark import script for Databricks.")
    parser.add_argument("csv_path", help="Path to the CSV file.")
    parser.add_argument("schema_json_path", help="Path to the schema JSON file.")
    parser.add_argument("--output", help="Path to output PySpark script file.")
    args = parser.parse_args()
    script = generate_pyspark_import(args.csv_path, args.schema_json_path, args.output)
    print(script)
