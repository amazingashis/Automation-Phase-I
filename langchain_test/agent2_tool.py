# agent2_tool.py
# LangChain Tool for generating PySpark import script for Databricks

import json
from langchain.tools import tool

@tool
def generate_pyspark_import(csv_path: str, schema_json: str) -> str:
    """
    Generates a PySpark import script for Databricks using the provided schema JSON string.
    """
    schema = json.loads(schema_json)
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
    return script
