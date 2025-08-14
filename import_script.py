
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, BooleanType, StringType, DateType

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    StructField('id', IntegerType(), False),
    StructField('first_name', StringType(), False),
    StructField('last_name', StringType(), False),
    StructField('email', StringType(), False),
    StructField('gender', StringType(), False),
    StructField('plan', StringType(), False),
    StructField('effective_date', DateType(), False),
    StructField('termination_date', DateType(), False),
    StructField('record_type', StringType(), False),
    StructField('state', StringType(), False),
    StructField('address 1', StringType(), False),
    StructField('address 2', StringType(), True),
    StructField('medical_record_number', StringType(), True),
    StructField('division name', StringType(), False)
])
df = spark.read.csv('source_files\member file csv.csv', header=True, schema=schema)
df.show(5)
