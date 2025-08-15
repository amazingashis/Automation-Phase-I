
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, BooleanType, StringType, DateType

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    StructField('MemberID', IntegerType(), False),
    StructField('Name', StringType(), False),
    StructField('FamilyName', StringType(), False),
    StructField('Email', StringType(), False),
    StructField('Gender', StringType(), False),
    StructField('PlanType', StringType(), False),
    StructField('EnrollmentStartDate', DateType(), False),
    StructField('TerminationDate', DateType(), False),
    StructField('Benefits', StringType(), False),
    StructField('State', StringType(), False),
    StructField('AddressLine1', StringType(), False),
    StructField('AddressLine2', StringType(), True),
    StructField('MemberID.1', StringType(), True),
    StructField('InsuranceType', StringType(), False)
])
df = spark.read.csv('source_files\member file csv no header.csv', header=True, schema=schema)
df.show(5)
