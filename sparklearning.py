from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import time 
from pyspark.sql.functions import col,hour


# Set number of partitions
num_partitions = 5

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HouseholdConsumptionAnalysis") \
    .master("local[2]") \
    .config("spark.sql.shuffle.partitions", "5") \
    .getOrCreate()


# File path
file_path = "household_power_consumption.txt"

# Define the schema
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Global_active_power", DoubleType(), True),
    StructField("Global_reactive_power", DoubleType(), True),
    StructField("Voltage", DoubleType(), True),
    StructField("Global_intensity", DoubleType(), True),
    StructField("Sub_metering_1", DoubleType(), True),
    StructField("Sub_metering_2", DoubleType(), True),
    StructField("Sub_metering_3", DoubleType(), True)
])

# Read the TXT file with the defined schema
consumption_df = spark.read.option("header", "true").option("delimiter", ";").schema(schema).csv(file_path)
select_date_time_df = consumption_df.select("Date", "Time").filter(col("Date").substr(6, 2) == '01')


# Print logical plan
print("Logical Plan for consumption dataframe : --> ")
consumption_df.explain(extended=True)
print("Logical Plan for Date and time dataframe : --> ")
select_date_time_df.explain(extended=True)
print("Number of rows for consumption dataframe:", consumption_df.count())
print("Number of rows for date and time dataframe:", select_date_time_df.count())

# Stop the SparkSession
spark.stop()