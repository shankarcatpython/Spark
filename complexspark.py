from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

import time 
from pyspark.sql.functions import col,hour,expr,concat, col, lit , when , to_timestamp
from pyspark.sql import functions as F


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

# Clean the "Date" column to ensure it's in the format "dd/MM/yyyy"
consumption_df = consumption_df.withColumn("Date", when(col("Date").isNull(), None).otherwise(col("Date"))) \
    .withColumn("Date", when(col("Date").isNull() | (col("Date") == "null"), None).otherwise(col("Date"))) \
    .withColumn("Date", when(col("Date").isNull(), None).otherwise(
        to_timestamp(col("Date"), "d/M/y")
    )).drop("Date")

# Clean the "Time" column to ensure it's in the format "HH:mm:ss"
consumption_df = consumption_df.withColumn("Time", when(col("Time").isNull(), None).otherwise(col("Time"))) \
    .withColumn("Time", when(col("Time").isNull() | (col("Time") == "null"), None).otherwise(col("Time"))) \
    .withColumn("Time", when(col("Time").isNull(), None).otherwise(
        to_timestamp(col("Time"), "H:mm:ss")
    )).drop("Time")


# Define complex transformation
total_power_yearly = consumption_df \
    .withColumn("Total_power_consumption", F.expr("Global_active_power + Global_reactive_power + Sub_metering_1 + Sub_metering_2 + Sub_metering_3")) \
    .withColumn("Time")\
    .groupBy("Year") \
    .agg(F.sum("Total_power_consumption").alias("Total_power_yearly"))

# Show the result
total_power_yearly.explain(extended=True)
total_power_yearly.show()

# Stop the SparkSession
spark.stop()