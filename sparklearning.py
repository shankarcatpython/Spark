from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import time 

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read Schema of text file") \
    .master("local[2]") \
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
schema_df = spark.read.option("header", "true").option("delimiter", ";").schema(schema).csv(file_path)

# Print the schema
schema_df.printSchema()

time.sleep(30)

# Stop the SparkSession
spark.stop()
