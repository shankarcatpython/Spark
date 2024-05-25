from pyspark.sql import SparkSession

import logging
from logging.handlers import RotatingFileHandler

# Set up logging to a file
log_file = "spark_application.log"  # Specify the log file path

# Configure the root logger
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(log_file, maxBytes=1000000, backupCount=10)  # Adjust maxBytes and backupCount as needed
    ]
)


# Create or get the active SparkSession with Hive support Enabled
spark = SparkSession.builder \
    .appName("SampleApp") \
    .enableHiveSupport() \
    .getOrCreate()

# Set configurations
# spark.conf.set("spark.sql.catalogImplementation", "hive")
print("Hive Support Enabled:", spark)

# Create a new session
new_session = SparkSession.builder.getOrCreate()
print("New Session:", new_session)

# Get the active SparkSession
active_session = SparkSession.getActiveSession()
print("Active SparkSession (via getActiveSession):", active_session)

# Create a DataFrame
data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
df = spark.createDataFrame(data, ["id", "name"])
print("DataFrame:", df.show())

# Generate a DataFrame with values from 0 to 99
df_range = spark.range(100)
print("DataFrame Range:", df_range.show())

# Read data from a file
df_read = spark.read.csv("sparkInput.csv", header=False)
print("DataFrame from File Read:", df_read.show())

# Read data from a stream
#df_read_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "host1:port1,host2:port2").load()
#print("DataFrame from Stream Read:", df_read_stream)

# Execute SQL queries
#new_session.sql("SELECT * FROM table_name").show()


print("SparkSession Stopped.")

# Access the catalog
catalog = spark.catalog
print("Catalog:", catalog)

# Access the version of Spark
spark_version = spark.version
print("Spark Version:", spark_version)

# Access streaming queries
#streams = spark.streams
#print("Streams:", streams)

# Access tables
#tables = spark.table("table_name")
#print("Tables:", tables)

# Define User Defined Functions (UDFs)
def my_udf(value):
    return value.upper()

spark.udf.register("my_udf", my_udf)
print("UDF Registered.")

# Access configuration settings
conf_settings = spark.sparkContext.getConf().getAll()

# Retrieve all Spark configuration settings
for setting in conf_settings:
    settings_final= setting
    for values in settings_final:
        print("Configuration Settings:", values)

# Access master URL
master_url = spark.sparkContext.master
print("Master URL:", master_url)


# Access the SparkSession's configuration
conf = spark.conf
print("SparkSession Configuration:", conf)

# Access the SparkSession's streams attribute
# session_streams = spark.streams
# print("SparkSession Streams:", session_streams)

# Access the SparkSession's SQL context
sql_context = spark.sql
print("SparkSession SQL Context:", sql_context)

# Access the SparkSession's udf attribute
session_udf = spark.udf
print("SparkSession UDF:", session_udf)

# Access the SparkSession's udtf attribute
session_udtf = spark.udtf
print("SparkSession UDTF:", session_udtf)

# Access the SparkSession's active attribute
active_spark_session = SparkSession.active
print("Active SparkSession (via active attribute):", active_spark_session)

'''

# Sample data to create DataFrame
data = [("John", 30), ("Alice", 25), ("Bob", 35)]

# Define schema for DataFrame
schema = ["name", "age"]

# Create DataFrame from sample data and schema
df = spark.createDataFrame(data, schema)

# Write DataFrame to a table in the default database (typically 'default')
table_name = "person_table"
df.write.saveAsTable(table_name)

# Select data from the table
result = spark.sql(f"SELECT * FROM {table_name}")

# Show the result
result.show()

'''

# Stop the SparkSession
spark.stop()
