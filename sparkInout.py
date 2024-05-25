
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Text File Conversion") \
    .getOrCreate()

# Define the file path of the text file
text_file_path = "nutritionData.csv"

# Read the text file into a DataFrame
df = spark.read.csv(text_file_path)

# Define the output paths for different formats with "data_" prefix
parquet_output_path = "data_output_parquet"
avro_output_path = "data_output_avro"
orc_output_path = "data_output_orc"
csv_output_path = "data_output_csv"
json_output_path = "data_output_json"

# Write the DataFrame into Parquet format
df.write.parquet(parquet_output_path)

# Write the DataFrame into Avro format
#df.write.format("avro").save(avro_output_path)

# Write the DataFrame into ORC format
df.write.orc(orc_output_path)

# Write the DataFrame into CSV format
df.write.csv(csv_output_path)

# Write the DataFrame into JSON format
df.write.json(json_output_path)

# Stop SparkSession
spark.stop()
