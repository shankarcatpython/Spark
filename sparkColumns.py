from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit , when
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Column Functions Demo") \
    .getOrCreate()

# Load data from CSV
df = spark.read.csv("nutritionData.csv", header=True, inferSchema=True).dropna()

# Print original DataFrame
print("Original DataFrame:")
df.show()
print('-' * 105)

# Define a sample column
sample_col = col("Calories")

# pyspark.sql.Column.alias
# Rename the column
alias_col = sample_col.alias("new_alias")
print("DataFrame after applying alias:")
df.withColumn("new_alias", alias_col).show()
print('-' * 105)

# pyspark.sql.Column.asc
# Sort the values in the column in ascending order
asc_col = sample_col.asc()
print("DataFrame after sorting in ascending order:")
df.orderBy(asc_col).show()
print('-' * 105)

# pyspark.sql.Column.asc_nulls_first
# Sort the values in ascending order, with null values coming first
asc_nulls_first_col = sample_col.asc_nulls_first()
print("DataFrame after sorting in ascending order with nulls first:")
df.orderBy(asc_nulls_first_col).show()
print('-' * 105)

# pyspark.sql.Column.asc_nulls_last
# Sort the values in ascending order, with null values coming last
asc_nulls_last_col = sample_col.asc_nulls_last()
print("DataFrame after sorting in ascending order with nulls last:")
df.orderBy(asc_nulls_last_col).show()
print('-' * 105)

# pyspark.sql.Column.astype
# Change the data type of the column
astype_col = sample_col.astype("int")
print("DataFrame after type casting to integer:")
df.withColumn("example_column", astype_col).show()
print('-' * 105)

# pyspark.sql.Column.between
# Check if the values in the column fall within a specified range
between_col = sample_col.between(1, 10)
print("DataFrame after filtering values between 1 and 10:")
df.filter(between_col).show()
print('-' * 105)

# pyspark.sql.Column.bitwiseAND
# Perform bitwise AND operation on the values in the column
bitwise_and_col = sample_col.bitwiseAND(5)
# Add more operations and print statements for the remaining functions

# Sample DataFrame with 'Calories' column
data = [(100,), (200,), (300,), (400,), (500,)]
calories_df = spark.createDataFrame(data, ["Calories"])

# Function examples

'''
# bitwiseAND: Performs bitwise AND operation with the specified value.
calories_df.select(expr("bitwiseAND(Calories, 100)").alias("result")).show()
print('-' * 105)

# bitwiseOR: Performs bitwise OR operation with the specified value.
calories_df.select(expr("bitwiseOR(Calories, 100)").alias("result")).show()
print('-' * 105)

# bitwiseXOR: Performs bitwise XOR operation with the specified value.
calories_df.select(expr("bitwiseXOR(Calories, 100)").alias("result")).show()
print('-' * 105)

'''

# cast: Casts the column to a different data type.
calories_df.select(calories_df["Calories"].cast("string").alias("Calories_as_string")).show()
print('-' * 105)

# contains: Checks if the column contains a specific value.
calories_df.filter(calories_df["Calories"].contains(100)).show()
print('-' * 105)

# desc: Sorts the DataFrame in descending order based on the column.
calories_df.orderBy(calories_df["Calories"].desc()).show()
print('-' * 105)

# desc_nulls_first: Sorts the DataFrame in descending order with null values first based on the column.
calories_df.orderBy(calories_df["Calories"].desc_nulls_first()).show()
print('-' * 105)

# desc_nulls_last: Sorts the DataFrame in descending order with null values last based on the column.
calories_df.orderBy(calories_df["Calories"].desc_nulls_last()).show()
print('-' * 105)

# dropFields: Drops the specified fields from the column.
#calories_df.withColumn("Calories_without_fields", calories_df["Calories"].dropFields()).show()
#print('-' * 105)

# endswith: Checks if the column ends with a specific value.
calories_df.filter(calories_df["Calories"].endswith("00")).show()
print('-' * 105)

# eqNullSafe: Checks if the column is equal to another column, handling null values.
calories_df.filter(calories_df["Calories"].eqNullSafe(100)).show()
print('-' * 105)

# getField: Extracts a field from a StructType column.
#calories_df.select(calories_df["Calories"].getField("field_name")).show()
#print('-' * 105)

# getItem: Extracts an item from an array or map column.
#calories_df.select(calories_df["Calories"].getItem(0)).show()
#print('-' * 105)

# ilike: Case-insensitive version of 'like' for string columns.
calories_df.filter(calories_df["Calories"].ilike("%apple%")).show()
print('-' * 105)

# isNotNull: Checks if the column is not null.
calories_df.filter(calories_df["Calories"].isNotNull()).show()
print('-' * 105)

# isNull: Checks if the column is null.
calories_df.filter(calories_df["Calories"].isNull()).show()
print('-' * 105)

# isin: Checks if the column value is in a list of specified values.
calories_df.filter(calories_df["Calories"].isin([100, 200])).show()
print('-' * 105)

# like: Checks if the column matches a pattern.
calories_df.filter(calories_df["Calories"].like("%apple%")).show()
print('-' * 105)

# name: Returns the name of the column.
print(calories_df["Calories"].name)
print('-' * 105)

# otherwise: If-else expression for the column.
# calories_df.select(when(calories_df["Calories"] > 100, "High").otherwise("Low").alias("Calories_category")).show()
# print('-' * 105)

# over: Defines a window specification to use with window functions.
# window_spec = Window.orderBy(calories_df["Calories"])
# calories_df.select(row_number().over(window_spec).alias("row_number")).show()
# print('-' * 105)

# rlike: Checks if the column matches a regex pattern.
calories_df.filter(calories_df["Calories"].rlike("^[0-9]*$")).show()
print('-' * 105)

# startswith: Checks if the column starts with a specific value.
calories_df.filter(calories_df["Calories"].startswith("10")).show()
print('-' * 105)

# substr: Extracts a substring from the column.
calories_df.select(calories_df["Calories"].substr(1, 3)).show()
print('-' * 105)

# when: If-else expression for the column.
calories_df.select(when(calories_df["Calories"] > 100, "High").otherwise("Low").alias("Calories_category")).show()
print('-' * 105)

# withField: Adds or updates a field in a StructType column.
# calories_df.withColumn("Calories_with_new_field", calories_df["Calories"].withField("new_field", lit(100).cast(IntegerType()))).show()
# print('-' * 105)

# End of script
spark.stop()

