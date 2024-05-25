from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Spark Internal Plans") \
    .getOrCreate()

# Sample data
data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
columns = ["name", "age"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)
dataframe_filter_select = df.filter(df.age > 30).select(df.name)

print('-' * 105)
print(r"Explain:")
dataframe_filter_select.explain()
print('-' * 105)
print(r"Explain(True):")
dataframe_filter_select.explain(True)
print('-' * 105)
print(r"Explain(Extended=True):")
dataframe_filter_select.explain(extended=True)
print('-' * 105)
print(r"Explain(Mode=Formatted):")
dataframe_filter_select.explain(mode='formatted')
print('-' * 105)


# Stop the SparkSession
spark.stop()
