from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, sum, trim
import pandas

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Grouped Example") \
    .getOrCreate()

# Read data from CSV file into DataFrame
df= spark.read.csv("car_sales.csv", header=True)

print('-'*105)
# pyspark.sql.GroupedData.agg
# It computes aggregates on the grouped data.
grouped = df.groupby('Manufacturer')
agg_result = grouped.agg({'Sales in thousands': 'sum', 'Price in thousands': 'mean'})
print("agg_result:")
agg_result.show()
print('-'*105)


# pyspark.sql.GroupedData.avg
# It computes the average for each numeric column in the grouped data.
avg_result = grouped.agg({'Sales in thousands': 'avg'})
print("avg_result:")
avg_result.show()
print('-'*105)

# pyspark.sql.GroupedData.count
# It counts the number of elements for each group.
count_result = grouped.count()
print("count_result:")
count_result.show()
print('-'*105)

# pyspark.sql.GroupedData.max
# It computes the max value for each numeric column in the grouped data.
max_result = grouped.max()
print("max_result:")
max_result.show()
print('-'*105)

# pyspark.sql.GroupedData.mean
# It computes the mean for each numeric column in the grouped data.
mean_result = grouped.mean()
print("mean_result:")
mean_result.show()
print('-'*105)

# pyspark.sql.GroupedData.min
# It computes the min value for each numeric column in the grouped data.
min_result = grouped.min()
print("min_result:")
min_result.show()
print('-'*105)


# pyspark.sql.GroupedData.sum
# It computes the sum for each numeric column in the grouped data.
sum_result = grouped.sum()
print("sum_result:")
sum_result.show()
print('-'*105)


# Stop the SparkSession
spark.stop()