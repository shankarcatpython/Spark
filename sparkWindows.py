from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, sum

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Window Functions Example") \
    .getOrCreate()

# Read data from CSV file into DataFrame
sales_dataframe= spark.read.csv("car_sales.csv", header=True)

# Define a window specification partitioned by "Manufacturer" and ordered by "Sales"
window_spec_sales = Window.partitionBy("Manufacturer").orderBy(col("Sales in thousands").desc())

# Add a column indicating the current row number within each partition
sales_dataframe= sales_dataframe.withColumn("Row_Number", row_number().over(window_spec_sales))
sales_dataframe.select("Manufacturer","Model","Sales in thousands","Row_Number").show(100)

# Add a column for cumulative sum within each partition
sales_dataframe= sales_dataframe.withColumn("Cumulative_Sales", sum("Sales in thousands").over(window_spec_sales.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
sales_dataframe.select("Manufacturer","Model","Sales in thousands","Row_Number","Cumulative_sales").orderBy(sales_dataframe["Cumulative_sales"].desc()).show(100)


# Add a column for cumulative sum with a range of 1 preceding and 1 following the current row
sales_dataframe= sales_dataframe.withColumn("Cumulative_Sales_Range", sum("Sales in thousands").over(window_spec_sales.rowsBetween(-1, 1)))
sales_dataframe.select("Manufacturer","Model","Sales in thousands","Row_Number","Cumulative_sales","Cumulative_Sales_Range").orderBy(sales_dataframe["Cumulative_sales"].desc()).show(100)

# Show the DataFrame
sales_dataframe.show()

# Stop the SparkSession
spark.stop()
