import pyarrow as pa
import pyarrow.parquet as pq
import pandas

# Define Parquet file path
parquet_file = r'data_output_parquet\part-00000-9887683d-5108-408a-9e43-6bd00716aea6-c000.snappy.parquet'

print('-' * 105)

# Read Parquet file
table = pq.read_table(parquet_file)
print("Table:")
print(table)

print('-' * 105)

# Write Parquet file
df = table.to_pandas()
table_written = pa.Table.from_pandas(df)
pq.write_table(table_written, 'example_written.parquet')



# Open Parquet file
pq_file = pq.ParquetFile(parquet_file)

# Get schema information
schema = pq_file.schema
print("\nSchema:")
print(schema)

# Get metadata information
metadata = pq_file.metadata
print("\nMetadata:")
print(metadata)

print('-' * 105)

# Get number of row groups
num_row_groups = pq_file.num_row_groups
print("\nNumber of Row Groups:", num_row_groups)

print('-' * 105)
# Iterate over row groups
for i in range(num_row_groups):
    print("\nRow Group:", i)
    # Read row group
    row_group_table = pq_file.read_row_group(i)
    print(row_group_table)

print('-' * 105)
# Get number of columns in the schema
num_columns = len(schema)
print("\nNumber of Columns:", num_columns)

print('-' * 105)

print(dir(pq_file))
