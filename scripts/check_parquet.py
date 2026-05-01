import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pandas as pd

trades_dir = "data/trades"

# Read entire trades directory as a partitioned dataset
dataset = ds.dataset(trades_dir, format="parquet", partitioning="hive")

print("Schema:")
print(dataset.schema)

# Convert to pandas DataFrame
df = dataset.to_table().to_pandas()

print(f"\nTotal rows: {len(df)}")

print(f"\n--- Sorted by event_timestamp ---")
df_by_event = df.sort_values('event_timestamp')
print(df_by_event.head(10))

print(f"\n--- Sorted by ingestion_timestamp ---")
df_by_ingestion = df.sort_values('ingestion_timestamp')
print(df_by_ingestion.head(10))