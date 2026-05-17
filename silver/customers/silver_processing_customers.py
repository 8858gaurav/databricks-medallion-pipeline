from pyspark.sql.functions import col, upper, trim, current_timestamp
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("cleaned_customers") \
    .getOrCreate()

# 1. Path Configurations
input_base = "abfss://bronze@misgauravstorageaccount.dfs.core.windows.net/customers/"
output_base = "abfss://silver@misgauravstorageaccount.dfs.core.windows.net/processed_customers/"
silver_checkpoint = "abfss://silver@misgauravstorageaccount.dfs.core.windows.net/_checkpoints/customers/"
# 1. Path for data processing offsets
offset_path = silver_checkpoint + "offsets"

spark.sql("USE CATALOG misgauravcatalog")
spark.sql("""CREATE SCHEMA IF NOT EXISTS retaildb""")

# 1. Read from the Bronze folder in ADLS
bronze_df = (spark.readStream
    .format("delta") 
    .load(input_base))

# 2. Transformation Logic (Cleansing)
silver_df = (bronze_df
    .filter(col("customer_id").isNotNull()) # Drop null IDs
    .withColumn("customer_id", trim(col("customer_id")).cast("integer")) 
    .withColumn("customer_name", trim(col("customer_name"))) 
    .withColumn("state", trim(col("state"))) 
    .withColumn("_silver_processed_at", current_timestamp()) # Audit column
    .dropDuplicates(["customer_id"]) # Deduplicate based on primary key
)

# 3. Write to Silver folder in ADLS & created delta table
query = (silver_df.writeStream
    .format("delta") 
    .option("checkpointLocation", offset_path) 
    .outputMode('append') 
    .option("path", output_base) 
    .trigger(availableNow=True) 
    .toTable('retaildb.silver_customer_data')
)

print("Streaming query started. Processing available batch data...")

query.awaitTermination()

print("Streaming batch complete. Data safely committed to Silver layer.")
print("Running file compaction and Z-Ordering maintenance...")

# small file problems in db: Optimize, delta.autoOptimize.optimizeWrite, delta.autoOptimize.autoCompact
# compaction/bin packing take multiple small files & merge them into 1 large files.
# in databricks, Optimize commands used to compact delta files upto 1 GB ; if we want > 128 MB of file use this.
# delta.autoOptimize.optimizeWrite = true ; before writing to the disk many small files are combine them to form a larger files (128MB), created bigger files (128MB). create a files around 128 MB after clubbing ; 
# delta.autoOptimize.autoCompact = true ; small files are already written to the disk, then compacted to form larger files (128MB), works only when we have > 50 smaill files. create a files around 128 MB after clubbing ; 
# 4. Maintenance: Now it is 100% safe to optimize because the data is fully written
spark.sql("OPTIMIZE retaildb.silver_customer_data ZORDER BY customer_id")

print("Optimization and Z-Ordering Complete.")