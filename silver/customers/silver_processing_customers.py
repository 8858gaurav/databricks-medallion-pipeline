from pyspark.sql.functions import col, upper, trim, current_timestamp

# 1. Path Configurations
input_base = "abfss://bronze@misgauravstorageaccount.dfs.core.windows.net/customers/"
output_base = "abfss://silver@misgauravstorageaccount.dfs.core.windows.net/customers/"
silver_checkpoint = "abfss://silver@misgauravstorageaccount.dfs.core.windows.net/_checkpoints/customers/"
# 1. Path for data processing offsets
offset_path = silver_checkpoint + "offsets"

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

# 3. Write to Silver folder in ADLS
query = (silver_df.writeStream 
    .trigger(availableNow=True)
    .format("delta")
    .option("checkpointLocation", offset_path)
    .outputMode("append")
    .start(output_base))

query.processAllAvailable()
query.stop()
spark.streams.awaitAnyTermination(20)