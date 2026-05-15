from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
print("STEP 1 - Script started")
# Initialize Spark Session test
spark = SparkSession.builder.appName("Customers_Pipeline").getOrCreate()

# 1. Path Configurations
# Note: Use the base directory for Auto Loader (remove *.json)
input_base = "abfss://input-path@misgauravstorageaccount.dfs.core.windows.net/customers/"
output_base = "abfss://bronze@misgauravstorageaccount.dfs.core.windows.net/customers/"
bronze_checkpoint = "abfss://bronze@misgauravstorageaccount.dfs.core.windows.net/_checkpoints/customers/"
# 1. Path for schema evolution tracking
schema_path = bronze_checkpoint + "schema"
# 2. Path for data processing offsets
offset_path = bronze_checkpoint + "offsets"

# 2. Define Schema testing2.
# Using the same structure as your manual read for consistency
customers_schema = StructType([
    StructField("data", ArrayType(
        StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("customer_name", StringType(), True),
            StructField("state", StringType(), True)
        ])
    ))
])

# 3. Read using Auto Loader (Incremental Batch Mode)
# This replaces the manual 'latest_date' filter logic
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .schema(customers_schema) # Providing schema for better performance
    .load(input_base))

# 4. Processing Logic
# Using 'inline' as in your original code to flatten the 'data' array
processed_df = (raw_df
    .select(inline("data"))
    .dropDuplicates(["customer_id"])
    .withColumn("ingestion_timestamp", current_timestamp())
    .repartition(1)) # it'll create only 1 file inside the output folder

print("STEP 2 - About to start stream")
# 5. Write to Output (Trigger Once / Batch Mode)
query = (processed_df.writeStream
    .trigger(availableNow=True)
    .format("delta") 
    .option("checkpointLocation", offset_path)
    .outputMode("append")
    .start(output_base))
print("STEP 3 - Stream started")
print("STEP 4 - Before processAllAvailable")
query.processAllAvailable()
query.stop()
print("=== STREAM FINISHED ===")