from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 1. Path Configurations
# Note: Use the base directory for Auto Loader (remove *.json)
input_base = "abfss://input-path@misgauravstorageaccount.dfs.core.windows.net/customers/"
output_base = "abfss://bronze@misgauravstorageaccount.dfs.core.windows.net/customers/"
bronze_checkpoint = "abfss://bronze@misgauravstorageaccount.dfs.core.windows.net/_checkpoints/customers/"
# 1. Path for schema evolution tracking
schema_path = bronze_checkpoint + "schema"
# 2. Path for data processing offsets
offset_path = bronze_checkpoint + "offsets"

# 2. Define Schema.
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
raw_df = (spark.read
    .format("json")
    .schema(customers_schema)
    .load(input_base))

# 4. Processing Logic
# Using 'inline' as in your original code to flatten the 'data' array
processed_df = (raw_df
    .select(inline("data"))
    .dropDuplicates(["customer_id"])
    .withColumn("ingestion_timestamp", current_timestamp())
    .repartition(1)) # it'll create only 1 file inside the output folder

# 5. Write to Output (Trigger Once / Batch Mode)
query = (processed_df.write
    .format("delta") 
    .outputMode("append")
    .save(output_base))