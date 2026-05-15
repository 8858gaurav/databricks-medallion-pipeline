from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Orders_Pipeline").getOrCreate()

# 1. Path Configurations
input_base = "abfss://input-path@misgauravstorageaccount.dfs.core.windows.net/orders/"
output_base = "abfss://bronze@misgauravstorageaccount.dfs.core.windows.net/orders/"
bronze_checkpoint = "abfss://bronze@misgauravstorageaccount.dfs.core.windows.net/_checkpoints/orders/"
# 1. Path for schema evolution tracking
schema_path = bronze_checkpoint + "schema"
# 2. Path for data processing offsets
offset_path = bronze_checkpoint + "offsets"

# 2. Define the Schema (Crucial for Auto Loader)
# This describes the root JSON structure
orders_schema = StructType([
    StructField("orders", ArrayType(StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("amount", FloatType(), True),
        StructField("order_status", StringType(), True)
    ])), True)
])

# 3. Read using Auto Loader with EXPLICIT SCHEMA
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .schema(orders_schema)  # <--- ADD THIS LINE HERE
    .load(input_base))

# 4. Processing Logic
processed_df = (raw_df
    .select(explode("orders").alias("order_item"))
    .select("order_item.*")
    .dropDuplicates(["order_id"])
    .withColumn("processing_timestamp", current_timestamp())
    .repartition(1)) # it'll create only 1 file inside the output folder

# 5. Write to Output
query = (processed_df.writeStream
    .trigger(availableNow=True)
    .format("delta")
    .option("checkpointLocation", offset_path)
    .outputMode("append")
    .start(output_base))

query.processAllAvailable()
query.stop()
spark.streams.awaitAnyTermination(20)