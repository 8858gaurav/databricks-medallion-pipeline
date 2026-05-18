from pyspark.sql.functions import col, sum, count, current_timestamp, window, expr

# 1. Path Configurations
output_base = "abfss://gold@misgauravstorageaccount.dfs.core.windows.net/customer_sales_summary/"
gold_checkpoint = "abfss://gold@misgauravstorageaccount.dfs.core.windows.net/_checkpoints/"
# 1. Path for data processing offsets
offset_path = gold_checkpoint + "offsets"

print("catalog name")
spark.sql("show catalogs").show()
spark.sql("use catalog misgauravcatalog")
spark.sql("create schema if not exists golddb")

# 2. READ as Streaming DataFrames and correctly apply watermarks
orders_df = spark.readStream.table("misgauravcatalog.silverdb.silver_order_data").withWatermark("_silver_order_processed_at", "30 minutes")
customers_df = spark.readStream.table("misgauravcatalog.silverdb.silver_customer_data").withWatermark("_silver_customer_processed_at", "30 minutes")


joined_df = orders_df.join(
    customers_df,
    expr("""
        orders_df.customer_id = customers_df.customer_id AND
        _silver_customer_processed_at >= _silver_order_processed_at - interval 30 minutes AND
        _silver_customer_processed_at <= _silver_order_processed_at + interval 30 minutes
    """),
    "inner"
)
# 3. Transformation & Aggregation testing.
window_agg_df = (
    joined_df.withWatermark("_silver_order_processed_at", "30 minutes")
    .groupBy(
        window(col("_silver_order_processed_at"), "15 minutes"), 
        col("customers_df.customer_id").alias("customer_id"), 
        col("customer_name"), 
        col("state")
    )
    .agg(
        sum(col("amount")).alias("total_spent"),
        count(col("order_id")).alias("total_orders")
    )
    .withColumn("_gold_processed_at", current_timestamp())
)

gold_df = window_agg_df.select(
    col("window.start").alias("start"), 
    col("window.end").alias("end"), 
    col("customer_id"), 
    col("customer_name"), 
    col("state"), 
    col("total_spent"), 
    col("total_orders"),
    col("_gold_processed_at")
)

query = (gold_df.writeStream
    .format("delta") 
    .option("checkpointLocation", offset_path) 
    .outputMode('append') 
    .option("path", output_base) 
    .trigger(availableNow=True) 
    .toTable('misgauravcatalog.golddb.cust_summary')
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
spark.sql("OPTIMIZE misgauravcatalog.golddb.cust_summary ZORDER BY customer_id")

print("Optimization and Z-Ordering Complete.")

print("Gold Layer Processing Complete.")