from pyspark.sql.functions import col, sum, count, current_timestamp, window, expr

# 1. Path Configurations
output_base = "abfss://gold@misgauravstorageaccount.dfs.core.windows.net/customer_sales_summary/"
gold_checkpoint = "abfss://gold@misgauravstorageaccount.dfs.core.windows.net/_checkpoints/"
offset_path = gold_checkpoint + "offsets"

print("catalog name")
spark.sql("show catalogs").show()
spark.sql("use catalog misgauravcatalog")
spark.sql("create schema if not exists golddb")

# 2. READ as Streaming DataFrames and correctly apply watermarks (DEFINED HERE)
orders_df = spark.read.table("misgauravcatalog.silverdb.silver_order_data")
customers_df = spark.read.table("misgauravcatalog.silverdb.silver_customer_data")

joined_df = orders_df.join(customers_df, "customer_id", "left")

# 3. Transformation & Aggregation testing.
window_agg_df = (
    joined_df.groupBy( 
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
    col("customer_id"), 
    col("customer_name"), 
    col("state"), 
    col("total_spent"), 
    col("total_orders"),
    col("_gold_processed_at")
)

query = (gold_df.write
    .format("delta") 
    .option("checkpointLocation", offset_path) 
    .outputMode('append') 
    .option("path", output_base) 
    .saveAsTable('misgauravcatalog.golddb.cust_summary')
)

# 4. Maintenance
print("Running file compaction and Z-Ordering maintenance...")
spark.sql("OPTIMIZE misgauravcatalog.golddb.cust_summary ZORDER BY (customer_id)")

print("Optimization and Z-Ordering Complete.")
print("Gold Layer Processing Complete.")