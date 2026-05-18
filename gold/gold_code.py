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
orders_df = spark.readStream.table("misgauravcatalog.silverdb.silver_order_data").withWatermark("_silver_order_processed_at", "30 minutes")
customers_df = spark.readStream.table("misgauravcatalog.silverdb.silver_customer_data").withWatermark("_silver_customer_processed_at", "30 minutes")

# Give explicit SQL aliases to the DataFrames so expr() and col() can read them
orders_aliased = orders_df.alias("ords")
customers_aliased = customers_df.alias("cust")

joined_df = customers_aliased.join(
    orders_aliased,
    expr("""
        cust.customer_id = ords.customer_id AND
        ords._silver_order_processed_at >= cust._silver_customer_processed_at AND
        ords._silver_order_processed_at <= cust._silver_customer_processed_at + interval 2 hours
    """), 
    "left"
)

# 3. Transformation & Aggregation testing.
# FIX: Removed the duplicate .withWatermark() call here
window_agg_df = (
    joined_df.groupBy(
        window(col("_silver_order_processed_at"), "15 minutes"), 
        col("cust.customer_id").alias("customer_id"), 
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
print("Streaming batch complete. Data safely committed to Gold layer.")

# 4. Maintenance
print("Running file compaction and Z-Ordering maintenance...")
spark.sql("OPTIMIZE misgauravcatalog.golddb.cust_summary ZORDER BY (customer_id)")

print("Optimization and Z-Ordering Complete.")
print("Gold Layer Processing Complete.")