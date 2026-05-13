from pyspark.sql.functions import col, sum, count, current_timestamp

# 1. Path Configurations
silver_orders_path = "abfss://silver@misgauravstorageaccount.dfs.core.windows.net/orders/"
silver_customers_path = "abfss://silver@misgauravstorageaccount.dfs.core.windows.net/customers/"
gold_output_path = "abfss://gold@misgauravstorageaccount.dfs.core.windows.net/customer_sales_summary/"

# 2. READ as Static DataFrames
orders_df = spark.read.format("delta").load(silver_orders_path)
customers_df = spark.read.format("delta").load(silver_customers_path)

# 3. Transformation & Aggregation
gold_df = (orders_df.join(customers_df, "customer_id", "inner")
    .groupBy("customer_id", "customer_name", "state")
    .agg(
        sum("amount").alias("total_spent"),
        count("order_id").alias("total_orders")
    )
    .withColumn("_gold_processed_at", current_timestamp())
)

(gold_df.write
    .format("delta")
    .mode("overwrite") 
    .save(gold_output_path))

print("Gold Layer Batch Processing Complete.")