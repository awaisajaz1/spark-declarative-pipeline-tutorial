from pyspark import pipelines as dp
from pyspark.sql.functions import *

# We shall do the following:
# 1. Read the data from the bronze table
# 2. Apply transformations
# 3. Write the data to the silver table
# Read the data from the bronze table


rules = {
    "rule1": "order_state is not null",
    "rule2": "revenue < 0"
    }



@dp.table(
    comment='Sales Silver Table',
    name='sdp.silver.silver_sales_order'
    )
@dp.expect_all(rules)
# @dp.expect_all_or_fail(rules)
# @dp.expect_all_or_drop(rules)
def read_bronze():
    return (
        spark.readStream
        .table("sdp.bronze.sales_order")
        .withColumn(
            'order_state',
            expr("""
                CASE order_status
                    WHEN 0 THEN 'Failed'
                    WHEN 1 THEN 'Paid'
                    WHEN 2 THEN 'Shipped'
                    WHEN 3 THEN 'Cancelled'
                    ELSE order_status
                END
            """)
        )
        .withColumn(
            'customer_sentiments',
            expr("ai_analyze_sentiment(comments)")
        )
        .withColumn(
            'profit',
            col("revenue") * 0.25
        )
        .select(
            'order_id',
            'product_id',
            'revenue',
            col('date').alias('order_date'),
            'store_id',
            'comments',
            'customer_sentiments',
            'order_state',
            'profit',
            'read_ts'
            )
    )
