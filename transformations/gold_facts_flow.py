from pyspark import pipelines as dp
from pyspark.sql.functions import *

# we shall follow upsert approch to load the fact table in gold layer
# we shall utilized temporary view to to prepare the date by joining dimension and the push to downstream with SCD1

# Step 1: Read transactional data and join with dimensions
@dp.view(
    name="sales_transaction_with_dims",
)

def sales_transaction_with_dims():
    silver_sales = spark.readStream.table("sdp.silver.silver_sales_order")
    dim_product = spark.read.table("sdp.gold.dim_products")
    dim_store = spark.read.table("sdp.gold.dim_stores") 

    return (
        silver_sales.join(
            dim_product, 
            silver_sales.product_id == dim_product.product_id, "left"
        ).filter(dim_product["__END_AT"].isNull()).
        join(
            dim_store,
            silver_sales.store_id == dim_store.store_id, "left"
        ).
        select(
            silver_sales.order_id,
            silver_sales.order_date,
            silver_sales.store_id,
            dim_store.product_sk.alias("store_sk"), # too much lazy to change the name is mid stream ;)
            dim_store.store_name,
            silver_sales.product_id,
            dim_product.product_sk,
            dim_product.product_name,
            silver_sales.revenue,
            silver_sales.comment.alias("cusomer_review"),
            silver_sales.customer_sentiments,
            silver_sales.order_state,
            silver_sales.profit,
            silver_sales.read_ts.alias("record_timestamp")
        )
    )

# Step 2: Create the target streaming table for sales fact
dp.create_streaming_table(
    name="sdp.gold.sales_fact"
)

# Step 3: Upsert the using auto CDC
dp.create_auto_cdc_flow(
    target="sdp.gold.sales_fact",
    source="sales_transaction_with_dims",
    keys=["order_id"],
    sequence_by=col("record_timestamp"),
    stored_as_scd_type=1
)