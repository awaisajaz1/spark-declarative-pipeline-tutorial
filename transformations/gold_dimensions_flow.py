from pyspark import pipelines as dp
from pyspark.sql.functions import *


# We shall load product by using SCD2 and store dimensions with SCD1 concept from the bronze table

## Product Dimension Loading 
@dp.view
def products_with_sk():
    return (
        spark.readStream.table("sdp.bronze.products")
        # .withColumn("product_sk", monotonically_increasing_id())
        .withColumn(
            "product_sk",
            hash(concat(col("product_id"), col("read_ts")))
        )
    )

dp.create_streaming_table(
    name="sdp.gold.dim_products",
    comment="Product dimension"
)

dp.create_auto_cdc_flow(
    target="sdp.gold.dim_products",
    source="products_with_sk",
    keys=["product_id"],
    sequence_by=col("read_ts"),
    stored_as_scd_type=2,
    track_history_except_column_list=[
        "product_name", 
        "category",
        "price"
    ],
    ignore_null_updates=True
)


## Store Dimension Loading Strategy
@dp.materialized_view(
    name="sdp.gold.dim_stores",
    comment="Store dimension"
)

def dim_stores():
    return (
        spark.read.table("sdp.bronze.stores")
        .withColumn("load_ts", current_timestamp())
        # .withColumn("store_sk", monotonically_increasing_id())
        .withColumn(
            "product_sk",
            hash(concat(col("store_id"), col("read_ts")))
        )
    )