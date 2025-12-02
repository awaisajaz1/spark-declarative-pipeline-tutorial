from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Materialized View from Source to Bronze
@dp.materialized_view(
    comment='Store Bronze Table',
    name='sdp.bronze.stores'
)
def bronze_stores():
    return (
        spark.read
        .table('sdp.source.stores')
        .withColumn('read_ts', current_timestamp())
    )


@dp.materialized_view(
    comment='Product Bronze Table',
    name='sdp.bronze.products'
)
def bronze_products():
    return(
        spark.read
        .table('sdp.source.products')
        .withColumn('read_ts', current_timestamp())
    )