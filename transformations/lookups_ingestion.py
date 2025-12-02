from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Materialized View from Source to Bronze
@dp.materialized_view(
    comment='Sales Bronze Table',
    name='sdp.bronze.sales'
)
def bronze_stores():
    return (
        spark.read
        .table('sdp.source.sales')
        .withColumn('read_ts', current_timestamp())
    )