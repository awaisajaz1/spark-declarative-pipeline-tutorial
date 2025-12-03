from pyspark import pipelines as dp
from pyspark.sql.functions import *


# We shall load product and store dimensions from the bronze table
@dp.
