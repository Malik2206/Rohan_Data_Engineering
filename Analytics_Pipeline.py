from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#Initialize spark
spark = SparkSession.builder \
    .appName("AnalyticsPipeline") \
    .getOrCreate()

analytics = spark.read.csv(
    "gs://my-bucket/analytics/*.csv", #GCS
    header=True,
    inferSchema=True
)

print(f"Total rows:{analytics.count():,}")

analytics.show(5)


'''#Data Quality checks (Distributed)
#Check for nulls across billions rows!
null_counts = sales.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in sales.columns
])

null_counts.show()

#order_id | customer_id | amount|  order_date
0             5              17         2'''
