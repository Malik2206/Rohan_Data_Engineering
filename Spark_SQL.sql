from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Spark_SQL").getOrCreate()

--register DataFrame as a temp table

data.createOrReplaceTempView("data")

--Now can write SQL
result = spark.sql("""
      SELECT
          YEAR(order_date) as year,
          MONTH(order_date) as month,
          SUM(amount) as revenue,
          COUNT(*) as orders
      FROM data
      WHERE amount > 100
      GROUP BY YEAR(order_date), MONTH(order_date)
      ORDER BY year, month
""")

result.show()
