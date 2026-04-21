from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Create Spark session
spark = SparkSession.builder.appName("LazyEvalDemo").getOrCreate()

# Create sample data
data = [
    (1, "John", "CA", 50),
    (2, "Jane", "NY", 200),
    (3, "Bob", "CA", 150),
    (4, "Alice", "NY", 300),
    (5, "Charlie", "CA", 75),
    (6, "Diana", "NY", 250),
]
columns = ["id", "name", "state", "amount"]

df = spark.createDataFrame(data, columns)

print("=" * 60)
print("ORIGINAL DATA:")
print("=" * 60)
df.show()
