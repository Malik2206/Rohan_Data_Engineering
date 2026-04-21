from pyspark.sql import SparkSession

#Create Spark session (always first step)
spark = SparkSession.builder \
    .appName("myFirstSparkApp") \
    .master("local[*]") \
    .getOrCreate()


#Check if it works

print(f"Spark version: {spark.version}")
print(f"Spark running on: {spark.sparkContext.master}")

