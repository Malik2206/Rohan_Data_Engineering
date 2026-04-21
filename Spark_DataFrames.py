import os
import sys

python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count
from airflow  import
spark = (SparkSession.builder
         .appName("DataFrameExample")
         .config("spark.pyspark.python", python_path)
         .config("spark.pyspark.driver.python", python_path)
         .getOrCreate())

#Method 1: From Python List
data = [
    (1, "Rohan", "Mumbai", 100),
    (2, "Malik", "Pune", 200),
    (3, "Aditya", "Goa", 300),
    (4, "Ronaldo", "Portugal", 400)
]

columns = ["id", "name", "state", "amount"]

df = spark.createDataFrame(data, columns)

#SHow data
df.show()

#Read CSV file (100 GB file - spark handles it!)
df = spark.read.csv(
    "data.csv",
    header=True,
    inferSchema= True #auto detects data types
)

#Read parquet (columnar format - FASTER for analytics)
df = spark.read.parquet("pipelines/data.parquet")

#Read from database
df = spark.read.jdbc(
    url ="jdbc:postgresql:/localhost:5432/warehouse",
    table="fact_data",
    properties={
        "user": "postgres",
        "password": "password"
    }
)

df.printSchema()

#filter rows
df = df.filter(col("state") == "Mumbai")
#OR
df = df.where(col("state") == "Mumbai")

#select columns
df = df.select("name", "state", "amount")

#Add new column
df = df.withColumn("age")

#Rename column
df = df.withColumnRenamed("amount", "salary")

#Drop column
df = df.drop("id")

#Sort
df = df.orderBy(col("amount").desc())

#Remove duplicates
df = df.dropDuplicates(["customer_id"])

#Group and aggregate
df = df.groupBy("state").agg(
    sum("amount").alias("total"),
    avg("amount").alias("average"),
    count("*").alias("count")
)