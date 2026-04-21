from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Lazy_Evaluation").getOrCreate()

data = [
    (1, "Messi", "Argentina", 50),
    (2, "Ronaldo", "Portugal", 200),
    (3, "Pele", "Brazil", 150),
    (4, "Maradona", "Argentina", 300),
    (5, "Zidane", "France", 75),
    (6, "Rooney", "England", 250)
]

columns = ["id", "name", "country", "goals"]

df = spark.createDataFrame(data, columns)

print("ORIGINAL DATA:")
df.show()


#This is a transformation and spark just records it
df_filtered = df.filter(col('goals') > 100)

print("Code executed: df_filtered = df.filter(col('goals') > 100)")
print("Did Spark scan the data? NO!")
print("DId Spark filter anything? NO!")
print("What happened? Spark just recorded: 'When executed, filter goals > 100'")
print("df_filtered type:", type(df_filtered))
print("df_filtered is a:", df_filtered)

#Apply Another transformation (still lazy)
print("Step 3: GROUP BY (Transformation is still lazy)")

df_grouped = df_filtered.groupBy("country")

print("Code executed: df_grouped = df_filtered.groupBy('country')")
print("Did Spark scan the data? NO!")
print("DId Spark even read the data? NO!")
print("What happened? Spark just recorded: After filtering, group by state'")
print("df_grouped type:", type(df_grouped))
print("df_grouped is a:", df_grouped)


#Apply Aggregation
print("Step 4: Aggregate (Transformation is still lazy)")

df_result = df_grouped.agg(sum("goals").alias("total_goals"))

print("Code executed: df_result = df_grouped.agg(sum('goals').alias('total_goals'))")
print("Did Spark sum anything? NO!")
print("DId Spark calculate totals? NO!")
print("What happened? Spark just recorded: After grouping, sum of goals'")
print("Spark's execution plan so far:")
print(" 1. Filter: goals > 100")
print(" 2. GroupBy: country")
print(" 3. Sum: goals")
print( "STATUS: Nothing has run yet")


print("STEP 5: SHOW() - ACTION - EXECUTES Everything")

# There is an ACTION. SPark will now execute
df_result.show()

