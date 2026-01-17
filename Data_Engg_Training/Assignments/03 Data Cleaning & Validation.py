import pandas as pd
import sqlite3 as sql

#1.	Read CSV file containing sales data with missing values
df = pd.read_csv("sales_raw_data.csv")
print(df.head())

#2.	Create SQLite database with `sales` table
conn = sql.connect("sales_raw_data.db")
cursor = conn.cursor()

#3.	Load CSV data into database using Python
df.to_sql("sales", conn, if_exists='replace', index=False)

# Checking if the data is loaded properly
cursor.execute("SELECT * FROM sales")
rows = cursor.fetchall()
for row in rows:
    print(row)

#4.	Use SQL queries to identify NULL values: `SELECT * WHERE column IS NULL`
query ="""
    SELECT *
    FROM sales
    WHERE sale_date IS NULL
    OR city IS NULL
    OR quantity IS NULL
    OR price IS NULL;
"""

df_null = pd.read_sql(query, conn)
print(df_null)

#5.	Update missing values using SQL UPDATE with aggregate functions

cursor.executescript("""
UPDATE sales
SET quantity = (SELECT ROUND(AVG(quantity)) FROM sales WHERE quantity IS NOT NULL)
WHERE quantity IS NULL;

UPDATE sales
SET price = (SELECT ROUND(AVG(price), 2) FROM sales WHERE price IS NOT NULL)
WHERE price IS NULL;

UPDATE sales
SET sale_date = (SELECT MIN(sale_date) FROM sales WHERE sale_date IS NOT NULL)
WHERE sale_date IS NULL;

UPDATE sales
SET city = (
    SELECT city FROM sales
    WHERE city IS NOT NULL
    GROUP BY city
    ORDER BY COUNT(*) DESC
    LIMIT 1
)
WHERE city IS NULL;
""")

conn.commit()

# Checking if the data is updated properly
cursor.execute("SELECT * FROM sales")
rows = cursor.fetchall()
for row in rows:
    print(row)
print("____________________________")
#6.	Remove duplicates using SQL: `DELETE FROM table WHERE rowid NOT IN (SELECT MIN(rowid)...)`
cursor.execute("""
    DELETE FROM sales WHERE rowid NOT IN 
    (SELECT MIN(rowid) FROM sales 
    GROUP BY sale_id, sale_date, city, product, quantity, price)
""")
conn.commit()
cursor.execute("SELECT * FROM sales")
rows = cursor.fetchall()
for row in rows:
    print(row)

#7.	Standardize text fields using SQL UPPER() and LOWER() functions
cursor.execute("""
    UPDATE sales set city = upper(city), 
    product = upper(product) 
""")
conn.commit()

#Export cleaned data to new CSV file
query = "select * from sales"
df_clean = pd.read_sql(query, conn)
df_clean.to_csv("sales_clean.csv")

#9.	Compare original vs cleaned data statistics
raw_df = pd.read_csv("sales_raw_data.csv")
clean_df = pd.read_csv("sales_clean.csv")

print(raw_df.describe())
print(clean_df.describe())




