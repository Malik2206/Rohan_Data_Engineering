import sqlite3
import pandas as pd
conn = sqlite3.connect('sales_data.db')

#Create a cursor object to execute a sql query
cursor = conn.cursor()

#SQL Create table statement
cursor.execute("""
Create table if not exists products(
    id integer primary key autoincrement,
    name text not null, 
    price double not null,
    discounted_price double);
""")

conn.commit()
print("Table created successfully")
#print(pd.read_sql_query("SELECT * FROM products", conn))

#Read csv file
df = pd.read_csv('products.csv')
#Write csv file to table
df.to_sql('products', conn, if_exists='replace', index=False)
print("CSV file ingested successfully")
#print(pd.read_sql_query("SELECT * FROM products", conn))

#Update statement for discounted_price
cursor.execute("""
update products 
set discounted_price = 
        case when price > 50 then price - price *0.15
        else null
end; 
""")
conn.commit()
print("Table updated successfully")
print(pd.read_sql_query("SELECT * FROM products", conn))


#Query products below $20 using SELECT with WHERE clause
print(pd.read_sql_query("SELECT * FROM products where price<20", conn))

#Calculate aggregate statistics using SQL (SUM, AVG, MAX, MIN)
print(pd.read_sql_query("""
SELECT sum(price),avg(price),max(price),min(price) FROM products;                        
""", conn))

#Use Python loops to fetch query results and process data
for row in cursor.execute("select * from products"):
    print(row)

#Export updated product list to CSV file
df = pd.read_sql_query("""select * from products""", conn)
df.to_csv('output_products.csv', index=False)
print("CSV file created successfully")
conn.close()
