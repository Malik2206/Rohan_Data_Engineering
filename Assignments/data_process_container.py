import pandas as pd
import sys

print("Started data processing...")

#Creating sample data
data = {
    'product':['Laptop', 'Mouse','Keyboard'],
    'price':[12000, 2500, 7500],
    'quantity':[10,50, 75]
}

df = pd.DataFrame(data)

df['revenue'] = df['price']*df['quantity']

print("Processed data:")

print(df)

print(f"Total revenue: ${df['revenue'].sum()}")
print("Processing complete")