import pandas as pd
import sys

print("Starting data processing...")

# Create sample data
data = {
    'product': ['Laptop', 'Mouse', 'Keyboard'],
    'price': [1200, 25, 75],
    'quantity': [5, 50, 20]
}

df = pd.DataFrame(data)

# Calculate
df['revenue'] = df['price'] * df['quantity']

print("\nProcessed Data:")
print(df)

print(f"\nTotal Revenue: ${df['revenue'].sum()}")
print("\n✓ Processing complete!")