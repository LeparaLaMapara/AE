
import pandas as pd
import numpy as np

# Generate synthetic sales data
np.random.seed(0)
dates = pd.date_range(start='2023-01-01', periods=100)
product_ids = np.random.choice(['P001', 'P002', 'P003'], size=100)
quantities_sold = np.random.randint(1, 20, size=100)
prices = np.random.uniform(10, 100, size=100)

data = pd.DataFrame({
    'date': dates,
    'product_id': product_ids,
    'quantity_sold': quantities_sold,
    'price': prices
})

# Save to CSV as a simple data source example
data.to_csv('synthetic_sales_data.csv', index=False)
