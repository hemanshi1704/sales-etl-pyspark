"""
Generates a synthetic, intentionally messy raw sales dataset to simulate
data arriving from an upstream source (POS system, e-commerce export, etc.)

This mirrors real-world raw data problems the ETL pipeline is built to handle:
- inconsistent casing in categorical fields
- missing values in numeric and categorical columns
- duplicate rows
- inconsistent date formats
- occasional negative/invalid quantities (data entry errors)

Output: raw_zone/sales_raw.csv  (the "raw" landing zone of the pipeline,
        analogous to an S3 raw/ prefix in a real data lake)
"""

import random
import csv
from datetime import datetime, timedelta

random.seed(42)

REGIONS = ["North", "south", "EAST", "West", "north", "South"]  # inconsistent casing on purpose
PRODUCTS = [
    ("Wireless Mouse", 799.0),
    ("Mechanical Keyboard", 3499.0),
    ("USB-C Hub", 1299.0),
    ("Laptop Stand", 1999.0),
    ("Webcam 1080p", 2499.0),
    ("Noise Cancelling Headphones", 5999.0),
    ("Portable SSD 1TB", 6999.0),
    ("Monitor 24-inch", 8999.0),
]
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]  # inconsistent formats on purpose

start_date = datetime(2024, 1, 1)
rows = []

for i in range(5000):
    order_id = f"ORD{100000 + i}"
    customer_id = f"CUST{random.randint(1000, 1500)}"
    product, unit_price = random.choice(PRODUCTS)
    region = random.choice(REGIONS)
    quantity = random.choice([1, 1, 2, 2, 3, 5, -1, 0])  # occasional bad data
    order_date = start_date + timedelta(days=random.randint(0, 365))
    date_str = order_date.strftime(random.choice(DATE_FORMATS))

    # randomly null out some fields to simulate missing data
    if random.random() < 0.03:
        region = ""
    if random.random() < 0.02:
        quantity = None

    rows.append([order_id, customer_id, product, unit_price, quantity, region, date_str])

# inject duplicate rows on purpose (simulates upstream re-sends / retries)
rows += random.sample(rows, 150)
random.shuffle(rows)

with open("/home/claude/sales-etl-pyspark/raw_zone/sales_raw.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "customer_id", "product_name", "unit_price", "quantity", "region", "order_date"])
    writer.writerows(rows)

print(f"Generated {len(rows)} raw rows -> raw_zone/sales_raw.csv")
