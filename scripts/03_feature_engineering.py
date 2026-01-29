import pandas as pd
from datetime import datetime
from pathlib import Path

# ---------------------------
# Paths
# ---------------------------
INPUT_PATH = "data_lake/processed/online_retail/cleaned_online_retail.csv"
OUTPUT_DIR = Path("data_lake/features")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH = OUTPUT_DIR / "customer_features.csv"

# ---------------------------
# Load cleaned data
# ---------------------------
df = pd.read_csv(INPUT_PATH, parse_dates=["InvoiceDate"])

print("Rows loaded:", len(df))

# ---------------------------
# Feature Engineering
# ---------------------------
df["Revenue"] = df["Quantity"] * df["UnitPrice"]

customer_features = (
    df.groupby("CustomerID")
    .agg(
        total_transactions=("InvoiceNo", "nunique"),
        total_quantity=("Quantity", "sum"),
        total_revenue=("Revenue", "sum"),
        avg_order_value=("Revenue", "mean"),
        first_purchase_date=("InvoiceDate", "min"),
        last_purchase_date=("InvoiceDate", "max"),
    )
    .reset_index()
)

# ---------------------------
# Recency Feature
# ---------------------------
reference_date = df["InvoiceDate"].max()
customer_features["recency_days"] = (
    reference_date - customer_features["last_purchase_date"]
).dt.days

# ---------------------------
# Save features
# ---------------------------
customer_features.to_csv(OUTPUT_PATH, index=False)

print("Customer features created:", customer_features.shape)
print(f"✅ Feature file written to: {OUTPUT_PATH}")
