# scripts/02_cleaning.py

import os
import pandas as pd

RAW_PATH = "data_lake/raw/online_retail/online_retail.csv"
PROCESSED_DIR = "data_lake/processed/online_retail"
OUTPUT_PATH = os.path.join(PROCESSED_DIR, "cleaned_online_retail.csv")

os.makedirs(PROCESSED_DIR, exist_ok=True)

# Load data
df = pd.read_csv(RAW_PATH)

print("Raw row count:", len(df))

# Cleaning logic (same semantics as Spark version)
df_clean = (
    df[~df["InvoiceNo"].astype(str).str.startswith("C")]
    .loc[df["Quantity"] > 0]
    .loc[df["UnitPrice"] > 0]
    .dropna(subset=["CustomerID"])
)

df_clean["Description"] = df_clean["Description"].astype(str).str.strip().str.upper()
df_clean["Country"] = df_clean["Country"].astype(str).str.strip()

df_clean = df_clean.drop_duplicates()

print("Cleaned row count:", len(df_clean))

# Save output
df_clean.to_csv(OUTPUT_PATH, index=False)

print(f"✅ Cleaned file written to: {OUTPUT_PATH}")
