import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler

# --------------------------------------------------
# Paths
# --------------------------------------------------
INPUT_PATH = "data_lake/processed/customer_features_spark.csv"
OUTPUT_PATH = "data_lake/processed/customer_features_scaled.csv"

# --------------------------------------------------
# Load data
# --------------------------------------------------
df = pd.read_csv(INPUT_PATH)

print("Initial shape:", df.shape)

# --------------------------------------------------
# Feature selection
# --------------------------------------------------
features = [
    "total_transactions",
    "total_quantity",
    "total_revenue",
    "avg_order_value",
    "recency_days"
]

X = df[features].copy()

# --------------------------------------------------
# Log transform skewed features
# --------------------------------------------------
skewed_features = [
    "total_quantity",
    "total_revenue",
    "avg_order_value"
]

for col in skewed_features:
    X[col] = np.log1p(X[col])

print("Log transformation applied.")

# --------------------------------------------------
# Scaling
# --------------------------------------------------
scaler = RobustScaler()
X_scaled = scaler.fit_transform(X)

X_scaled_df = pd.DataFrame(
    X_scaled,
    columns=features
)

# --------------------------------------------------
# Save prepared dataset
# --------------------------------------------------
X_scaled_df.to_csv(OUTPUT_PATH, index=False)

print("✅ Scaled clustering dataset saved to:", OUTPUT_PATH)
print("Final shape:", X_scaled_df.shape)
