import pandas as pd
import matplotlib.pyplot as plt

# -----------------------------
# Load customer features
# -----------------------------
DATA_PATH = "data_lake/processed/customer_features_spark.csv"

df = pd.read_csv(DATA_PATH)

print("Dataset shape:", df.shape)
print("\nColumn names:")
print(df.columns.tolist())

# -----------------------------
# Basic overview
# -----------------------------
print("\nData types:")
print(df.dtypes)

print("\nMissing values:")
print(df.isna().sum())

print("\nStatistical summary:")
print(df.describe())

# -----------------------------
# Distribution analysis
# -----------------------------
numeric_cols = [
    "total_transactions",
    "total_quantity",
    "total_revenue",
    "avg_order_value",
    "recency_days"
]

df[numeric_cols].hist(
    bins=30,
    figsize=(12, 8),
    edgecolor="black"
)
plt.suptitle("Customer Feature Distributions", fontsize=14)
plt.tight_layout()
plt.show()

# -----------------------------
# Outlier inspection (boxplots)
# -----------------------------
plt.figure(figsize=(12, 6))
df[numeric_cols].boxplot()
plt.title("Boxplot of Customer Features")
plt.xticks(rotation=30)
plt.show()

# -----------------------------
# Top customers by revenue
# -----------------------------
top_revenue_customers = df.sort_values(
    by="total_revenue",
    ascending=False
).head(10)

print("\nTop 10 customers by revenue:")
print(top_revenue_customers[
    ["CustomerID", "total_revenue", "total_transactions", "avg_order_value"]
])

# -----------------------------
# Correlation analysis
# -----------------------------
corr = df[numeric_cols].corr()

print("\nCorrelation matrix:")
print(corr)

plt.figure(figsize=(8, 6))
plt.imshow(corr, cmap="coolwarm")
plt.colorbar()
plt.xticks(range(len(numeric_cols)), numeric_cols, rotation=45)
plt.yticks(range(len(numeric_cols)), numeric_cols)
plt.title("Correlation Heatmap")
plt.tight_layout()
plt.show()
