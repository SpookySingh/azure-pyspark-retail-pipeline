import pandas as pd

# Load original (unscaled) customer features
features_df = pd.read_csv(
    "data_lake/processed/customer_features_spark.csv"
)

# Load clustered (scaled) data with labels
clusters_df = pd.read_csv(
    "data_lake/processed/customer_segments.csv"
)

# Attach CustomerID back using row alignment
clusters_df["CustomerID"] = features_df["CustomerID"].values

# Merge cluster labels onto original features
df = features_df.merge(
    clusters_df[["CustomerID", "cluster"]],
    on="CustomerID",
    how="inner"
)

# Cluster sizes
cluster_sizes = (
    df.groupby("cluster")
      .size()
      .reset_index(name="customer_count")
)

# Cluster profiling using REAL (unscaled) values
cluster_profile = (
    df.groupby("cluster")
      .agg({
          "total_transactions": "mean",
          "total_quantity": "mean",
          "total_revenue": "mean",
          "avg_order_value": "mean",
          "recency_days": "mean"
      })
      .reset_index()
)

# Merge sizes
cluster_profile = cluster_profile.merge(
    cluster_sizes,
    on="cluster"
)

# Round for readability
cluster_profile = cluster_profile.round(2)

print("\nCluster Profile Summary (REAL VALUES):")
print(cluster_profile)

# Save output
OUTPUT_PATH = "data_lake/processed/cluster_profiles.csv"
cluster_profile.to_csv(OUTPUT_PATH, index=False)

print(f"\n✅ Cluster profiling saved to: {OUTPUT_PATH}")

