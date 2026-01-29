import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import yaml

# -------------------------------------------------
# Load configuration
# -------------------------------------------------
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# -------------------------------------------------
# Load clustering data
# -------------------------------------------------
DATA_PATH = config["paths"]["customer_features_scaled"]
OUTPUT_PATH = config["paths"]["customer_segments"]

FINAL_K = config["clustering"]["final_k"]
K_RANGE = range(
    config["clustering"]["k_min"],
    config["clustering"]["k_max"] + 1
)

df = pd.read_csv(DATA_PATH)

# Use all columns for clustering (already scaled)
X = df.copy()

# -------------------------------------------------
# Elbow Method
# -------------------------------------------------
inertia = []

for k in K_RANGE:
    model = KMeans(
        n_clusters=k,
        random_state=42,
        n_init=10
    )
    model.fit(X)
    inertia.append(model.inertia_)

plt.figure(figsize=(6, 4))
plt.plot(K_RANGE, inertia, marker="o")
plt.xlabel("Number of Clusters (K)")
plt.ylabel("Inertia")
plt.title("Elbow Method for Optimal K")
plt.tight_layout()
plt.show()

# -------------------------------------------------
# Silhouette Score
# -------------------------------------------------
silhouette_scores = []

for k in K_RANGE:
    model = KMeans(
        n_clusters=k,
        random_state=42,
        n_init=10
    )
    labels = model.fit_predict(X)
    score = silhouette_score(X, labels)
    silhouette_scores.append(score)
    print(f"K={k}, Silhouette Score={score:.3f}")

plt.figure(figsize=(6, 4))
plt.plot(K_RANGE, silhouette_scores, marker="o")
plt.xlabel("Number of Clusters (K)")
plt.ylabel("Silhouette Score")
plt.title("Silhouette Score vs K")
plt.tight_layout()
plt.show()

# -------------------------------------------------
# Final KMeans model
# -------------------------------------------------
kmeans_final = KMeans(
    n_clusters=FINAL_K,
    random_state=42,
    n_init=10
)

df["cluster"] = kmeans_final.fit_predict(X)

final_sil_score = silhouette_score(X, df["cluster"])
print(f"✅ Silhouette Score (k={FINAL_K}): {final_sil_score:.4f}")

# -------------------------------------------------
# Save clustered data
# -------------------------------------------------
df.to_csv(OUTPUT_PATH, index=False)

print(f"✅ Customer segmentation completed with K={FINAL_K}")
print(f"✅ Output saved to: {OUTPUT_PATH}")
