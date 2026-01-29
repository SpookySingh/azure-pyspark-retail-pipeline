from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, countDistinct, min, max,
    datediff, lit
)

# --------------------------------------------------
# Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("Customer Feature Engineering") \
    .master("local[*]") \
    .getOrCreate()

# --------------------------------------------------
# Paths
# --------------------------------------------------
INPUT_PATH = "data_lake/processed/online_retail/cleaned_online_retail.csv"
OUTPUT_FILE = "data_lake/processed/customer_features_spark.csv"

# --------------------------------------------------
# Load cleaned data
# --------------------------------------------------
df = spark.read.option("header", True).csv(INPUT_PATH)

# Cast numeric columns
df = df.withColumn("Quantity", col("Quantity").cast("int")) \
       .withColumn("UnitPrice", col("UnitPrice").cast("double"))

# Create revenue column
df = df.withColumn("revenue", col("Quantity") * col("UnitPrice"))

# --------------------------------------------------
# Aggregate to customer level
# --------------------------------------------------
customer_features = (
    df.groupBy("CustomerID")
      .agg(
          countDistinct("InvoiceNo").alias("total_transactions"),
          sum("Quantity").alias("total_quantity"),
          sum("revenue").alias("total_revenue"),
          min("InvoiceDate").alias("first_purchase_date"),
          max("InvoiceDate").alias("last_purchase_date")
      )
)

# Average order value
customer_features = customer_features.withColumn(
    "avg_order_value",
    col("total_revenue") / col("total_transactions")
)

# Recency (reference date = last date in dataset)
max_date = df.agg(max("InvoiceDate")).collect()[0][0]

customer_features = customer_features.withColumn(
    "recency_days",
    datediff(lit(max_date), col("last_purchase_date"))
)

# --------------------------------------------------
# Convert to pandas and write safely
# --------------------------------------------------
customer_features_pd = customer_features.toPandas()
customer_features_pd.to_csv(OUTPUT_FILE, index=False)


print("✅ Spark customer features written to:", OUTPUT_FILE)

spark.stop()
