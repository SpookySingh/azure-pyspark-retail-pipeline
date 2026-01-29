from pyspark.sql import SparkSession

RAW_DATA_PATH = "data_lake/raw/online_retail/online_retail.csv"

spark = SparkSession.builder \
    .appName("Retail PySpark Pipeline - Ingestion") \
    .master("local[*]") \
    .getOrCreate()

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(RAW_DATA_PATH)

print("===== SCHEMA =====")
df_raw.printSchema()

print("===== ROW COUNT =====")
print(df_raw.count())

print("===== SAMPLE DATA =====")
df_raw.show(5, truncate=False)

spark.stop()
