from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = (
    SparkSession.builder
    .appName("MarketPulse_To_Parquet")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

print("\n--- [MIO JOB SPARK] TRASFORMAZIONE CSV -> PARQUET ---")

# Carico i dati dal Bronze Layer
df_prices = spark.read.option("header", True).option("inferSchema", True).csv("hdfs://namenode:9000/data/raw/prices/prices_intraday_2m.csv")

# Applico la pulizia dei dati
prices_cleaned = df_prices.withColumn("ts", to_timestamp(col("timestamp"))).drop("timestamp")

# Salvo nel Silver Layer (Parquet) partizionato
output_path = "hdfs://namenode:9000/data/processed/prices/prices_parquet"
print(f"Sto salvando i dati ottimizzati su: {output_path}")

(
    prices_cleaned.write
    .mode("overwrite")
    .partitionBy("ticker")
    .parquet(output_path)
)

print("--- TRASFORMAZIONE COMPLETATA ---")
spark.stop()