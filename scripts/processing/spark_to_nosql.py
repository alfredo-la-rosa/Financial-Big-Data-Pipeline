from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = (
    SparkSession.builder
    .appName("MarketPulse_To_NoSQL")
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1")
    .config("spark.cassandra.connection.host", "cassandra")
    .getOrCreate()
)

print("\n--- [MIO JOB SPARK] CARICAMENTO SU CASSANDRA E MONGODB ---")

# 1. Carico i prezzi su Cassandra
# Nota: leggo dal Parquet appena generato (più veloce!)
print("Caricamento prezzi su Cassandra...")
df_prices = spark.read.parquet("hdfs://namenode:9000/data/processed/prices/prices_parquet")

(
    df_prices.write
    .format("org.apache.spark.sql.cassandra")
    .mode("append")
    .options(table="prices_by_ticker", keyspace="market")
    .save()
)

# Carico le news e filtro quelle sporche (senza titolo o senza ticker)
# Sostituisci la lettura del JSON con questa versione più "robusta"
df_news = spark.read.option("multiLine", "true") \
                    .option("mode", "PERMISSIVE") \
                    .json("hdfs://namenode:9000/data/raw/news/market_news.json")

news_final = df_news.select(
    col("ticker"),
    to_timestamp(col("publish_time")).alias("ts"),
    col("title"),
    col("publisher").alias("source")
).filter(col("title").isNotNull() & col("ticker").isNotNull()) # <--- AGGIUNTO QUESTO FILTRO

print("Caricamento news su MongoDB (filtrate)...")

(
    news_final.write
    .format("mongodb")
    .mode("append")
    .option("connection.uri", "mongodb://mongodb:27017")
    .option("database", "market")
    .option("collection", "news")
    .save()
)

print("--- CARICAMENTO NOSQL COMPLETATO ---")
spark.stop()