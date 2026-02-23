from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# Configurazione sessione con URI espliciti per evitare il timeout su localhost
spark = SparkSession.builder \
    .appName("News_Trigger_Strategy") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017/market.news") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1. Carico le News da MongoDB (Trigger)
df_news = spark.read.format("mongodb").option("database", "market").option("collection", "news").load()
df_news = df_news.withColumn("ticker", F.trim(F.col("ticker"))) \
                 .withColumn("date", F.to_date("ts"))

# 2. Carico i Prezzi da Cassandra e calcolo la Media Mobile (Trend)
df_prices = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="prices_by_ticker", keyspace="market").load()
df_prices = df_prices.withColumn("ticker", F.trim(F.col("ticker")))

# Calcolo media mobile a 20 periodi per definire il trend
window_trend = Window.partitionBy("ticker").orderBy("ts").rowsBetween(-20, 0)
df_trend = df_prices.withColumn("sma_20", F.avg("close").over(window_trend)) \
                     .withColumn("date", F.to_date("ts"))

# 3. JOIN: Per ogni news, recupero lo stato tecnico del titolo in quel giorno
df_analysis = df_news.join(df_trend, ["ticker", "date"], "inner")

# 4. LOGICA DECISIONALE: Valido la news con l'analisi tecnica
df_decision = df_analysis.withColumn("entry_advice", 
    F.when(F.col("close") > F.col("sma_20"), "APPROVE - Bullish Trend")
    .otherwise("REJECT - Bearish Trend")
)

print("\n" + "="*60)
print("SISTEMA DI VALIDAZIONE NEWS-DRIVEN (MONGODB -> CASSANDRA)")
print("="*60)

# Mostro i risultati filtrando i duplicati temporali
df_decision.select("ticker", "date", "title", "close", "sma_20", "entry_advice") \
    .distinct() \
    .orderBy("date", ascending=False) \
    .show(20, truncate=50)

spark.stop()