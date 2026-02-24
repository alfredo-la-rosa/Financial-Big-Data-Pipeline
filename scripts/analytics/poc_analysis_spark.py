from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# Inizializzazione Spark
spark = SparkSession.builder \
    .appName("Daily_POC_Analysis") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1. Caricamento dati da Cassandra
df_prices = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="prices_by_ticker", keyspace="market").load()

# 2. Preparazione Dati e Filtro Temporale (Ultimi 5 giorni)
df_dates = df_prices.withColumn("date", F.to_date("ts"))
last_5_days = df_dates.select("date").distinct().orderBy(F.desc("date")).limit(5)
df_filtered = df_dates.join(F.broadcast(last_5_days), "date")

# 3. Definizione del POC (Point of Control)
# Arrotondiamo il prezzo al primo decimale per creare i "livelli di prezzo" (binning)
df_binned = df_filtered.withColumn("price_level", F.round(F.col("close"), 1))

# Calcoliamo il volume totale per ogni livello di prezzo per giorno/ticker
df_volume_dist = df_binned.groupBy("ticker", "date", "price_level") \
    .agg(F.sum("volume").alias("total_volume_at_level"))

# 4. Window Function per trovare il livello con il volume massimo (il POC)
window_poc = Window.partitionBy("ticker", "date").orderBy(F.desc("total_volume_at_level"))

df_poc = df_volume_dist.withColumn("rank", F.row_number().over(window_poc)) \
    .filter(F.col("rank") == 1) \
    .select("date", "ticker", "price_level", "total_volume_at_level") \
    .withColumnRenamed("price_level", "POC_Price")

print("\n" + "="*70)
print("ANALISI VALUE DISTRIBUTION: DAILY POINT OF CONTROL (LAST 5 DAYS)")
print("="*70)

# Mostriamo il risultato ordinato
df_poc.orderBy("ticker", "date", ascending=[True, False]).show(25)

spark.stop()