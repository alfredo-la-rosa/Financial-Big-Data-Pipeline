Write-Host "--- AVVIO AUTOMAZIONE PIPELINE SPARK ---" -ForegroundColor Cyan

# 1. Copia degli script aggiornati
Write-Host "1. Aggiornamento script nel container..."
docker cp scripts/processing/spark_to_parquet.py spark-master:/tmp/
docker cp scripts/processing/spark_to_nosql.py spark-master:/tmp/

# 2. Esecuzione Trasformazione Parquet
Write-Host "2. Lancio Trasformazione Silver Layer (HDFS Parquet)..." -ForegroundColor Yellow
docker exec spark-master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    /tmp/spark_to_parquet.py

# 3. Esecuzione Caricamento NoSQL
Write-Host "3. Lancio Caricamento Gold Layer (Cassandra & MongoDB)..." -ForegroundColor Yellow
docker exec -u root spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 `
  /tmp/spark_to_nosql.py

Write-Host "--- PIPELINE COMPLETATA CON SUCCESSO ---" -ForegroundColor Green