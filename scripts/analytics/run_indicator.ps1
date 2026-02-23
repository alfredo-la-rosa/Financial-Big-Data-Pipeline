# --- SCRIPT DI LANCIO ANALISI INDICATORI ---
Write-Host "1. Caricamento indicator_spark.py nel cluster..." -ForegroundColor Cyan
docker cp .\scripts\analytics\indicator_spark.py spark-master:/tmp/indicator_spark.py

Write-Host "2. Esecuzione Job Spark (Analisi Cross-Database)..." -ForegroundColor Yellow
docker exec -u root spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 `
  /tmp/indicator_spark.py

Write-Host "`n--- PROCESSO COMPLETATO ---" -ForegroundColor Green