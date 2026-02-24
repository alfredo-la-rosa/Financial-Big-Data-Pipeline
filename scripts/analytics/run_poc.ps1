Write-Host "1. Upload dello script POC..." -ForegroundColor Cyan
docker cp .\scripts\analytics\poc_analysis_spark.py spark-master:/tmp/poc_analysis_spark.py

Write-Host "2. Calcolo Point of Control (Distribuzione del Valore)..." -ForegroundColor Yellow
docker exec -u root spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 `
  /tmp/poc_analysis_spark.py

Write-Host "`n--- ANALISI COMPLETATA ---" -ForegroundColor Green