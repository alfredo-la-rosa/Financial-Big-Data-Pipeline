Write-Host "Inizio creazione della struttura gerarchica del Data Lake su HDFS..." -ForegroundColor Cyan

# --- DEFINIZIONE STRUTTURA ---
# BRONZE LAYER: Dati grezzi (Prezzi e News)
# SILVER LAYER: Dati trasformati (Parquet)
# GOLD LAYER: Risultati finali (Features)

docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p `
    /data/raw/prices `
    /data/raw/news `
    /data/processed/prices `
    /data/features

Write-Host "`nStruttura HDFS completata. Il Data Lake è ora pronto per l'ingestion." -ForegroundColor Green