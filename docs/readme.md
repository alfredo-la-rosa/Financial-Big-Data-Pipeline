Esame -  Algoritmi e struttura Big Data  
Professore: Giovanni Farina  
Studente: Alfredo La Rosa  
Tipologia: Financial Big Data Pipeline  

Questo progetto implementa una pipeline end-to-end per la raccolta, processamento e analisi di dati finanziari (prezzi intraday e news) utilizzando un'architettura a microservizi containerizzata.  

Architettura e Tecnologie  
La piattaforma è strutturata secondo il modello Data Lake (Bronze, Silver, Gold Layer) e utilizza:  

Orchestrazione: Docker & Docker Compose.  

Storage Distribuito: Hadoop HDFS.  

Processing: Apache Spark.  

Polyglot Persistence: Apache Cassandra (Serie temporali) e MongoDB (Documenti JSON).  

Overview Architetturale  

La pipeline implementa un flusso dati end-to-end:  

Data Source → HDFS (Bronze)  
→ Spark Processing (Silver)  
→ Cassandra / MongoDB (Gold)  
→ Analytics Engine (Decision Support System)  

Struttura del Progetto  
Plaintext  
.
├── docker-compose.yml       # Definizione dello stack tecnologico  
├── .env                     # Variabili d'ambiente  
├── scripts/  
│   ├── analytics/           # Engine decisionale (Join Mongo + Cassandra)  
│   ├── automation/          # Script di bootstrap per HDFS e DB NoSQL  
│   ├── ingestion/           # Script Python per download da Yahoo Finance  
│   └── processing/          # Job Spark per trasformazione Parquet e caricamento  
└── docs/                    # Documentazione e report di progetto  

Requisiti  

- Docker Desktop  
- Docker Compose  
- Python 3.10+  
- Connessione Internet (Yahoo Finance API)  
- RAM consigliata: ≥ 8GB  

Quick Start (Step-by-Step)
1. Avvio dell'Infrastruttura
Apri il terminale nella root del progetto ed esegui lo stack Docker:

PowerShell
docker-compose up -d

Verifica che tutti i 6 container siano in stato "Running" tramite Docker Desktop.

2. Inizializzazione (Bootstrap)
Esegui gli script di automazione per configurare le directory HDFS e gli schemi NoSQL:

PowerShell
.\scripts\automation\init_hdfs.ps1
.\scripts\automation\init_cassandra.ps1
.\scripts\automation\init_mongo.ps1
3. Ingestion dei Dati (Raw Layer)
Recupera i dati aggiornati dalle API esterne e caricali su HDFS:

PowerShell
# Ingestion prezzi (CSV) e news (JSON)
python .\scripts\ingestion\download_yahoo_prices_2min.py
python .\scripts\ingestion\download_yahoo_news.py
4. Processing e Caricamento (Silver & Gold Layer)
Esegui la pipeline Spark per trasformare i dati in Parquet e popolare i database operativi:

PowerShell
.\scripts\processing\run_pipeline.ps1

Questo script automatizza l'esecuzione di spark_to_parquet.py e spark_to_nosql.py nel cluster Spark.

5. Analisi Finale (Insight)
Genera il report decisionale incrociando i dati di MongoDB e Cassandra:

PowerShell
.\scripts\analytics\run_indicator.ps1

L'output mostrerà i segnali di trading (APPROVE/REJECT) basati sulla SMA-20.

Monitoraggio e Verifica

Hadoop Web UI: http://localhost:9870 (Browsing HDFS).

Spark Master UI: http://localhost:8080 (Monitoraggio Job).


MongoDB: docker exec -it mongodb mongosh (Validazione news).


Cassandra: docker exec -it cassandra cqlsh (Validazione prezzi).

Note sul Troubleshooting
In caso di problemi durante il caricamento delle news, verificare la presenza del flag --multiLine nel job Spark e assicurarsi che la struttura del JSON sorgente rispetti la navigazione nel campo content.

Studente: Alfredo La Rosa

Matricola: IN32000135
