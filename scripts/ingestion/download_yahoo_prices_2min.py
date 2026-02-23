import yfinance as yf
import pandas as pd
from hdfs import InsecureClient
from io import StringIO

# --- CONFIGURAZIONE INFRASTRUTTURA ---
# Configuro l'accesso a HDFS. Utilizzo 'localhost' sulla porta 9870 perché, 
# pur essendo in Docker, lo script gira sul mio PC (Host) e comunica con il NameNode 
# tramite l'interfaccia WebHDFS che ho mappato nel docker-compose.
HDFS_URL = 'http://localhost:9870' 
HDFS_USER = 'root' 
HDFS_PATH = '/data/raw/prices/prices_intraday_2m.csv'

# Selezione di un portafoglio diversificato (Tech USA e Mercato Italiano) 
# per testare la capacità del sistema di gestire ticker con estensioni differenti.
tickers = ["AAPL", "NVDA", "TSLA", "ENI.MI", "ISP.MI"]

def ingest_to_hdfs_docker():
    # Inizializzo il client per interagire con il file system distribuito di Hadoop.
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    all_data = []
    
    print(f"--- START INGESTION (DOCKER SETUP) ---")
    
    for ticker in tickers:
        print(f"Scaricamento {ticker}...", end=" ", flush=True)
        # Scarico dati intraday con granularità a 2 minuti per gli ultimi 60 giorni.
        # Questa scelta simula un carico di lavoro "Big Data" ad alta frequenza.
        df = yf.download(ticker, period="60d", interval="2m", progress=False)
        
        if df.empty:
            print("FALLITO")
            continue
            
        # --- NORMALIZZAZIONE E PULIZIA (Pre-processing) ---
        # Resetto l'indice per trasformare il Timestamp da indice a colonna esplicita.
        df = df.reset_index()
        
        # Yahoo Finance restituisce spesso colonne MultiIndex (es. 'Close', 'AAPL').
        # Ho implementato un controllo per "appiattire" i nomi delle colonne prendendo 
        # solo il primo livello, rendendo il DataFrame compatibile con Spark.
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        # Standardizzo i nomi delle colonne in minuscolo per evitare errori di case-sensitivity
        # tra i vari database (HDFS, Spark, Cassandra). Rinomino 'date' o 'datetime' 
        # in un generico 'timestamp' per uniformità.
        df.columns = df.columns.str.lower()
        df = df.rename(columns={'datetime': 'timestamp', 'date': 'timestamp'})
        
        # Aggiungo la colonna 'ticker' per poter distinguere i dati una volta unificati.
        df['ticker'] = ticker
        
        all_data.append(df)
        print(f"OK ({len(df)} righe)")

    if all_data:
        # Concateno tutti i DataFrame in un unico dataset globale.
        final_df = pd.concat(all_data, ignore_index=True)
        
        # --- OTTIMIZZAZIONE I/O (Memory Buffer) ---
        # Utilizzo 'StringIO' per convertire il DataFrame in CSV direttamente in memoria RAM.
        # Questa scelta è fondamentale: evita di scrivere file temporanei sul disco locale,
        # accelerando il processo di trasferimento verso Hadoop.
        csv_buffer = StringIO()
        final_df.to_csv(csv_buffer, index=False)
        
        print(f"\nTentativo di scrittura su HDFS (Docker)...", end=" ")
        try:
            # Scrittura diretta nel Data Lake. Uso 'overwrite=True' per garantire che il 
            # Bronze Layer contenga sempre la versione più recente dei dati grezzi.
            with client.write(HDFS_PATH, overwrite=True) as writer:
                writer.write(csv_buffer.getvalue().encode('utf-8'))
            print("SUCCESSO!")
            print(f"File salvato in: {HDFS_PATH}")
        except Exception as e:
            # Gestione degli errori di rete: se il container namenode non è raggiungibile,
            # lo script avvisa l'utente invece di crashare silenziosamente.
            print(f"\nERRORE DI CONNESSIONE: {e}")
            print("\nSUGGERIMENTO: Assicurati che il container 'namenode' sia attivo.")
    else:
        print("Nessun dato raccolto.")

if __name__ == "__main__":
    ingest_to_hdfs_docker()