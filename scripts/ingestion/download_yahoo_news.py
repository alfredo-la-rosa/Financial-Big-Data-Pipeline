import yfinance as yf
import json
from hdfs import InsecureClient
from datetime import datetime
# --- CONFIGURAZIONE INFRASTRUTTURA ---
# Configuro l'accesso al mio Data Lake. Punto a localhost sulla porta 9870 
# perché comunico dal mio PC host verso il container Namenode tramite WebHDFS.
HDFS_URL = 'http://localhost:9870' 
HDFS_USER = 'root'
HDFS_PATH = '/data/raw/news/market_news.json'

# --- DEFINIZIONE TARGET ---
# Per simulare un ambiente "Big Data" reale, ho esteso la lista a 28 titoli globali.
# Ho incluso i giganti del NASDAQ (AAPL, NVDA), i principali player del FTSE MIB 
# (ENI, RACE) e altri leader di settore per avere un volume di news significativo.
tickers = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "V", "JNJ",
    "ENI.MI", "ISP.MI", "UCG.MI", "ENEL.MI", "STLAM.MI", "RACE.MI", "G.MI", "A2A.MI",
    "AMD", "INTC", "NFLX", "PYPL", "BABA", "DIS", "BA", "CSCO", "PEP", "KO"
]

def ingest_news_to_hdfs():
    # Inizializzo il client HDFS per interagire con il file system distribuito di Hadoop.
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    all_news = []
    
    print("--- START NEWS INGESTION (HDFS) ---")
    
    for t in tickers:
        print(f"Recupero news per {t}...", end=" ", flush=True)
        try:
            # Interrogo l'oggetto Ticker di Yahoo Finance per estrarre i feed informativi.
            stock = yf.Ticker(t)
            news_list = stock.news
            
            if not news_list:
                print("Nessuna news trovata.")
                continue
                
            for n in news_list:
                # 1. Accesso sicuro al dizionario 'content'
                content = n.get("content")
                if not content or not isinstance(content, dict):
                    continue # Salta questa news se il contenuto non è un dizionario valido
                
                # 2. Recupero sicuro del timestamp
                raw_ts = content.get("pubDate")
                publish_time = raw_ts if raw_ts else datetime.now().isoformat()
                
                # 3. Recupero sicuro del link
                click_url_obj = content.get("clickThroughUrl")
                link = click_url_obj.get("url") if click_url_obj else content.get("canonicalUrl")

                # 4. Creazione oggetto finale
                news_obj = {
                    "ticker": t,
                    "title": content.get("title"),
                    "publisher": content.get("publisher"),
                    "link": link,
                    "publish_time": publish_time,
                    "type": content.get("contentType"),
                    "ingestion_date": datetime.now().isoformat()
                }
                all_news.append(news_obj)
            print(f"OK ({len(news_list)} news)")
            
        except Exception as e:
            # Ho inserito un blocco try-except per ogni ticker: se il download di un titolo 
            # fallisce, la pipeline non si ferma e procede con il successivo.
            print(f"ERRORE su {t}: {e}")

    if all_news:
        # Trasformo l'intera lista di oggetti news in una stringa JSON formattata.
        # L'indentazione a 4 rende il file leggibile anche per ispezioni manuali su HDFS.
        json_data = json.dumps(all_news, indent=4)
        
        print(f"\nScrittura file JSON su HDFS ({HDFS_PATH})...", end=" ")
        try:
            # Scrittura finale nel Data Lake. Uso la codifica utf-8 per gestire 
            # correttamente caratteri speciali o simboli valuta nei titoli.
            with client.write(HDFS_PATH, overwrite=True) as writer:
                writer.write(json_data.encode('utf-8'))
            print("SUCCESSO!")
        except Exception as e:
            print(f"FALLITO: {e}")
    else:
        print("Nessun dato raccolto.")

if __name__ == "__main__":
    ingest_news_to_hdfs()