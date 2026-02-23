-- Ho definito un Keyspace con fattore di replicazione 1, ideale per il mio ambiente di test locale
CREATE KEYSPACE IF NOT EXISTS market 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE market;

-- Ho progettato questa tabella per gestire serie storiche ad alta frequenza (2 minuti).
-- Ho preferito il tipo 'timestamp' rispetto a 'date' per non perdere la precisione del minuto.
CREATE TABLE IF NOT EXISTS prices_by_ticker (
    ticker text,
    ts timestamp, 
    open double,
    high double,
    low double,
    close double,
    volume bigint,
    -- La mia scelta per la Primary Key: partiziono per 'ticker' per distribuire il carico 
    -- e uso 'ts' come clustering key per avere i dati già ordinati cronologicamente sul disco.
    PRIMARY KEY ((ticker), ts)
) WITH CLUSTERING ORDER BY (ts DESC);