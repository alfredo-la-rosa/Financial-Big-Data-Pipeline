// Ho scelto di utilizzare il database 'market' per separare logicamente i dati finanziari
db = db.getSiblingDB('market');

// Creo la collezione 'news' in modo esplicito per una migliore gestione del namespace
db.createCollection('news');

// Questa è la mia scelta tecnica fondamentale: ho impostato un indice unico composto.
// In questo modo, garantisco l'idempotenza della mia pipeline: se dovessi rilanciare 
// lo script Spark, non caricherò mai la stessa notizia due volte, evitando dati ridondanti.
db.news.createIndex(
    { "ticker": 1, "title": 1 }, 
    { unique: true }
);

// Ho aggiunto un indice discendente sul timestamp per velocizzare le mie future query 
// di analisi, ordinando le notizie dalla più recente alla meno recente.
db.news.createIndex({ "ts": -1 }); 

print("--- Configurazione MongoDB completata: ho preparato il database 'market' ---");