# SDCC_Gossiping
## Set up sistema locale

Per poter eseguire il setup locale del progetto bisogna eseguire il clone della repository tramite:

**`git clone https://github.com/kuro1999/SDCC_Gossiping.git`**

per l'esecuzione del progetto è necessario aver installato nel proprio sistema docker-compose per l'esecuzione dei container.
Dopo l'installazione di docker sulla propria macchina si può procedere con l'avvio locale del progetto facendo:

`cd SDCC_Gossiping/gossip-fd`

successivamente se si vuole vedere il normale funzionamento del sistema basta eseguire il comando:

`docker-compose up --build`

se si vuole testare singolarmente i meccanismi implementati vedere la sezione di testing del readme

## Set up EC2
Avviata una istanza EC2, collegarsi via SSH e installare il progetto nell'istanza EC2. Un modo per installare il progetto è installare git con il comando **`sudo yum install git -y`** e successivamente installare il progetto con il comando

`git clone https://github.com/kuro1999/SDCC_Gossiping.git`

Successivamente si procede con l'installazione dei software necessari per il funzionamento del progetto tramite:

`sudo apt update`

`sudo apt install docker.io docker-compose -y`

Per eseguire i singoli script per testare il sistema bisogna prima fornire i permessi tramite il comando:

`sudo chmod +x ${name_script}.sh`

# Test del Sistema SDCC

Questo progetto include una serie di script che permettono di testare le diverse funzionalità del sistema, mostrando come i vari componenti interagiscono tra loro e come il sistema reagisce a diversi scenari. Gli script sono divisi in varie categorie e ciascuno si concentra su un aspetto specifico del progetto.

## Script di Test

### 1. **`services.sh`**
Questo script esegue la build di 5 container: uno per il **registry**, tre per i nodi (`node1`, `node2`, `node3`), e uno per `test_services`, che si occupa di effettuare le chiamate HTTP per il testing dei servizi.

In particolare, lo script eseguirà le seguenti operazioni:
- Verifica i servizi **attivi** di tipo `SERVICE_NAME` (ad esempio `calc`) su un nodo locale.
- Verifica i servizi **presenti nella rete**.
- **Registra** un nuovo servizio.
- **Chiama** un servizio registrato.
- **Deregistra** un servizio.
- Verifica i servizi **locali** e **nella rete** dopo ogni operazione.

### 2. **`crash.sh`**
Questo script costruisce 9 container: uno per il **registry**, 7 per i nodi e uno per `test_services`. Successivamente, esegue il comando per "uccidere" il container `node1`, simulando il fallimento di un nodo e mostrando l'effetto che questo ha sulla rete.

### 3. **`multi_crash.sh`**
Simile a `crash.sh`, questo script costruisce lo stesso numero di container, ma esegue **più kill** di container a intervalli di tempo diversi. Questo permette di testare l'effetto di più fallimenti simultanei sulla rete e osservare la reazione del sistema.

### 4. **`recover.sh`**
Questo script costruisce lo stesso numero di container di `crash.sh` e mette in pausa `node1` utilizzando il comando:

`docker-compose pause node1`

Dopo aver atteso che gli altri nodi dichiarino node1 come DEAD, esegue:

`docker-compose unpause node1`

per verificare l'effetto del meccaniscmo di recover sulla rete.


### Variabili Ambiente

Di seguito sono elencate tutte le variabili ambiente che vengono impostate in ogni container, necessarie per configurare il comportamento del programma.

#### Global / Registry
- **`NETWORK_NAME`**: Nome della rete per il gossiping.  

- **`REGISTRY_HOST`**: Nome host del registry.  

- **`REGISTRY_PORT`**: Porta su cui il registry è in ascolto.  

- **`REG_FANOUT`**: Numero di peer che il registry deve restituire a un nuovo nodo (se impostato a 0 entra in modalità dinamica restituendo log2(N)+1).  

#### Common Node Settings
- **`GOSSIP_INTERVAL`**: Intervallo di tempo tra l'invio di messaggi di gossip (in millisecondi).  

- **`HEARTBEAT_INTERVAL`**: Intervallo di tempo per l'aggiornamento dello stato del nodo (in millisecondi).  

- **`SUSPECT_TIMEOUT`**: Timeout per considerare un nodo come sospetto (in millisecondi).  

- **`DEAD_TIMEOUT`**: Timeout per considerare un nodo come morto (in millisecondi).  

- **`SERVICE_TTL`**: Time-to-live per i servizi registrati (in secondi).  

- **`SERVICE_REFRESH_TIMEOUT`**: Timeout per il refresh del servizio (in secondi).  

- **`MAX_SERVICE_DIGEST`**: Numero massimo di servizi da includere nel digest.  

- **`MAX_DIGEST_PEERS`**: Numero massimo di peer nel digest.  

- **`FANOUT_K`**: Numero di nodi da contattare per il gossiping (se impostato a 0 entra in modalità dinamica restituendo log2(N)+1).  

- **`QUORUM_K`**: Numero di nodi necessari per raggiungere il quorum.  

- **`VOTE_WINDOW`**: Finestra di tempo in cui i voti sono validi (in secondi).  

- **`CERT_TTL`**: Durata del certificato di morte (in secondi).  

- **`MAX_VOTE_DIGEST`**: Numero massimo di voti da includere nel digest.  

- **`MAX_CERT_DIGEST`**: Numero massimo di certificati di morte nel digest.  

- **`CERT_PRIORITY_ROUNDS`**: Numero di round di priorità per i certificati di morte.  

- **`VOTE_PRIORITY_ROUNDS`**: Numero di round di priorità per i voti.  

#### Node-specific Variables

##### NodeX
- **`NODEX_ID`**: ID del nodo X.  

- **`NODEX_PORT`**: Porta su cui il nodo X è in ascolto.  

- **`NODEX_CALC_PORT`**: Porta per il servizio di calcolo di nodo X.  

- **`NODEX_SERVICES`**: Servizi offerti dal nodo X in partenza.