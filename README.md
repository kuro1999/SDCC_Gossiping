# SDCC_Gossiping
## Set up sistema locale

Per poter eseguire il setup locale del progetto bisogna eseguire il clone della repository tramite:
git clone https://github.com/kuro1999/SDCC_Gossiping.git

per l'esecuzione del progetto è necessario aver installato nel proprio sistema docker-compose per l'esecuzione dei container.
Dopo l'installazione di docker sulla propria macchina si può procedere con l'avvio locale del progetto facendo:

cd SDCC_Gossiping/gossip-fd

e successivamente se si vuole avviare il progetto nella sua versione base ovvero con il container registry ed i 7 container dei nodi bisogna usare:

docker compose up --build

che andrà a creare le 2 immagini necessarie:
1) **registry-image-sdcc**: ovvero l'immagine del registry(entrypoint centralizzato)
2) **node-image-sdcc**: ovvero l'immagine del nodo (componente decentralizzato)

e contemporaneamente istanzierà 8 container 1 per il registry e 7 per i nodi e così cominceranno le comunicazioni.

## Set up EC2
Avviata una istanza EC2, collegarsi via SSH e installare il progetto nell'istanza EC2. Un modo per installare il progetto è installare git con il comando **sudo yum install git -y** e successivamente installare il progetto con il comando **git clone https://github.com/kuro1999/SDCC_Gossiping.git**.
