#!/bin/bash

# Avvia il servizio in background
docker-compose up --build -d

echo "killing node1 in 10s"
# Pausa di 10 secondi per permettere al servizio di avviarsi correttamente
sleep 10

# Uccidi il container "node1"
docker-compose kill node1
echo "node1 killed"
echo "killing node2 and node 3 in 5s"
sleep 5
docker-compose kill node2
docker-compose kill node3


# Pausa di 20 secondi per osservare gli effetti del kill
sleep 40
echo"stopping simulation"
# Ferma e rimuove container, immagini e volumi
docker-compose down --rmi all --volumes --remove-orphans

# --- Se vuoi vedere i log in un altro terminale, usa il comando qui sotto ---
# docker-compose logs -f
