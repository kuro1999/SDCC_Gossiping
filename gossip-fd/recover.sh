#!/bin/bash

# Avvia il servizio in background
docker-compose up --build -d

echo "pausing node1 in 10s"
# Pausa di 10 secondi per permettere al servizio di avviarsi correttamente
sleep 10

# Uccidi il container "node1"
docker-compose pause node1
echo "node1 paused"
echo "unpausing node1 in 10s"
sleep 10
docker-compose unpause node1
# Pausa di 35 secondi per osservare gli effetti del recover
sleep 35
echo"stopping simulation"
# Ferma e rimuove container, immagini e volumi
docker-compose down --rmi all --volumes --remove-orphans

# --- Se vuoi vedere i log in un altro terminale, usa il comando qui sotto ---
# docker-compose logs -f
