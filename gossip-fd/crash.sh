#!/bin/bash

# Avvia il servizio in background
docker-compose up --build -d

echo "killing node1 in 10s"
# Pausa di 10 secondi per permettere al servizio di avviarsi correttamente
sleep 10

# Uccidi il container "node1"
docker-compose kill node1
echo "node1 killed"

# Pausa di 20 secondi per osservare gli effetti del kill
sleep 35
echo"stopping simulation"
# Ferma e rimuove container, immagini e volumi
docker-compose down --rmi all --volumes --remove-orphans

# docker-compose logs -f
