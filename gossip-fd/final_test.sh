#!/bin/bash

# Avvia il servizio in background
docker-compose up --build

# Aspetta 120 secondi
sleep 120

# Ferma e rimuove container, immagini e volumi
docker-compose down --rmi all --volumes --remove-orphans

# Visualizza i log in un altro terminale
# docker-compose logs -f


#usa questo comando per simulare le kill
#docker-compose pause node7
#cd /GolandProjects/SDCC_Gossiping/gossip-fd