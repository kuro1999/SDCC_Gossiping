#!/bin/bash

# Avvia il servizio in background
docker-compose up --build test_services -d

# Aspetta 36 secondi
sleep 120

# Ferma e rimuove container, immagini e volumi
docker-compose down --rmi all --volumes --remove-orphans

# Visualizza i log in un altro terminale
# docker-compose logs -f

#cd /GolandProjects/SDCC_Gossiping/gossip-fd