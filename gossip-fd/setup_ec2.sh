#!/bin/bash

# Aggiornamento del sistema
echo "Updating system..."
sudo yum update -y

# Installazione di Git
echo "Installing Git..."
sudo yum install -y git

# Installazione di Docker
echo "Installing Docker..."
sudo yum install -y docker

# Avvio di Docker
echo "Starting Docker..."
sudo service docker start

# Aggiunta dell'utente al gruppo Docker (opzionale)
echo "Adding user to Docker group..."
sudo usermod -aG docker ec2-user

# Verifica di Docker
echo "Verifying Docker installation..."
docker --version
docker run hello-world

# Installazione di altre dipendenze se necessarie (modifica se il tuo progetto ha altre dipendenze)
echo "Installing additional dependencies..."
sudo yum install -y gcc make

# Clonazione del repository Git (sostituisci con il tuo link)
echo "Cloning GitHub repository..."
git clone https://github.com/kuro1999/SDCC_Gossiping.git

# Naviga nella cartella del progetto
# shellcheck disable=SC2164
cd SDCC_Gossiping
cd gossip-fd

# Costruzione dell'immagine Docker (assicurati che ci sia un Dockerfile)
echo "Building Docker image..."
docker build -Dockerfile .

# Esecuzione del container Docker
echo "Running Docker container..."
docker run -d SDCC_Gossiping

echo "Setup completed successfully!"
