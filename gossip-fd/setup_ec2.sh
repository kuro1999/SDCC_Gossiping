#!/bin/bash

# Deployment script for EC2 instance to setup the project and run Docker Compose
#
# Usage: ./setup_ec2.sh <public-ip>
#   <public-ip>  Public IPv4 address of the EC2 instance
#
# This script assumes you have exactly one .pem file in a sibling directory called
# `keys/` which contains your private EC2 key. It will package the current project,
# copy it to the EC2 instance via scp, install Docker, Docker Compose, and Git on the remote machine if necessary,
# clone the GitHub repository, and run `docker compose up -d --build` to start the services.

set -e

# Define script and key directory
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
KEY_DIR="${SCRIPT_DIR}/keys"
REPO_URL="https://github.com/kuro1999/SDCC_Gossiping.git"
PROJECT_NAME="SDCC_Gossiping"
PROJECT_DIR="/home/ec2-user/${PROJECT_NAME}"

# Logging helpers
log_info() {
    echo "INFO: $1"
}

log_error() {
    echo "ERROR: $1" >&2
    exit 1
}

# Validate arguments
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <public-ip>"
    exit 1
fi

EC2_HOST=$1
EC2_USER="ec2-user"
EC2_TARGET="${EC2_USER}@${EC2_HOST}"

# Locate the .pem key
log_info "Searching for .pem key in ${KEY_DIR}..."
PEM_FILES=()
while IFS= read -r -d $'\0'; do
    PEM_FILES+=("$REPLY")
done < <(find "$KEY_DIR" -maxdepth 1 -type f -name "*.pem" -print0)
if [ "${#PEM_FILES[@]}" -eq 0 ]; then
    log_error "No .pem key found in ${KEY_DIR}."
fi
if [ "${#PEM_FILES[@]}" -gt 1 ]; then
    echo "Found keys:"
    printf " - %s\n" "${PEM_FILES[@]##*/}"
    log_error "Multiple .pem keys found; ensure only one is present."
fi
SSH_KEY_PATH="${PEM_FILES[0]}"
log_info "Using key ${SSH_KEY_PATH}"

# Create a secure temporary copy of the key
TEMP_KEY=$(mktemp)
trap 'rm -f "$TEMP_KEY"' EXIT
cat "$SSH_KEY_PATH" > "$TEMP_KEY"
chmod 600 "$TEMP_KEY"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)

# Test SSH connectivity
log_info "Testing SSH connectivity to ${EC2_TARGET}..."
if ! ssh "${SSH_OPTS[@]}" -i "$TEMP_KEY" -o ConnectTimeout=10 "$EC2_TARGET" "exit" 2>/dev/null; then
    log_error "Unable to connect to ${EC2_TARGET}. Check instance state, IP and security group."
fi
log_info "SSH connection OK."

# Install prerequisites on remote: Docker, Compose plugin, and Git
log_info "Ensuring Docker, Docker Compose and Git are installed on remote..."
ssh "${SSH_OPTS[@]}" -i "$TEMP_KEY" "$EC2_TARGET" << 'REMOTESETUP'
set -e
# Determine package manager
if command -v dnf &>/dev/null; then
    PKG=dnf
elif command -v yum &>/dev/null; then
    PKG=yum
elif command -v apt-get &>/dev/null; then
    PKG=apt-get
else
    echo "Unsupported package manager." >&2
    exit 1
fi

install_pkgs() {
    if [ "$PKG" = "apt-get" ]; then
        sudo apt-get update -y
        sudo apt-get install -y "$@"
    else
        sudo "$PKG" install -y "$@"
    fi
}

# Docker
if ! command -v docker &>/dev/null; then
    echo "Installing Docker..."
    if [ "$PKG" = "apt-get" ]; then
        install_pkgs docker.io
    else
        install_pkgs docker
    fi
    sudo systemctl start docker
    sudo systemctl enable docker
    sudo usermod -aG docker "$USER"
fi

# Docker Compose plugin
if ! docker compose version &>/dev/null; then
    echo "Installing Docker Compose v2 plugin..."
    if ! command -v curl &>/dev/null; then
        install_pkgs curl
    fi
    dest="/usr/local/lib/docker/cli-plugins"
    sudo mkdir -p "$dest"
    ver=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep -oP '"tag_name": "\K(v[0-9]+\.[0-9]+\.[0-9]+)')
    [ -z "$ver" ] && ver="v2.27.0"
    sudo curl -SL "https://github.com/docker/compose/releases/download/${ver}/docker-compose-linux-$(uname -m)" -o "${dest}/docker-compose"
    sudo chmod +x "${dest}/docker-compose"
fi

# Git
if ! command -v git &>/dev/null; then
    echo "Installing Git..."
    install_pkgs git
fi
REMOTESETUP

log_info "Remote prerequisites installed."

# Clone the GitHub repository
log_info "Cloning GitHub repository..."
ssh "${SSH_OPTS[@]}" -i "$TEMP_KEY" "$EC2_TARGET" "git clone ${REPO_URL} ${PROJECT_DIR}"

# Write .env file for frontend config
log_info "Writing .env file on remote host..."
ssh "${SSH_OPTS[@]}" -i "$TEMP_KEY" "$EC2_TARGET" "echo \"REACT_APP_API_BASE_URL=http://${EC2_HOST}:8000\" > ${PROJECT_DIR}/.env"

# Start Docker Compose
log_info "Starting services via docker compose..."
ssh "${SSH_OPTS[@]}" -i "$TEMP_KEY" "$EC2_TARGET" "cd ${PROJECT_DIR} && (docker compose version &>/dev/null && sudo docker compose up -d --build || sudo docker-compose up -d --build)"

log_info "Deployment finished!"
echo "Application (frontend) URL: http://${EC2_HOST}:8090"
echo "API gateway URL:       http://${EC2_HOST}:8000"
