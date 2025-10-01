#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# Deploy Progetto SDCC (o altro repo) su EC2 clonando direttamente da GitHub.
#
# USO:
#   ./deploy_sdcc_ec2.sh <user> <public-ip> <repo-url> [branch]
#
# Esempio:
#   ./setup_ec2.sh ec2-user ec2-52-91-53-189.compute-1.amazonaws.com
#
# Assunzioni:
# - Hai una singola chiave .pem in ./key/ (come nel tuo script precedente).
# - Sul repo è presente un docker-compose (compose.yaml o docker-compose.yml).
# - Se il repo è privato via HTTPS, usa un PAT in REPO_URL (oppure usa URL SSH + chiave).
# ------------------------------------------------------------------------------

# ---- Logging semplice
log()  { echo "INFO: $*"; }
fail() { echo "ERROR: $*" >&2; exit 1; }

# ---- Argomenti
if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
  echo "Uso: $0 <user> <public-ip> <repo-url> [branch]"
  exit 1
fi
EC2_USER="ec2-user"
EC2_HOST="ec2-34-201-103-97.compute-1.amazonaws.com"
REPO_URL="https://github.com/kuro1999/SDCC_Gossiping.git"
REPO_BRANCH="main"

EC2_TARGET="${EC2_USER}@${EC2_HOST}"

# ---- Individua la chiave .pem in ./key come nel tuo script
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
KEY_DIR="${SCRIPT_DIR}/key"

log "Cerco la chiave .pem in ${KEY_DIR}..."
mapfile -d '' PEM_FILES < <(find "$KEY_DIR" -maxdepth 1 -type f -name "*.pem" -print0 || true)
[ "${#PEM_FILES[@]}" -eq 0 ] && fail "Nessuna chiave .pem trovata in ${KEY_DIR}"
[ "${#PEM_FILES[@]}" -gt 1 ] && { printf "Trovate più chiavi:\n - %s\n" "${PEM_FILES[@]##*/}"; fail "Rimuovi le chiavi extra e lascia una sola .pem"; }

SSH_KEY_PATH="${PEM_FILES[0]}"
log "Uso la chiave ${SSH_KEY_PATH}"

# Copia temporanea con permessi stretti
TEMP_KEY="$(mktemp)"
trap 'rm -f "$TEMP_KEY"' EXIT
cat "$SSH_KEY_PATH" > "$TEMP_KEY"
chmod 600 "$TEMP_KEY"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=30)

# ---- Test SSH
log "Verifico connettività SSH verso ${EC2_TARGET}..."
if ! ssh "${SSH_OPTS[@]}" -i "$TEMP_KEY" -o ConnectTimeout=10 "$EC2_TARGET" "exit" 2>/dev/null; then
  fail "Connessione fallita a ${EC2_TARGET}. Controlla IP, stato istanza e security group."
fi
log "SSH ok."

# ---- Variabili di deploy (puoi cambiare la directory di destinazione)
# Nome cartella dal nome repo (senza .git)
REPO_NAME="$(basename -s .git "$REPO_URL")"
PROJECT_DIR="/opt/${REPO_NAME}"

# ---- Script remoto: installa prerequisiti, clona/aggiorna repo, up compose
log "Eseguo setup remoto: pacchetti, repo e compose..."

ssh "${SSH_OPTS[@]}" -i "$TEMP_KEY" "$EC2_TARGET" \
  "export REPO_URL='$REPO_URL' REPO_BRANCH='$REPO_BRANCH' PROJECT_DIR='$PROJECT_DIR' REPO_NAME='$REPO_NAME'; bash -s" <<'REMOTE'
set -euo pipefail

# ----------------- helper -----------------
need_sudo() { [ "$(id -u)" -ne 0 ] && echo "sudo" || true; }
SUDO="$(need_sudo)"

pkg_detect() {
  if command -v apt-get >/dev/null 2>&1; then echo "apt";
  elif command -v dnf >/dev/null 2>&1; then echo "dnf";
  elif command -v yum >/dev/null 2>&1; then echo "yum";
  else echo "unknown"; fi
}

install_base() {
  case "$(pkg_detect)" in
    apt)
      $SUDO apt-get update -y
      $SUDO apt-get install -y git ca-certificates curl
      ;;
    dnf|yum)
      # Installa Git e ca-certificates. NON forzare 'curl' su AL2023.
      $SUDO "${PKG:-$(pkg_detect)}" -y install git ca-certificates

      # Installa 'curl' solo se né curl né curl-minimal sono presenti
      if ! command -v curl >/dev/null 2>&1 && ! rpm -q curl-minimal >/dev/null 2>&1; then
        $SUDO "${PKG:-$(pkg_detect)}" -y install curl || true
      fi
      ;;
    *)
      echo "Package manager non riconosciuto: installa manualmente git/curl." >&2
      ;;
  esac
}


install_docker() {
  if command -v docker >/dev/null 2>&1; then
    echo "[INFO] Docker già presente"
  else
    . /etc/os-release || true
    case "$ID" in
      amzn)
        if command -v amazon-linux-extras >/dev/null 2>&1; then
          $SUDO amazon-linux-extras install -y docker
        else
          $SUDO dnf install -y docker || $SUDO yum install -y docker
        fi
        ;;
      ubuntu|debian)
        $SUDO apt-get update -y
        $SUDO apt-get install -y docker.io docker-compose-plugin
        ;;
      rhel|rocky|almalinux|centos|fedora)
        $SUDO dnf install -y docker docker-compose-plugin || $SUDO yum install -y docker
        ;;
      *)
        # fallback ufficiale
        curl -fsSL https://get.docker.com | $SUDO sh
        ;;
    esac
  fi
  $SUDO systemctl enable docker
  $SUDO systemctl start docker || true
}

ensure_compose_plugin() {
  if docker compose version >/dev/null 2>&1; then
    echo "[INFO] Docker Compose v2 già installato"
    return 0
  fi
  # Installazione manuale del plugin v2 (fallback)
  if ! command -v curl >/dev/null 2>&1; then
    case "$(pkg_detect)" in
      apt) $SUDO apt-get install -y curl ;;
      dnf|yum) $SUDO "${PKG:-$(pkg_detect)}" -y install curl ;;
    esac
  fi
  dest="/usr/local/lib/docker/cli-plugins"
  arch="$(uname -m)"
  ver="v2.27.0"
  $SUDO mkdir -p "$dest"
  $SUDO curl -fsSL "https://github.com/docker/compose/releases/download/${ver}/docker-compose-linux-${arch}" -o "${dest}/docker-compose"
  $SUDO chmod +x "${dest}/docker-compose"
  echo "[INFO] Installato Docker Compose plugin ${ver}"
}

# ----------------- esecuzione -----------------
install_base
install_docker
ensure_compose_plugin

# Se l'utente corrente non è nel gruppo docker, consumeremo 'sudo docker' più giù
# (aggiungere al gruppo richiede logout/login per avere effetto)
if ! id -nG | grep -qw docker; then
  $SUDO usermod -aG docker "$(id -un)" || true
fi

# Clona o aggiorna il repo
if [ -d "$PROJECT_DIR/.git" ]; then
  echo "[INFO] Repo esistente in $PROJECT_DIR → fetch&reset"
  cd "$PROJECT_DIR"
  git fetch --all --prune
  git checkout "$REPO_BRANCH"
  git reset --hard "origin/$REPO_BRANCH"
else
  echo "[INFO] Clono $REPO_URL in $PROJECT_DIR"
  $SUDO mkdir -p "$PROJECT_DIR"
  $SUDO chown -R "$(id -u)":"$(id -g)" "$PROJECT_DIR"
  git clone --branch "$REPO_BRANCH" "$REPO_URL" "$PROJECT_DIR"
  cd "$PROJECT_DIR"
fi

# Se esiste .env.example e manca .env, crealo
if [ -f ".env.example" ] && [ ! -f ".env" ]; then
  cp .env.example .env
  echo "[INFO] Creato .env da .env.example (modifica i valori se necessario)"
fi

# Tenta compose con plugin v2; fallback a docker-compose v1 se presente
set +e
docker compose version >/dev/null 2>&1
HAS_V2=$?
set -e

if [ "$HAS_V2" -eq 0 ]; then
  echo "[INFO] Avvio servizi con 'docker compose up -d --build'"
  $SUDO docker compose up -d --build
else
  if command -v docker-compose >/dev/null 2>&1; then
    echo "[INFO] Avvio servizi con 'docker-compose up -d --build'"
    $SUDO docker-compose up -d --build
  else
    echo "Né 'docker compose' v2 né 'docker-compose' v1 disponibili." >&2
    exit 1
  fi
fi

echo "[OK] Deploy completato in $PROJECT_DIR"
REMOTE

log "Deploy completato."

# ---- Promemoria porte (non possiamo aprirle qui: va fatto nel Security Group)
echo
echo "Promemoria porte EC2 da aprire nel Security Group (in base al tuo progetto):"
echo " - Registry/Nodes UDP/TCP (esempi): 9000-9015"
echo " - Eventuali frontend/API (esempi): 8000, 8090"
echo
echo "Se usi compose con mapping porte, accedi con http://${EC2_HOST}:<porta-mappata>"