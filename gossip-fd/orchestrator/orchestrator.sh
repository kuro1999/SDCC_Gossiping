#!/usr/bin/env bash
set -euo pipefail

# === Config via env ===
: "${DISCOVERY_NODES:=node1:8080,node2:8081,node3:8082}"  # nodi per /discover
: "${SERVICE:=calc}"                                       # nome servizio da cercare
: "${INTERVAL:=5}"                                         # secondi tra un giro e l'altro
: "${JITTER_MS:=500}"                                      # jitter extra in millisecondi
: "${OPS:=sum,sub}"                                        # operazioni da invocare (comma-separate)
: "${A_MIN:=0}" : "${A_MAX:=100}"                          # range param a
: "${B_MIN:=0}" : "${B_MAX:=100}"                          # range param b
: "${STARTUP_WAIT:=8}"                                     # attesa iniziale per bootstrap cluster
: "${STOP_INTERVAL:=30}"                                   # secondi prima di fermare un nodo
: "${START_INTERVAL:=15}"                                  # secondi dopo fermare prima di riavviare
: "${NODES_TO_STOP:=node2}"                                # nodi da fermare (comma separati)

IFS=',' read -ra NODES <<< "$DISCOVERY_NODES"
IFS=',' read -ra OPS_ARR <<< "$OPS"
IFS=',' read -ra NODES_STOP <<< "$NODES_TO_STOP" # nodi da fermare

# --- funzioni utili ---

rand_between() { # intero in [min, max]
  local min=$1 max=$2
  if (( max < min )); then max=$min; fi
  echo $(( min + (RANDOM % (max - min + 1)) ))
}

pick_random_node() {
  local i=$(( RANDOM % ${#NODES[@]} ))
  echo "${NODES[$i]}"
}

pick_random_op() {
  local i=$(( RANDOM % ${#OPS_ARR[@]} ))
  echo "${OPS_ARR[$i]}"
}

discover_addrs() {
  local node=$1
  curl -fsS "http://$node/discover?service=$SERVICE" | jq -r '.[].addr'
}

invoke_service() {
  local addr=$1 op=$2
  local a b url out
  a=$(rand_between "$A_MIN" "$A_MAX")
  b=$(rand_between "$B_MIN" "$B_MAX")
  url="http://$addr/$op?a=$a&b=$b"
  if out=$(curl -fsS "$url"); then
    echo "$(date -Iseconds) OK  addr=$addr op=$op a=$a b=$b -> $out"
  else
    echo "$(date -Iseconds) ERR addr=$addr op=$op a=$a b=$b"
    return 1
  fi
}

sleep_ms() { local ms=$1; awk -v ms="$ms" 'BEGIN{system("sleep " ms/1000)}'; }

# --- Gestione nodi ---

stop_nodes() {
  echo "$(date -Iseconds) Fermando i nodi: ${NODES_STOP[@]}..."
  for node in "${NODES_STOP[@]}"; do
    docker compose stop "$node" || true
  done
  echo "$(date -Iseconds) Nodi fermati: ${NODES_STOP[@]}"
}

restart_nodes() {
  echo "$(date -Iseconds) Riavviando i nodi: ${NODES_STOP[@]}..."
  for node in "${NODES_STOP[@]}"; do
    docker compose start "$node" || true
  done
  echo "$(date -Iseconds) Nodi riavviati: ${NODES_STOP[@]}"
}

# --- main loop ---

echo "Orchestrator: attendo ${STARTUP_WAIT}s per bootstrap..."
sleep "$STARTUP_WAIT"

while true; do
  node=$(pick_random_node)
  addrs=$(discover_addrs "$node" || true)
  if [[ -z "${addrs:-}" ]]; then
    echo "$(date -Iseconds) WARN: nessuna istanza '$SERVICE' da $node"
  else
    mapfile -t ARR <<< "$addrs"
    addr="${ARR[$((RANDOM % ${#ARR[@]}))]}"
    op=$(pick_random_op)
    invoke_service "$addr" "$op" || true
  fi

  # Fermiamo i nodi dopo un intervallo di tempo
  echo "$(date -Iseconds) Attesa di $STOP_INTERVAL secondi prima di fermare i nodi..."
  sleep "$STOP_INTERVAL"
  stop_nodes

  # Pausa, con jitter
  sleep "$INTERVAL"
  jitter=$(rand_between 0 "$JITTER_MS")
  sleep_ms "$jitter"

  # Riavviamo i nodi dopo un intervallo di tempo
  echo "$(date -Iseconds) Attesa di $START_INTERVAL secondi prima di riavviare i nodi..."
  sleep "$START_INTERVAL"
  restart_nodes

  # Pausa finale prima del prossimo giro
  sleep "$INTERVAL"
  jitter=$(rand_between 0 "$JITTER_MS")
  sleep_ms "$jitter"
done
