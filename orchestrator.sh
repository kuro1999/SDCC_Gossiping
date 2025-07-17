#!/usr/bin/env sh
set -eux

# 1) pausa / unpausa di node2
echo "[ORCH] Waiting ${PAUSE_AFTER}s before pausing node2..."
sleep "${PAUSE_AFTER}"

echo "[ORCH] Pausing node2"
docker pause node2

echo "[ORCH] Sleeping for ${PAUSE_DURATION}s"
sleep "${PAUSE_DURATION}"

echo "[ORCH] Unpausing node2"
docker unpause node2

echo "[ORCH] Done pause/unpause cycle."

# 2) voluntary‐leave: lo faccio partire via TCP, non con i segnali
echo "[ORCH] Will send voluntary LEAVE to node3 in ${LEAVE_AFTER}s"
sleep "${LEAVE_AFTER}"

echo "[ORCH] Sending LEAVE to node"

# prima avviso il nodo stesso, così parte il gossip
printf "LEAVE %s\n" "node3:8000" | nc "node3" 8000

echo "killing:node4 in ${KILL_AFTER}s"
sleep "${KILL_AFTER}"
docker kill node4



echo "[ORCH] All orchestrations complete."