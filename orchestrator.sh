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
echo "[ORCH] Will send voluntary LEAVE to ${TARGET} in ${LEAVE_AFTER}s"
sleep "${LEAVE_AFTER}"

echo "[ORCH] Sending LEAVE to ${TARGET}"

# prima avviso il nodo stesso, così parte il gossip
printf "LEAVE %s\n" "${TARGET}:8000" | nc "${TARGET}" 8000


echo "[ORCH] All orchestrations complete."