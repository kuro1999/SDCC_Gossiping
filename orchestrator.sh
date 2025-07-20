#!/usr/bin/env sh
set -eux

# 1) pausa / unpausa di node2
echo "[ORCH] Waiting ${PAUSE_AFTER}s before pausing node2..."
sleep "${PAUSE_AFTER}"

echo "[ORCH] Pausing ${TARGET_PAUSE}"
docker pause "${TARGET_PAUSE}"

echo "[ORCH] Sleeping for ${PAUSE_DURATION}s"
sleep "${PAUSE_DURATION}"

echo "[ORCH] Unpausing ${TARGET_PAUSE}"
docker unpause "${TARGET_PAUSE}"

echo "[ORCH] Done pause/unpause cycle."

# 2) voluntary‐leave: lo faccio partire via TCP, non con i segnali
echo "[ORCH] Will send voluntary LEAVE to ${TARGET_LEAVE} in ${LEAVE_AFTER}s"
sleep "${LEAVE_AFTER}"

echo "[ORCH] Sending LEAVE to ${TARGET_LEAVE}"

# prima avviso il nodo stesso, così parte il gossip
printf "LEAVE %s\n" "${TARGET_LEAVE}:8000" | nc "$TARGET_LEAVE" 8000

echo "killing: ${TARGET_KILL} in ${KILL_AFTER}s"
sleep "${KILL_AFTER}"
docker kill "${TARGET_KILL}"



echo "[ORCH] All orchestrations complete."