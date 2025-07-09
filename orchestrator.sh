#!/usr/bin/env sh
set -eux

echo "[ORCH] Waiting ${PAUSE_AFTER}s before pausing node2..."
sleep "${PAUSE_AFTER}"

echo "[ORCH] Pausing node2"
docker pause node2

echo "[ORCH] Sleeping for ${PAUSE_DURATION}s"
sleep "${PAUSE_DURATION}"

echo "[ORCH] Unpausing node2"
docker unpause node2

echo "[ORCH] Done."
