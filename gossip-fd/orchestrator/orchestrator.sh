#!/usr/bin/env bash

# =======================
# Parametri configurabili
# =======================

# Durata della pausa tra gli step
PAUSE_SECS=2

echo "===== [BOOTSTRAP] Avvio sequenza test ====="
sleep $PAUSE_SECS

# Messaggio di bootstrap
echo "[BOOTSTRAP] Inizio bootstrap della rete..."
sleep $PAUSE_SECS

# Chiamata al servizio 'sum' su node1
echo "[STEP] Chiamo servizio SUM su node1..."
ADDR="$(curl -sS "http://node1:9000/discover?service=calc" | jq -r '.[0].addr // empty' || true)"
if [ -n "${ADDR}" ]; then
  URL="http://${ADDR}/sum?a=5&b=5"
  echo "$(date -Iseconds) [CALL] ${URL}"
  RESP="$(curl -sS "${URL}" || true)"
  echo "$(date -Iseconds) [CALL][DONE] result=${RESP}"
else
  echo "$(date -Iseconds) [DISCOVER][WARN] nessun indirizzo trovato"
fi
sleep $PAUSE_SECS

# Registrazione del servizio 'calc' su node1
echo "[STEP] Registro servizio SUB su node1..."
REG_CODE="$(curl -sS -o /dev/null -w "%{http_code}" -X POST "http://node1:9000/service/register" \
  -H 'Content-Type: application/json' \
  -d "{\"service\":\"calc\",\"instance_id\":\"dyn-node1\",\"addr\":\"node1:18080\",\"ttl\":15}")"
if [ "${REG_CODE}" = "200" ]; then
  echo "$(date -Iseconds) [REGISTER][OK] http=${REG_CODE}"
else
  echo "$(date -Iseconds) [REGISTER][ERR] http=${REG_CODE}"
fi
sleep $PAUSE_SECS


# Chiamata al servizio 'sum' su node1
echo "[STEP] Chiamo servizio SUM su node1..."
ADDR="$(curl -sS "http://node1:9000/discover?service=calc" | jq -r '.[0].addr // empty' || true)"
if [ -n "${ADDR}" ]; then
  URL="http://${ADDR}/sum?a=5&b=5"
  echo "$(date -Iseconds) [CALL] ${URL}"
  RESP="$(curl -sS "${URL}" || true)"
  echo "$(date -Iseconds) [CALL][DONE] result=${RESP}"
else
  echo "$(date -Iseconds) [DISCOVER][WARN] nessun indirizzo trovato"
fi
sleep $PAUSE_SECS

# Chiamata al servizio 'sub'
echo "[STEP] Chiamo servizio SUB su node1..."
ADDR="$(curl -sS "http://node1:9000/discover?service=calc" | jq -r '.[0].addr // empty' || true)"
if [ -n "${ADDR}" ]; then
  URL="http://${ADDR}/sub?a=5&b=4"
  echo "$(date -Iseconds) [CALL] ${URL}"
  RESP="$(curl -sS "${URL}" || true)"
  echo "$(date -Iseconds) [CALL][DONE] result=${RESP}"
else
  echo "$(date -Iseconds) [DISCOVER][WARN] nessun indirizzo trovato"
fi
sleep $PAUSE_SECS

# Deregistra il servizio 'calc'
echo "[STEP] Deregistro servizio SUB..."
DEREG_CODE="$(curl -sS -o /dev/null -w "%{http_code}" -X POST "http://node1:9000/service/deregister" \
  -H 'Content-Type: application/json' \
  -d "{\"service\":\"calc\",\"instance_id\":\"dyn-node1\",\"addr\":\"node1:18080\"}")"
if [ "${DEREG_CODE}" = "200" ]; then
  echo "$(date -Iseconds) [DEREG][OK] http=${DEREG_CODE}"
else
  echo "$(date -Iseconds) [DEREG][ERR] http=${DEREG_CODE}"
fi
sleep $PAUSE_SECS


# Chiamata al servizio 'sum' su node1
echo "[STEP] Chiamo servizio SUM su node1..."
ADDR="$(curl -sS "http://node1:9000/discover?service=calc" | jq -r '.[0].addr // empty' || true)"
if [ -n "${ADDR}" ]; then
  URL="http://${ADDR}/sum?a=5&b=5"
  echo "$(date -Iseconds) [CALL] ${URL}"
  RESP="$(curl -sS "${URL}" || true)"
  echo "$(date -Iseconds) [CALL][DONE] result=${RESP}"
else
  echo "$(date -Iseconds) [DISCOVER][WARN] nessun indirizzo trovato"
fi
sleep $PAUSE_SECS

echo "===== [FINE] Sequenza completata ====="
