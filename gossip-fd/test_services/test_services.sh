#!/usr/bin/env bash

# =======================
# Parametri configurabili
# =======================
PAUSE_SECS=3

echo "===== [BOOTSTRAP] Avvio sequenza test ====="
sleep $PAUSE_SECS

echo "[BOOTSTRAP] Inizio bootstrap della rete..."
sleep $PAUSE_SECS

echo "[STEP] Chiamo SUM su node1..."
URL="http://node1:18080/sum?a=5&b=5"
echo "$(date -Iseconds) [CALL] ${URL}"
RESP="$(curl -sS "${URL}" || true)"
echo "$(date -Iseconds) [CALL][DONE] result=${RESP}"
sleep $PAUSE_SECS


# --- servizi locali PRIMA della registrazione ---
echo "[STEP] Servizi locali su node1 (PRIMA della registrazione)..."
URL_SL_PRIMA="http://node1:9000/services/local"
echo "$(date -Iseconds) [GET] ${URL_SL_PRIMA}"
RESP="$(curl -sS "${URL_SL_PRIMA}" || true)"
if [ -n "${RESP}" ]; then
  echo "${RESP}" | jq .
else
  echo "$(date -Iseconds) [WARN] nessuna risposta da /services/local"
fi
sleep $PAUSE_SECS

# --- discover PRIMA della registrazione ---
echo "[STEP] Discover calc (PRIMA della registrazione)..."
URL_DISC_PRIMA="http://node1:9000/discover?service=calc"
echo "$(date -Iseconds) [GET] ${URL_DISC_PRIMA}"
RESP="$(curl -sS "${URL_DISC_PRIMA}" || true)"
if [ -n "${RESP}" ]; then
  echo "${RESP}" | jq .
else
  echo "$(date -Iseconds) [WARN] nessuna risposta da /discover"
fi
sleep $PAUSE_SECS


echo "[STEP] Registro servizio CALC su node1..."
REG_CODE="$(curl -sS -o /dev/null -w "%{http_code}" -X POST "http://node1:9000/service/register" \
  -H 'Content-Type: application/json' \
  -d "{\"service\":\"calc\",\"instance_id\":\"dyn-node1\",\"addr\":\"node1:18080\"}")"
if [ "${REG_CODE}" = "200" ]; then
  echo "$(date -Iseconds) [REGISTER][OK] http=${REG_CODE}"
else
  echo "$(date -Iseconds) [REGISTER][ERR] http=${REG_CODE}"
fi
sleep $PAUSE_SECS

# --- Sonda: servizi locali DOPO la registrazione ---
echo "[STEP] Servizi locali su node1 (DOPO la registrazione)..."
URL_SL_DOPO_REG="http://node1:9000/services/local"
echo "$(date -Iseconds) [GET] ${URL_SL_DOPO_REG}"
RESP="$(curl -sS "${URL_SL_DOPO_REG}" || true)"
if [ -n "${RESP}" ]; then
  echo "${RESP}" | jq .
else
  echo "$(date -Iseconds) [WARN] nessuna risposta da /services/local"
fi
sleep $PAUSE_SECS

# --- Sonda: discover DOPO la registrazione ---
echo "[STEP] Discover calc (DOPO la registrazione)..."
URL_DISC_DOPO_REG="http://node1:9000/discover?service=calc"
echo "$(date -Iseconds) [GET] ${URL_DISC_DOPO_REG}"
RESP="$(curl -sS "${URL_DISC_DOPO_REG}" || true)"
if [ -n "${RESP}" ]; then
  echo "${RESP}" | jq .
else
  echo "$(date -Iseconds) [WARN] nessuna risposta da /discover"
fi
sleep $PAUSE_SECS

echo "[STEP] Chiamo SUM su node1..."
URL="http://node1:18080/sum?a=5&b=5"
echo "$(date -Iseconds) [CALL] ${URL}"
RESP="$(curl -sS "${URL}" || true)"
echo "$(date -Iseconds) [CALL][DONE] result=${RESP}"
sleep $PAUSE_SECS

echo "[STEP] Chiamo SUB su node1..."
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

echo "[STEP] Deregistro servizio CALC..."
DEREG_CODE="$(curl -sS -o /dev/null -w "%{http_code}" -X POST "http://node1:9000/service/deregister" \
  -H 'Content-Type: application/json' \
  -d "{\"service\":\"calc\",\"instance_id\":\"dyn-node1\",\"addr\":\"node1:18080\"}")"
if [ "${DEREG_CODE}" = "200" ]; then
  echo "$(date -Iseconds) [DEREG][OK] http=${DEREG_CODE}"
else
  echo "$(date -Iseconds) [DEREG][ERR] http=${DEREG_CODE}"
fi
sleep $PAUSE_SECS

# --- Sonda: servizi locali DOPO la deregistrazione ---
echo "[STEP] Servizi locali su node1 (DOPO la deregistrazione)..."
URL_SL_DOPO_DEREG="http://node1:9000/services/local"
echo "$(date -Iseconds) [GET] ${URL_SL_DOPO_DEREG}"
RESP="$(curl -sS "${URL_SL_DOPO_DEREG}" || true)"
if [ -n "${RESP}" ]; then
  echo "${RESP}" | jq .
else
  echo "$(date -Iseconds) [WARN] nessuna risposta da /services/local"
fi
sleep $PAUSE_SECS

# --- Sonda: discover DOPO la deregistrazione ---
echo "[STEP] Discover calc (DOPO la deregistrazione)..."
URL_DISC_DOPO_DEREG="http://node1:9000/discover?service=calc"
echo "$(date -Iseconds) [GET] ${URL_DISC_DOPO_DEREG}"
RESP="$(curl -sS "${URL_DISC_DOPO_DEREG}" || true)"
if [ -n "${RESP}" ]; then
  echo "${RESP}" | jq .
else
  echo "$(date -Iseconds) [WARN] nessuna risposta da /discover"
fi
sleep $PAUSE_SECS

echo "[STEP] Chiamo SUM su node1..."
URL="http://node1:18080/sum?a=5&b=5"
echo "$(date -Iseconds) [CALL] ${URL}"
RESP="$(curl -sS "${URL}" || true)"
echo "$(date -Iseconds) [CALL][DONE] result=${RESP}"
sleep $PAUSE_SECS

echo "===== [FINE] Sequenza completata ====="
