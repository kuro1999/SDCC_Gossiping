#!/usr/bin/env bash
set -euo pipefail

# ===== Config essenziale (override via env) =====
: "${DISCOVERY_NODES:=node1:9000,node2:9001,node3:9002}"  # nodi API (host:port) - Opzione 1
: "${SERVICE:=calc}"                                       # servizio target
: "${OP:=sum}"                                             # operazione da invocare
: "${A:=1}" : "${B:=2}"                                    # parametri per l'operazione
: "${STARTUP_WAIT:=2}"                                     # attesa iniziale (s)
: "${REG_TTL:=15}"                                         # TTL per la register
: "${REG_ID_PREFIX:=dyn}"                                  # prefisso instance id
: "${REMOVE_DELAY:=10}"                                    # attesa prima della deregister (s)

# ===== Derivate minime =====
API_NODE="${DISCOVERY_NODES%%,*}"          # primo nodo della lista
HOST="${API_NODE%%:*}"                      # es. "node3"
CALC_PORT="1808${HOST#node}"                # euristica: nodeX -> :1808X
GUESS_ADDR="${HOST}:${CALC_PORT}"          # addr stimato per il servizio
INSTANCE="${REG_ID_PREFIX}-${HOST}"        # id deterministico

echo "$(date -Iseconds) [BOOT] wait ${STARTUP_WAIT}s"
sleep "$STARTUP_WAIT"

echo "$(date -Iseconds) [DISCOVER] node=${API_NODE} service=${SERVICE}"
ADDR="$(curl -sS "http://${API_NODE}/discover?service=${SERVICE}" | jq -r '.[0].addr // empty' || true)"
if [ -n "${ADDR}" ]; then
  URL="http://${ADDR}/${OP}?a=${A}&b=${B}"
  echo "$(date -Iseconds) [CALL] ${URL}"
  RESP="$(curl -sS "${URL}" || true)"
  echo "$(date -Iseconds) [CALL][DONE] result=${RESP}"
else
  echo "$(date -Iseconds) [DISCOVER][WARN] nessun indirizzo trovato"
fi

echo "$(date -Iseconds) [REGISTER] node=${API_NODE} service=${SERVICE} id=${INSTANCE} addr=${GUESS_ADDR} ttl=${REG_TTL}"
REG_CODE="$(curl -sS -o /dev/null -w "%{http_code}" -X POST "http://${API_NODE}/service/register" \
  -H 'Content-Type: application/json' \
  -d "{\"service\":\"${SERVICE}\",\"instance_id\":\"${INSTANCE}\",\"addr\":\"${GUESS_ADDR}\",\"ttl\":${REG_TTL}}")"
if [ "${REG_CODE}" = "200" ]; then
  echo "$(date -Iseconds) [REGISTER][OK] http=${REG_CODE}"
else
  echo "$(date -Iseconds) [REGISTER][ERR] http=${REG_CODE}"
fi

sleep "$STARTUP_WAIT"
echo "$(date -Iseconds) [DISCOVER] node=${API_NODE} service=${SERVICE}"
ADDR="$(curl -sS "http://${API_NODE}/discover?service=${SERVICE}" | jq -r '.[0].addr // empty' || true)"
if [ -n "${ADDR}" ]; then
  URL="http://${ADDR}/${OP}?a=${A}&b=${B}"
  echo "$(date -Iseconds) [CALL] ${URL}"
  RESP="$(curl -sS "${URL}" || true)"
  echo "$(date -Iseconds) [CALL][DONE] result=${RESP}"
else
  echo "$(date -Iseconds) [DISCOVER][WARN] nessun indirizzo trovato"
fi

echo "$(date -Iseconds) [WAIT] remove in ${REMOVE_DELAY}s"
sleep "$REMOVE_DELAY"

echo "$(date -Iseconds) [DEREG] node=${API_NODE} service=${SERVICE} id=${INSTANCE}"
DEREG_CODE="$(curl -sS -o /dev/null -w "%{http_code}" -X POST "http://${API_NODE}/service/deregister" \
  -H 'Content-Type: application/json' \
  -d "{\"service\":\"${SERVICE}\",\"instance_id\":\"${INSTANCE}\"}")"
if [ "${DEREG_CODE}" = "200" ]; then
  echo "$(date -Iseconds) [DEREG][OK] http=${DEREG_CODE}"
else
  echo "$(date -Iseconds) [DEREG][ERR] http=${DEREG_CODE}"
fi

sleep "$STARTUP_WAIT"
echo "$(date -Iseconds) [DISCOVER] node=${API_NODE} service=${SERVICE}"
ADDR="$(curl -sS "http://${API_NODE}/discover?service=${SERVICE}" | jq -r '.[0].addr // empty' || true)"
if [ -n "${ADDR}" ]; then
  URL="http://${ADDR}/${OP}?a=${A}&b=${B}"
  echo "$(date -Iseconds) [CALL] ${URL}"
  RESP="$(curl -sS "${URL}" || true)"
  echo "$(date -Iseconds) [CALL][DONE] result=${RESP}"
else
  echo "$(date -Iseconds) [DISCOVER][WARN] nessun indirizzo trovato"
fi