FROM alpine:3.20
RUN apk add --no-cache bash curl jq ca-certificates
WORKDIR /opt/orch
COPY orchestrator.sh /usr/local/bin/orchestrator.sh
RUN chmod +x /usr/local/bin/orchestrator.sh
ENTRYPOINT ["/usr/local/bin/orchestrator.sh"]
