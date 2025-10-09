FROM alpine:3.20
RUN apk add --no-cache bash curl jq ca-certificates
WORKDIR /opt/orch
COPY test_services.sh /usr/local/bin/test_services.sh
RUN chmod +x /usr/local/bin/test_services.sh
ENTRYPOINT ["/usr/local/bin/test_services.sh"]
