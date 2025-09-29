FROM golang:1.22 AS build
WORKDIR /app
# Meglio copiare anche go.sum per cache stabile
COPY go.mod go.sum ./
RUN go mod download

# Copia il resto
COPY . .

# ⚠️ Costruisci l'intero pacchetto, non "main.go"
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /gossip-fd .

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /gossip-fd /gossip-fd
USER nonroot:nonroot

# HTTP API di default ora è sulla stessa porta dell’UDP (es. 9000),
# quindi esponiamo 9000 sia TCP che UDP. 18080 è il demo "calc".
EXPOSE 9000/udp 9000/tcp 18080/tcp

ENTRYPOINT ["/gossip-fd"]
