# --- STAGE 1: Builder ---
FROM golang:1.24-alpine AS builder
WORKDIR /app

# 1) caching moduli
COPY go.mod go.sum ./
RUN go mod download

# 2) copia codice e compila gossipnode
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o gossipnode ./cmd/gossipnode

# --- STAGE 2: Runtime ---
FROM alpine:3.18
WORKDIR /app

# copia il binario
COPY --from=builder /app/gossipnode /app/gossipnode

# punto di ingresso
ENTRYPOINT ["./gossipnode"]
