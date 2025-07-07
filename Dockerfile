# → STAGE 1: builder, builds gossipnode + registrygrpc
FROM golang:1.24-alpine AS builder
WORKDIR /app

# cache modules
COPY go.mod go.sum ./
RUN go mod download

# copy all sources
COPY . .

# build the gossip node
RUN CGO_ENABLED=0 GOOS=linux go build -o gossipnode ./cmd/gossipnode

# build the gRPC registry
RUN CGO_ENABLED=0 GOOS=linux go build -o registrygrpc ./cmd/registrygrpc

# → STAGE 2: runtime, a tiny image containing just the two binaries
FROM alpine:3.18
WORKDIR /app

# for HTTPS (if/when you need it)
RUN apk add --no-cache ca-certificates

# copy our two executables
COPY --from=builder /app/gossipnode    /app/gossipnode
COPY --from=builder /app/registrygrpc /app/registrygrpc

# default entrypoint is gossipnode, you can override in compose
ENTRYPOINT ["./gossipnode"]
