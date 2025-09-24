FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod tidy
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o registry registry.go

FROM alpine:latest
COPY --from=builder /app/registry /usr/local/bin/registry
EXPOSE 8089
ENTRYPOINT ["registry"]