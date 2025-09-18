FROM golang:1.22 AS build
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /gossip-fd main.go

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /gossip-fd /gossip-fd
USER nonroot:nonroot
EXPOSE 9000/udp 8080/tcp 18080/tcp
ENTRYPOINT ["/gossip-fd"]
