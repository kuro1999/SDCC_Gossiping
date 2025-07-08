package main

import (
	"log"
	"net"
	"os"
	"strings"
	"time"

	"SDCC_gossiping/gossip"
	"SDCC_gossiping/registry"
	pb "SDCC_gossiping/registry/registrypb"

	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) < 4 {
		log.Fatalf("usage: registrygrpc <id> <bindAddr> <peer1,peer2,...>")
	}
	id, bind, peersArg := os.Args[1], os.Args[2], os.Args[3]
	peers := strings.Split(peersArg, ",")

	// 1) Avvia gossip core
	g, err := gossip.Config(id, bind, 2*time.Second, peers)
	if err != nil {
		log.Fatal(err)
	}
	if err := g.Start(); err != nil {
		log.Fatal(err)
	}

	// 2) Crea il registry e il server gRPC
	r := registry.New(g)
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterRegistryServer(srv, registry.NewGRPCServer(r))

	log.Println("gRPC Registry listening on :50051")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve error: %v", err)
	}
}
