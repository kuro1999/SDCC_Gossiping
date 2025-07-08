// registry.go

package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

type Peer struct {
	ID   string
	Addr string
}

var (
	peers   []Peer
	peersMu sync.Mutex
)

func handleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// 1) Leggi ID e indirizzo (es. "node123 10.0.0.5:8000\n")
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[REGISTRY] Errore lettura ID e address dal nodo: %v", err)
		return
	}
	parts := strings.Fields(strings.TrimSpace(line))
	if len(parts) != 2 {
		log.Printf("[REGISTRY] Formato non valido: %q", line)
		return
	}
	nodeID, nodeAddr := parts[0], parts[1]

	// 2) Invia la lista dei peer esistenti (solo gli indirizzi)
	peersMu.Lock()
	for _, p := range peers {
		log.Printf("[REGISTRY] Inviando peer.Addr = %q", p.Addr)
		fmt.Fprintln(conn, p.Addr) // <— solo “node1:8000”
	}
	peersMu.Unlock()

	// 3) Aggiungi il nodo se non già presente (controllo su ID)
	peersMu.Lock()
	exists := false
	for _, p := range peers {
		if p.ID == nodeID {
			exists = true
			break
		}
	}
	if !exists {
		peers = append(peers, Peer{ID: nodeID, Addr: nodeAddr})
		log.Printf("[REGISTRY] Nodo registrato: %s (%s)", nodeID, nodeAddr)
	} else {
		log.Printf("[REGISTRY] Nodo già presente: %s", nodeID)
	}
	peersMu.Unlock()
}

func main() {
	port := os.Getenv("REGISTRY_PORT")
	if port == "" {
		port = "8080"
	}
	ln, _ := net.Listen("tcp", ":"+port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Accept fallito: %v", err)
			continue
		}
		go handleConn(conn)
	}
}
