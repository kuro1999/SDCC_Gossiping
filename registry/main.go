// registry.go – compatibile con SWIM

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

func init() { log.SetFlags(0) }

type Peer struct {
	ID   string
	Addr string
}

var (
	peers   []Peer
	peersMu sync.Mutex
)

// ------------------------------------------------------------------
// utilità
// ------------------------------------------------------------------

func peerExists(addr string) bool {
	for _, p := range peers {
		if p.Addr == addr {
			return true
		}
	}
	return false
}

func addPeer(id, addr string) {
	if peerExists(addr) {
		return
	}
	peers = append(peers, Peer{ID: id, Addr: addr})
}

func removePeer(addr string) {
	newPeers := peers[:0]
	for _, p := range peers {
		if p.Addr != addr {
			newPeers = append(newPeers, p)
		}
	}
	peers = newPeers
}

// ------------------------------------------------------------------
// connessione singola
// ------------------------------------------------------------------

func handleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	line, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[REGISTRY] read error: %v", err)
		return
	}
	line = strings.TrimSpace(line)
	fields := strings.Fields(line)
	if len(fields) != 2 {
		log.Printf("[REGISTRY] malformed line: %q", line)
		return
	}
	cmd, arg := fields[0], fields[1]

	switch cmd {
	case "FAIL":
		peersMu.Lock()
		removePeer(arg)
		peersMu.Unlock()
		log.Printf("[REGISTRY] Peer %s removed (FAIL)", arg)
		return

	case "LEAVE":
		peersMu.Lock()
		removePeer(arg)
		peersMu.Unlock()
		log.Printf("[REGISTRY] Peer %s removed (LEAVE)", arg)
		return

	case "ALIVE":
		peersMu.Lock()
		addPeer(arg, arg) // ID = addr se non noto
		peersMu.Unlock()
		log.Printf("[REGISTRY] Peer %s resurrected (ALIVE)", arg)
		return

		// dentro handleConn, versione minimale:
	default:
		nodeID, nodeAddr := cmd, arg
		peersMu.Lock()
		for _, p := range peers {
			fmt.Fprintln(conn, p.Addr)
		}
		addPeer(nodeID, nodeAddr)
		log.Printf("[REGISTRY] Registered Node: %s (%s)", nodeID, nodeAddr)
		peersMu.Unlock()
	}
}

// ------------------------------------------------------------------
// main
// ------------------------------------------------------------------

func main() {
	port := os.Getenv("REGISTRY_PORT")
	if port == "" {
		port = "8080"
	}

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("[REGISTRY] listen failed: %v", err)
	}
	log.Printf("[REGISTRY] Listening on :%s", port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[REGISTRY] accept error: %v", err)
			continue
		}
		go handleConn(conn)
	}
}
