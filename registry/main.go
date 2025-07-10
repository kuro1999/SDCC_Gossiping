// registry.go – compatibile con SWIM + quorum-based DEAD removal
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

	// reports[victim] = set di reporter che lo hanno dichiarato DEAD
	reports = map[string]map[string]struct{}{}

	quorumFrac = 0.5 // maggioranza semplice: > N/2
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
	delete(reports, addr) // pulizia eventuali report pendenti
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
	if len(fields) < 2 {
		log.Printf("[REGISTRY] malformed line: %q", line)
		return
	}
	cmd, arg := fields[0], fields[1]

	switch cmd {

	// --------------------------------------------------------------
	case "DEAD": // formato: DEAD <victimAddr>
		victim := arg
		reporter := strings.Split(conn.RemoteAddr().String(), ":")[0] // IP del reporter

		peersMu.Lock()
		if reports[victim] == nil {
			reports[victim] = map[string]struct{}{}
		}
		reports[victim][reporter] = struct{}{}

		aliveCnt := len(peers)            // nodi vivi registrati
		reportCnt := len(reports[victim]) // quanti hanno segnalato

		log.Printf("[REGISTRY] DEAD report: %s by %s (%d/%d)", victim, reporter, reportCnt, aliveCnt)

		if float64(reportCnt) > quorumFrac*float64(aliveCnt) {
			removePeer(victim)
			log.Printf("[REGISTRY] %s removed (quorum reached)", victim)
		}
		peersMu.Unlock()
		return

	// --------------------------------------------------------------
	case "FAIL", "LEAVE":
		peersMu.Lock()
		removePeer(arg)
		peersMu.Unlock()
		log.Printf("[REGISTRY] Peer %s removed (%s)", arg, cmd)
		return

	case "ALIVE":
		peersMu.Lock()
		delete(reports, arg) // <-- se era stato acusato, azzera i report
		addPeer(arg, arg)    // (ri)aggiungi
		peersMu.Unlock()
		log.Printf("[REGISTRY] Peer %s resurrected (ALIVE)", arg)
		return

	// --------------------------------------------------------------
	default:
		// registrazione classica: <nodeID> <nodeAddr>
		nodeID, nodeAddr := cmd, arg
		peersMu.Lock()
		for _, p := range peers {
			fmt.Fprintln(conn, p.Addr) // invia la lista corrente
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
