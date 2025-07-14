package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

func init() { log.SetFlags(0) }

// grace period before final removal after quorum DEAD
var (
	gracePeriod     = 20 * time.Second
	pendingRemovals = make(map[string]*time.Timer)
)

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

func sendFrame(dest, format string, a ...any) {
	conn, err := net.DialTimeout("tcp", dest, 2*time.Second)
	if err != nil {
		log.Printf("[NET] dial %s failed: %v", dest, err)
		return
	}
	fmt.Fprintf(conn, format, a...)
	_ = conn.Close()
}

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
	case "DEAD":
		victim := arg
		reporterIP := strings.Split(conn.RemoteAddr().String(), ":")[0]

		peersMu.Lock()
		defer peersMu.Unlock()

		if !peerExists(victim) { // già rimosso
			return
		}

		// mappa IP → addr completo (best-effort)
		reporterAddr := reporterIP
		for _, p := range peers {
			if strings.Split(p.Addr, ":")[0] == reporterIP {
				reporterAddr = p.Addr
				break
			}
		}

		if reports[victim] == nil {
			reports[victim] = map[string]struct{}{}
		}
		reports[victim][reporterAddr] = struct{}{}

		aliveCnt := len(peers)
		if peerExists(victim) {
			aliveCnt--
		}
		reportCnt := len(reports[victim])
		log.Printf("[REGISTRY] DEAD report: %s by %s (%d/%d)", victim, reporterAddr, reportCnt, aliveCnt)

		if float64(reportCnt) > quorumFrac*float64(aliveCnt) {
			// schedule final removal after gracePeriod
			if timer := pendingRemovals[victim]; timer != nil {
				timer.Stop()
			}
			pendingRemovals[victim] = time.AfterFunc(gracePeriod, func() {
				peersMu.Lock()
				defer peersMu.Unlock()

				if peerExists(victim) {
					removePeer(victim)
					log.Printf("[REGISTRY] %s removed after grace period", victim)
					for _, peer := range peers {
						go sendFrame(peer.Addr, "REMOVE %s\n", victim)
					}
				}
				delete(pendingRemovals, victim)
			})
			log.Printf("[REGISTRY] scheduled removal of %s in %s", victim, gracePeriod)
		}

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
		// cancel pending removal if any
		if timer := pendingRemovals[arg]; timer != nil {
			timer.Stop()
			delete(pendingRemovals, arg)
			log.Printf("[REGISTRY] canceled removal of %s (ALIVE)", arg)
		}
		delete(reports, arg) // azzera i report
		addPeer(arg, arg)
		peersMu.Unlock()
		log.Printf("[REGISTRY] Peer %s resurrected (ALIVE)", arg)
		return

	// --------------------------------------------------------------
	default:
		// registrazione classica: <nodeID> <nodeAddr>
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
