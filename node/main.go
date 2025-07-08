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

var (
	peers        []string // lista dinamica di peer
	peersMu      sync.Mutex
	pendingJoins []string // indirizzi JOIN da piggybackare
	joinsMu      sync.Mutex
	lastSeen     = make(map[string]time.Time)
	lastSeenMu   sync.Mutex
)

// init configura il logger con timestamp e prefix
func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

// getPeers invia "myID myAddr" al registry e riceve la lista iniziale di indirizzi
func getPeers(registryAddr, myID, myAddr string) []string {
	for {
		conn, err := net.Dial("tcp", registryAddr)
		if err != nil {
			log.Printf("[BOOT] Registry non raggiungibile (%v), riprovo in 5s...", err)
			time.Sleep(5 * time.Second)
			continue
		}
		defer conn.Close()

		// invio ID + indirizzo
		fmt.Fprintf(conn, "%s %s\n", myID, myAddr)

		// leggo la lista dei peer (solo indirizzi)
		var list []string
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			list = append(list, scanner.Text())
		}
		log.Printf("[BOOT] Peer iniziali ricevuti: %v", list)
		return list
	}
}

// announceJoins invia immediatamente le JOIN pendenti a tutti i peer conosciuti
func announceJoins(myAddr string) {
	peersMu.Lock()
	targets := append([]string{}, peers...)
	peersMu.Unlock()

	joinsMu.Lock()
	toAnnounce := append([]string{}, pendingJoins...)
	joinsMu.Unlock()

	for _, addr := range targets {
		if addr == myAddr {
			continue
		}
		go func(a string) {
			conn, err := net.Dial("tcp", a)
			if err != nil {
				return
			}
			defer conn.Close()
			for _, j := range toAnnounce {
				fmt.Fprintf(conn, "JOIN %s\n", j)
				log.Printf("[GOSSIP] Inviato JOIN %s a %s", j, a)
			}
		}(addr)
	}
}

// gossipLoop invia JOIN (se targets non vuoti) e PING ai peer ogni 5s
func gossipLoop(myAddr string) {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		// snapshot lista peers
		peersMu.Lock()
		targets := append([]string{}, peers...)
		peersMu.Unlock()

		// snapshot JOIN pendenti
		joinsMu.Lock()
		toAnnounce := append([]string{}, pendingJoins...)
		// reset pendings solo se c'è un target
		if len(targets) > 0 {
			pendingJoins = nil
		}
		joinsMu.Unlock()

		log.Printf("[GOSSIP] Targets: %v", targets)
		for _, addr := range targets {
			if addr == myAddr {
				continue
			}
			go func(a string) {
				conn, err := net.Dial("tcp", a)
				if err != nil {
					log.Printf("[GOSSIP] Fallito gossip verso %s: %v", a, err)
					return
				}
				defer conn.Close()

				// invio JOIN pendenti
				for _, j := range toAnnounce {
					fmt.Fprintf(conn, "JOIN %s\n", j)
					log.Printf("[GOSSIP] Inviato JOIN %s a %s", j, a)
				}
				// invio PING
				fmt.Fprintf(conn, "PING %s\n", myAddr)
				log.Printf("[GOSSIP] Inviato PING a %s", a)
			}(addr)
		}
	}
}

// handleConn elabora JOIN e PING, aggiorna peers e lastSeen
func handleConn(c net.Conn) {
	defer c.Close()
	scanner := bufio.NewScanner(c)
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) != 2 {
			continue
		}
		cmd, addr := parts[0], parts[1]
		switch cmd {
		case "JOIN":
			peersMu.Lock()
			exists := false
			for _, p := range peers {
				if p == addr {
					exists = true
					break
				}
			}
			if !exists {
				peers = append(peers, addr)
				log.Printf("[EVENT] Aggiunto peer: %s", addr)
				log.Printf("[EVENT] Lista peers: %v", peers)
				lastSeenMu.Lock()
				lastSeen[addr] = time.Now()
				lastSeenMu.Unlock()
			}
			peersMu.Unlock()
		case "PING":
			lastSeenMu.Lock()
			lastSeen[addr] = time.Now()
			lastSeenMu.Unlock()
			log.Printf("[EVENT] Ricevuto PING da %s", addr)
		}
	}
}

func main() {
	if len(os.Args) < 4 {
		log.Println("Usage: node <myID> <myAddr> <registryAddr>")
		os.Exit(1)
	}
	myID := os.Args[1]
	myAddr := os.Args[2]
	registry := os.Args[3]

	// prefisso per i log
	log.SetPrefix(fmt.Sprintf("[%s] ", myID))
	log.Printf("[MAIN] Avvio nodo %s, registry=%s", myID, registry)

	// 1) bootstrap dal registry
	initial := getPeers(registry, myID, myAddr)

	// inizializza peers e lastSeen
	peersMu.Lock()
	peers = append(peers, initial...)
	peersMu.Unlock()
	lastSeenMu.Lock()
	for _, p := range initial {
		lastSeen[p] = time.Now()
	}
	lastSeenMu.Unlock()
	log.Printf("[MAIN] Lista peers iniziale: %v", peers)

	// 2) registra la JOIN pendente del nodo stesso
	joinsMu.Lock()
	pendingJoins = append(pendingJoins, myAddr)
	joinsMu.Unlock()
	log.Printf("[MAIN] JOIN pendente schedulata: %s", myAddr)

	// 3) avvia listener per gossip in arrivo
	port := strings.Split(myAddr, ":")[1]
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("[MAIN] Listen fallito su %s: %v", myAddr, err)
	}
	log.Printf("[MAIN] Listener avviato su %s", myAddr)

	// invia subito le JOIN pendenti, se ci sono peer
	go announceJoins(myAddr)

	// gestisci connessioni in entrata
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				log.Printf("[MAIN] Accept fallito: %v", err)
				continue
			}
			go handleConn(c)
		}
	}()

	// 4) avvia gossip loop
	go gossipLoop(myAddr)
	log.Println("[MAIN] Gossip avviato")

	// 5) avvia fault detector
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		for range ticker.C {
			lastSeenMu.Lock()
			for p, ts := range lastSeen {
				if time.Since(ts) > 10*time.Second {
					log.Printf("[FAULT] Peer %s considerato DEAD", p)
					delete(lastSeen, p)
				}
			}
			lastSeenMu.Unlock()
		}
	}()
	log.Println("[MAIN] Fault detector avviato")

	select {}
}
