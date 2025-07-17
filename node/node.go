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
	// udpConn è il listener/gateway UDP per gossip e failure detector
	udpConnection *net.UDPConn

	// ackWaiters tiene i canali su cui bloccare il ping
	ackWaiters sync.Map // mappa[string]chan struct{}

	resurrectNotified   = make(map[string]uint64) // addr → incarnation per cui ho già notificato
	resurrectNotifiedMu sync.Mutex
)

// -------------------------------------------------------------------
// bootstrap dal registry
// -------------------------------------------------------------------
func getPeers(registryAddr, myID, myAddr string) []string {
	for {
		conn, err := net.Dial("tcp", registryAddr)
		if err != nil {
			log.Printf("[BOOT] Registry not reached (%v), retry in 5s...", err)
			time.Sleep(5 * time.Second)
			continue
		}
		defer conn.Close()

		fmt.Fprintf(conn, "%s %s\n", myID, myAddr)

		var list []string
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			list = append(list, scanner.Text())
		}
		log.Printf("[BOOT] Initial Peers: %v", list)
		return list
	}
}

// helper: restituisce tutte le chiavi di members
func memberAddrs() []string {
	memMu.Lock()
	defer memMu.Unlock()
	addrs := make([]string, 0, len(members))
	for addr := range members {
		addrs = append(addrs, addr)
	}
	return addrs
}

func addInitialPeers(myAddr string, peers []string) {
	memMu.Lock()
	// 1) aggiungo sempre me stesso
	members[myAddr] = &Member{
		Addr:        myAddr,
		State:       Alive,
		Incarnation: 0,
		LastAck:     time.Now(),
	}
	// 2) aggiungo gli altri peer
	for _, p := range peers {
		if p == myAddr {
			continue
		}
		members[p] = &Member{
			Addr:        p,
			State:       Alive,
			Incarnation: 0,
			LastAck:     time.Now(),
		}
	}
	memMu.Unlock()

	// 3) loggiamo lo stato reale della membership
	log.Printf("[BOOT] initial membership: %v", memberAddrs())
}

// -------------------------------------------------------------------
// main
// -------------------------------------------------------------------
func main() {
	if len(os.Args) < 4 {
		log.Println("Usage: node <myID> <myAddr> <registryAddr>")
		os.Exit(1)
	}
	myID := os.Args[1]
	myAddr := os.Args[2]
	selfAddr = myAddr
	registryAddr = os.Args[3]

	log.SetFlags(0)
	log.SetPrefix(fmt.Sprintf("[%s] ", myID))
	log.Printf("[BOOT] starting node %s  (%s)", myID, myAddr)

	// --- 1) apri il listener UDP per gossip + ping/ack ---
	udpAddr, err := net.ResolveUDPAddr("udp", myAddr)
	if err != nil {
		log.Fatalf("[BOOT] invalid UDP addr %q: %v", myAddr, err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("[BOOT] cannot listen UDP on %q: %v", myAddr, err)
	}
	udpConnection = conn
	log.Printf("[BOOT] gossip+ping listener on UDP %s", myAddr)
	go listenGossipUDP(udpConnection)

	// --- 2) bootstrap dal registry (TCP) ---
	initial := getPeers(registryAddr, myID, myAddr)

	// --- 3) inizializza view locale e JOIN gossip ---
	addInitialPeers(myAddr, initial)
	log.Printf("[BOOT] init incarnation=%d", members[selfAddr].Incarnation)
	// node.go  (nel main, dopo addInitialPeers)
	if wasAlreadyMember() { // usa un flag su disco o nel registry
		// ↑ persistenza semplice: file .inc
		var selfInc uint64
		for addr, m := range members {
			if addr == selfAddr {
				selfInc = m.Incarnation
			}
		}
		// enqueue ALIVE con nuova incarnation
		evMu.Lock()
		selfInc++
		eventQ = append(eventQ, Event{
			Kind: EvAlive, Addr: myAddr,
			Incarnation: selfInc, HopsLeft: getHops(),
		})
		evMu.Unlock()

		go notifyRegistryAlive(myAddr) // TCP: "ALIVE <addr>\n"
		go gossipNowUDP()
	} else if len(initial) > 0 {
		diffuseJoin(myAddr) // primo avvio reale
	}

	// --- 5) avvio manuale dei loop SWIM → tutti via UDP ---
	go probeLoopUDP()                       // ping periodici + indirect → Suspect/Dead
	go reaperLoop()                         // promozione da Suspect a Dead
	go antiEntropyLoopUDP(10 * time.Second) // push periodico di tutta la eventQ
	go dumpMembership()                     // log snapshot (opzionale)

	port := strings.Split(selfAddr, ":")[1]
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("TCP listen failed: %v", err)
	}
	log.Printf("TCP listener on %s", selfAddr)

	// Avvio il loop di Accept in una goroutine
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Printf("accept failed: %v", err)
				continue
			}
			go handleConn(conn)
		}
	}()

	// Tengo vivo il main, così tutte le goroutine (listener UDP, probeLoop, ecc.) restano attive.
	select {}
}
