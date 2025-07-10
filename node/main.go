package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

/*
   swim.go espone:
     StartSWIM(myAddr string, registryAddr string)
     addInitialPeers(myAddr string, peers []string)
     HandleSWIM(msg string)
*/

// -------------------------------------------------------------------
// utilità di rete
// -------------------------------------------------------------------
func sendFrame(dest, format string, a ...any) {
	conn, err := net.DialTimeout("tcp", dest, 2*time.Second)
	if err != nil {
		log.Printf("[NET] dial %s failed: %v", dest, err)
		return
	}
	fmt.Fprintf(conn, format, a...)
	_ = conn.Close()
}

// -------------------------------------------------------------------
// broadcast JOIN
// -------------------------------------------------------------------
func broadcastJoin(myAddr string, peers []string) {
	for _, p := range peers {
		if p == myAddr {
			continue
		}
		go sendFrame(p, "JOIN %s\n", myAddr)
		log.Printf("[JOIN] sent JOIN to %s", p)
	}
}

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

// -------------------------------------------------------------------
// listener TCP
// -------------------------------------------------------------------
func handleConn(c net.Conn) {
	defer c.Close()
	scanner := bufio.NewScanner(c)
	remote := c.RemoteAddr().String()

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		parts := strings.Fields(line)
		switch parts[0] {
		case "PING":
			// formato: PING <originAddr>
			if len(parts) == 2 {
				origin := parts[1]

				// 1) il mittente è vivo
				HandleSWIM(fmt.Sprintf("ALIVE %s 0", origin))

				// 2) rispondiamo **con una connessione nuova**, non su 'c'
				go func() {
					// l’ACK deve contenere l’indirizzo del bersaglio del ping,
					// cioè il nostro (selfAddr).
					sendFrame(origin, "ACK %s\n", selfAddr)
				}()

				// 3) piggy-back (eventi nostri) sulla stessa connessione entrante
				//    finché è aperta – opzionale ma comodo.
				sendPiggyback(c)
			}

		case "ACK":
			// formato: ACK <who>
			if len(parts) == 2 {
				addr := parts[1]
				HandleSWIM(fmt.Sprintf("ACK %s", addr)) // mette su ackCh
				// nessun bisogno di piggy-back qui
			}

		case "PING-REQ":
			// formato: PING-REQ <target> <origin>
			if len(parts) == 3 {
				target, origin := parts[1], parts[2]

				// lanciamo un ping diretto al bersaglio
				go func() {
					if directPing(target, origin) {
						// se riceviamo l’ACK dal target, giriamo un ACK all’origin
						sendFrame(origin, "ACK %s\n", target)
					}
				}()
				// niente risposta immediata al proxy-caller
			}

		case "JOIN":
			addr := parts[1]
			updateFromRemote(EvJoin, addr, 0)
			sendPiggyback(c)

		default:
			// tutte le altre righe (EVT piggyback o comandi custom)
			HandleSWIM(line)
			sendPiggyback(c)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("[NET] scanner error from %s: %v", remote, err)
	}
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
	registryAddr := os.Args[3]

	log.SetFlags(0)
	log.SetPrefix(fmt.Sprintf("[%s] ", myID))
	log.Printf("[BOOT] starting node %s  (%s)", myID, myAddr)

	// 1) snapshot iniziale
	initial := getPeers(registryAddr, myID, myAddr)

	// 2) membership locale
	addInitialPeers(myAddr, initial)

	// 3) JOIN gossip
	if len(initial) > 0 {
		broadcastJoin(myAddr, initial)
	}

	// 4) me stesso ALIVE inc=0
	HandleSWIM(fmt.Sprintf("ALIVE %s 0", myAddr))

	// 5) listener
	port := strings.Split(myAddr, ":")[1]
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("[BOOT] listen %s failed: %v", myAddr, err)
	}
	log.Printf("[BOOT] listening on %s", myAddr)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				log.Printf("[NET] accept failed: %v", err)
				continue
			}
			go handleConn(c)
		}
	}()

	// 6) failure detector
	StartSWIM(myAddr, initial, registryAddr)

	select {} // blocca
}
