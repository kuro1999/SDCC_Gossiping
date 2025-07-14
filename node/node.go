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
     StartSWIM(...)
     addInitialPeers(...)
     HandleSWIM(...)
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
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "PING": // PING <origin>
			if len(parts) != 2 {
				continue
			}
			origin := parts[1]

			HandleSWIM(fmt.Sprintf("ALIVE %s 0", origin)) // mittente è vivo

			// Rispondiamo con ACK su nuova connessione
			go sendFrame(origin, "ACK %s\n", selfAddr)

			// Piggy-back dei nostri eventi sulla conn. entrante
			sendPiggyback(c)

		case "ACK": // ACK <who>
			if len(parts) != 2 {
				continue
			}
			addr := parts[1]
			if chAny, ok := ackWaiters.LoadAndDelete(addr); ok {
				close(chAny.(chan struct{}))
			}
			recordAck(addr)

		case "PING-REQ": // PING-REQ <target> <origin>
			if len(parts) != 3 {
				continue
			}
			target, origin := parts[1], parts[2]
			go func() {
				if directPing(target, origin) {
					sendFrame(origin, "ACK %s\n", target)
				}
			}()

		case "JOIN":
			if len(parts) != 2 {
				continue
			}
			updateFromRemote(EvJoin, parts[1], 0)
			sendPiggyback(c)

		case "REMOVE":
			if len(parts) != 2 {
				continue
			}
			target := parts[1]
			memMu.Lock()
			delete(members, target)
			memMu.Unlock()
			log.Printf("[SWIM] Removed from %s", target)

		case "LEAVE":
			if len(parts) < 2 {
				continue
			}
			victim := parts[1]
			if victim == selfAddr {
				// voluntary leave indirizzato a me → faccio l’uscita ordinata
				gracefulLeave()
			} else {
				// leave di un altro peer: lo rimuovo dalla mia membership
				memMu.Lock()
				delete(members, victim)
				memMu.Unlock()
				log.Printf("[SWIM] %s voluntarily left", victim)
			}

		default:
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
