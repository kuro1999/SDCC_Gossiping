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

// ---------- [SWIM-INTEGRATION] ----------
/*
   swim.go (stesso package main) espone:

   // inizializza tabelle interne e parte la probe-loop
   func StartSWIM(myAddr string)

   // inserisce peer già noti come ALIVE (incarnation 0)
   func AddInitialPeers(peers []string)

   // parser/dispatcher per tutti i frame SWIM
   func HandleSWIM(msg string)
*/
//----------------------------------------

var registryAddr string // per le chiamate dirette / JOIN bootstrap

// init: timestamp + prefix nel logger
func init() { log.SetFlags(0) }

// getPeers → invariato: contatta il registry e restituisce gli indirizzi noti
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

// -------------------------------- LISTENER ----------------------------------

func handleConn(c net.Conn) {
	defer c.Close()

	remote := c.RemoteAddr().String()
	scanner := bufio.NewScanner(c)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Passo SEMPRE al parser SWIM (gestisce PING/ACK, ALIVE, SUSPECT, DEAD, ecc.)
		HandleSWIM(line)

		// Supporto JOIN iniziale (resta fuori da SWIM per semplicità di bootstrap)
		if strings.HasPrefix(line, "JOIN ") {
			addr := strings.TrimSpace(strings.TrimPrefix(line, "JOIN"))
			log.Printf("[JOIN] learned %s from %s", addr, remote)
			HandleSWIM(fmt.Sprintf("ALIVE %s 0", addr)) // inserisco subito come ALIVE
		}
	}
}

// ---------------------------------------------------------------------------

func main() {
	if len(os.Args) < 4 {
		log.Println("Usage: node <myID> <myAddr> <registryAddr>")
		os.Exit(1)
	}
	myID := os.Args[1]
	myAddr := os.Args[2]
	registryAddr = os.Args[3]

	log.SetPrefix(fmt.Sprintf("[%s] ", myID))
	log.Printf("[BOOT] starting node %s  (%s)", myID, myAddr)

	// 1) bootstrap
	initial := getPeers(registryAddr, myID, myAddr)

	// ---------- [SWIM-INTEGRATION] ----------
	addInitialPeers(myAddr, initial) // popola la membership con lo snapshot iniziale
	//----------------------------------------

	// 2) registro la JOIN del mio indirizzo (la propaga SWIM come ALIVE)
	HandleSWIM(fmt.Sprintf("ALIVE %s 0", myAddr))

	// 3) socket in ascolto
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

	// ---------- [SWIM-INTEGRATION] ----------
	StartSWIM(myAddr) // parte la probe-loop (non ritorna)
	//----------------------------------------
	select {}
}
