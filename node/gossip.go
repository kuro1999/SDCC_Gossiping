package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"net"
	"strings"
	"time"
)

const (
	probePeriod    = 1 * time.Second
	probeTimeout   = 1 * time.Second
	dumpInterval   = 10 * time.Second
	suspectTimeout = 5 * time.Second
)

func enqueue(e Event) {
	evMu.Lock()
	eventQ = append(eventQ, e)
	evMu.Unlock()
}

func getHops() int {
	memMu.Lock()
	N := len(members)
	memMu.Unlock()
	if N < 1 {
		return 1
	}
	hops := int(math.Ceil(math.Log2(float64(N) + 1)))

	return hops
}

// -----------------------------------------------------------------
// 1) Listener UDP per il gossip (JOIN, ALIVE, etc.)
// -----------------------------------------------------------------

func listenGossipUDP(conn *net.UDPConn) {
	buf := make([]byte, 64<<10)
	for {
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("[GOSSIP] read error: %v", err)
			continue
		}
		for _, line := range strings.Split(string(buf[:n]), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			parts := strings.Fields(line)
			hops := getHops()
			switch parts[0] {

			case "PING": // PING <origin> <inc>
				hops = getHops()
				if len(parts) != 3 {
					continue
				}
				origin := parts[1]
				var remoteInc uint64
				fmt.Sscanf(parts[2], "%d", &remoteInc)

				// ── resurrection logic ──
				memMu.Lock()
				m, exists := members[origin]
				if !exists || m.State == Dead {
					remoteInc++

					// notifico il registry UNA SOLA volta per questa incarnation
					resurrectNotifiedMu.Lock()
					if resurrectNotified[origin] != remoteInc {
						notifyRegistryAlive(origin)
						resurrectNotified[origin] = remoteInc
					}
					resurrectNotifiedMu.Unlock()

					members[origin] = &Member{
						Addr:        origin,
						State:       Alive,
						Incarnation: remoteInc,
						LastAck:     time.Now(),
					}
					enqueue(Event{
						Kind:        EvAlive,
						Addr:        origin,
						Incarnation: remoteInc,
						HopsLeft:    getHops(),
					})
					log.Printf("[SWIM] resurrected %s with inc=%d via PING", origin, remoteInc)
					go gossipNowUDP()
				}
				memMu.Unlock()

				// normale processing Alive
				updateFromRemote(EvAlive, origin, remoteInc)

				// rispondo con ACK
				reply := []byte(fmt.Sprintf("ACK %s\n", selfAddr))
				_, _ = conn.WriteToUDP(reply, src)

			case "PING-REQ": // PING-REQ <target> <origin> <inc>
				hops = getHops()
				if len(parts) != 4 {
					continue
				}
				target := parts[1]
				origin := parts[2]
				var remoteInc uint64
				fmt.Sscanf(parts[3], "%d", &remoteInc)

				// resurrection logic per l’origin
				memMu.Lock()
				m, exists := members[origin]
				if !exists || m.State == Dead {
					remoteInc++

					resurrectNotifiedMu.Lock()
					if resurrectNotified[origin] != remoteInc {
						notifyRegistryAlive(origin)
						resurrectNotified[origin] = remoteInc
					}
					resurrectNotifiedMu.Unlock()

					members[origin] = &Member{
						Addr:        origin,
						State:       Alive,
						Incarnation: remoteInc,
						LastAck:     time.Now(),
					}
					enqueue(Event{
						Kind:        EvAlive,
						Addr:        origin,
						Incarnation: remoteInc,
						HopsLeft:    getHops(),
					})
					log.Printf("[SWIM] resurrected %s with inc=%d via PING-REQ", origin, remoteInc)
					go gossipNowUDP()
				}
				memMu.Unlock()

				// giro il PING reale verso target, piggy‑back ecc.
				udpAddr, err := net.ResolveUDPAddr("udp", target)
				if err == nil {
					msg := []byte(fmt.Sprintf("PING %s %d\n", origin, remoteInc))
					_, _ = conn.WriteToUDP(msg, udpAddr)
				}
				sendPiggybackUDP(conn, src)

			case "ACK": // ACK <who>
				hops = getHops()
				if len(parts) != 2 {
					continue
				}
				who := parts[1]
				if chAny, ok := ackWaiters.LoadAndDelete(who); ok {
					close(chAny.(chan struct{}))
				}
				// -----------------------------------------------------------------
				// 2) listenGossipUDP – snippet con la parte JOIN aggiornata
				//    • Accetta sia "JOIN <addr>" che "JOIN <addr> <inc>"
				//      (l’incarnation ricevuta viene ignorata → trattata come 0)
				// -----------------------------------------------------------------
			case "JOIN": // JOIN <addr> [inc]
				hops = getHops()
				if len(parts) < 2 || len(parts) > 3 {
					continue // formato non valido
				}
				newAddr := parts[1]

				// 1) Se già conosco il peer, ignoro TUTTO (no update, no piggyback)
				memMu.Lock()
				_, known := members[newAddr]
				memMu.Unlock()
				if known {
					continue
				}

				// 2) Altrimenti è davvero un nuovo join: lo processiamo
				memMu.Lock()
				members[newAddr] = &Member{
					Addr:        newAddr,
					State:       Alive,
					Incarnation: 0,
					LastAck:     time.Now(), // ← inizializzo l’ack qui
				}
				memMu.Unlock()

				enqueue(Event{
					Kind:        EvJoin,
					Addr:        newAddr,
					Incarnation: 0,
					HopsLeft:    getHops(),
				})
				log.Printf("[GOSSIP] membership after JOIN: %v", memberAddrs())
				sendPiggybackUDP(conn, src)

			case "SUSPECT": // SUSPECT <addr> <inc>
				hops = getHops()
				if len(parts) != 3 {
					continue
				}
				target := parts[1]
				var remoteInc uint64
				fmt.Sscanf(parts[2], "%d", &remoteInc)

				// ── se mi auto‑sospettano, faccio l’unico bump e gossipo Alive ──
				if target == selfAddr {
					memMu.Lock()
					self := members[selfAddr]
					if remoteInc >= self.Incarnation {
						self.Incarnation = remoteInc + 1 // unico bump
						enqueue(Event{                   // gossipo Alive con nuovo inc
							Kind:        EvAlive,
							Addr:        selfAddr,
							Incarnation: self.Incarnation,
							HopsLeft:    hops,
						})
						log.Printf("[SWIM] self‑suspect: bump+Alive inc=%d", self.Incarnation)
						go gossipNowUDP() // push immediato
					}
					memMu.Unlock()
					continue
				}

				// ── altrimenti, aggiornamento standard per gli altri peer ──
				updateFromRemote(EvSuspect, target, remoteInc)

			case "DEAD": // DEAD <addr> <inc>
				hops = getHops()
				if len(parts) != 3 {
					continue
				}
				target := parts[1]
				var remoteInc uint64
				fmt.Sscanf(parts[2], "%d", &remoteInc)

				// ── se ci dichiarano morti, bump & Alive ──
				if target == selfAddr {
					memMu.Lock()
					self := members[selfAddr]
					if remoteInc >= self.Incarnation {
						self.Incarnation = remoteInc + 1 // unico bump
						enqueue(Event{                   // gossipa Alive
							Kind:        EvAlive,
							Addr:        selfAddr,
							Incarnation: self.Incarnation,
							HopsLeft:    hops,
						})
						log.Printf("[SWIM] self‑dead: bump+Alive inc=%d", self.Incarnation)
						go notifyRegistryAlive(selfAddr) // ←── aggiungi questa linea
						go gossipNowUDP()                // push immediato
					}
					memMu.Unlock()
					continue
				}

				// ── altrimenti, comportamento standard per gli altri ──
				var inc uint64
				fmt.Sscanf(parts[2], "%d", &inc)
				updateFromRemote(EvDead, target, inc)
			case "LEAVE": // LEAVE <addr>
				hops = getHops()
				if len(parts) != 2 {
					continue
				}
				victim := parts[1]
				memMu.Lock()
				if _, ok := members[victim]; ok {
					delete(members, victim)
					log.Printf("[SWIM] %s left the cluster", victim)
				}
				memMu.Unlock()
				// Optionally piggy‑back this event further
				enqueue(Event{Kind: EvLeave, Addr: victim, Incarnation: 0, HopsLeft: hops})
				go gossipNowUDP()
			}
		}
	}
}

// pingPeerUDP invia un PING via UDP e attende l'ACK entro timeout.
func pingPeerUDP(peer string, timeout time.Duration) bool {
	// prepara il waiter
	ch := make(chan struct{})
	ackWaiters.Store(peer, ch)
	defer ackWaiters.Delete(peer)

	// 1) prendi la tua incarnation corrente
	memMu.Lock()
	myInc := members[selfAddr].Incarnation
	memMu.Unlock()

	// 2) invia PING <selfAddr> <incarnation>
	udpAddr, err := net.ResolveUDPAddr("udp", peer)
	if err != nil {
		log.Printf("[PING-UDP] bad addr %s: %v", peer, err)
		return false
	}
	msg := []byte(fmt.Sprintf("PING %s %d\n", selfAddr, myInc))
	if _, err := udpConnection.WriteToUDP(msg, udpAddr); err != nil {
		log.Printf("[PING-UDP] write to %s failed: %v", peer, err)
		return false
	}

	// 3) aspetta ACK o timeout
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

// directPingUDP sostituisce directPing: fa un solo ping e ritorna se è vivo.
func directPingUDP(target string, timeout time.Duration) bool {
	ok := pingPeerUDP(target, timeout)
	if ok {
		// aggiorno lo stato interno
		memMu.Lock()
		if m, exists := members[target]; exists {
			m.State = Alive
			m.LastAck = time.Now()
		}
		memMu.Unlock()
	}
	return ok
}

// indirectPingUDP invia PING-REQ a proxy per target→origin
func indirectPingUDP(proxy, target, origin string, timeout time.Duration) bool {
	// preparo il waiter su origin
	ch := make(chan struct{})
	ackWaiters.Store(origin, ch)
	defer ackWaiters.Delete(origin)

	// invia PING-REQ <target> <origin>
	udpAddr, err := net.ResolveUDPAddr("udp", proxy)
	if err != nil {
		return false
	}
	msg := []byte(fmt.Sprintf("PING-REQ %s %s\n", target, origin))
	if _, err := udpConnection.WriteToUDP(msg, udpAddr); err != nil {
		return false
	}

	// attendo ACK o timeout
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

func antiEntropyLoopUDP(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		// ——— SELF-DEFENCE: “risveglio” automatico ———
		// Prendo la mia incarnation corrente
		memMu.Lock()
		self := members[selfAddr]
		inc := self.Incarnation
		memMu.Unlock()

		// Metto in coda un EvAlive per me stesso
		enqueue(Event{
			Kind:        EvAlive,
			Addr:        selfAddr,
			Incarnation: inc,
			HopsLeft:    getHops(),
		})

		// ——— PUSH GOSSIP vero e proprio ———
		gossipNowUDP()
	}
}

// markDead marca il peer addr come DEAD e innesca side‐effect
func markDead(addr string) {
	if addr == selfAddr {
		return
	}

	// 1) Calcolo hops fuori dal lock
	hopsLeft := getHops()

	// 2) Sotto lock brevissimo cambio stato, inc e queue
	memMu.Lock()
	if m, ok := members[addr]; ok && m.State != Dead {
		m.State = Dead
		enqueue(Event{
			Kind:        EvDead,
			Addr:        addr,
			Incarnation: m.Incarnation,
			HopsLeft:    hopsLeft,
		})
	}
	memMu.Unlock()

	// 3) FUORI lock: notifico registry e faccio gossip
	go notifyRegistryDead(addr)
	go gossipNowUDP()
}

// probeLoopUDP esegue periodicamente il failure‐detector tramite UDP
// -----------------------------------------------------------------------------
// STRICT‑SWIM IMPLEMENTATION
//   • probeLoopUDP  → solo PING, SUSPECT, ping‑req; non marca mai DEAD
//   • reaperLoop    → unico punto in cui si passa da SUSPECT a DEAD
//   • commenti in inglese, come richiesto
// -----------------------------------------------------------------------------

// probeLoopUDP implements the original SWIM failure‑detector strictly:
//  1. choose a random ALIVE peer every probePeriod
//  2. send a direct PING and wait up to probeTimeout for an ACK
//  3. if no ACK, mark the peer SUSPECT and fan‑out k PING‑REQ helpers
//  4. if still no ACK, the peer REMAINS SUSPECT               ←── key change
//  5. promotion to DEAD happens only in reaperLoop, once suspectTimeout expires
func probeLoopUDP() {
	ticker := time.NewTicker(probePeriod)
	defer ticker.Stop()

	for range ticker.C {
		// 1) pick a random ALIVE peer (excluding self)
		target := pickRandomPeer(selfAddr)
		if target == "" {
			continue
		}
		log.Printf("[SWIM] probe → %s", target)

		// 2) direct PING
		if directPingUDP(target, probeTimeout) {
			recordAck(target) // got ACK → ALIVE
			continue
		}

		// 3) no direct ACK → mark SUSPECT
		markSuspect(target)

		// 4) send k PING‑REQ helpers
		helpers := chooseKRandomExcept([]string{selfAddr, target}, getFanout())
		gotAck := make(chan struct{}, 1)

		for _, h := range helpers {
			go func(proxy string) {
				if indirectPingUDP(proxy, target, selfAddr, probeTimeout) {
					gotAck <- struct{}{}
				}
			}(h)
		}

		// 5) wait for an indirect ACK
		select {
		case <-gotAck:
			log.Printf("[SWIM] indirect ACK from %s via helper", target)
			recordAck(target)
		case <-time.After(probeTimeout):
			memMu.Lock()
			state := members[target].State
			memMu.Unlock()
			if state == Suspect {
				log.Printf("[SWIM] no ACK (direct+indirect) for %s, remains SUSPECT", target)
			}

			// STRICT‑SWIM: do *not* mark DEAD here.

		}
	}
}

// reaperLoop is the *only* place where a SUSPECT peer becomes DEAD.
// It checks every suspectTimeout/2 and promotes peers whose LastAck is older
// than suspectTimeout. This implements SWIM’s “subsequent accusation” rule.
func reaperLoop() {
	ticker := time.NewTicker(suspectTimeout / 2)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		var toKill []string

		// collect expired suspects under lock
		memMu.Lock()
		for _, m := range members {
			if m.Addr == selfAddr {
				continue
			}
			if m.State == Suspect && now.Sub(m.LastAck) > suspectTimeout {
				toKill = append(toKill, m.Addr)
			}
		}
		memMu.Unlock()

		// promote to DEAD outside the lock
		for _, addr := range toKill {
			log.Printf("[SWIM:REAPER] mark DEAD %s", addr)

			markDead(addr)
		}
	}
}

// markSuspect marca il peer addr come SUSPECT e innesca gossip immediato
func markSuspect(addr string) {
	if addr == selfAddr {
		return
	}
	// 0) calcolo hops FUORI dal lock per evitare deadlock
	hops := getHops()

	// 1) Sotto lock brevissimo cambio stato, incarnation e queue
	memMu.Lock()
	if m, ok := members[addr]; ok && m.State == Alive {
		m.State = Suspect
		m.Incarnation++
		susTime := time.Now()
		m.LastAck = susTime
		enqueue(Event{
			Kind:        EvSuspect,
			Addr:        addr,
			Incarnation: m.Incarnation,
			HopsLeft:    hops,
		})
		log.Printf("[SWIM] mark SUSPECT %s (inc=%d)", addr, m.Incarnation)
	}
	memMu.Unlock()

	// 2) Spin‐off immediato di un gossip push verso un sottoinsieme di peer
	gossipNowUDP()
}

// recordAck registra l'arrivo di un ACK per addr:
// - aggiorna LastAck
// - mette in coda un EvAlive per piggy-backing
// recordAck registra l'arrivo di un ACK per addr:
// - aggiorna LastAck
// - mette in coda un EvAlive per piggy-backing
func recordAck(addr string) {
	// 1) calcola hops fuori dal lock
	hopsLeft := getHops()

	// 2) muta stato e queue sotto lock brevissimo
	memMu.Lock()
	m, ok := members[addr]
	if ok {
		m.LastAck = time.Now()
		enqueue(Event{
			Kind:        EvAlive,
			Addr:        addr,
			Incarnation: m.Incarnation,
			HopsLeft:    hopsLeft,
		})
	}
	memMu.Unlock()
}

// -----------------------------------------------------------------
// 2) Invia in UDP tutti gli eventi in coda (eventQ) a dst
// -----------------------------------------------------------------
func sendPiggybackUDP(conn *net.UDPConn, dst *net.UDPAddr) {
	evMu.Lock()
	defer evMu.Unlock()

	var buf bytes.Buffer
	nextQ := make([]Event, 0, len(eventQ))

	for _, e := range eventQ {
		switch e.Kind {
		case EvJoin:
			// JOIN ha solo 2 campi
			buf.WriteString(fmt.Sprintf("JOIN %s\n", e.Addr))
		default:
			// tutti gli altri: "<KIND> <addr> <inc>\n"
			buf.WriteString(fmt.Sprintf("%s %s %d\n", e.Kind, e.Addr, e.Incarnation))
		}

		e.HopsLeft--
		if e.HopsLeft > 0 {
			nextQ = append(nextQ, e)
		}
	}
	eventQ = nextQ

	if buf.Len() == 0 {
		return
	}

	if conn.RemoteAddr() != nil {
		_, _ = conn.Write(buf.Bytes())
	} else {
		_, _ = conn.WriteToUDP(buf.Bytes(), dst)
	}
}

func getFanout() int {
	memMu.Lock()
	var peers []string
	for addr, m := range members {
		if addr != selfAddr && m.State == Alive {
			peers = append(peers, addr)
		}
	}
	memMu.Unlock()
	N := len(peers)
	if N < 1 {
		return 1
	}
	fanout := int(math.Ceil(math.Log2(float64(N) + 1)))
	return fanout
}

// -----------------------------------------------------------------
// 3) Tornata attiva di gossip: seleziona fanout peer e invia piggyback
// -----------------------------------------------------------------
func gossipNowUDP() {
	// fanout = ceil(log2(N+1))
	fanout := getFanout()
	// scegli k peer Alive (escludendo self) usando la stessa utilità
	targets := chooseKRandomExcept([]string{selfAddr}, fanout)
	for _, addr := range targets {
		go func(a string) {
			udpAddr, err := net.ResolveUDPAddr("udp", a)
			if err != nil {
				log.Printf("[GOSSIP] bad UDP addr %s: %v", a, err)
				return
			}
			conn, err := net.DialUDP("udp", nil, udpAddr)
			if err != nil {
				log.Printf("[GOSSIP] dial UDP %s: %v", a, err)
				return
			}
			defer conn.Close()
			sendPiggybackUDP(conn, udpAddr)
			log.Printf("[GOSSIP] sent piggyback to %s", a)
		}(addr)
	}
}

// -----------------------------------------------------------------
// 4) Enqueue dell’evento JOIN e avvio immediato del gossip
// -----------------------------------------------------------------
func diffuseJoin(selfAddr string) {
	e := Event{
		Kind:        EvJoin,
		Addr:        selfAddr,
		Incarnation: 0,
		HopsLeft:    getHops(),
	}
	evMu.Lock()
	eventQ = append(eventQ, e)
	evMu.Unlock()
	go gossipNowUDP()
}

// -----------------------------------------------------------------
// 5) Helper per convertire la stringa in EventType
// -----------------------------------------------------------------
func parseEventType(s string) EventType {
	switch strings.ToUpper(s) {
	case "JOIN":
		return EvJoin
	case "ALIVE":
		return EvAlive
	case "SUSPECT":
		return EvSuspect
	case "DEAD":
		return EvDead
	case "LEAVE":
		return EvLeave
	default:
		return EvUnknown
	}
}

func alreadySeen(e Event) bool {
	k := fmt.Sprintf("%d|%s", e.Kind, e.Addr)
	seenMu.Lock()
	defer seenMu.Unlock()
	old, exists := seen[k]
	if exists && e.Incarnation <= old {
		return true
	}
	seen[k] = e.Incarnation
	return false
}
