package main

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"log"
	"math"
	"net"
	"strings"
	"time"
)

const (
	probePeriod    = 1 * time.Second
	probeTimeout   = 900 * time.Millisecond
	suspectTimeout = 6 * time.Second
	dumpInterval   = 10 * time.Second
	// gossip.go (o swim.go)
	maxPiggybackEntries = 100
)

func enqueue(e Event) {
	evMu.Lock()
	defer evMu.Unlock()

	// Se è un EvAlive per questo nodo, rimuovo quelli vecchi
	if e.Kind == EvAlive && e.Addr == selfAddr {
		filtered := eventQ[:0] // riutilizzo lo slice
		for _, old := range eventQ {
			// tengo tutto tranne i miei EvAlive
			if !(old.Kind == EvAlive && old.Addr == selfAddr) {
				filtered = append(filtered, old)
			}
		}
		eventQ = filtered
	}

	// Ora aggiungo l’evento (sia EvAlive che tutti gli altri)
	eventQ = append(eventQ, e)
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

// computeGraceHops restituisce quanti round di probe saltare:
// un numero pari a log₂(N+1) + suspectTimeout/probePeriod
func computeGraceHops() int {
	// hops per topologia
	base := getHops()
	// round necessari a coprire suspectTimeout
	extra := int(math.Ceil(float64(suspectTimeout) / float64(probePeriod)))
	return base + extra
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
			gracehops := computeGraceHops()

			switch parts[0] {

			case "PING": // PING <origin> <inc> <uuid>
				if len(parts) != 4 {
					continue
				}
				origin := parts[1]
				var remoteInc uint64
				fmt.Sscanf(parts[2], "%d", &remoteInc)
				id := parts[3]

				// ── resurrection / implicit‑join logic ──
				memMu.Lock()
				m, exists := members[origin]
				if exists && m.State == Dead {
					// resurrect
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
						GraceHops:   gracehops,
					}
					enqueue(Event{Kind: EvAlive, Addr: origin, Incarnation: remoteInc, HopsLeft: hops})
					log.Printf("[SWIM] resurrected %s with inc=%d via PING", origin, remoteInc)
					go gossipNowUDP()

				} else if !exists {
					// new peer discovered
					members[origin] = &Member{
						Addr:        origin,
						State:       Alive,
						Incarnation: remoteInc,
						LastAck:     time.Now(),
						GraceHops:   gracehops,
					}
					enqueue(Event{Kind: EvJoin, Addr: origin, Incarnation: remoteInc, HopsLeft: hops})
					log.Printf("[SWIM] discovered new peer %s via PING", origin)
					go gossipNowUDP()
				}
				memMu.Unlock()

				// ── inline EvAlive ──
				memMu.Lock()
				if m, ok := members[origin]; ok {
					if remoteInc >= m.Incarnation {
						changed := remoteInc > m.Incarnation || m.State != Alive
						if changed {
							if m.State == Dead {
								log.Printf("[SWIM] %s resurrected → ALIVE (inc=%d)", origin, remoteInc)
								go notifyRegistryAlive(origin)
							}
							m.State = Alive
							m.Incarnation = remoteInc
							go gossipNowUDP()
						}
						m.LastAck = time.Now()
					}
				}
				memMu.Unlock()

				reply := []byte(fmt.Sprintf("ACK %s\n", id))
				_, _ = conn.WriteToUDP(reply, src)
				sendPiggybackUDP(conn, src)

			case "PING-REQ": // PING-REQ <target> <origin> <inc> <uuid>
				if len(parts) != 5 {
					continue
				}
				target := parts[1]
				origin := parts[2]
				var remoteInc uint64
				fmt.Sscanf(parts[3], "%d", &remoteInc)
				id := parts[4]

				// ── resurrection / implicit‑join logic per origin ──
				memMu.Lock()
				m, exists := members[origin]
				if exists && m.State == Dead {
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
						GraceHops:   gracehops,
					}
					enqueue(Event{Kind: EvAlive, Addr: origin, Incarnation: remoteInc, HopsLeft: hops})
					log.Printf("[SWIM] resurrected %s with inc=%d via PING-REQ", origin, remoteInc)
					go gossipNowUDP()

				} else if !exists {
					members[origin] = &Member{
						Addr:        origin,
						State:       Alive,
						Incarnation: remoteInc,
						LastAck:     time.Now(),
						GraceHops:   gracehops,
					}
					enqueue(Event{Kind: EvJoin, Addr: origin, Incarnation: remoteInc, HopsLeft: hops})
					log.Printf("[SWIM] discovered new peer %s via PING-REQ", origin)
					go gossipNowUDP()
				}
				memMu.Unlock()

				// inoltro PING al target mantenendo lo stesso UUID
				if udpAddr, err := net.ResolveUDPAddr("udp", target); err == nil {
					msg := []byte(fmt.Sprintf("PING %s %d %s\n", origin, remoteInc, id))
					_, _ = conn.WriteToUDP(msg, udpAddr)
				}
				sendPiggybackUDP(conn, src)

			case "ACK": // ACK <uuid>
				if len(parts) != 2 {
					continue
				}
				id := parts[1]
				if chAny, ok := ackWaiters.LoadAndDelete(id); ok {
					close(chAny.(chan struct{}))
				}
				// piggy‑back gossip sulla risposta
				sendPiggybackUDP(conn, src)

			case "JOIN": // JOIN <addr> [inc]
				if len(parts) < 2 || len(parts) > 3 {
					continue
				}
				newAddr := parts[1]
				memMu.Lock()
				if _, known := members[newAddr]; !known {
					members[newAddr] = &Member{
						Addr:        newAddr,
						State:       Alive,
						Incarnation: 0,
						LastAck:     time.Now(),
						GraceHops:   gracehops,
					}
					enqueue(Event{Kind: EvJoin, Addr: newAddr, Incarnation: 0, HopsLeft: hops})
					log.Printf("[GOSSIP] membership after JOIN: %v", memberAddrs())
					go gossipNowUDP()
				}
				memMu.Unlock()
				sendPiggybackUDP(conn, src)

			case "SUSPECT": // SUSPECT <addr> <inc>
				if len(parts) != 3 {
					continue
				}
				target := parts[1]
				var remoteInc uint64
				fmt.Sscanf(parts[2], "%d", &remoteInc)

				if target == selfAddr {
					// self‑suspect bump
					memMu.Lock()
					self := members[selfAddr]
					if remoteInc >= self.Incarnation {
						self.Incarnation = remoteInc + 1
						enqueue(Event{Kind: EvAlive, Addr: selfAddr, Incarnation: self.Incarnation, HopsLeft: hops})
						log.Printf("[SWIM] self‑suspect: bump+Alive inc=%d", self.Incarnation)
						go gossipNowUDP()
					}
					memMu.Unlock()
					continue
				}

				// inline EvSuspect
				memMu.Lock()
				if m, ok := members[target]; ok && remoteInc >= m.Incarnation && m.State == Alive {
					m.State = Suspect
					m.Incarnation = remoteInc
					m.LastAck = time.Now()
					enqueue(Event{Kind: EvSuspect, Addr: target, Incarnation: remoteInc, HopsLeft: hops})
					log.Printf("[SWIM] %s → SUSPECT (inc=%d)", target, remoteInc)
					go gossipNowUDP()
				}
				memMu.Unlock()

			case "DEAD": // DEAD <addr> <inc>
				if len(parts) != 3 {
					continue
				}
				target := parts[1]
				var inc uint64
				fmt.Sscanf(parts[2], "%d", &inc)

				if target == selfAddr {
					// self‑dead bump
					memMu.Lock()
					self := members[selfAddr]
					if inc >= self.Incarnation {
						self.Incarnation = inc + 1
						enqueue(Event{Kind: EvAlive, Addr: selfAddr, Incarnation: self.Incarnation, HopsLeft: hops})
						log.Printf("[SWIM] self‑dead: bump+Alive inc=%d", self.Incarnation)
						go notifyRegistryAlive(selfAddr)
						go gossipNowUDP()
					}
					memMu.Unlock()
					continue
				}

				// inline EvDead
				memMu.Lock()
				if m, ok := members[target]; ok && inc >= m.Incarnation && m.State != Dead {
					m.State = Dead
					m.Incarnation = inc
					log.Printf("[SWIM] %s → DEAD (inc=%d)", target, inc)
					go notifyRegistryDead(target)
					go gossipNowUDP()
				}
				memMu.Unlock()

			case "LEAVE": // LEAVE <addr>
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
				go gossipNowUDP()

			case "ALIVE": // ALIVE <addr> <inc> <graceHops>
				if len(parts) != 4 {
					continue
				}
				origin := parts[1]
				var remoteInc uint64
				fmt.Sscanf(parts[2], "%d", &remoteInc)
				var remoteGrace int
				fmt.Sscanf(parts[3], "%d", &remoteGrace)

				// inline EvAlive + new‑peer via gossip
				memMu.Lock()
				m, known := members[origin]
				if !known {
					members[origin] = &Member{
						Addr:        origin,
						State:       Alive,
						Incarnation: remoteInc,
						LastAck:     time.Now(),
						GraceHops:   remoteGrace,
					}
					memMu.Unlock()
					enqueue(Event{Kind: EvJoin, Addr: origin, Incarnation: remoteInc, HopsLeft: hops})
					log.Printf("[SWIM] discovered new peer %s via ALIVE", origin)
					go gossipNowUDP()
					break
				}
				// già noto
				if remoteInc >= m.Incarnation {
					changed := remoteInc > m.Incarnation || m.State != Alive
					if changed {
						if m.State == Dead {
							log.Printf("[SWIM] %s resurrected → ALIVE (inc=%d)", origin, remoteInc)
							go notifyRegistryAlive(origin)
						}
						m.State = Alive
						m.Incarnation = remoteInc
						if remoteGrace > m.GraceHops {
							m.GraceHops = remoteGrace
						}
						go gossipNowUDP()
					}
					m.LastAck = time.Now()
				}
				memMu.Unlock()
			}
		}
	}
}

// pingPeerUDP invia un PING via UDP e attende l'ACK entro timeout.
func pingPeerUDP(peer string, timeout time.Duration) bool {
	// 1) prepara canale e UUID
	ch := make(chan struct{})
	id := uuid.New().String()
	ackWaiters.Store(id, ch)
	defer ackWaiters.Delete(id)

	// 2) prendi la mia incarnation corrente
	memMu.Lock()
	myInc := members[selfAddr].Incarnation
	memMu.Unlock()

	// 3) invio PING <origin> <inc> <uuid>
	udpAddr, err := net.ResolveUDPAddr("udp", peer)
	if err != nil {
		log.Printf("[PING-UDP] bad addr %s: %v", peer, err)
		return false
	}
	msg := []byte(fmt.Sprintf("PING %s %d %s\n", selfAddr, myInc, id))
	if _, err := udpConnection.WriteToUDP(msg, udpAddr); err != nil {
		log.Printf("[PING-UDP] write to %s failed: %v", peer, err)
		return false
	}

	// 4) aspetto ACK <uuid> o timeout
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
	ch := make(chan struct{})
	id := uuid.New().String()
	ackWaiters.Store(id, ch)
	defer ackWaiters.Delete(id)

	// prendi la mia incarnation corrente
	memMu.Lock()
	myInc := members[selfAddr].Incarnation
	memMu.Unlock()

	// invio PING-REQ <target> <origin> <inc> <uuid>
	udpAddr, err := net.ResolveUDPAddr("udp", proxy)
	if err != nil {
		return false
	}
	msg := []byte(fmt.Sprintf("PING-REQ %s %s %d %s\n", target, origin, myInc, id))
	if _, err := udpConnection.WriteToUDP(msg, udpAddr); err != nil {
		return false
	}

	// aspetto ACK <uuid> o timeout
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
		// ——— 1) SELF‑HEARTBEAT ———
		memMu.Lock()
		inc := members[selfAddr].Incarnation
		memMu.Unlock()
		enqueue(Event{
			Kind:        EvAlive,
			Addr:        selfAddr,
			Incarnation: inc,
			HopsLeft:    getHops(),
		})
		log.Printf("[ANTI‑ENTROPY] enqueued self‑heartbeat (inc=%d)", inc)

		// ——— 2) FAN‑OUT GOSSIP PUSH ———
		// Sfruttiamo gossipNowUDP che già sceglie K peer e chiama sendPiggybackUDP.
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
		memMu.Lock()
		for _, m := range members {
			if m.GraceHops > 0 && m.Addr == target {
				m.GraceHops--
			}
		}
		memMu.Unlock()

		// 2) direct PING
		if directPingUDP(target, probeTimeout) {
			recordAck(target) // got ACK → ALIVE
			continue
		}

		// 3) no direct ACK → mark SUSPECT (skip if still in grace)
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

// reaperLoop è l’unico punto dove un peer SUSPECT → DEAD
func reaperLoop() {
	ticker := time.NewTicker(suspectTimeout / 2)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		var toKill []string

		// 1) raccolgo in lock tutti i suspect scaduti
		memMu.Lock()
		for _, m := range members {
			if m.Addr == selfAddr {
				continue
			}
			if m.State == Suspect && now.Sub(m.LastAck) > suspectTimeout {
				if m.GraceHops > 0 {
					m.GraceHops-- // un altro “colpo di grazia” consumato
					continue      // gli concedo un giro in più
				}
				toKill = append(toKill, m.Addr) // niente più vite → DEAD
			}

		}
		memMu.Unlock()

		// 2) per ognuno, log di CONFIRM e poi markDead
		for _, addr := range toKill {
			log.Printf("[SWIM] CONFIRM DEAD %s", addr)
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
		if m.GraceHops > 0 {
			m.GraceHops--
			// ancora nel periodo di grazia → ignoro
			memMu.Unlock()
			log.Printf("[SWIM] skipping SUSPECT for %s (%d rounds left)", addr, m.GraceHops)
			return
		}
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
func recordAck(addr string) {
	// 1) calcola hops fuori dal lock
	hopsLeft := getHops()

	// 2) muta stato e queue sotto lock brevissimo
	memMu.Lock()
	m, ok := members[addr]
	if ok {
		prevState := m.State
		m.LastAck = time.Now()
		// se veniva da SUSPECT, facciamo un log specifico
		if prevState == Suspect {
			log.Printf("[SWIM] CANCEL SUSPECT %s", addr)
		}
		m.State = Alive
		enqueue(Event{
			Kind:        EvAlive,
			Addr:        addr,
			Incarnation: m.Incarnation,
			HopsLeft:    hopsLeft,
		})
	}
	memMu.Unlock()
}

// sendPiggybackUDP invia in UDP fino a maxPiggybackEntries eventi in coda (eventQ) a dst
func sendPiggybackUDP(conn *net.UDPConn, dst *net.UDPAddr) {
	evMu.Lock()
	defer evMu.Unlock()

	if len(eventQ) == 0 {
		return
	}

	// 1) Suddividi in tre bucket in base alla priorità
	var crit, joins, alives []Event
	for _, e := range eventQ {
		switch e.Kind {
		case EvSuspect, EvDead, EvLeave:
			crit = append(crit, e)
		case EvJoin:
			joins = append(joins, e)
		case EvAlive:
			alives = append(alives, e)
		}
	}

	// 2) Scegli fino a maxPiggybackEntries seguendo la priorità
	toSend := make([]Event, 0, maxPiggybackEntries)
	pick := func(list []Event) {
		for _, e := range list {
			if len(toSend) >= maxPiggybackEntries {
				return
			}
			toSend = append(toSend, e)
		}
	}
	pick(crit)
	pick(joins)
	pick(alives)

	// 3) Log di quanti ne andremo a inviare
	log.Printf("[GOSSIP] piggyback: invio %d/%d eventi (crit=%d, join=%d, alive=%d)",
		len(toSend), len(eventQ), len(crit), len(joins), len(alives))

	// 4) Costruisci il payload
	var buf bytes.Buffer
	nextQ := make([]Event, 0, len(eventQ))
	sendSet := make(map[int]struct{}, len(toSend))
	for i, e := range toSend {
		sendSet[i] = struct{}{} // marcatore per decrement hops
		switch e.Kind {
		case EvJoin:
			buf.WriteString(fmt.Sprintf("JOIN %s\n", e.Addr))
		case EvAlive:
			gh := members[e.Addr].GraceHops
			buf.WriteString(fmt.Sprintf("ALIVE %s %d %d\n", e.Addr, e.Incarnation, gh))
		case EvLeave:
			// due campi, senza incarnation
			buf.WriteString(fmt.Sprintf("LEAVE %s\n", e.Addr))
		default: // EvSuspect, EvDead
			buf.WriteString(fmt.Sprintf("%s %s %d\n", e.Kind, e.Addr, e.Incarnation))
		}
	}

	// 5) Ricostruisci la coda con gli eventi non ancora scaduti (hopsLeft>0)
	for _, e := range eventQ {
		e.HopsLeft--
		if e.HopsLeft > 0 {
			nextQ = append(nextQ, e)
		}
	}
	eventQ = nextQ

	// 6) Invio effettivo
	if buf.Len() > 0 {
		if conn.RemoteAddr() != nil {
			_, err := conn.Write(buf.Bytes())
			if err != nil {
				log.Printf("[GOSSIP] piggyback write error: %v", err)
			}
		} else {
			_, err := conn.WriteToUDP(buf.Bytes(), dst)
			if err != nil {
				log.Printf("[GOSSIP] piggyback write error: %v", err)
			}
		}
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

// voluntaryLeave inietta un leave nel gossip e poi notifica il registry.
func voluntaryLeave() {
	// 1) inc e hops
	memMu.Lock()
	inc := members[selfAddr].Incarnation
	memMu.Unlock()
	hops := getHops()

	// 2) enqueue + gossip push immediato
	enqueue(Event{Kind: EvLeave, Addr: selfAddr, Incarnation: inc, HopsLeft: hops})
	log.Printf("[SWIM] voluntary LEAVE enqueued (inc=%d)", inc)
	go gossipNowUDP()

	// 3) notifica registry via TCP
	notifyRegistryLeave(selfAddr)
}
