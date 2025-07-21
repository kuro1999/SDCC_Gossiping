package main

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"log"
	"math"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	probePeriod    = 1*time.Second + 500*time.Millisecond
	probeTimeout   = 1*time.Second + 100*time.Millisecond
	suspectTimeout = 8 * time.Second
	dumpInterval   = 10 * time.Second
	// gossip.go (o swim.go)
	maxPiggybackEntries = 128
)

type MemberDigestEntry struct {
	Addr        string `json:"addr"`
	State       string `json:"state"` // "Alive", "Suspect", "Dead"
	Incarnation uint64 `json:"incarnation"`
	LastAck     int64  `json:"last_ack"` // unix nano
	GraceHops   int    `json:"grace_hops"`
}

type DigestMessage struct {
	Type    string              `json:"type"` // "DIGEST" o "SYNC"
	Entries []MemberDigestEntry `json:"entries"`
}

func sendDigest(peer string) {
	// 1) Raccogli lo stato corrente
	memMu.Lock()
	entries := make([]MemberDigestEntry, 0, len(members))
	for _, m := range members {
		entries = append(entries, MemberDigestEntry{
			Addr:        m.Addr,
			State:       m.State.String(),
			Incarnation: m.Incarnation,
			LastAck:     m.LastAck.UnixNano(),
			GraceHops:   m.GraceHops,
		})
	}
	memMu.Unlock()

	// 2) Crea il messaggio JSON
	msg := DigestMessage{Type: "DIGEST", Entries: entries}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("[ANTI‑ENTROPY] marshal digest: %v", err)
		return
	}

	// 3) Invia in UDP
	udpAddr, err := net.ResolveUDPAddr("udp", peer)
	if err != nil {
		log.Printf("[ANTI‑ENTROPY] bad addr %s: %v", peer, err)
		return
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		log.Printf("[ANTI‑ENTROPY] dial %s: %v", peer, err)
		return
	}
	defer conn.Close()

	if _, err := conn.Write(data); err != nil {
		log.Printf("[ANTI‑ENTROPY] send digest to %s: %v", peer, err)
	}
}

// ——— 3) Gestione in ingresso di DIGEST e SYNC ———

func handleDigest(src *net.UDPAddr, dm DigestMessage, conn *net.UDPConn) {
	var toSync []MemberDigestEntry

	// 1) Reconciliation: per ogni entry in dm, confronta con local members
	for _, e := range dm.Entries {
		memMu.Lock()
		local, exists := members[e.Addr]
		if !exists || e.Incarnation > local.Incarnation {
			// aggiornamento: nuovo peer / stato più recente
			st := parseState(e.State)
			members[e.Addr] = &Member{
				Addr:        e.Addr,
				State:       st,
				Incarnation: e.Incarnation,
				LastAck:     time.Unix(0, e.LastAck),
				GraceHops:   e.GraceHops,
			}
			enqueue(Event{
				Kind:        stateToEventKind(st),
				Addr:        e.Addr,
				Incarnation: e.Incarnation,
				HopsLeft:    getHops(),
			})
		} else if local.Incarnation > e.Incarnation {
			// ho io stato più nuovo → lo includo in risposta SYNC
			toSync = append(toSync, MemberDigestEntry{
				Addr:        local.Addr,
				State:       local.State.String(),
				Incarnation: local.Incarnation,
				LastAck:     local.LastAck.UnixNano(),
				GraceHops:   local.GraceHops,
			})
		}
		memMu.Unlock()
	}

	// 2) Rispondi con un SYNC solo se hai delle entry da inviare
	if len(toSync) > 0 {
		resp := DigestMessage{Type: "SYNC", Entries: toSync}
		data, err := json.Marshal(resp)
		if err != nil {
			log.Printf("[ANTI‑ENTROPY] marshal sync: %v", err)
			return
		}
		if _, err := conn.WriteToUDP(data, src); err != nil {
			log.Printf("[ANTI‑ENTROPY] send sync: %v", err)
		}
	}
}

func handleSync(dm DigestMessage) {
	// 1) Applica esattamente la stessa reconciliation di handleDigest, senza rispondere
	for _, e := range dm.Entries {
		memMu.Lock()
		local, exists := members[e.Addr]
		if !exists || e.Incarnation > local.Incarnation {
			st := parseState(e.State)
			members[e.Addr] = &Member{
				Addr:        e.Addr,
				State:       st,
				Incarnation: e.Incarnation,
				LastAck:     time.Unix(0, e.LastAck),
				GraceHops:   e.GraceHops,
			}
			enqueue(Event{
				Kind:        stateToEventKind(st),
				Addr:        e.Addr,
				Incarnation: e.Incarnation,
				HopsLeft:    getHops(),
			})
		}
		memMu.Unlock()
	}
}

// ——— 4) Helper per parse dello stato e mapping verso gli EventKind ———

func parseState(s string) State {
	switch s {
	case "Alive":
		return Alive
	case "Suspect":
		return Suspect
	case "Dead":
		return Dead
	default:
		return Alive
	}
}

func stateToEventKind(st State) EventType {
	switch st {
	case Alive:
		return EvAlive
	case Suspect:
		return EvSuspect
	case Dead:
		return EvDead
	}
	return EvAlive
}

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
		data := buf[:n]
		// 5.1) Provo a interpretarlo come JSON DIGEST/SYNC
		var dm DigestMessage
		if err := json.Unmarshal(data, &dm); err == nil {
			switch dm.Type {
			case "DIGEST":
				handleDigest(src, dm, conn)
				continue
			case "SYNC":
				handleSync(dm)
				continue
			}
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

// ——— 6) Nuovo loop anti‑entropy push‑pull ———

func antiEntropyLoopUDP(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		// 6.2) Push‑pull verso un peer casuale
		peer := pickRandomPeer(selfAddr)
		if peer != "" {
			go sendDigest(peer)
		} else {
			return
		}
		// 6.1) Self‑heartbeat
		memMu.Lock()
		inc := members[selfAddr].Incarnation
		memMu.Unlock()
		enqueue(Event{Kind: EvAlive, Addr: selfAddr, Incarnation: inc, HopsLeft: getHops()})
		log.Printf("[ANTI‑ENTROPY] self‑heartbeat enqueued (inc=%d)", inc)

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
		// A) scegli k peer da pingare
		targets := chooseKRandomExcept([]string{selfAddr}, getFanout())
		if len(targets) == 0 {
			continue
		}

		// B) PING diretti in parallelo
		failed := make(chan string, len(targets))
		var wg sync.WaitGroup
		for _, t := range targets {
			wg.Add(1)
			go func(target string) {
				defer wg.Done()
				if directPingUDP(target, probeTimeout) {
					recordAck(target) // è vivo
				} else {
					failed <- target // niente ACK diretto
				}
			}(t)
		}
		wg.Wait()
		close(failed)

		// C) Per ogni peer che NON ha risposto: PING‑REQ
		for target := range failed {
			helpers := chooseKRandomExcept([]string{selfAddr, target}, getFanout())

			// se non ci sono helper possibili, ci arrendiamo subito:
			if len(helpers) == 0 {
				markSuspect(target)
				continue
			}

			gotAck := make(chan struct{}, 1)
			for _, h := range helpers {
				go func(proxy string) {
					if indirectPingUDP(proxy, target, selfAddr, probeTimeout) {
						gotAck <- struct{}{}
					}
				}(h)
			}

			select {
			case <-gotAck:
				recordAck(target) // ack indiretto → ALIVE
			case <-time.After(probeTimeout):
				markSuspect(target) // fallito anche l’indiretto
			}
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

// allAlivePeers restituisce la lista di tutti i peer attualmente in stato ALIVE
// (escludendo se stesso).  La slice ritornata è una *copia*, quindi chi la
// usa può modificarla senza correre il rischio di data‑race.
func allAlivePeers() []string {
	memMu.Lock()
	defer memMu.Unlock()

	peers := make([]string, 0, len(members))
	for addr, m := range members {
		if m.State == Alive && addr != selfAddr {
			peers = append(peers, addr)
		}
	}
	return peers
}

// -----------------------------------------------------------------
// 3) Tornata attiva di gossip: seleziona fanout peer e invia piggyback
// -----------------------------------------------------------------
func gossipNowUDP() {
	// fanout = ceil(log2(N+1))
	fanout := getFanout()
	// scegli k peer Alive (escludendo self) usando la stessa utilità
	targets := chooseKRandomExcept(allAlivePeers(), fanout)
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
