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
	probePeriod    = 5 * time.Second
	probeTimeout   = 500 * time.Millisecond
	dumpInterval   = 10 * time.Second
	suspectTimeout = 15 * time.Second
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
			log.Printf("[GOSSIP-UDP] read error: %v", err)
			continue
		}
		for _, line := range strings.Split(string(buf[:n]), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			parts := strings.Fields(line)
			switch parts[0] {
			case "PING": // PING <origin>
				if len(parts) != 2 {
					continue
				}
				origin := parts[1]
				// 1) aggiorna subito Alive via SWIM
				updateFromRemote(EvAlive, origin, 0)
				// 2) rispondi con ACK via UDP
				reply := []byte(fmt.Sprintf("ACK %s\n", selfAddr))
				if _, err := conn.WriteToUDP(reply, src); err != nil {
					log.Printf("[GOSSIP-UDP] ACK to %s failed: %v", src.String(), err)
				}
				// 3) piggy-back dei tuoi eventi sul canale UDP di reply
				//    (se vuoi, puoi piggyback anche qui, ma non è necessario)
			case "ACK": // ACK <who>
				if len(parts) != 2 {
					continue
				}
				who := parts[1]
				if chAny, ok := ackWaiters.LoadAndDelete(who); ok {
					close(chAny.(chan struct{}))
				}
			case "JOIN": // JOIN <newAddr>
				if len(parts) != 2 {
					continue
				}
				newAddr := parts[1]

				// 1) aggiorno la membership locale
				updateFromRemote(EvJoin, newAddr, 0)

				// 2) stampo subito la lista aggiornata
				memMu.Lock()
				var peers []string
				for addr := range members {
					peers = append(peers, addr)
				}
				memMu.Unlock()
				log.Printf("[GOSSIP-UDP] membership after JOIN: %v", peers)

				// 3) piggy-back dei nostri eventi verso chi ha inviato il JOIN
				sendPiggybackUDP(conn, src)
			case "SUSPECT": // SUSPECT <addr> <inc>
				if len(parts) != 3 {
					continue
				}
				var inc uint64
				fmt.Sscanf(parts[2], "%d", &inc)
				updateFromRemote(EvSuspect, parts[1], inc)
			case "DEAD": // DEAD <addr> <inc>
				if len(parts) != 3 {
					continue
				}
				var inc uint64
				fmt.Sscanf(parts[2], "%d", &inc)
				updateFromRemote(EvDead, parts[1], inc)

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

	// invia PING <selfAddr>
	udpAddr, err := net.ResolveUDPAddr("udp", peer)
	if err != nil {
		log.Printf("[PING-UDP] bad addr %s: %v", peer, err)
		return false
	}
	msg := []byte(fmt.Sprintf("PING %s\n", selfAddr))
	if _, err := udpConnection.WriteToUDP(msg, udpAddr); err != nil {
		log.Printf("[PING-UDP] write to %s failed: %v", peer, err)
		return false
	}

	// aspetta ACK o timeout
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

// antiEntropyLoopUDP effettua periodicamente un push gossip
// a k = ceil(log2(N+1)) peer scelti a caso (skip self/SUSPECT/DEAD)
func antiEntropyLoopUDP(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		gossipNowUDP()
	}
}

func reaperLoop() {
	ticker := time.NewTicker(suspectTimeout / 2)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		var toKill []string

		// 1) sotto lock raccolgo i suspect scaduti
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

		// 2) fuori dal lock promuovo a DEAD
		for _, addr := range toKill {
			markDead(addr)
		}
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
		m.Incarnation++
		enqueue(Event{
			Kind:        EvDead,
			Addr:        addr,
			Incarnation: m.Incarnation,
			HopsLeft:    hopsLeft,
		})
		log.Printf("[SWIM] mark DEAD %s (inc=%d)", addr, m.Incarnation)
	}
	memMu.Unlock()

	// 3) FUORI lock: notifico registry e faccio gossip
	go notifyRegistryDead(addr)
	go gossipNowUDP()
}

// probeLoopUDP esegue periodicamente il failure‐detector tramite UDP
func probeLoopUDP() {
	ticker := time.NewTicker(probePeriod)
	defer ticker.Stop()

	for range ticker.C {
		// 1) sceglie un peer Alive a caso (escludendo se stesso)
		target := pickRandomPeer(selfAddr)
		if target == "" {
			continue
		}
		log.Printf("[SWIM] probe → %s", target)

		// 2) ping diretto via UDP
		if directPingUDP(target, probeTimeout) {
			// ACK ricevuto: mark Alive e gossipa ALIVE
			recordAck(target)
		} else {
			// 3) nessun ACK: mark Suspect e avvia ping-req indiretti
			markSuspect(target)

			helpers := chooseKRandomExcept([]string{selfAddr, target}, getFanout())
			gotAck := make(chan struct{}, 1)

			for _, h := range helpers {
				go func(proxy string) {
					if indirectPingUDP(proxy, target, selfAddr, probeTimeout) {
						gotAck <- struct{}{}
					}
				}(h)
			}

			// 4) aspetta il primo ACK indiretto o timeout
			select {
			case <-gotAck:
				log.Printf("[SWIM] indirect ACK from %s via helper", target)
				recordAck(target)
			case <-time.After(probeTimeout):
				// 5) ancora nulla: mark Dead e gossipa DEAD
				markDead(target)
			}
		}
	}
}

// markSuspect marca il peer addr come SUSPECT e innesca gossip immediato
func markSuspect(addr string) {
	if addr == selfAddr {
		return
	}
	// 1) Sotto lock brevissimo cambio stato e inc e queue
	memMu.Lock()
	if m, ok := members[addr]; ok && m.State == Alive {
		m.State = Suspect
		m.Incarnation++
		enqueue(Event{
			Kind:        EvSuspect,
			Addr:        addr,
			Incarnation: m.Incarnation,
			HopsLeft:    getHops(),
		})
		log.Printf("[SWIM] mark SUSPECT %s (inc=%d)", addr, m.Incarnation)
	}
	memMu.Unlock()
	// 2) Spin off immediato di un gossip push verso un sottoinsieme di peer
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
		buf.WriteString(fmt.Sprintf("%s %s %d\n", e.Kind, e.Addr, e.Incarnation))
		e.HopsLeft--
		if e.HopsLeft > 0 {
			nextQ = append(nextQ, e)
		}
	}
	eventQ = nextQ

	data := buf.Bytes()
	if len(data) == 0 {
		return
	}

	// Se il socket è "connected" (DialUDP), usa Write; altrimenti WriteToUDP
	if conn.RemoteAddr() != nil {
		// socket connesso → invia con Write
		if _, err := conn.Write(data); err != nil {
			log.Printf("[GOSSIP-UDP] write (connected) to %s failed: %v", conn.RemoteAddr(), err)
		}
	} else {
		// socket non connesso (listener) → invia verso dst
		if _, err := conn.WriteToUDP(data, dst); err != nil {
			log.Printf("[GOSSIP-UDP] writeToUDP to %s failed: %v", dst.String(), err)
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
