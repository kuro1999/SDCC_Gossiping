// swim.go — minimal SWIM‑like detector & membership service (with debug logs)
// -----------------------------------------------------------------------------
package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

// -------------------------------------------------------------
// 1.  Membership record & states
// -------------------------------------------------------------

type State int

const gossipTTL = 3

const (
	Alive State = iota
	Suspect
	Dead
)

func (s State) String() string {
	switch s {
	case Alive:
		return "ALIVE"
	case Suspect:
		return "SUSPECT"
	case Dead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

type Member struct {
	Addr        string
	State       State
	Incarnation uint64
	LastAck     time.Time
}

var (
	members  = map[string]*Member{}
	memMu    sync.Mutex
	eventQ   []Event
	evMu     sync.Mutex
	selfAddr string
	ackCh    = make(chan string, 16)

	// riempito da StartSWIM per i notifier verso il registry
	registryAddr string
)

// -------------------------------------------------------------
// 2.  Events disseminated via gossip
// -------------------------------------------------------------

type EventType int

const (
	EvJoin EventType = iota
	EvSuspect
	EvDead
	EvAlive
)

func (t EventType) String() string {
	switch t {
	case EvJoin:
		return "JOIN"
	case EvSuspect:
		return "SUSPECT"
	case EvDead:
		return "DEAD"
	case EvAlive:
		return "ALIVE"
	default:
		return "UNK"
	}
}

type Event struct {
	Kind        EventType
	Addr        string
	Incarnation uint64
	HopsLeft    int // nuovo
}

func enqueue(e Event) {
	evMu.Lock()
	eventQ = append(eventQ, e)
	evMu.Unlock()
}

// -------------------------------------------------------------
// 3.  SWIM hooks per node.go
// -------------------------------------------------------------

func addInitialPeers(myAddr string, peers []string) {
	memMu.Lock()
	defer memMu.Unlock()

	members[myAddr] = &Member{Addr: myAddr, State: Alive, Incarnation: 0, LastAck: time.Now()}
	for _, p := range peers {
		if p == myAddr {
			continue
		}
		members[p] = &Member{Addr: p, State: Alive, Incarnation: 0, LastAck: time.Now()}
	}
	log.Printf("[SWIM] initial membership: %v", peers)
}

func updateFromRemote(kind EventType, addr string, inc uint64) {
	// 1) costruisci subito l’evento e scarta i duplicati
	e := Event{Kind: kind, Addr: addr, Incarnation: inc, HopsLeft: gossipTTL}
	if alreadySeen(e) {
		return
	}
	enqueue(e)

	memMu.Lock()
	defer memMu.Unlock()

	// ------------------------------------------------------------------
	//  AUTO-RESURRECTION: se sono io e non è un alive, mi difendo
	// ------------------------------------------------------------------
	if addr == selfAddr && kind != EvAlive {
		self := members[selfAddr]
		self.Incarnation++
		self.State = Alive
		self.LastAck = time.Now()
		enqueue(Event{Kind: EvAlive, Addr: selfAddr, Incarnation: self.Incarnation, HopsLeft: gossipTTL})
		log.Printf("[SWIM] self-defence → ALIVE (inc=%d)", self.Incarnation)
		go notifyRegistryAlive(selfAddr)
		return
	}

	// 2) normal processing: crea il membro se non esiste
	m, ok := members[addr]
	if !ok {
		m = &Member{Addr: addr}
		members[addr] = m
	}

	// 3) accetta solo incarnation più nuove
	if inc >= m.Incarnation {
		m.Incarnation = inc

		switch kind {
		case EvJoin:
			// se non lo conosciamo ancora, lo aggiungiamo subito come Alive
			if _, exists := members[addr]; !exists {
				members[addr] = &Member{
					Addr: addr, State: Alive, Incarnation: inc, LastAck: time.Now(),
				}
				log.Printf("[SWIM] %s → JOINED (inc=%d)", addr, inc)
				go notifyRegistryAlive(addr)
			}
			return
		case EvAlive:
			if m.State == Dead {
				log.Printf("[SWIM] %s resurrected → ALIVE (inc=%d)", addr, inc)
				go notifyRegistryAlive(addr)
			}
			m.State = Alive
			m.LastAck = time.Now()

		case EvSuspect:
			if m.State == Alive {
				log.Printf("[SWIM] %s → SUSPECT (inc=%d)", addr, inc)
				m.State = Suspect
			}

		case EvDead:
			if m.State != Dead {
				log.Printf("[SWIM] %s → DEAD (inc=%d)", addr, inc)
			}
			m.State = Dead
			// notifica registry solo quando lo stato diventa veramente DEAD
			go notifyRegistryDead(addr)
		}
	}
}

func updateMember(addr, state string, inc int) {
	switch state {
	case "ALIVE":
		updateFromRemote(EvAlive, addr, uint64(inc))
	case "SUSPECT":
		updateFromRemote(EvSuspect, addr, uint64(inc))
	case "DEAD":
		updateFromRemote(EvDead, addr, uint64(inc))
	}
}

func recordAck(addr string) {
	memMu.Lock()
	if m, ok := members[addr]; ok {
		if m.State != Alive {
			m.State = Alive
			m.Incarnation++
			enqueue(Event{Kind: EvAlive, Addr: addr, Incarnation: m.Incarnation, HopsLeft: gossipTTL})
			log.Printf("[SWIM] %s resurrected (ACK)", addr)
			go notifyRegistryAlive(addr)
		}
		m.LastAck = time.Now()
	}
	memMu.Unlock()
}

func sendPiggyback(conn net.Conn) {
	evMu.Lock()
	nextQ := make([]Event, 0, len(eventQ))
	for _, e := range eventQ {
		fmt.Fprintf(conn, "%s %s %d\n", e.Kind, e.Addr, e.Incarnation)
		e.HopsLeft--
		if e.HopsLeft > 0 {
			nextQ = append(nextQ, e) // rimetti in coda se deve ancora girare
		}
	}
	eventQ = nextQ
	evMu.Unlock()
}

// -------------------------------------------------------------
// 4.  Periodic prober
// -------------------------------------------------------------

// swim.go

func StartSWIM(myAddr string, initialPeers []string, reg string) {
	// salva registry e indirizzo self
	registryAddr = reg
	selfAddr = myAddr

	// seed per le scelte random
	rand.Seed(time.Now().UnixNano())

	// 1) inizializza la membership locale (+ self)
	addInitialPeers(myAddr, initialPeers)

	// 2) metti in coda il JOIN di questo nodo per farlo gossipare
	evMu.Lock()
	eventQ = append(eventQ, Event{
		Kind:        EvJoin,
		Addr:        myAddr,
		Incarnation: 0,
		HopsLeft:    gossipTTL,
	})
	evMu.Unlock()

	// 3) avvia il prober e il dump di debug
	go probeLoop(myAddr)
	if dumpInterval > 0 {
		go dumpMembership()
	}
}

const (
	probePeriod  = 5 * time.Second
	probeTimeout = 500 * time.Millisecond
	kIndirect    = 3
	dumpInterval = 10 * time.Second
)

func probeLoop(me string) {
	ticker := time.NewTicker(probePeriod)
	for range ticker.C {
		target := pickRandomPeer(me)
		if target == "" {
			continue
		}
		log.Printf("[SWIM] probe → %s", target)
		if !directPing(target, me) {
			markSuspect(target)

			helpers := chooseKRandomExcept([]string{me, target}, kIndirect)
			gotAck := make(chan bool, 1)
			for _, h := range helpers {
				go func(proxy string) {
					if indirectPing(proxy, target, me) {
						gotAck <- true
					}
				}(h)
			}

			select {
			case <-gotAck:
				log.Printf("[SWIM] indirect ACK from %s via helper", target)
				recordAck(target)
			case <-time.After(probeTimeout):
				markDead(target)
			}
		}
	}
}

func dumpMembership() {
	t := time.NewTicker(dumpInterval)
	for range t.C {
		memMu.Lock()
		var snap []string
		for addr, m := range members {
			if addr == selfAddr {
				continue
			}
			snap = append(snap, fmt.Sprintf("%s:%s(i=%d)", addr, m.State, m.Incarnation))
		}
		memMu.Unlock()
		log.Printf("[SWIM] membership snapshot (sans self): %v", snap)
	}
}

func markSuspect(addr string) {
	memMu.Lock()
	if m, ok := members[addr]; ok && m.State == Alive {
		m.State = Suspect
		m.Incarnation++
		enqueue(Event{Kind: EvSuspect, Addr: addr, Incarnation: m.Incarnation, HopsLeft: gossipTTL})
		log.Printf("[SWIM] mark SUSPECT %s (inc=%d)", addr, m.Incarnation)
	}
	memMu.Unlock()
}

func markDead(addr string) {
	memMu.Lock()
	if m, ok := members[addr]; ok && m.State != Dead {
		m.State = Dead
		m.Incarnation++
		enqueue(Event{Kind: EvDead, Addr: addr, Incarnation: m.Incarnation, HopsLeft: gossipTTL})
		log.Printf("[SWIM] mark DEAD %s (inc=%d)", addr, m.Incarnation)
		go notifyRegistryDead(addr)
	}
	memMu.Unlock()
}

// -------------------------------------------------------------
// 5.  I/O helpers
// -------------------------------------------------------------

func directPing(target, me string) bool {
	conn, err := net.DialTimeout("tcp", target, probeTimeout)
	if err != nil {
		log.Printf("[SWIM] dial %s: %v", target, err)
		return false
	}
	fmt.Fprintf(conn, "PING %s\n", me)
	sendPiggyback(conn)
	_ = conn.Close()

	timeout := time.After(probeTimeout)
	for {
		select {
		case a := <-ackCh:
			if a == target {
				return true
			}
		case <-timeout:
			return false
		}
	}
}

func indirectPing(proxy, target, origin string) bool {
	conn, err := net.DialTimeout("tcp", proxy, probeTimeout)
	if err != nil {
		log.Printf("[SWIM] dial proxy %s: %v", proxy, err)
		return false
	}
	defer conn.Close()

	fmt.Fprintf(conn, "PING-REQ %s %s\n", target, origin)
	sendPiggyback(conn)

	timeout := time.After(probeTimeout)
	for {
		select {
		case a := <-ackCh:
			if a == target {
				return true
			}
		case <-timeout:
			return false
		}
	}
}

// -------------------------------------------------------------
// 6.  Random helpers
// -------------------------------------------------------------

func pickRandomPeer(me string) string {
	memMu.Lock()
	defer memMu.Unlock()
	var list []string
	for addr, m := range members {
		if addr != me && m.State == Alive {
			list = append(list, addr)
		}
	}
	if len(list) == 0 {
		return ""
	}
	return list[rand.Intn(len(list))]
}

func chooseKRandomExcept(exclude []string, k int) []string {
	ex := map[string]struct{}{}
	for _, e := range exclude {
		ex[e] = struct{}{}
	}

	memMu.Lock()
	defer memMu.Unlock()

	var pool []string
	for addr, m := range members {
		if _, skip := ex[addr]; !skip && m.State == Alive {
			pool = append(pool, addr)
		}
	}

	rand.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	if len(pool) < k {
		k = len(pool)
	}
	return pool[:k]
}

// -------------------------------------------------------------
// 7.  Frame parser
// -------------------------------------------------------------

func HandleSWIM(frame string) {
	parts := strings.Fields(strings.TrimSpace(frame))
	if len(parts) < 2 {
		return
	}
	kind, addr := parts[0], parts[1]

	switch kind {
	case "JOIN":
		// 1) costruisci l’evento JOIN con TTL
		e := Event{
			Kind:        EvJoin,
			Addr:        addr,
			Incarnation: 0,
			HopsLeft:    gossipTTL,
		}
		// 2) deduplica
		if alreadySeen(e) {
			return
		}
		// 3) applica localmente
		memMu.Lock()
		if addr != selfAddr {
			members[addr] = &Member{
				Addr:        addr,
				State:       Alive,
				Incarnation: 0,
				LastAck:     time.Now(),
			}
		}
		memMu.Unlock()
		log.Printf("[SWIM] %s → JOINED (inc=0)", addr)
		// 4) re-inserisci in eventQ per il gossip
		enqueue(e)

	case "ALIVE", "SUSPECT", "DEAD":
		inc := 0
		if len(parts) == 3 {
			fmt.Sscanf(parts[2], "%d", &inc)
		}
		eType := map[string]EventType{"ALIVE": EvAlive, "SUSPECT": EvSuspect, "DEAD": EvDead}[kind]
		e := Event{Kind: eType, Addr: addr, Incarnation: uint64(inc), HopsLeft: gossipTTL}
		if alreadySeen(e) {
			return
		}
		// 1) aggiorna stato locale
		updateMember(addr, kind, inc)
		// 2) rimetti in coda per far girare il gossip altri TTL hop
		enqueue(e)

		// non dovrebbe più arrivarci: ormai il PING è gestito in handleConn.
		// lasciamo il case vuoto per sicurezza.
	case "PING":
		return

	case "ACK":
		// len(parts) == 2 già garantito dal chiamante
		select {
		case ackCh <- addr: // addr = parts[1]
		default: // canale pieno? pazienza, ignora
		}
	}
}

// -------------------------------------------------------------
// 8.  Registry notifiers
// -------------------------------------------------------------

func notifyRegistryAlive(addr string) {
	conn, err := net.DialTimeout("tcp", registryAddr, 2*time.Second)
	if err != nil {
		log.Printf("[SWIM] cannot notify registry ALIVE: %v", err)
		return
	}
	fmt.Fprintf(conn, "ALIVE %s\n", addr)
	_ = conn.Close()
}

func notifyRegistryDead(victim string) {
	conn, err := net.DialTimeout("tcp", registryAddr, 2*time.Second)
	if err != nil {
		log.Printf("[SWIM] cannot notify registry DEAD: %v", err)
		return
	}
	fmt.Fprintf(conn, "DEAD %s\n", victim)
	_ = conn.Close()
}

// -------------------------------------------------------------
// (nuovo)  Deduplica delle notizie viste
// -------------------------------------------------------------

// chiave: kind|addr  →  ultima incarnation gossippata
var seenMu sync.Mutex
var seen = map[string]uint64{}

func alreadySeen(e Event) bool {
	k := fmt.Sprintf("%d|%s", e.Kind, e.Addr)
	seenMu.Lock()
	defer seenMu.Unlock()
	if e.Incarnation <= seen[k] {
		return true // duplicato
	}
	// non visto (o incarnation più nuova): lo segniamo
	seen[k] = e.Incarnation
	return false
}
