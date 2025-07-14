// swim.go — minimal SWIM-like detector & membership service
// -------------------------------------------------------------------------
package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

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
	members      = map[string]*Member{}
	memMu        sync.Mutex
	eventQ       []Event
	evMu         sync.Mutex
	selfAddr     string
	ackWaiters   sync.Map // key = addr, value = chan struct{}
	registryAddr string
	seenMu       sync.Mutex
	seen         = map[string]uint64{} // key = kind|addr -> last incarnation
)

// -------------------------------------------------------------
// 2.  Events disseminated via gossip
// -------------------------------------------------------------
type EventType int

const (
	EvJoin  EventType = iota
	EvLeave           // <- nuovo
	EvSuspect
	EvDead
	EvAlive
)

func (t EventType) String() string {
	switch t {
	case EvJoin:
		return "JOIN"
	case EvLeave:
		return "LEAVE"
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
	HopsLeft    int
}

func enqueue(e Event) {
	evMu.Lock()
	eventQ = append(eventQ, e)
	evMu.Unlock()
}

// -------------------------------------------------------------
// 3.  SWIM hooks richiamati da node.go
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
	e := Event{Kind: kind, Addr: addr, Incarnation: inc, HopsLeft: gossipTTL}

	// deduplica: solo ALIVE viaggia sempre
	if alreadySeen(e) && kind != EvAlive {
		return
	}
	enqueue(e)

	memMu.Lock()
	defer memMu.Unlock()

	// -----------------------------------------------------------------
	// 1) SELF-DEFENCE (se qualcuno ci dà per morti / sospetti / leave)
	// -----------------------------------------------------------------
	if addr == selfAddr && kind != EvAlive {
		self := members[selfAddr]
		self.Incarnation++
		self.State = Alive
		self.LastAck = time.Now()
		enqueue(Event{Kind: EvAlive, Addr: selfAddr, Incarnation: self.Incarnation, HopsLeft: gossipTTL})
		log.Printf("[SWIM] self-defence → ALIVE (inc=%d)", self.Incarnation)
		go notifyRegistryAlive(selfAddr)
		go gossipNow()
		return
	}

	// -----------------------------------------------------------------
	// 2) se non esiste l’entry, creala solo per ALIVE o JOIN
	// -----------------------------------------------------------------
	m, ok := members[addr]
	if !ok && (kind == EvAlive || kind == EvJoin) {
		m = &Member{Addr: addr}
		members[addr] = m
	} else if !ok { // ignoriamo SUSPECT / DEAD / LEAVE di ignoti
		return
	}

	// -----------------------------------------------------------------
	// 3) transizioni di stato
	// -----------------------------------------------------------------
	switch kind {
	case EvJoin:
		if inc >= m.Incarnation {
			*m = Member{Addr: addr, State: Alive, Incarnation: inc, LastAck: time.Now()}
			log.Printf("[SWIM] %s → JOINED (inc=%d)", addr, inc)
			go notifyRegistryAlive(addr)
			go gossipNow()
		}

	case EvAlive:
		if inc < m.Incarnation {
			return
		}
		if m.State == Dead {
			log.Printf("[SWIM] %s resurrected → ALIVE (inc=%d)", addr, inc)
			go notifyRegistryAlive(addr)
			go gossipNow()
		}
		m.State = Alive
		m.Incarnation = inc
		m.LastAck = time.Now()

	case EvSuspect:
		if inc < m.Incarnation || m.State != Alive {
			return
		}
		m.State = Suspect
		m.Incarnation = inc
		log.Printf("[SWIM] %s → SUSPECT (inc=%d)", addr, inc)

	case EvDead:
		if inc < m.Incarnation || m.State == Dead {
			return
		}
		m.State = Dead
		m.Incarnation = inc
		log.Printf("[SWIM] %s → DEAD (inc=%d)", addr, inc)
		go notifyRegistryDead(addr)
		go gossipNow()

	case EvLeave:
		// rimuoviamo subito il nodo dalla membership
		delete(members, addr)
		log.Printf("[SWIM] %s → voluntarily LEFT", addr)
		go notifyRegistryLeave(addr)
		go gossipNow()
	}
}

// —————————————————————————————————————————————————
func updateMember(addr, state string, inc int) {
	switch state {
	case "JOIN":
		updateFromRemote(EvJoin, addr, 0)
	case "ALIVE":
		updateFromRemote(EvAlive, addr, uint64(inc))
	case "SUSPECT":
		updateFromRemote(EvSuspect, addr, uint64(inc))
	case "DEAD":
		updateFromRemote(EvDead, addr, uint64(inc))
	case "LEAVE":
		updateFromRemote(EvLeave, addr, 0)
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
			go gossipNow() // fan-out immediato dell’ALIVE
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
			nextQ = append(nextQ, e)
		}
	}
	eventQ = nextQ
	evMu.Unlock()
}

// -------------------------------------------------------------
// 4.  Periodic prober + reaper
// -------------------------------------------------------------
const (
	probePeriod    = 5 * time.Second
	probeTimeout   = 500 * time.Millisecond
	kIndirect      = 3
	dumpInterval   = 10 * time.Second
	suspectTimeout = 15 * time.Second
)

func StartSWIM(myAddr string, initialPeers []string, reg string) {
	registryAddr = reg
	selfAddr = myAddr
	rand.Seed(time.Now().UnixNano())

	addInitialPeers(myAddr, initialPeers)
	enqueue(Event{Kind: EvJoin, Addr: myAddr, Incarnation: 0, HopsLeft: gossipTTL})

	go antiEntropyLoop()
	go probeLoop(myAddr)
	go reaperLoop()
	if dumpInterval > 0 {
		go dumpMembership()
	}
}

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
			gotAck := make(chan struct{}, 1)
			for _, h := range helpers {
				go func(proxy string) {
					if indirectPing(proxy, target, me) {
						gotAck <- struct{}{}
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

func reaperLoop() {
	t := time.NewTicker(suspectTimeout / 2)
	for range t.C {
		now := time.Now()
		memMu.Lock()
		for _, m := range members {
			if m.Addr == selfAddr { // NEW
				continue // mai toccare se stessi
			}
			if m.State == Suspect && now.Sub(m.LastAck) > suspectTimeout {
				markDead(m.Addr)
			}
		}
		memMu.Unlock()
	}
}

// swim.go  (patch sintetico)

// 1.  logSnapshot: forza la stampa anche se identico
var lastSnap string

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

		cur := strings.Join(snap, " ")
		if cur != lastSnap {
			log.Printf("[SWIM] membership snapshot: [%s]", cur)
			lastSnap = cur
		}
	}
}

func markSuspect(addr string) {
	if addr == selfAddr {
		return
	}
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
	if addr == selfAddr {
		return
	}
	memMu.Lock()
	if m, ok := members[addr]; ok && m.State != Dead {
		m.State = Dead
		m.Incarnation++
		enqueue(Event{Kind: EvDead, Addr: addr, Incarnation: m.Incarnation, HopsLeft: gossipTTL})
		log.Printf("[SWIM] mark DEAD %s (inc=%d)", addr, m.Incarnation)
		go notifyRegistryDead(addr)
		go gossipNow() // <-- disseminazione immediata
	}
	memMu.Unlock()
}

func gossipNow() {
	peers := alivePeers()
	for _, p := range peers {
		go func(dest string) {
			conn, err := net.DialTimeout("tcp", dest, probeTimeout)
			if err != nil {
				return
			}
			sendPiggyback(conn)
			_ = conn.Close()
		}(p)
	}
}

func alivePeers() []string {
	memMu.Lock()
	defer memMu.Unlock()
	var list []string
	for addr, m := range members {
		if addr != selfAddr && m.State == Alive {
			list = append(list, addr)
		}
	}
	return list
}

// -------------------------------------------------------------
// 5.  I/O helpers
// -------------------------------------------------------------
func directPing(target, me string) bool {
	wait := make(chan struct{})
	ackWaiters.Store(target, wait)
	defer ackWaiters.Delete(target)

	conn, err := net.DialTimeout("tcp", target, probeTimeout)
	if err != nil {
		log.Printf("[SWIM] dial %s: %v", target, err)
		return false
	}
	fmt.Fprintf(conn, "PING %s\n", me)
	sendPiggyback(conn)
	_ = conn.Close()

	select {
	case <-wait:
		return true
	case <-time.After(probeTimeout):
		return false
	}
}

func indirectPing(proxy, target, origin string) bool {
	// Prepara il waiter per il target
	wait := make(chan struct{})
	ackWaiters.Store(target, wait)
	defer ackWaiters.Delete(target)

	// Invia PING-REQ al proxy
	conn, err := net.DialTimeout("tcp", proxy, probeTimeout)
	if err != nil {
		log.Printf("[SWIM] dial proxy %s: %v", proxy, err)
		return false
	}
	fmt.Fprintf(conn, "PING-REQ %s %s\n", target, origin)
	sendPiggyback(conn)
	_ = conn.Close()

	// Attendi l’ACK o il timeout
	select {
	case <-wait:
		return true
	case <-time.After(probeTimeout):
		return false
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
		if _, skip := ex[addr]; skip {
			continue
		}
		if m.State != Alive {
			continue
		} // <-- salta SUSPECT/DEAD
		pool = append(pool, addr)
	}

	rand.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	if len(pool) < k {
		k = len(pool)
	}
	return pool[:k]
}

// -------------------------------------------------------------
// 7.  Frame parser (per piggy-back & comandi interni)
// -------------------------------------------------------------
func HandleSWIM(frame string) {
	parts := strings.Fields(strings.TrimSpace(frame))
	if len(parts) < 2 {
		return
	}
	kind, addr := parts[0], parts[1]

	switch kind {
	case "JOIN":
		updateFromRemote(EvJoin, addr, 0)

	case "ALIVE", "SUSPECT", "DEAD":
		inc := 0
		if len(parts) == 3 {
			fmt.Sscanf(parts[2], "%d", &inc)
		}
		updateMember(addr, kind, inc)

	case "ACK":
		// idem: rimuove il waiter e chiude il canale solo una volta
		if chAny, ok := ackWaiters.LoadAndDelete(addr); ok {
			close(chAny.(chan struct{}))
		}
		recordAck(addr)
	case "LEAVE": // ← NEW
		log.Printf("[DEBUG] received LEAVE from %s", addr)
		updateFromRemote(EvLeave, addr, 0)
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
func notifyRegistryLeave(addr string) {
	conn, err := net.DialTimeout("tcp", registryAddr, 2*time.Second)
	if err != nil {
		log.Printf("[SWIM] cannot notify registry LEAVE: %v", err)
		return
	}
	fmt.Fprintf(conn, "LEAVE %s\n", addr)
	_ = conn.Close()
}

// -------------------------------------------------------------
// 9.  Deduplica notizie viste
// -------------------------------------------------------------

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

func antiEntropyLoop() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		memMu.Lock()
		peers := make([]string, 0, len(members))
		for addr, m := range members {
			if addr != selfAddr && m.State == Alive {
				peers = append(peers, addr)
			}
		}
		memMu.Unlock()
		for _, p := range peers {
			go func(dest string) {
				conn, err := net.DialTimeout("tcp", dest, probeTimeout)
				if err != nil {
					return
				}
				sendPiggyback(conn)
				_ = conn.Close()
			}(p)
		}
	}
}

// ─────────────────────────────────────────────────────────────────
func gracefulLeave() {
	// 1) enque e primo fan-out
	enqueue(Event{Kind: EvLeave, Addr: selfAddr, Incarnation: 0, HopsLeft: gossipTTL})
	log.Printf("[SWIM] Initiated graceful leave from %s (gossip)", selfAddr)
	gossipNow()

	// 3) notifica il registry
	notifyRegistryLeave(selfAddr)

	// 4) esci
	os.Exit(0)
}
