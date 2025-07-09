// swim.go — minimal SWIM-like detector & membership service, **with debug logs**
// -----------------------------------------------------------------------------
// Per disattivare la stampa periodica della membership imposta dumpInterval = 0
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
	eventQ   []Event // piggyback queue
	evMu     sync.Mutex
	selfAddr string
	ackCh    = make(chan string, 16)
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
	memMu.Lock()
	m, ok := members[addr]
	if !ok {
		m = &Member{Addr: addr}
		members[addr] = m
	}
	if inc >= m.Incarnation {
		m.Incarnation = inc
		switch kind {
		case EvAlive:
			if m.State != Alive {
				log.Printf("[SWIM] %s → ALIVE (inc=%d)", addr, inc)
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
		}
	}
	memMu.Unlock()
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
			log.Printf("[SWIM] %s resurrected (ACK)", addr)
			m.State = Alive
			m.Incarnation++
			enqueue(Event{Kind: EvAlive, Addr: addr, Incarnation: m.Incarnation})
		}
		m.LastAck = time.Now()
	}
	memMu.Unlock()
}

func sendPiggyback(conn net.Conn) {
	evMu.Lock()
	evs := eventQ
	eventQ = nil
	evMu.Unlock()
	for _, e := range evs {
		fmt.Fprintf(conn, "%s %s %d\n", e.Kind.String(), e.Addr, e.Incarnation)
	}
}

// -------------------------------------------------------------
// 4.  Periodic prober
// -------------------------------------------------------------

func StartSWIM(myAddr string) {
	rand.Seed(time.Now().UnixNano())
	selfAddr = myAddr
	go probeLoop(myAddr)
	if dumpInterval > 0 {
		go dumpMembership()
	}
}

const (
	probePeriod  = 5 * time.Second
	probeTimeout = 500 * time.Millisecond
	kIndirect    = 3
	dumpInterval = 10 * time.Second // stampa la membership; 0 = disattiva
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
		snap := make([]string, 0, len(members))
		for addr, m := range members {
			snap = append(snap, fmt.Sprintf("%s:%s(i=%d)", addr, m.State, m.Incarnation))
		}
		memMu.Unlock()
		log.Printf("[SWIM] membership snapshot: %v", snap)
	}
}

func markSuspect(addr string) {
	memMu.Lock()
	if m, ok := members[addr]; ok && m.State == Alive {
		m.State = Suspect
		m.Incarnation++
		enqueue(Event{Kind: EvSuspect, Addr: addr, Incarnation: m.Incarnation})
		log.Printf("[SWIM] mark SUSPECT %s (inc=%d)", addr, m.Incarnation)
	}
	memMu.Unlock()
}

func markDead(addr string) {
	memMu.Lock()
	if m, ok := members[addr]; ok && m.State != Dead {
		m.State = Dead
		m.Incarnation++
		enqueue(Event{Kind: EvDead, Addr: addr, Incarnation: m.Incarnation})
		log.Printf("[SWIM] mark DEAD %s (inc=%d)", addr, m.Incarnation)
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
	log.Printf("[SWIM] PING-REQ %s → %s", proxy, target)
	fmt.Fprintf(conn, "PING-REQ %s %s\n", target, origin)
	sendPiggyback(conn)
	_ = conn.Close()
	return true
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
	case "ALIVE", "SUSPECT", "DEAD":
		inc := 0
		if len(parts) == 3 {
			fmt.Sscanf(parts[2], "%d", &inc)
		}
		updateMember(addr, kind, inc)

	case "PING":
		go func(dest string) {
			if conn, err := net.Dial("tcp", dest); err == nil {
				fmt.Fprintf(conn, "ACK %s\n", selfAddr)
				_ = conn.Close()
			}
		}(addr)

	case "PING-REQ":
		if len(parts) == 3 {
			target, origin := parts[1], parts[2]
			go indirectPing(target, target, origin)
		}

	case "ACK":
		select {
		case ackCh <- addr:
		default:
		}
	}
}
