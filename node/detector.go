// swim.go — minimal SWIM-like detector & membership service
// -------------------------------------------------------------------------
package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

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
	members      = map[string]*Member{}
	memMu        sync.Mutex
	eventQ       []Event
	evMu         sync.Mutex
	selfAddr     string
	registryAddr string
	seenMu       sync.Mutex
	seen         = map[string]uint64{} // key = kind|addr -> last incarnation
)

type EventType int

const (
	EvJoin EventType = iota
	EvLeave
	EvSuspect
	EvDead
	EvAlive
	EvUnknown
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

func updateFromRemote(kind EventType, addr string, inc uint64) {
	e := Event{Kind: kind, Addr: addr, Incarnation: inc, HopsLeft: getHops()}

	hopsleft := getHops()
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
		enqueue(Event{Kind: EvAlive, Addr: selfAddr, Incarnation: self.Incarnation, HopsLeft: hopsleft})
		log.Printf("[SWIM] self-defence → ALIVE (inc=%d)", self.Incarnation)
		go notifyRegistryAlive(selfAddr)
		go gossipNowUDP()
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
			go gossipNowUDP()
		}

	case EvAlive:
		if inc < m.Incarnation {
			return
		}
		if m.State == Dead {
			log.Printf("[SWIM] %s resurrected → ALIVE (inc=%d)", addr, inc)
			go notifyRegistryAlive(addr)
			go gossipNowUDP()
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
		go gossipNowUDP()

	case EvLeave:
		// rimuoviamo subito il nodo dalla membership
		delete(members, addr)
		log.Printf("[SWIM] %s → voluntarily LEFT", addr)
		go notifyRegistryLeave(addr)
		go gossipNowUDP()
	}
}
func wasAlreadyMember() bool {
	var savedIncarnation uint64
	for addr, m := range members {
		if addr == selfAddr {
			savedIncarnation = m.Incarnation
		}
	}
	// Confronta l'incarnazione salvata con quella attuale
	if savedIncarnation > 0 {
		log.Printf("[INFO] Found previous incarnation %d", savedIncarnation)
		return true
	}
	return false
}

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

func handleConn(c net.Conn) {
	defer c.Close()
	scanner := bufio.NewScanner(c)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {

		// … gli altri casi PING, ACK, JOIN, LEAVE, ecc. …

		case "REMOVE": // Registry dice: “Elimina questo peer dalla tua view”
			if len(parts) != 2 {
				continue
			}
			victim := parts[1]

			// 1) Elimino il peer dalla membership map
			memMu.Lock()
			if _, ok := members[victim]; ok {
				delete(members, victim)
				log.Printf("[SWIM] %s removed by registry", victim)

				// 2) (opzionale) se vuoi far girare il leave via gossip interno:
				evMu.Lock()
				eventQ = append(eventQ, Event{
					Kind:        EvLeave,
					Addr:        victim,
					Incarnation: 0,
					HopsLeft:    getHops(),
				})
				evMu.Unlock()
				go gossipNowUDP()
			}
			memMu.Unlock()

		default:
			// … eventuale fallback per piggyback e comandi extra …
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("[NET] scanner error from %s: %v", c.RemoteAddr(), err)
	}
}
