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
	GraceHops   int
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

// -----------------------------------------------------------------
// updateFromRemote: applica un evento ricevuto da gossip/SWIM.
// Ordine:
//  0. calcola hops fuori lock
//  1. crea l’entry se mancante (solo JOIN/ALIVE)
//  2. deduplica; se già visto & non ALIVE => esce
//  3. enqueue per piggy-back
//  4. transizioni di stato + side-effects
//
// -----------------------------------------------------------------
func updateFromRemote(kind EventType, addr string, inc uint64) {
	// 0) calcolo hops fuori dal lock
	hops := getHops()
	gracehops := computeGraceHops()
	evt := Event{Kind: kind, Addr: addr, Incarnation: inc, HopsLeft: hops}

	// --- 1) self-defence: se qualcuno ci marca SUSPECT/DEAD, resuscitiamo subito ---
	if addr == selfAddr && kind != EvAlive {
		// bump incarnation sotto lock
		memMu.Lock()
		self := members[selfAddr]
		self.Incarnation++
		self.State = Alive
		self.LastAck = time.Now()
		newInc := self.Incarnation
		memMu.Unlock()

		// enqueue e broadcast del nuovo ALIVE
		enqueue(Event{Kind: EvAlive, Addr: selfAddr, Incarnation: newInc, HopsLeft: hops})
		log.Printf("[SWIM] self-defence → ALIVE (inc=%d)", newInc)

		go notifyRegistryAlive(selfAddr)
		go gossipNowUDP()
		return
	}

	// --- 2) assicuro che l’entry esista e inizializzo GraceHops se è nuova ---
	memMu.Lock()
	m, ok := members[addr]
	if !ok && (kind == EvAlive || kind == EvJoin) {
		m = &Member{
			Addr:        addr,
			State:       Alive,
			Incarnation: inc,
			LastAck:     time.Now(),
			GraceHops:   gracehops, // ← inizializzo qui la grazia in hops
		}
		members[addr] = m
	}
	memMu.Unlock()

	// --- 3) deduplica (SUSPECT/DEAD/LEAVE deduplicati, ALIVE sempre passa) ---
	if alreadySeen(evt) && kind != EvAlive {
		return
	}

	// --- 4) metto in coda l’evento vero e proprio ---
	enqueue(evt)

	// --- 5) transizioni di stato sotto lock ---
	memMu.Lock()
	defer memMu.Unlock()

	// recupero di nuovo entry (potrebbe non esistere per SUSPECT/DEAD ignoti)
	m, ok = members[addr]
	if !ok {
		return
	}

	switch kind {
	case EvJoin:
		if inc >= m.Incarnation {
			changed := inc > m.Incarnation || m.State != Alive
			// aggiorno solo i campi, mantenendo GraceHops
			m.State = Alive
			m.Incarnation = inc
			m.LastAck = time.Now()
			m.GraceHops = hops // ← reset grazia su JOIN vero
			if changed {
				log.Printf("[SWIM] %s → JOINED (inc=%d)", addr, inc)
				go notifyRegistryAlive(addr)
				go gossipNowUDP()
			}
		}

	case EvAlive:
		if inc < m.Incarnation {
			return
		}
		changed := inc > m.Incarnation || m.State != Alive
		if changed {
			if m.State == Dead {
				log.Printf("[SWIM] %s resurrected → ALIVE (inc=%d)", addr, inc)
				go notifyRegistryAlive(addr)
			}
			m.State = Alive
			m.Incarnation = inc
			m.LastAck = time.Now()
			// NOTA: non resettiamo GraceHops qui, lo sincronizziamo via gossip ALIVE
			go gossipNowUDP()
		} else {
			// semplice refresh di LastAck
			m.LastAck = time.Now()
		}

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
	defer t.Stop()

	for range t.C {
		memMu.Lock()
		var snap []string
		for addr, m := range members {
			if addr == selfAddr {
				continue
			}
			age := time.Since(m.LastAck).Seconds()
			snap = append(snap, fmt.Sprintf(
				"%s:%s(i=%d, grace=%d, ack=%.1fs)",
				addr, m.State, m.Incarnation, m.GraceHops, age,
			))
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
	log.Printf("[SWIM] notificato registry: ALIVE %s", addr)
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

func handleConn(c net.Conn) {
	defer c.Close()
	scanner := bufio.NewScanner(c)

	for scanner.Scan() {
		parts := strings.Fields(strings.TrimSpace(scanner.Text()))
		if len(parts) == 0 {
			continue
		}
		switch parts[0] {
		case "REMOVE":
			hops := getHops()
			if len(parts) != 2 {
				log.Printf("[SWIM] malformed REMOVE, closing")
				return
			}
			victim := parts[1]

			// 1) Elimino il peer dalla membership map
			memMu.Lock()
			if _, ok := members[victim]; ok {
				delete(members, victim)
				log.Printf("[SWIM] %s removed by registry", victim)
			}
			memMu.Unlock()

			// 2) Enqueue di un LEAVE per gossip UDP
			evMu.Lock()
			eventQ = append(eventQ, Event{
				Kind:        EvLeave,
				Addr:        victim,
				Incarnation: 0,
				HopsLeft:    hops,
			})
			evMu.Unlock()
			go gossipNowUDP()

			// 3) chiudo subito l’handler
			return

		default:
			continue
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("[NET] scanner error: %v", err)
	}
}
