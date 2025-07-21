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

// scegli fino a k peer ALIVE, escludendo quelli passati in `except`.
func chooseKRandomExcept(except []string, k int) []string {
	// costruisco un set O(1) per i peer da escludere
	skip := make(map[string]struct{}, len(except))
	for _, addr := range except {
		skip[addr] = struct{}{}
	}

	// ottengo i candidati (solo ALIVE) – slice già separata e thread‑safe
	candidates := allAlivePeers()

	// filtro i candidati da escludere
	kept := candidates[:0] // reuse della stessa slice
	for _, addr := range candidates {
		if _, found := skip[addr]; !found {
			kept = append(kept, addr)
		}
	}

	// niente da scegliere?
	if len(kept) == 0 || k <= 0 {
		return nil
	}

	// mescola in modo pseudo‑random
	// (se non hai già seedato altrove, fallo una tantum nel tuo main)
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(kept), func(i, j int) { kept[i], kept[j] = kept[j], kept[i] })

	// restituisci al massimo k elementi
	if len(kept) > k {
		kept = kept[:k]
	}
	// slice di ritorno indipendente
	out := make([]string, len(kept))
	copy(out, kept)
	return out
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

		case "LEAVE":
			// Voluntary leave richiesta via TCP:
			voluntaryLeave()
			log.Printf("[SWIM] voluntary LEAVE triggered by external command")
			return

		default:
			continue
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("[NET] scanner error: %v", err)
	}
}
