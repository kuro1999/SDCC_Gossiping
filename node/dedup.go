package main

import (
	"encoding/json"
	"fmt"
	"net"
)

type EventKind int

var kindPriority = map[EventType]int{
	EvLeave:   3,
	EvDead:    3,
	EvSuspect: 2,
	EvAlive:   1,
	EvJoin:    0,
}

type eventKey struct {
	Kind EventKind
	Addr string
}

var (
	pending = make(map[eventKey]Event, 64) // indice dedup
)

const (
	maxPiggybackEntries = 256
)

// true se e2 deve sostituire e1.
func shouldReplace(e1, e2 Event) bool {
	p1 := kindPriority[e1.Kind]
	p2 := kindPriority[e2.Kind]

	switch {
	case p2 > p1:
		return true
	case p2 < p1:
		return false
	default:
		return e2.Incarnation > e1.Incarnation
	}
}

// Rimpiazza un elemento già presente in coda (O(n), ma n è piccolo).
func replaceInQueue(old, neu Event) {
	for i := range eventQ {
		if eventQ[i] == old {
			eventQ[i] = neu
			return
		}
	}
}

func enqueueEvent(e Event) {
	evMu.Lock()
	defer evMu.Unlock()

	k := eventKey{Kind: EventKind(e.Kind), Addr: e.Addr}

	if old, ok := pending[k]; ok {
		if shouldReplace(old, e) {
			pending[k] = e
			replaceInQueue(old, e)
		}
		return // duplicato: non reinseriamo
	}

	pending[k] = e
	eventQ = append(eventQ, e)
}

const maxDatagramSize = 1400

func sendPiggybackUDP(conn *net.UDPConn, target *net.UDPAddr) error {
	if len(eventQ) == 0 {
		return nil // nothing to send
	}

	// We grow the window one event at a time until we would overflow. Then we
	// back off by one and emit that datagram. Complexity is O(n) and super
	// small, so no need to get fancy.
	start := 0
	for start < len(eventQ) {
		end := start + 1 // slice is [start:end)
		var payload []byte

		for ; end <= len(eventQ); end++ {
			candidate, err := json.Marshal(eventQ[start:end])
			if err != nil {
				return fmt.Errorf("failed to marshal events: %w", err)
			}

			if len(candidate) > maxDatagramSize {
				// The slice [start:end) is now *too big*; use [start:end‑1).
				if end-start == 1 {
					// Single event larger than maxDatagramSize – bail.
					return fmt.Errorf("gossip event exceeds datagram size (%d > %d)", len(candidate), maxDatagramSize)
				}
				end-- // step back to last fitting event
				payload, _ = json.Marshal(eventQ[start:end])
				break
			}
			payload = candidate
		}

		// Safety net: if for‑loop finished naturally (every event fitted) we
		// still need to send the accumulated payload.
		if payload == nil {
			var err error
			payload, err = json.Marshal(eventQ[start:end])
			if err != nil {
				return fmt.Errorf("failed to marshal events: %w", err)
			}
		}

		if _, err := conn.WriteToUDP(payload, target); err != nil {
			return fmt.Errorf("sending UDP datagram: %w", err)
		}

		start = end // move window forward
	}

	return nil
}

// newAlive crea un evento ALIVE pre‑configurato e lo mette in coda.
func newAlive(addr string, inc int) {
	e := Event{
		Kind:        EvAlive,
		Addr:        addr,
		Incarnation: uint64(inc),
		HopsLeft:    getHops(),
	}
	enqueueEvent(e)
}
