package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
)

/* ---------------------------------------------------------------------- */
/* 1. Tipi base                                                           */
/* ---------------------------------------------------------------------- */

type EventKind int

/* ---------------------------------------------------------------------- */
/* 2. Priorità degli eventi & chiave dedup                                */
/* ---------------------------------------------------------------------- */

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

/* ---------------------------------------------------------------------- */
/* 3. Stato globale (queue + indice dedup)                                */
/* ---------------------------------------------------------------------- */

var (
	pending = make(map[eventKey]Event, 64) // indice dedup
)

/* ---------------------------------------------------------------------- */
/* 4. Funzioni di utilità                                                 */
/* ---------------------------------------------------------------------- */

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

/* ---------------------------------------------------------------------- */
/* 5. enqueueEvent – inserimento con deduplicazione                       */
/* ---------------------------------------------------------------------- */

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

/* ---------------------------------------------------------------------- */
/* 6. Invio piggy‑back con pulizia hop/dedup                              */
/* ---------------------------------------------------------------------- */

func sendPiggybackUDP(conn *net.UDPConn, dst *net.UDPAddr) {
	evMu.Lock()
	if len(eventQ) == 0 {
		evMu.Unlock()
		return
	}

	/* 1) bucket per priorità */
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

	/* 2) scegli gli n eventi da spedire */
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

	/* 3) costruisci payload */
	var buf bytes.Buffer
	for _, e := range toSend {
		switch e.Kind {
		case EvJoin:
			buf.WriteString(fmt.Sprintf("JOIN %s\n", e.Addr))
		case EvAlive:
			gh := members[e.Addr].GraceHops
			buf.WriteString(fmt.Sprintf("ALIVE %s %d %d\n", e.Addr, e.Incarnation, gh))
		case EvLeave:
			buf.WriteString(fmt.Sprintf("LEAVE %s\n", e.Addr))
		default: // EvSuspect, EvDead
			buf.WriteString(fmt.Sprintf("%s %s %d\n", e.Kind, e.Addr, e.Incarnation))
		}
	}

	/* 4) decrementa gli hop, rimuovi scaduti da coda & indice */
	nextQ := make([]Event, 0, len(eventQ))
	for _, e := range eventQ {
		e.HopsLeft--
		if e.HopsLeft > 0 {
			nextQ = append(nextQ, e)
		} else {
			delete(pending, eventKey{Kind: EventKind(e.Kind), Addr: e.Addr})
		}
	}
	eventQ = nextQ
	evMu.Unlock()

	/* 5) invio su socket */
	if buf.Len() == 0 {
		return
	}

	var err error
	if conn.RemoteAddr() != nil {
		_, err = conn.Write(buf.Bytes())
	} else {
		_, err = conn.WriteToUDP(buf.Bytes(), dst)
	}
	if err != nil {
		log.Printf("[GOSSIP] piggyback write error: %v", err)
	}

	log.Printf("[GOSSIP] piggyback: inviati %d/%d eventi (crit=%d, join=%d, alive=%d)",
		len(toSend), len(eventQ)+len(toSend), len(crit), len(joins), len(alives))
}

/* ---------------------------------------------------------------------- */
/* 7. Esempio di generazione evento                                       */
/* ---------------------------------------------------------------------- */

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
