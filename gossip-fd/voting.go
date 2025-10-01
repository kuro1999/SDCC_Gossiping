package main

import (
	"log"
	"math"
	"math/rand/v2"
	"sort"
	"time"
)

// Evidenza di morte = "quanto è aggiornata la mia vista su X"
type DeathEvidence struct {
	Inc uint64 `json:"inc"` // incarnation osservata al momento della morte
	Hb  uint64 `json:"hb"`  // heartbeat associato
}

// Voto di un witness: "io W dichiaro morto X con evidenza E in istante Ts"
type DeathVote struct {
	Target  string `json:"target"` // ID del nodo morto (X)
	Inc     uint64 `json:"inc"`
	Hb      uint64 `json:"hb"`
	Witness string `json:"w"`  // ID del witness (mittente originario del voto)
	Ts      int64  `json:"ts"` // unix seconds del voto
}

// Certificato (obituary): "si è raggiunto quorum K su X con evidenza E"
// iat/ttl servono a evitare rigenerazioni infinite (il TTL non si resetta).
type Obituary struct {
	Target   string `json:"target"`
	Inc      uint64 `json:"inc"`
	Hb       uint64 `json:"hb"`
	IssuedAt int64  `json:"iat"` // unix seconds di emissione iniziale
	TtlSec   int    `json:"ttl"` // durata totale del certificato
	K        int    `json:"k"`   // quorum raggiunto
}

// ====== Stato locale per voti/certificati ======

// bucket dei voti ricevuti su un target X
type voteBucket struct {
	Evidence  DeathEvidence    // evidenza massima vista per X
	Witness   map[string]int64 // witnessID -> ts unix (solo per finestra Δ)
	ExpiresAt time.Time        // scadenza del bucket (Δ)
	Priority  int              // round di priorità residui per piggyback voti
}

// certificato attivo da ridiffondere
type obitEntry struct {
	Evidence  DeathEvidence
	IssuedAt  int64     // unix seconds dell'emissione originale (preservato)
	ExpiresAt time.Time // IssuedAt + ObitTTL
	Priority  int       // round di priorità residui per piggyback obits
}

func evidenceNewer(a, b DeathEvidence) bool {
	if a.Inc != b.Inc {
		return a.Inc > b.Inc
	}
	return a.Hb > b.Hb
}

// registra un voto locale quando porti X a DEAD
func (n *Node) recordLocalVote(target string, ev DeathEvidence) {
	now := time.Now()

	n.mu.Lock()
	defer n.mu.Unlock()

	// ignora auto-voto su me stesso per sicurezza
	if target == n.cfg.SelfID {
		return
	}

	b, ok := n.votes[target]
	if !ok {
		b = &voteBucket{
			Evidence:  ev,
			Witness:   make(map[string]int64),
			ExpiresAt: now.Add(n.cfg.VoteWindow),
			Priority:  n.cfg.VotePriorityRounds,
		}
		n.votes[target] = b
	}
	// mantieni l'evidenza massima
	if evidenceNewer(ev, b.Evidence) {
		b.Evidence = ev
	}
	// aggiorna finestra Δ e witness set
	b.ExpiresAt = now.Add(n.cfg.VoteWindow)
	b.Witness[n.cfg.SelfID] = now.Unix()
	if b.Priority < n.cfg.VotePriorityRounds {
		b.Priority = n.cfg.VotePriorityRounds
	}
}

// ingest dei certificati: applica prima dei merge
func (n *Node) ingestObits(obits []Obituary) (evicted int) {
	now := time.Now()

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, o := range obits {
		// scarta obit scaduti (rispetta iat originale)
		exp := time.Unix(o.IssuedAt, 0).Add(time.Duration(o.TtlSec) * time.Second)
		if now.After(exp) {
			continue
		}

		// se ho ALIVE più "nuovo" dell'evidenza del certificato, ignoro
		if m, ok := n.members[o.Target]; ok && m.State == StateAlive {
			local := DeathEvidence{Inc: m.Incarnation, Hb: m.Heartbeat}
			// se local > obit.Evidence => obit è vecchio rispetto alla mia vista
			log.Printf("[OBIT-IGNORE] target=%s ignorato (local ALIVE inc=%d hb=%d > cert inc=%d hb=%d)",
				o.Target, m.Incarnation, m.Heartbeat, o.Inc, o.Hb)
			if evidenceNewer(local, DeathEvidence{Inc: o.Inc, Hb: o.Hb}) {
				continue
			}
		}

		// esegui eviction (membro + servizi)
		n.evictMemberLocked(o.Target)

		// memorizza l'obit per ridiffusione (preserva IssuedAt)
		entry, ok := n.obits[o.Target]
		if !ok {
			entry = &obitEntry{
				Evidence:  DeathEvidence{Inc: o.Inc, Hb: o.Hb},
				IssuedAt:  o.IssuedAt,
				ExpiresAt: exp,
				Priority:  n.cfg.ObitPriorityRounds,
			}
			n.obits[o.Target] = entry
		} else {
			// aggiorna eventuale evidenza se più forte
			ev := DeathEvidence{Inc: o.Inc, Hb: o.Hb}
			if evidenceNewer(ev, entry.Evidence) {
				entry.Evidence = ev
			}
			if exp.After(entry.ExpiresAt) {
				entry.ExpiresAt = exp
			}
			if entry.Priority < n.cfg.ObitPriorityRounds {
				entry.Priority = n.cfg.ObitPriorityRounds
			}
		}
		evicted++
	}
	return
}

// ingest dei voti: aggiorna bucket e promuove a certificato se quorum
func (n *Node) ingestVotes(votes []DeathVote) (promoted int) {
	now := time.Now()

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, v := range votes {
		// voto scaduto rispetto a Δ
		if now.Sub(time.Unix(v.Ts, 0)) > n.cfg.VoteWindow {
			continue
		}
		b, ok := n.votes[v.Target]
		if !ok {
			b = &voteBucket{
				Evidence:  DeathEvidence{Inc: v.Inc, Hb: v.Hb},
				Witness:   make(map[string]int64),
				ExpiresAt: now.Add(n.cfg.VoteWindow),
				Priority:  n.cfg.VotePriorityRounds,
			}
			log.Printf("[VOTE] creato bucket per %s (inc=%d hb=%d, Δ=%s)", v.Target, v.Inc, v.Hb, n.cfg.VoteWindow)
			n.votes[v.Target] = b
		}
		// mantieni evidenza massima
		ev := DeathEvidence{Inc: v.Inc, Hb: v.Hb}
		if evidenceNewer(ev, b.Evidence) {
			b.Evidence = ev
		}
		// aggiorna finestra Δ e witness set
		b.ExpiresAt = now.Add(n.cfg.VoteWindow)
		b.Witness[v.Witness] = v.Ts
		log.Printf("[VOTE] witness=%s per %s (|W|=%d/%d)", v.Witness, v.Target, len(b.Witness), n.cfg.QuorumK)
		if b.Priority < n.cfg.VotePriorityRounds {
			b.Priority = n.cfg.VotePriorityRounds
		}

		// quorum raggiunto?
		if len(b.Witness) >= n.cfg.QuorumK {
			log.Printf("[OBIT] quorum raggiunto per %s (K=%d, inc=%d hb=%d) → certificato emesso (TTL=%s)",
				v.Target, n.cfg.QuorumK, b.Evidence.Inc, b.Evidence.Hb, n.cfg.ObitTTL)
			// promuovi a obit (una sola volta)
			if _, exists := n.obits[v.Target]; !exists {
				iat := now.Unix()
				n.obits[v.Target] = &obitEntry{
					Evidence:  b.Evidence,
					IssuedAt:  iat,
					ExpiresAt: time.Unix(iat, 0).Add(n.cfg.ObitTTL),
					Priority:  n.cfg.ObitPriorityRounds,
				}
				promoted++
			}
		}
	}
	return
}

// selezione obits da piggybackare (con priorità e cappi)
func (n *Node) selectObitsLocked(max int) []Obituary {
	now := time.Now()
	if max <= 0 {
		return nil
	}
	type rec struct {
		id string
		*obitEntry
	}
	list := make([]rec, 0, len(n.obits))
	for id, e := range n.obits {
		if now.After(e.ExpiresAt) {
			continue
		}
		list = append(list, rec{id: id, obitEntry: e})
	}
	// ordina per priorità prima
	sort.Slice(list, func(i, j int) bool {
		if list[i].Priority != list[j].Priority {
			return list[i].Priority > list[j].Priority
		}
		return list[i].ExpiresAt.Before(list[j].ExpiresAt)
	})
	if len(list) > max {
		list = list[:max]
	}
	out := make([]Obituary, 0, len(list))
	for _, r := range list {
		out = append(out, Obituary{
			Target:   r.id,
			Inc:      r.Evidence.Inc,
			Hb:       r.Evidence.Hb,
			IssuedAt: r.IssuedAt,
			TtlSec:   int(n.cfg.ObitTTL.Seconds()),
			K:        n.cfg.QuorumK,
		})
		// consuma un round di priorità
		if r.Priority > 0 {
			r.Priority--
		}
	}
	return out
}

// selezione voti da piggybackare (campiona gli witness per target)
func (n *Node) selectVotesLocked(max int) []DeathVote {
	now := time.Now()
	if max <= 0 || len(n.votes) == 0 {
		return nil
	}
	type rec struct {
		target string
		*voteBucket
	}
	bks := make([]rec, 0, len(n.votes))
	for id, b := range n.votes {
		if now.After(b.ExpiresAt) {
			continue
		}
		bks = append(bks, rec{target: id, voteBucket: b})
	}
	if len(bks) == 0 {
		return nil
	}
	// quante entry per target?
	per := int(math.Max(1, math.Ceil(float64(max)/float64(len(bks)))))

	out := make([]DeathVote, 0, max)
	for _, r := range bks {
		// estrai witness in ordine casuale
		ws := make([]string, 0, len(r.Witness))
		for w := range r.Witness {
			ws = append(ws, w)
		}
		rand.Shuffle(len(ws), func(i, j int) { ws[i], ws[j] = ws[j], ws[i] })
		take := per
		for _, w := range ws {
			if take == 0 || len(out) >= max {
				break
			}
			ts := r.Witness[w]
			// scarta voti vecchi oltre Δ
			if now.Sub(time.Unix(ts, 0)) > n.cfg.VoteWindow {
				continue
			}
			out = append(out, DeathVote{
				Target:  r.target,
				Inc:     r.Evidence.Inc,
				Hb:      r.Evidence.Hb,
				Witness: w,
				Ts:      ts,
			})
			take--
		}
		// consuma round di priorità a livello bucket
		if r.Priority > 0 {
			r.Priority--
		}
		if len(out) >= max {
			break
		}
	}
	return out
}

// eviction "hard" (membro e servizi), da chiamare sotto lock
func (n *Node) evictMemberLocked(id string) {
	now := time.Now()
	if _, ok := n.members[id]; ok {
		delete(n.members, id)
		// ripulisci servizi ospitati da quel nodo
		for k, s := range n.services {
			if s.NodeID == id {
				delete(n.services, k)
			}
		}
		n.recentlyEvicted[id] = now
		log.Printf("[EVICT] rimosso %s per certificato/quorum", id)
	}
}

// GC periodico per voti/obits
func (n *Node) gcDeathMeta() {
	now := time.Now()
	n.mu.Lock()
	defer n.mu.Unlock()

	for id, b := range n.votes {
		if now.After(b.ExpiresAt) {
			delete(n.votes, id)
		}
	}
	for id, e := range n.obits {
		if now.After(e.ExpiresAt) {
			delete(n.obits, id)
		}
	}
	// pulizia anti-rumore log
	for id, t := range n.recentlyEvicted {
		if now.Sub(t) > 1*time.Minute {
			delete(n.recentlyEvicted, id)
		}
	}
	removedVotes, removedObits := 0, 0
	for id, b := range n.votes {
		if now.After(b.ExpiresAt) {
			delete(n.votes, id)
			removedVotes++
		}
	}
	for id, e := range n.obits {
		if now.After(e.ExpiresAt) {
			delete(n.obits, id)
			removedObits++
		}
	}
	if removedVotes > 0 || removedObits > 0 {
		log.Printf("[DEATH-GC] rimossi votes=%d obits=%d", removedVotes, removedObits)
	}
}
