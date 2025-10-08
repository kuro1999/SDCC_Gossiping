package main

import (
	"log"
	"math"
	"math/rand/v2"
	"sort"
	"time"
)

type DeathEvidence struct {
	Inc uint64 `json:"inc"` // incarnation osservata a Dead
	Hb  uint64 `json:"hb"`  // heartbeat osservato a Dead
}

type DeathVote struct {
	Target  string    `json:"target"` // ID del nodo Dead
	Inc     uint64    `json:"inc"`
	Hb      uint64    `json:"hb"`
	Witness string    `json:"w"` // ID del witness (mittente originario del voto)
	Ts      time.Time `json:"ts"`
}

type DeathCertificate struct {
	Target   string    `json:"target"`
	Inc      uint64    `json:"inc"`
	Hb       uint64    `json:"hb"`
	IssuedAt time.Time `json:"iat"`
	TtlSec   int       `json:"ttl"` // durata totale del certificato
	Quorum   int       `json:"k"`   // quorum
}

// bucket dei voti ricevuti su un target
type voteBucket struct {
	Evidence  DeathEvidence // evidenza massima vista per X
	Witness   map[string]time.Time
	ExpiresAt time.Time // scadenza del bucket
	Priority  int       // round di priorità residui per piggyback voti
}

// certificato attivo da propagare
type certificateEntry struct {
	Evidence  DeathEvidence
	IssuedAt  time.Time
	ExpiresAt time.Time // IssuedAt + CertificateTTL
	Priority  int       // round di priorità residui per piggyback certificates
}

// se si ha un evidenza più recente basata su incarnation o heartbeat
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
			Witness:   make(map[string]time.Time),
			ExpiresAt: now.Add(n.cfg.VoteWindow),
			Priority:  n.cfg.VotePriorityRounds,
		}
		n.votes[target] = b
	}
	// mantieni l'evidenza più nuova
	if evidenceNewer(ev, b.Evidence) {
		b.Evidence = ev
	}
	// aggiorna finestra e lista witness
	b.ExpiresAt = now.Add(n.cfg.VoteWindow)
	b.Witness[n.cfg.SelfID] = now
	if b.Priority < n.cfg.VotePriorityRounds {
		b.Priority = n.cfg.VotePriorityRounds
	}
}

// ingest dei certificati: applica prima di fare merge
func (n *Node) ingestCertificates(certs []DeathCertificate) (evicted int) {
	now := time.Now()
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, o := range certs {
		// scarta certificati scaduti
		exp := o.IssuedAt.Add(time.Duration(o.TtlSec) * time.Second)
		if now.After(exp) {
			continue
		}
		// se ho ALIVE più recenti dell'evidenza del certificato, ignoro
		if m, ok := n.members[o.Target]; ok && m.State == StateAlive {
			local := DeathEvidence{Inc: m.Incarnation, Hb: m.Heartbeat}
			log.Printf("[CERT-IGNORE] target=%s ignorato (local ALIVE inc=%d hb=%d > cert inc=%d hb=%d)",
				o.Target, m.Incarnation, m.Heartbeat, o.Inc, o.Hb)
			if evidenceNewer(local, DeathEvidence{Inc: o.Inc, Hb: o.Hb}) {
				continue
			}
		}

		// esegui eviction
		n.evictMemberLocked(o.Target)

		// memorizza il certificato per ridiffusione
		entry, ok := n.DeathCertificates[o.Target]
		if !ok {
			entry = &certificateEntry{
				Evidence:  DeathEvidence{Inc: o.Inc, Hb: o.Hb},
				IssuedAt:  o.IssuedAt,
				ExpiresAt: exp,
				Priority:  n.cfg.CertPriorityRounds,
			}
			n.DeathCertificates[o.Target] = entry
		} else {
			// aggiorna eventuale evidenza se più forte
			ev := DeathEvidence{Inc: o.Inc, Hb: o.Hb}
			if evidenceNewer(ev, entry.Evidence) {
				entry.Evidence = ev
			}
			if exp.After(entry.ExpiresAt) {
				entry.ExpiresAt = exp
			}
			if entry.Priority < n.cfg.CertPriorityRounds {
				entry.Priority = n.cfg.CertPriorityRounds
			}
		}
		evicted++
	}
	return
}

// ingest dei voti: aggiorna bucket e promuove a certificato se quorum raggiunto
func (n *Node) ingestVotes(votes []DeathVote) (promoted int) {
	now := time.Now()
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, v := range votes {
		// voto scaduto
		if now.Sub(v.Ts) > n.cfg.VoteWindow {
			continue
		}
		b, ok := n.votes[v.Target]
		if !ok {
			b = &voteBucket{
				Evidence:  DeathEvidence{Inc: v.Inc, Hb: v.Hb},
				Witness:   make(map[string]time.Time),
				ExpiresAt: now.Add(n.cfg.VoteWindow),
				Priority:  n.cfg.VotePriorityRounds,
			}
			log.Printf("[VOTE] creato bucket per %s (inc=%d hb=%d, Δ=%s)", v.Target, v.Inc, v.Hb, n.cfg.VoteWindow)
			n.votes[v.Target] = b
		}
		// mantieni evidenza più recente
		ev := DeathEvidence{Inc: v.Inc, Hb: v.Hb}
		if evidenceNewer(ev, b.Evidence) {
			b.Evidence = ev
		}
		// aggiorna finestra e lista witness
		b.ExpiresAt = now.Add(n.cfg.VoteWindow)
		b.Witness[v.Witness] = v.Ts
		quorum := n.cfg.QuorumK
		log.Printf("[VOTE] witness=%s per %s (|W|=%d/%d)", v.Witness, v.Target, len(b.Witness), quorum)
		if b.Priority < n.cfg.VotePriorityRounds {
			b.Priority = n.cfg.VotePriorityRounds
		}

		// quorum raggiunto?
		if len(b.Witness) >= quorum {
			log.Printf("[EMISSION] quorum raggiunto per %s (K=%d, inc=%d hb=%d) → certificato emesso (TTL=%s)",
				v.Target, quorum, b.Evidence.Inc, b.Evidence.Hb, n.cfg.CertTTL)
			// promuovi a certificato di mort
			if _, exists := n.DeathCertificates[v.Target]; !exists {
				iat := now
				n.DeathCertificates[v.Target] = &certificateEntry{
					Evidence:  b.Evidence,
					IssuedAt:  iat,
					ExpiresAt: iat.Add(n.cfg.CertTTL),
					Priority:  n.cfg.CertPriorityRounds,
				}
				promoted++
			}
		}
	}
	return
}

// selezione certificati di morte da piggybackare
func (n *Node) selectCertsLocked(max int) []DeathCertificate {
	now := time.Now()
	if max <= 0 {
		return nil
	}
	type rec struct {
		id string
		*certificateEntry
	}
	list := make([]rec, 0, len(n.DeathCertificates))
	for id, e := range n.DeathCertificates {
		if now.After(e.ExpiresAt) {
			continue
		}
		list = append(list, rec{id: id, certificateEntry: e})
	}
	// ordina per priorità
	sort.Slice(list, func(i, j int) bool {
		if list[i].Priority != list[j].Priority {
			return list[i].Priority > list[j].Priority
		}
		return list[i].ExpiresAt.Before(list[j].ExpiresAt) //quello che scade prima
	})
	if len(list) > max {
		list = list[:max]
	}
	quorum := n.cfg.QuorumK
	out := make([]DeathCertificate, 0, len(list))
	for _, r := range list {
		out = append(out, DeathCertificate{
			Target:   r.id,
			Inc:      r.Evidence.Inc,
			Hb:       r.Evidence.Hb,
			IssuedAt: r.IssuedAt,
			TtlSec:   int(n.cfg.CertTTL.Seconds()),
			Quorum:   quorum,
		})
		// consuma un round di priorità
		if r.Priority > 0 {
			r.Priority--
		}
	}
	return out
}

// selezione voti da piggybackare
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
		//riduce bias dato dall ordine di arrivo dei witness
		rand.Shuffle(len(ws), func(i, j int) { ws[i], ws[j] = ws[j], ws[i] })
		take := per
		for _, w := range ws {
			if take == 0 || len(out) >= max {
				break
			}
			ts := r.Witness[w]
			// scarta voti vecchi
			if now.Sub(ts) > n.cfg.VoteWindow {
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

// eviction sotto lock
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
		log.Printf("[EVICT] rimosso %s per certificato", id)
	}
}

// garbage collector periodico per voti/certificati
func (n *Node) gcDeathMeta() {
	now := time.Now()
	n.mu.Lock()
	defer n.mu.Unlock()
	//rimozione voti scaduti
	for id, b := range n.votes {
		if now.After(b.ExpiresAt) {
			delete(n.votes, id)
		}
	}
	//rimozione certificati di morte scaduti
	for id, e := range n.DeathCertificates {
		if now.After(e.ExpiresAt) {
			delete(n.DeathCertificates, id)
		}
	}
}
