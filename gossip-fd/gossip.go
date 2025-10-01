package main

import (
	"log"
	"sort"
	"time"
)

type GossipMessage struct {
	FromID     string                `json:"from_id"`
	ReturnAddr string                `json:"ret_addr"`
	IsReply    bool                  `json:"is_reply"`
	Membership []MemberSummary       `json:"membership"`
	Services   []ServiceAnnouncement `json:"services"`

	// NEW: piggyback di voti e certificati
	Votes []DeathVote `json:"votes,omitempty"`
	Obits []Obituary  `json:"obits,omitempty"`
}

func (n *Node) gossipLoop() {
	t := time.NewTicker(n.cfg.GossipInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			n.sendGossip()
			n.gcDeathMeta() // pulizia voti/obits scaduti ad ogni tick
		}
	}
}

func (n *Node) heartbeatLoop() {
	t := time.NewTicker(n.cfg.HeartbeatInterval)
	defer t.Stop()
	for range t.C {
		n.mu.Lock()
		n.selfHB++
		self := n.members[n.cfg.SelfID]
		self.Heartbeat = n.selfHB
		self.LastSeen = time.Now()
		self.State = StateAlive
		n.mu.Unlock()
	}
}

func (n *Node) suspicionLoop() {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()

	for range t.C {
		now := time.Now()

		// raccolgo le transizioni SENZA fare I/O durante il lock
		type tr struct {
			old      MemberState
			new      MemberState
			id       string
			lastSeen time.Time
			delta    time.Duration
		}
		var transitions []tr
		deadTriggers := 0

		// NEW: raccoglitore dei peer appena diventati DEAD con la loro evidenza (inc,hb)
		type deadEv struct {
			id  string
			inc uint64
			hb  uint64
		}
		var localDead []deadEv

		// --- sezione critica minima ---
		n.mu.Lock()
		for id, m := range n.members {
			if id == n.cfg.SelfID || m.State == StateDead {
				continue
			}
			if m.LastSeen.IsZero() {
				continue
			}

			d := now.Sub(m.LastSeen)
			old := m.State
			newSt := old

			if d > n.cfg.DeadTimeout {
				newSt = StateDead
			} else if d > n.cfg.SuspectTimeout {
				newSt = StateSuspect
			} else {
				newSt = StateAlive
			}

			if newSt != old {
				m.State = newSt
				transitions = append(transitions, tr{
					old: old, new: newSt, id: id, lastSeen: m.LastSeen, delta: d,
				})

				// NEW: se passa a DEAD, snapshotta l'evidenza per emettere un voto (fuori lock)
				if newSt == StateDead {
					localDead = append(localDead, deadEv{
						id:  id,
						inc: m.Incarnation,
						hb:  m.Heartbeat,
					})
					deadTriggers++
				}
			}
		}
		n.mu.Unlock()
		// --- fine sezione critica ---

		// logging delle transizioni (fuori lock)
		for _, x := range transitions {
			log.Printf("[STATE] %s -> %s (peer=%s, lastSeen=%s, Δ=%s)",
				x.old, x.new, x.id, x.lastSeen.Format(time.RFC3339), x.delta.Truncate(time.Millisecond))
			if x.new == StateDead {
				// Aggiorna il messaggio: niente immediate gossip, ora usiamo piggyback con priorità
				log.Printf("[EVENTO] Nodo %s è diventato DEAD. Emesso voto locale (piggyback nei prossimi round).", x.id)
			}
		}
		// NEW: emetti i voti locali (uno per ciascun peer appena marcato DEAD)
		for _, d := range localDead {
			n.recordLocalVote(d.id, DeathEvidence{Inc: d.inc, Hb: d.hb})
			// LOG: voto locale emesso
			log.Printf("[VOTE] emesso voto locale per %s (inc=%d hb=%d)", d.id, d.inc, d.hb)
		}
		if len(localDead) > 0 {
			log.Printf("[VOTE] emessi %d voti locali (Δ=%s, K=%d)", len(localDead), n.cfg.VoteWindow, n.cfg.QuorumK)
		}

		// scadenze servizi (fa lock al suo interno)
		n.pruneExpiredServices()
	}
}

func (n *Node) sendGossip() {

	// 1) Calcolo fanout dinamico
	fanoutK := n.calculateDynamicFanout()
	if fanoutK <= 0 {
		return // niente log rumoroso se non inviamo
	}

	// 2) Scelta target
	targets := n.pickRandomTargets(fanoutK)
	if len(targets) == 0 {
		return
	}

	// 3) Costruzione messaggio (una volta sola)
	msg := n.buildMessage(false)

	// 4) Logging essenziale del batch PRIMA dell’invio
	log.Printf("[GOSSIP] send fanout=%d targets=%d", fanoutK, len(targets))

	// 5) Invio concorrente (fire-and-forget), con cattura sicura dei parametri
	for _, peer := range targets {
		addr := peer.Addr // copia locale per evitare la cattura del puntatore
		go func(a string) {
			// difesa extra: non lasciare che un panic in un goroutine uccida il processo
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[GOSSIP] panic while sending to %s: %v", a, r)
				}
			}()
			if err := n.sendTo(a, msg); err != nil {
				log.Printf("[GOSSIP] send-err addr=%s err=%v", a, err)
			}
		}(addr)
	}
}

// buildReplyDiff costruisce una reply "diff-only" senza usare timestamp.
// Confronta solo heartbeat (membership) e version/up (services).
func (n *Node) buildReplyDiff(gm *GossipMessage) GossipMessage {
	n.mu.Lock()
	defer n.mu.Unlock()

	// --- indicizza ciò che il mittente dichiara di sapere ---
	seenHB := make(map[string]MemberSummary, len(gm.Membership))
	for _, s := range gm.Membership {
		seenHB[s.ID] = s
	}
	seenSvcVer := make(map[string]uint64, len(gm.Services))
	seenSvcUp := make(map[string]bool, len(gm.Services))
	for _, a := range gm.Services {
		key := svcKey(a.Service, a.InstanceID)
		seenSvcVer[key] = a.Version
		seenSvcUp[key] = a.Up && !a.Tombstone
	}

	// --- membership: includi solo entry mancanti o con HB/stato più "forte" ---
	ms := make([]MemberSummary, 0, len(n.members))
	for _, m := range n.members {
		s, ok := seenHB[m.ID]
		need := !ok ||
			m.Incarnation > s.Incarnation ||
			(m.Incarnation == s.Incarnation && m.Heartbeat > s.Heartbeat) ||
			(m.State == StateAlive && s.State != StateAlive)
		if need {
			ms = append(ms, MemberSummary{
				ID:          m.ID,
				Addr:        m.Addr,
				Heartbeat:   m.Heartbeat,
				Incarnation: m.Incarnation, // NEW
				State:       m.State,
			})
		}
	}

	// Alive prima, poi heartbeat decrescente; fallback deterministico
	sort.Slice(ms, func(i, j int) bool {
		ai, aj := ms[i].State == StateAlive, ms[j].State == StateAlive
		if ai != aj {
			return ai && !aj
		}
		if ms[i].Incarnation != ms[j].Incarnation {
			return ms[i].Incarnation > ms[j].Incarnation
		}
		if ms[i].Heartbeat != ms[j].Heartbeat {
			return ms[i].Heartbeat > ms[j].Heartbeat
		}
		return ms[i].ID < ms[j].ID
	})
	if len(ms) > n.cfg.MaxDigestPeers {
		ms = ms[:n.cfg.MaxDigestPeers]
	}

	// --- services: includi solo (service,instance) mancanti o con Version maggiore o flip di stato ---
	sa := make([]ServiceAnnouncement, 0, len(n.services))
	for _, s := range n.services {
		key := svcKey(s.Service, s.InstanceID)
		lastKnownVer := seenSvcVer[key]
		lastKnownUp := seenSvcUp[key]
		thisUp := s.Up && !s.Tombstone

		if s.Version > lastKnownVer || thisUp != lastKnownUp {
			ttl := s.TTLSeconds
			if ttl <= 0 {
				ttl = n.cfg.ServiceTTL // fallback locale; non dipende da orologi remoti
			}
			sa = append(sa, ServiceAnnouncement{
				Service:    s.Service,
				InstanceID: s.InstanceID,
				NodeID:     s.NodeID,
				Addr:       s.Addr,
				Version:    s.Version,
				TTLSeconds: ttl,
				Up:         thisUp,
				Tombstone:  s.Tombstone,
			})
		}
	}
	obits := n.selectObitsLocked(n.cfg.MaxObitDigest)
	votes := n.selectVotesLocked(n.cfg.MaxVoteDigest)

	// Up prima, poi versione decrescente; fallback deterministico
	sort.Slice(sa, func(i, j int) bool {
		if sa[i].Up != sa[j].Up {
			return sa[i].Up && !sa[j].Up
		}
		if sa[i].Version != sa[j].Version {
			return sa[i].Version > sa[j].Version
		}
		if sa[i].Service != sa[j].Service {
			return sa[i].Service < sa[j].Service
		}
		return sa[i].InstanceID < sa[j].InstanceID
	})
	if len(sa) > n.cfg.MaxServiceDigest {
		sa = sa[:n.cfg.MaxServiceDigest]
	}

	return GossipMessage{
		FromID:     n.cfg.SelfID,
		ReturnAddr: n.cfg.SelfAddr,
		IsReply:    true,
		Membership: ms,
		Services:   sa,
		Obits:      obits, // NEW
		Votes:      votes, // NEW
	}
}

func (n *Node) buildMessage(isReply bool) GossipMessage {
	n.mu.Lock()
	defer n.mu.Unlock()

	// --- membership digest (senza timestamp) ---
	sums := make([]MemberSummary, 0, len(n.members))
	for _, m := range n.members {
		sums = append(sums, MemberSummary{
			ID:          m.ID,
			Addr:        m.Addr,
			Heartbeat:   m.Heartbeat,
			Incarnation: m.Incarnation, // NEW
			State:       m.State,
		})
	}

	// Alive prima, poi heartbeat decrescente; fallback deterministico su Incarnation, hb
	sort.Slice(sums, func(i, j int) bool {
		ai, aj := sums[i].State == StateAlive, sums[j].State == StateAlive
		if ai != aj {
			return ai && !aj
		}
		if sums[i].Incarnation != sums[j].Incarnation {
			return sums[i].Incarnation > sums[j].Incarnation
		}
		if sums[i].Heartbeat != sums[j].Heartbeat {
			return sums[i].Heartbeat > sums[j].Heartbeat
		}
		return sums[i].ID < sums[j].ID
	})
	if len(sums) > n.cfg.MaxDigestPeers {
		sums = sums[:n.cfg.MaxDigestPeers]
	}

	// --- service digest (senza timestamp) ---
	sann := make([]ServiceAnnouncement, 0, len(n.services))
	for _, s := range n.services {
		sann = append(sann, ServiceAnnouncement{
			Service:    s.Service,
			InstanceID: s.InstanceID,
			NodeID:     s.NodeID,
			Addr:       s.Addr,
			Version:    s.Version,
			TTLSeconds: s.TTLSeconds, // ok: è una durata, non un timestamp
			Up:         s.Up,
			Tombstone:  s.Tombstone,
		})
	}

	// Up prima, poi versione decrescente; fallback deterministico
	sort.Slice(sann, func(i, j int) bool {
		if sann[i].Up != sann[j].Up {
			return sann[i].Up && !sann[j].Up
		}
		if sann[i].Version != sann[j].Version {
			return sann[i].Version > sann[j].Version
		}
		if sann[i].Service != sann[j].Service {
			return sann[i].Service < sann[j].Service
		}
		return sann[i].InstanceID < sann[j].InstanceID
	})
	if len(sann) > n.cfg.MaxServiceDigest {
		sann = sann[:n.cfg.MaxServiceDigest]
	}
	// ====== piggyback obits e votes ======
	obits := n.selectObitsLocked(n.cfg.MaxObitDigest)
	votes := n.selectVotesLocked(n.cfg.MaxVoteDigest)

	return GossipMessage{
		FromID:     n.cfg.SelfID,
		ReturnAddr: n.cfg.SelfAddr,
		IsReply:    isReply,
		Membership: sums,
		Services:   sann,
		Obits:      obits, // NEW
		Votes:      votes, // NEW
	}
}

func (n *Node) mergeMembership(gm *GossipMessage) int {
	now := time.Now()
	updated := 0

	n.mu.Lock()
	defer n.mu.Unlock()

	// --- SELF-BUMP: se i peer mi riportano con una inc >= della mia, salto avanti ---
	self := n.members[n.cfg.SelfID]
	for _, s := range gm.Membership {
		if s.ID != n.cfg.SelfID {
			continue
		}
		if s.Incarnation > self.Incarnation || (s.Incarnation == self.Incarnation && s.State != StateAlive) {
			oldInc := self.Incarnation
			self.Incarnation = s.Incarnation + 1 // NEW: nuova "vita"
			self.Heartbeat = 0                   // riparti da 0
			self.State = StateAlive
			self.LastSeen = now
			updated++
			log.Printf("[SELF-BUMP] inc %d -> %d (refuting old view)", oldInc, self.Incarnation)
		}
	}

	// --- mittente come prima (bootstrap morbido) ---
	if gm.FromID != "" {
		if _, ok := n.members[gm.FromID]; !ok {
			n.members[gm.FromID] = &Member{
				ID:       gm.FromID,
				Addr:     gm.ReturnAddr,
				State:    StateAlive,
				LastSeen: now,
			}
			updated++
		} else if gm.ReturnAddr != "" && n.members[gm.FromID].Addr != gm.ReturnAddr {
			n.members[gm.FromID].Addr = gm.ReturnAddr
			updated++
		}
	}

	// --- merge degli altri membri con ordine (inc, hb) ---
	for _, s := range gm.Membership {
		if s.ID == n.cfg.SelfID {
			continue
		} // già gestito sopra

		m, ok := n.members[s.ID]
		if !ok {
			m = &Member{ID: s.ID, Addr: s.Addr}
			n.members[s.ID] = m
			updated++
			log.Printf("[MERGE] Nuovo membro aggiunto: %s", s.ID)
		}
		if s.Addr != "" && s.Addr != m.Addr {
			m.Addr = s.Addr
			updated++
		}

		// Accetta solo se (inc, hb) è più nuovo
		if s.Incarnation > m.Incarnation ||
			(s.Incarnation == m.Incarnation && s.Heartbeat > m.Heartbeat) {
			old := m.State
			m.Incarnation = s.Incarnation
			m.Heartbeat = s.Heartbeat
			m.State = StateAlive
			m.LastSeen = now
			if old != StateAlive {
				log.Printf("[RECOVER] %s: %s -> ALIVE (inc=%d hb=%d)", s.ID, old, s.Incarnation, s.Heartbeat)
			}
			updated++
		}

		// niente degradazioni su stato remoto: lasciale al tuo FD locale
	}

	return updated
}
