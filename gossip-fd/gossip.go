package main

import (
	"encoding/json"
	"log"
	"net"
	"sort"
	"time"
)

type deadEv struct {
	id  string
	inc uint64
	hb  uint64
}
type tr struct {
	old      MemberState
	new      MemberState
	id       string
	lastSeen time.Time
	delta    time.Duration
}
type GossipMessage struct {
	FromID     string                `json:"from_id"`
	ReturnAddr string                `json:"ret_addr"`
	IsReply    bool                  `json:"is_reply"`
	Membership []MemberSummary       `json:"membership"`
	Services   []ServiceAnnouncement `json:"services"`

	//piggyback di voti e certificati
	Votes []DeathVote        `json:"votes,omitempty"`
	Certs []DeathCertificate `json:"certs,omitempty"`
}

// ogni gossipinterval faccio gossip
func (n *Node) gossipLoop() {
	t := time.NewTicker(n.cfg.GossipInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			n.sendGossip()
			n.gcDeathMeta() // pulizia voti/certificati scaduti
		}
	}
}

// ogni heartbeat interval aggiorno il mio stato in modo che al prossimo gossip loop aggiorno il mio stato con gli altri peers
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
		var transitions []tr
		deadTriggers := 0

		// raccoglitore dei peer appena diventati DEAD con la loro evidenza (inc,hb)
		var localDead []deadEv
		n.mu.Lock()
		for id, m := range n.members {
			//salto me stesso oppure se il membro è già morto
			if id == n.cfg.SelfID || m.State == StateDead {
				continue
			}
			//se ho appena visto il membro salto
			if m.LastSeen.IsZero() {
				continue
			}
			delta := now.Sub(m.LastSeen) //da quanto non ricevo aggiornamenti
			old := m.State
			newSt := old
			//se non lo vedo da più tempo del timeout allora lo dichiaro Dead
			if delta > n.cfg.DeadTimeout {
				newSt = StateDead
				//se non lo vedo da più tempo di SuspectTimeout allora lo dichiaro Suspect
			} else if delta > n.cfg.SuspectTimeout {
				newSt = StateSuspect
				//altrimenti Alive
			} else {
				newSt = StateAlive
			}
			//se gli stati differiscono allora eseguo transizione di stato
			if newSt != old {
				m.State = newSt
				transitions = append(transitions, tr{
					old: old, new: newSt, id: id, lastSeen: m.LastSeen, delta: delta,
				})
				// se passa a DEAD, raccoglie l'evidenza per emettere un voto
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
		// logging delle transizioni
		for _, x := range transitions {
			log.Printf("[STATE] %s -> %s (peer=%s, lastSeen=%s, Δ=%s)",
				x.old, x.new, x.id, x.lastSeen.Format(time.RFC3339), x.delta.Truncate(time.Millisecond))
			if x.new == StateDead {
				log.Printf("[EVENTO] Node %s is DEAD. Emitted local vote.", x.id)
			}
		}
		//emessione dei voti locali
		for _, d := range localDead {
			n.recordLocalVote(d.id, DeathEvidence{Inc: d.inc, Hb: d.hb})
			log.Printf("[VOTE] emitted local vote for %s (inc=%d hb=%d)", d.id, d.inc, d.hb)
		}
		if len(localDead) > 0 {
			log.Printf("[VOTE] emitted %d local votes (Δ=%s, K=%d)", len(localDead), n.cfg.VoteWindow, n.cfg.QuorumK)
		}
		// elimina servizi scaduti
		n.pruneExpiredServices()
	}
}

func (n *Node) sendGossip() {
	//calcolo fanout dinamico
	fanoutK := n.calculateDynamicFanout()
	//Scelta target
	targets := n.pickRandomTargets(fanoutK)
	if len(targets) == 0 {
		return
	}
	//costruisco il  messaggio
	msg := n.buildMessage(false)
	log.Printf("[GOSSIP] send fanout=%d targets=%d", fanoutK, len(targets))
	// invio ai target il gossip message
	for _, peer := range targets {
		addr := peer.Addr // copia locale per evitare la cattura del puntatore
		go n.sendGossipMessage(addr, msg)
	}
}

// Funzione separata per inviare il messaggio e gestire il panic
func (n *Node) sendGossipMessage(addr string, msg GossipMessage) {
	defer func() {
		//check anti panico
		if r := recover(); r != nil {
			log.Printf("[GOSSIP] panic while sending to %s: %v", addr, r)
		}
	}()
	// Invio del messaggio
	if err := n.sendTo(addr, msg); err != nil {
		log.Printf("[GOSSIP] send-err addr=%s err=%v", addr, err)
	}
}

func (n *Node) sendTo(addr string, msg GossipMessage) error {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	b, _ := json.Marshal(msg)              //faccio il marshal del messaggio
	_, err = n.conn.WriteToUDP(b, udpAddr) //invio effettivo del messaggio
	return err
}

// buildReplyDiff costruisce una reply "diff-only" con la stessa logica di buildMessage
func (n *Node) buildReplyDiff(gm *GossipMessage) GossipMessage {
	n.mu.Lock()
	defer n.mu.Unlock()

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
				Incarnation: m.Incarnation,
				State:       m.State,
			})
		}
	}

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

	sa := make([]ServiceAnnouncement, 0, len(n.services))
	for _, s := range n.services {
		key := svcKey(s.Service, s.InstanceID)
		lastKnownVer := seenSvcVer[key]
		lastKnownUp := seenSvcUp[key]
		thisUp := s.Up && !s.Tombstone

		if s.Version > lastKnownVer || thisUp != lastKnownUp {
			if s.TTLSeconds <= 0 {
				s.TTLSeconds = n.cfg.ServiceTTL
			}
			sa = append(sa, ServiceAnnouncement{
				Service:    s.Service,
				InstanceID: s.InstanceID,
				NodeID:     s.NodeID,
				Addr:       s.Addr,
				Version:    s.Version,
				TTLSeconds: s.TTLSeconds,
				Up:         thisUp,
				Tombstone:  s.Tombstone,
			})
		}
	}
	certs := n.selectCertsLocked(n.cfg.MaxCertDigest)
	votes := n.selectVotesLocked(n.cfg.MaxVoteDigest)

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
		Certs:      certs,
		Votes:      votes,
	}
}

func (n *Node) buildMessage(isReply bool) GossipMessage {
	n.mu.Lock()
	defer n.mu.Unlock()
	// membership digest
	sums := make([]MemberSummary, 0, len(n.members))
	for _, m := range n.members {
		sums = append(sums, MemberSummary{
			ID:          m.ID,
			Addr:        m.Addr,
			Heartbeat:   m.Heartbeat,
			Incarnation: m.Incarnation,
			State:       m.State,
		})
	}
	// riordino in base alle novità ed invio solo quelle più nuove con delle priorità
	sort.Slice(sums, func(i, j int) bool {
		ai, aj := sums[i].State == StateAlive, sums[j].State == StateAlive
		if ai != aj {
			return ai && !aj //se un nodo non è alive
		}
		if sums[i].Incarnation != sums[j].Incarnation {
			return sums[i].Incarnation > sums[j].Incarnation //se vi sono incarnation maggiori
		}
		if sums[i].Heartbeat != sums[j].Heartbeat {
			return sums[i].Heartbeat > sums[j].Heartbeat //per heartbeat maggiore
		}
		return sums[i].ID < sums[j].ID //nel caso riordino per ID
	})
	if len(sums) > n.cfg.MaxDigestPeers {
		sums = sums[:n.cfg.MaxDigestPeers]
	}

	// service digest
	sann := make([]ServiceAnnouncement, 0, len(n.services))
	for _, s := range n.services {
		sann = append(sann, ServiceAnnouncement{
			Service:    s.Service,
			InstanceID: s.InstanceID,
			NodeID:     s.NodeID,
			Addr:       s.Addr,
			Version:    s.Version,
			TTLSeconds: s.TTLSeconds,
			Up:         s.Up,
			Tombstone:  s.Tombstone,
		})
	}
	//riordino in base all importanz degli aggiornamenti
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
	// certificati e voti
	certs := n.selectCertsLocked(n.cfg.MaxCertDigest)
	votes := n.selectVotesLocked(n.cfg.MaxVoteDigest)

	return GossipMessage{
		FromID:     n.cfg.SelfID,
		ReturnAddr: n.cfg.SelfAddr,
		IsReply:    isReply,
		Membership: sums,
		Services:   sann,
		Certs:      certs,
		Votes:      votes,
	}
}

func (n *Node) mergeMembership(gm *GossipMessage) int {
	now := time.Now()
	updated := 0
	n.mu.Lock()
	defer n.mu.Unlock()
	// SELF-BUMP: se i peer mi riportano con una inc >= della mia, salto avanti
	self := n.members[n.cfg.SelfID]
	for _, s := range gm.Membership {
		if s.ID != n.cfg.SelfID {
			continue
		}
		if s.Incarnation > self.Incarnation || (s.Incarnation == self.Incarnation && s.State != StateAlive) {
			oldInc := self.Incarnation
			self.Incarnation = s.Incarnation + 1 // nuova "vita"
			self.Heartbeat = 0                   // riparti da 0
			self.State = StateAlive
			self.LastSeen = now
			updated++
			log.Printf("[SELF-BUMP] inc %d -> %d (refuting old view)", oldInc, self.Incarnation)
		}
	}
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
	// merge degli altri membri (inc, hb)
	for _, s := range gm.Membership {
		if s.ID == n.cfg.SelfID {
			continue
		}
		m, ok := n.members[s.ID]
		if !ok {
			m = &Member{ID: s.ID, Addr: s.Addr}
			n.members[s.ID] = m
			updated++
			log.Printf("[MERGE] New member added: %s", s.ID)
		}
		if s.Addr != "" && s.Addr != m.Addr {
			m.Addr = s.Addr
			updated++
		}
		// Accetta solo se (inc, hb) è più alto
		if s.Incarnation > m.Incarnation || (s.Incarnation == m.Incarnation && s.Heartbeat > m.Heartbeat) {
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
	}
	return updated
}
