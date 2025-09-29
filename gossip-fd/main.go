// main.go
package main

import (
	"SDCC_Gossiping/utils"
	"bytes"
	"encoding/json"
	_ "errors"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*
  Failure detection + Service discovery via gossip (versione semplice)

  - Membership gossip: come prima (heartbeat, SUSPECT/DEAD via timeout).
  - Service discovery:
      * Ogni nodo mantiene una registry locale delle istanze di servizio.
      * Annunci (add/update/remove) si propagano come piggyback nei messaggi gossip.
      * Regole di merge: si accetta la versione più alta (version counter monotono),
        si onora TTL (se non refreshata oltre TTL*2, l'istanza è scartata).
      * L'API /discover filtra istanze su nodi non ALIVE.

  - Demo service: "calc" HTTP con /sum e /sub opzionali per la prova end-to-end.
*/

type svcRegisterReq struct {
	Service    string `json:"service"`
	InstanceID string `json:"instance_id"`
	Addr       string `json:"addr"`
	TTL        int    `json:"ttl_seconds"`
}

type svcDeregisterReq struct {
	Service    string `json:"service"`
	InstanceID string `json:"instance_id"`
}

type MemberState string

const (
	StateAlive   MemberState = "ALIVE"
	StateSuspect MemberState = "SUSPECT"
	StateDead    MemberState = "DEAD"
)

// =================== DEATH CERTIFICATES: TIPI WIRE ===================

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

// ---------------- Membership ----------------

type Member struct {
	ID          string      `json:"id"`
	Addr        string      `json:"addr"`
	Heartbeat   uint64      `json:"hb"`
	LastSeen    time.Time   `json:"last_seen"`
	State       MemberState `json:"state"`
	Incarnation uint64      `json:"inc"` // riservato
}

type MemberSummary struct {
	ID          string      `json:"id"`
	Addr        string      `json:"addr"`
	Heartbeat   uint64      `json:"hb"`
	Incarnation uint64      `json:"inc"`
	State       MemberState `json:"state"`
}

// ---------------- Service Registry ----------------

type ServiceAnnouncement struct {
	Service    string `json:"service"` // es: "calc"
	InstanceID string `json:"id"`      // es: "node1-calc"
	NodeID     string `json:"node"`    // chi ospita
	Addr       string `json:"addr"`    // es: "node1:18080"
	Version    uint64 `json:"ver"`     // contatore monotono
	TTLSeconds int    `json:"ttl"`     // es: 15
	Up         bool   `json:"up"`      // true se attivo
	Tombstone  bool   `json:"tomb"`    // per rimozione (non usato molto qui)
}

type ServiceInstance struct {
	Service     string
	InstanceID  string
	NodeID      string
	Addr        string
	Version     uint64
	TTLSeconds  int
	Up          bool
	LastUpdated time.Time
	Tombstone   bool
}

// chiave stabile per mappa locale
func svcKey(svc, id string) string { return svc + "##" + id }

// ---------------- Gossip Message ----------------

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

// ---------------- Config ----------------

type Node struct {
	cfg     utils.Config
	conn    *net.UDPConn
	mu      sync.Mutex
	members map[string]*Member

	// service registry
	services   map[string]*ServiceInstance // key = svcKey
	selfHB     uint64
	udpPort    int
	lastSvcVer map[string]uint64 // ultima versione vista per chiave
	//registry

	httpClient *http.Client // client HTTP con timeout

	// ====== NEW: meta per eviction by quorum ======
	votes           map[string]*voteBucket // key = targetID
	obits           map[string]*obitEntry  // key = targetID
	recentlyEvicted map[string]time.Time   // anti-rumore log/cleanup
}

// =============== Helper per death certificates ===============

// confronto lessicografico su (Inc, Hb)
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

// ---------------- Node init ----------------

func NewNode() (*Node, error) {
	cfg, err := utils.GetConfig()
	if err != nil {
		return nil, err
	}

	udpAddr := net.UDPAddr{IP: net.IPv4zero, Port: cfg.UDPPort}
	conn, err := net.ListenUDP("udp", &udpAddr)
	if err != nil {
		return nil, fmt.Errorf("errore ListenUDP: %w", err)
	}

	n := &Node{
		cfg:             cfg,
		conn:            conn,
		members:         make(map[string]*Member),
		services:        make(map[string]*ServiceInstance),
		udpPort:         cfg.UDPPort,
		votes:           make(map[string]*voteBucket),
		obits:           make(map[string]*obitEntry),
		recentlyEvicted: make(map[string]time.Time),
		httpClient:      &http.Client{Timeout: 1 * time.Second},
		lastSvcVer:      make(map[string]uint64),
	}

	now := time.Now()
	n.members[cfg.SelfID] = &Member{
		ID:          n.cfg.SelfID,
		Addr:        n.cfg.SelfAddr,
		Heartbeat:   0,
		LastSeen:    now,
		Incarnation: 1,
		State:       StateAlive,
	}

	if n.cfg.APIPort != n.udpPort {
		log.Printf("[WARN] API_PORT (%d) diverso dalla porta UDP (%d). "+
			"Il registry restituisce indirizzi HTTP (API). Se non coincidono con l'UDP, "+
			"il gossip verso i peer del registry potrebbe non raggiungerli. "+
			"Valuta la stessa porta o http_addr/udp_addr nel registry.",
			n.cfg.APIPort, n.udpPort)
	}
	return n, nil
}

func (n *Node) run() {
	log.Printf("[BOOT] %s su %s | API :%d | services=%q | registry=%s | seeds=%s",
		n.cfg.SelfID, n.cfg.SelfAddr, n.cfg.APIPort, n.cfg.ServicesCSV, n.cfg.RegistryURL, os.Getenv("SEEDS"))

	// host "canonico" del nodo (ID = solo host, mai con :porta)
	selfHost, _, _ := strings.Cut(n.cfg.SelfAddr, ":")

	// 1) Bootstrap dei peer dal registry (se configurato)
	if peers, err := n.bootstrapFromRegistry(); err != nil {
		log.Printf("[ERROR] Errore nel bootstrap dal registry: %v", err)
	} else {
		for _, p := range peers {
			// p è un addr HTTP "host:porta" restituito dal registry
			host, _, _ := strings.Cut(p, ":")
			if host == "" || host == selfHost {
				// salta indirizzi vuoti o riferimenti a se stesso,
				// anche se API_PORT e porta UDP differiscono
				continue
			}

			n.mu.Lock()
			if m, ok := n.members[host]; ok {
				// già presente (es. arrivato da SEEDS) → aggiorna solo l'Addr
				if m.Addr != p {
					m.Addr = p
				}
				// se per qualche motivo era DEAD al boot, riportalo a SUSPECT
				if m.State == StateDead {
					m.State = StateSuspect
					m.LastSeen = time.Time{}
				}
			} else {
				// nuovo peer in vista iniziale
				n.members[host] = &Member{
					ID:        host, // ID canonico = solo host
					Addr:      p,    // indirizzo completo host:porta per UDP/HTTP
					Heartbeat: 0,
					LastSeen:  time.Time{},
					State:     StateSuspect,
				}
			}
			n.mu.Unlock()
		}
	}

	// 2) Avvio goroutine di membership/failure detector
	go n.receiveLoop()
	go n.heartbeatLoop()
	go n.gossipLoop()
	go n.suspicionLoop()

	// 3) Avvio API HTTP (discovery + endpoint /api/membership/evict) e servizi demo
	go n.startDiscoveryAPI(n.cfg.APIPort)
	n.maybeStartDemoServices()

	// 4) Manutenzione TTL servizi
	go n.serviceRefreshLoop()

	// 5) Stampa periodica della view
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		n.printView()
	}
}

// bootstrapFromRegistry effettua il bootstrap del nodo contattando il registry per ottenere i peer
func (n *Node) bootstrapFromRegistry() ([]string, error) {
	// URL del registry per ottenere la lista dei peer
	url := fmt.Sprintf("http://%s/api/join", n.cfg.RegistryURL)

	// Costruisco l'addr da pubblicare al registry usando la porta HTTP API
	// (così il registry può inviare POST /api/membership/evict a questo endpoint)
	host, _, _ := strings.Cut(n.cfg.SelfAddr, ":")
	joinAddr := fmt.Sprintf("%s:%d", host, n.cfg.APIPort)

	// Corpo della richiesta POST con id e indirizzo HTTP del nodo
	body := map[string]string{
		"id":   n.cfg.SelfID,
		"addr": joinAddr, // IMPORTANTE: indirizzo HTTP per notifiche dal registry
	}
	b, _ := json.Marshal(body)

	// Creazione della richiesta HTTP
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("errore nella creazione della richiesta: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Invio della richiesta al registry (riuso il client con timeout)
	resp, err := n.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("errore nel contattare il registry: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("errore nel rispondere dal registry: status %d", resp.StatusCode)
	}

	// Decodifica della risposta JSON che contiene la lista dei peer (addr HTTP)
	var respBody struct {
		Peers []string `json:"peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		return nil, fmt.Errorf("errore nella decodifica della risposta del registry: %v", err)
	}

	// Ritorno la lista dei peer (si assume stessa porta per UDP e HTTP)
	return respBody.Peers, nil
}

// ---------------- Heartbeat & suspicion ----------------

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

func (n *Node) gossipLoop() {
	t := time.NewTicker(n.cfg.GossipInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			n.sendGossip("periodic")
			n.gcDeathMeta() // pulizia voti/obits scaduti ad ogni tick
		}
	}
}

// Funzione che invia gossip periodico
func (n *Node) sendGossip(origin string) {
	// normalizza il tag di origine per i log
	if origin == "" {
		origin = "unknown"
	}

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
	log.Printf("[GOSSIP][%s] send fanout=%d targets=%d", origin, fanoutK, len(targets))

	// 5) Invio concorrente (fire-and-forget), con cattura sicura dei parametri
	for _, peer := range targets {
		addr := peer.Addr // copia locale per evitare la cattura del puntatore
		go func(a string) {
			// difesa extra: non lasciare che un panic in un goroutine uccida il processo
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[GOSSIP][%s] panic while sending to %s: %v", origin, a, r)
				}
			}()
			if err := n.sendTo(a, msg); err != nil {
				log.Printf("[GOSSIP][%s] send-err addr=%s err=%v", origin, a, err)
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

func (n *Node) sendTo(addr string, msg GossipMessage) error {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	b, _ := json.Marshal(msg)
	_, err = n.conn.WriteToUDP(b, udpAddr)
	return err
}

func (n *Node) receiveLoop() {
	buf := make([]byte, 64*1024)
	for {
		nRead, src, err := n.conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("[RECV-ERR] %v", err)
			continue
		}
		data := make([]byte, nRead)
		copy(data, buf[:nRead])

		var gm GossipMessage
		if err := json.Unmarshal(data, &gm); err != nil {
			log.Printf("[DECODE-ERR] from %s: %v", src.String(), err)
			continue
		}

		// 1) Applica prima i certificati e i voti (possono causare eviction/promozioni a certificato)
		obitsApplied := n.ingestObits(gm.Obits)
		votesPromoted := n.ingestVotes(gm.Votes)
		if obitsApplied > 0 || votesPromoted > 0 {
			log.Printf("[DEATH] obits_applied=%d, obits_promoted=%d (da %s)", obitsApplied, votesPromoted, gm.FromID)
		}

		// 2) FILTRO anti-resurrezione: rimuovi dal digest membership le entry coperte da obit attivo
		filtered := 0
		func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			now := time.Now()

			dst := gm.Membership[:0]
			for _, s := range gm.Membership {
				if e, ok := n.obits[s.ID]; ok && now.Before(e.ExpiresAt) {
					// se l'evidenza del digest NON è più nuova del certificato, scarta l'entry
					if !evidenceNewer(DeathEvidence{Inc: s.Incarnation, Hb: s.Heartbeat}, e.Evidence) {
						filtered++
						continue
					}
				}
				dst = append(dst, s)
			}
			gm.Membership = dst
		}()
		if filtered > 0 {
			log.Printf("[DEATH] filtrate %d entry membership coperte da obit attivo (da %s)", filtered, gm.FromID)
		}

		// 3) Merge come prima
		mu := n.mergeMembership(&gm)
		su := n.mergeServices(&gm)

		// 4) PUSH-PULL a due passi: rispondi solo se NON è una reply
		if !gm.IsReply {
			ret := strings.TrimSpace(gm.ReturnAddr)
			if ret == "" {
				ret = src.String() // fallback prudente sull'indirizzo UDP sorgente
			}
			// opzionale: evita di rispondere a te stesso
			if ret != "" && ret != n.cfg.SelfAddr {
				reply := n.buildReplyDiff(&gm)
				if len(reply.Membership) > 0 || len(reply.Services) > 0 || len(reply.Obits) > 0 || len(reply.Votes) > 0 {
					if err := n.sendTo(ret, reply); err != nil {
						log.Printf("[REPLY-ERR] to %s: %v", ret, err)
					} else {
						log.Printf("[REPLY] to=%s members=%d services=%d obits=%d votes=%d",
							ret, len(reply.Membership), len(reply.Services), len(reply.Obits), len(reply.Votes))
					}
				}

			}
		}

		if mu+su > 0 {
			// log.Printf("[MERGE] da=%s, membri=%d, servizi=%d", gm.FromID, mu, su)
		}
	}
}

// ---------------- Merge logic ----------------

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

func (n *Node) mergeServices(gm *GossipMessage) int {
	now := time.Now() // usato solo localmente per l'ultimo "heard"
	updated := 0

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, ann := range gm.Services {
		key := svcKey(ann.Service, ann.InstanceID)
		cur, ok := n.services[key]

		// monotonicità globale: scarta se la versione è <= dell'ultima vista
		last := n.lastSvcVer[key]
		if ann.Version <= last {
			// NOTA: se vuoi che announce a stessa versione fungano da "keepalive",
			// puoi rimuovere questo check o gestire un ramo speciale per il solo refresh del LastUpdated.
			continue
		}
		// ridondante ma sicuro: se esiste cur e la versione non supera cur.Version, ignora
		if ok && ann.Version <= cur.Version {
			continue
		}

		// normalizza TTL (durata locale, non timestamp)
		ttl := ann.TTLSeconds
		if ttl <= 0 {
			ttl = n.cfg.ServiceTTL
		}

		up := ann.Up && !ann.Tombstone
		inst := &ServiceInstance{
			Service:     ann.Service,
			InstanceID:  ann.InstanceID,
			NodeID:      ann.NodeID,
			Addr:        ann.Addr,
			Version:     ann.Version,
			TTLSeconds:  ttl,
			Up:          up,
			Tombstone:   ann.Tombstone,
			LastUpdated: now, // solo clock locale
		}

		n.services[key] = inst

		// aggiorna la waterline di versione
		if ann.Version > n.lastSvcVer[key] {
			n.lastSvcVer[key] = ann.Version
		}
		updated++
	}
	return updated
}

// ---------------- Service registry (locale) ----------------

func (n *Node) registerLocalService(service, instanceID, addr string, ttl int) {
	now := time.Now()
	n.mu.Lock()
	defer n.mu.Unlock()

	key := svcKey(service, instanceID)
	cur, ok := n.services[key]

	// calcola la prossima versione > max(cur.Version, lastSvcVer[key])
	var base uint64
	if ok && cur.Version > base {
		base = cur.Version
	}
	if n.lastSvcVer[key] > base {
		base = n.lastSvcVer[key]
	}
	nextVer := base + 1

	// ttl di backup (come prima)
	if ttl <= 0 {
		ttl = 15
	}

	if !ok {
		// nuova entry
		cur = &ServiceInstance{
			Service:     service,
			InstanceID:  instanceID,
			NodeID:      n.cfg.SelfID,
			Addr:        addr,
			Version:     nextVer,
			TTLSeconds:  ttl,
			Up:          true,
			Tombstone:   false,
			LastUpdated: now,
		}
		n.services[key] = cur
		n.lastSvcVer[key] = nextVer
		log.Printf("[SVC] registrato %s id=%s addr=%s ttl=%ds ver=%d", service, instanceID, addr, ttl, nextVer)
		return
	}

	// refresh/ri-attivazione su entry esistente
	cur.Version = nextVer
	cur.Up = true
	cur.Tombstone = false
	cur.LastUpdated = now
	if addr != "" {
		cur.Addr = addr
	}
	cur.TTLSeconds = ttl // mantieni aggiornato anche il TTL se lo cambi
	n.lastSvcVer[key] = nextVer

	log.Printf("[SVC] refresh %s id=%s addr=%s ttl=%ds ver=%d", service, instanceID, cur.Addr, cur.TTLSeconds, nextVer)
}

func (n *Node) deregisterLocalService(service, instanceID string) {
	now := time.Now()

	n.mu.Lock()
	defer n.mu.Unlock()

	key := svcKey(service, instanceID)
	cur, ok := n.services[key]

	// calcola la prossima versione > max(cur.Version, lastSvcVer[key])
	var base uint64
	if ok && cur.Version > base {
		base = cur.Version
	}
	if n.lastSvcVer[key] > base {
		base = n.lastSvcVer[key]
	}
	nextVer := base + 1

	if !ok {
		// crea direttamente un tombstone per propagare la rimozione
		n.services[key] = &ServiceInstance{
			Service:     service,
			InstanceID:  instanceID,
			NodeID:      n.cfg.SelfID,
			Addr:        "", // opzionale; non serve per il tombstone
			Version:     nextVer,
			TTLSeconds:  n.cfg.ServiceTTL, // mantieni un TTL ragionevole
			Up:          false,
			Tombstone:   true,
			LastUpdated: now,
		}
	} else {
		cur.Version = nextVer
		cur.Up = false
		cur.Tombstone = true
		cur.LastUpdated = now
		// (lascia cur.Addr e cur.TTLSeconds come sono; non serve modificarli)
	}

	// aggiorna la waterline
	n.lastSvcVer[key] = nextVer

	log.Printf("[SVC] deregistrato %s id=%s ver=%d", service, instanceID, nextVer)
}

func (n *Node) pruneExpiredServices() {
	now := time.Now()
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, s := range n.services {
		ttl := time.Duration(s.TTLSeconds) * time.Second
		if ttl == 0 {
			ttl = 15 * time.Second
		}
		// se è passato oltre TTL*2 senza update, elimina
		if now.Sub(s.LastUpdated) > 2*ttl {
			delete(n.services, k)
			log.Printf("[SVC] scaduto %s/%s (last=%s)", s.Service, s.InstanceID, s.LastUpdated.Format(time.RFC3339))
		}
	}
}

func (n *Node) serviceRefreshLoop() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for range t.C {
		// refresha solo le istanze locali (NodeID == SelfID)
		n.mu.Lock()
		local := make([]*ServiceInstance, 0)
		for _, s := range n.services {
			if s.NodeID == n.cfg.SelfID && !s.Tombstone {
				local = append(local, s)
			}
		}
		n.mu.Unlock()

		for _, s := range local {
			n.registerLocalService(s.Service, s.InstanceID, s.Addr, s.TTLSeconds)
		}
	}
}

// ---------------- Discovery API + demo service ----------------

func (n *Node) startDiscoveryAPI(port int) {
	mux := http.NewServeMux()
	// --- POST /service/register ---
	// Aggiunge/refresh-a un'istanza locale e (se abilitato) scatena gossip immediato.
	mux.HandleFunc("/service/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		var req svcRegisterReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		req.Service = strings.TrimSpace(req.Service)
		req.InstanceID = strings.TrimSpace(req.InstanceID)
		req.Addr = strings.TrimSpace(req.Addr)
		if req.Service == "" || req.InstanceID == "" || req.Addr == "" {
			http.Error(w, "missing fields: service, instance_id, addr", http.StatusBadRequest)
			return
		}
		if req.TTL <= 0 {
			req.TTL = n.cfg.ServiceTTL
		}

		n.registerLocalService(req.Service, req.InstanceID, req.Addr, req.TTL)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})

	// --- POST /service/deregister ---
	// Marca un'istanza locale come tombstone e (se abilitato) scatena gossip immediato.
	mux.HandleFunc("/service/deregister", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		var req svcDeregisterReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		req.Service = strings.TrimSpace(req.Service)
		req.InstanceID = strings.TrimSpace(req.InstanceID)
		if req.Service == "" || req.InstanceID == "" {
			http.Error(w, "missing fields: service, instance_id", http.StatusBadRequest)
			return
		}

		n.deregisterLocalService(req.Service, req.InstanceID)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})

	// --- (opzionale) GET /services/local ---
	mux.HandleFunc("/services/local", func(w http.ResponseWriter, r *http.Request) {
		type out struct {
			Service    string `json:"service"`
			InstanceID string `json:"id"`
			Addr       string `json:"addr"`
			Version    uint64 `json:"ver"`
			TTL        int    `json:"ttl"`
			Up         bool   `json:"up"`
			AgeSec     int64  `json:"age_sec"`
		}
		now := time.Now()
		resp := []out{}
		n.mu.Lock()
		for _, s := range n.services {
			if s.NodeID != n.cfg.SelfID {
				continue
			}
			resp = append(resp, out{
				Service: s.Service, InstanceID: s.InstanceID, Addr: s.Addr,
				Version: s.Version, TTL: s.TTLSeconds, Up: s.Up,
				AgeSec: int64(now.Sub(s.LastUpdated).Seconds()),
			})
		}
		n.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	// --- GET /discover?service=calc (tuo handler esistente, invariato) ---
	mux.HandleFunc("/discover", func(w http.ResponseWriter, r *http.Request) {
		svc := r.URL.Query().Get("service")
		if strings.TrimSpace(svc) == "" {
			http.Error(w, "parametro 'service' mancante", http.StatusBadRequest)
			return
		}

		type Out struct {
			Service    string `json:"service"`
			InstanceID string `json:"id"`
			Addr       string `json:"addr"`
			Node       string `json:"node"`
			Version    uint64 `json:"ver"`
		}

		now := time.Now()
		out := []Out{}
		n.mu.Lock()
		for _, s := range n.services {
			if s.Service != svc || !s.Up || s.Tombstone {
				continue
			}
			// scarta scaduti
			if now.Sub(s.LastUpdated) > time.Duration(s.TTLSeconds)*time.Second {
				continue
			}
			// usa failure detector: il nodo ospite dev'essere ALIVE
			m, ok := n.members[s.NodeID]
			if !ok || m.State != StateAlive {
				continue
			}
			out = append(out, Out{
				Service: s.Service, InstanceID: s.InstanceID, Addr: s.Addr, Node: s.NodeID, Version: s.Version,
			})
		}
		n.mu.Unlock()

		sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}
	log.Printf("[HTTP] discovery API in ascolto su :%d", port)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("[HTTP] API errore: %v", err)
	}
}

func (n *Node) maybeStartDemoServices() {
	services := strings.Split(strings.TrimSpace(n.cfg.ServicesCSV), ",")
	for _, s := range services {
		s = strings.TrimSpace(s)
		switch s {
		case "calc":
			go startCalcService(n, n.cfg.CalcPort)
		case "":
			// no-op
		default:
			log.Printf("[WARN] servizio %q non riconosciuto (demo supporta solo 'calc')", s)
		}
	}
}

func startCalcService(n *Node, port int) {
	mux := http.NewServeMux()

	// GET /sum?a=1&b=2
	mux.HandleFunc("/sum", func(w http.ResponseWriter, r *http.Request) {
		a, b, err := readAB(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, "%d\n", a+b)
	})

	// GET /sub?a=5&b=3
	mux.HandleFunc("/sub", func(w http.ResponseWriter, r *http.Request) {
		a, b, err := readAB(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, "%d\n", a-b)
	})

	addr := fmt.Sprintf("%s:%d", strings.Split(n.cfg.SelfAddr, ":")[0], port)
	go func() {
		// registra l'istanza locale
		n.registerLocalService("calc", n.cfg.SelfID+"-calc", addr, n.cfg.ServiceTTL)
	}()

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}
	log.Printf("[HTTP] calc service su :%d (%s)", port, addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("[HTTP] calc errore: %v", err)
	}
}

func readAB(r *http.Request) (int, int, error) {
	aStr := r.URL.Query().Get("a")
	bStr := r.URL.Query().Get("b")
	if aStr == "" || bStr == "" {
		return 0, 0, fmt.Errorf("parametri 'a' e 'b' richiesti")
	}
	a, err := strconv.Atoi(aStr)
	if err != nil {
		return 0, 0, fmt.Errorf("a non è un intero")
	}
	b, err := strconv.Atoi(bStr)
	if err != nil {
		return 0, 0, fmt.Errorf("b non è un intero")
	}
	return a, b, nil
}

// ---------------- Peer selection & view ----------------

func (n *Node) calculateDynamicFanout() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Calcola il numero di nodi ALIVE
	aliveCount := 0
	for _, m := range n.members {
		if m.State == StateAlive {
			aliveCount++
		}
	}
	if aliveCount <= 1 {
		return 1 // se c'è un solo nodo, fanout è 1
	}
	// Calcolo del fanout con scala logaritmica
	fanout := int(math.Log2(float64(aliveCount))) + 1
	return fanout
}

func (n *Node) pickRandomTargets(k int) []*Member {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Crea una lista di nodi ALIVE e SUSPECT
	peers := make([]*Member, 0, len(n.members))
	for id, m := range n.members {
		if id == n.cfg.SelfID {
			continue
		}
		if m.State == StateAlive || m.State == StateSuspect {
			peers = append(peers, m)
		}
	}

	// Se non ci sono peer disponibili, ritorna nil
	if len(peers) == 0 {
		return nil
	}

	// Se il numero di peer è inferiore al fanoutK, ritorna tutti i peer
	if len(peers) <= k {
		return peers
	}

	// Se ci sono più peer di quelli che possiamo inviare, randomizza la selezione
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })

	// Ritorna solo i primi 'k' peer
	return peers[:k]
}

func (n *Node) printView() {
	n.mu.Lock()
	defer n.mu.Unlock()

	now := time.Now()

	// ================== DEDUPLICA PER ADDRESS ==================
	// Idee:
	// - Collassiamo le entry che condividono lo stesso Addr.
	// - Preferiamo la entry con ID "pulito" (senza ':'); a parità, quella con HB più alto,
	//   poi quella con LastSeen più recente.
	// - Manteniamo anche eventuali membri senza Addr (chiave fittizia "ID::<id>").
	canonical := make(map[string]*Member, len(n.members)) // chiave: Addr oppure "ID::<id>" se Addr vuoto

	for _, m := range n.members {
		addr := strings.TrimSpace(m.Addr)
		if addr == "" {
			key := "ID::" + m.ID
			if prev, ok := canonical[key]; ok {
				// scegli il migliore per ID "orfano"
				if m.Heartbeat > prev.Heartbeat || (m.Heartbeat == prev.Heartbeat && m.LastSeen.After(prev.LastSeen)) {
					canonical[key] = m
				}
			} else {
				canonical[key] = m
			}
			continue
		}

		if prev, ok := canonical[addr]; ok {
			prevIsEndpoint := strings.Contains(prev.ID, ":")
			curIsEndpoint := strings.Contains(m.ID, ":")

			better := false
			switch {
			// preferisci ID senza ":" rispetto a endpoint
			case prevIsEndpoint && !curIsEndpoint:
				better = true
			// a parità di "tipo" (entrambi puliti o entrambi endpoint), usa HB e recency
			case prevIsEndpoint == curIsEndpoint &&
				(m.Heartbeat > prev.Heartbeat || (m.Heartbeat == prev.Heartbeat && m.LastSeen.After(prev.LastSeen))):
				better = true
			}

			if better {
				canonical[addr] = m
			}
		} else {
			canonical[addr] = m
		}
	}

	// ================== COSTRUZIONE RIGHE MEMBRI ==================
	type row struct {
		ID          string
		St          MemberState
		HB          uint64
		Last        string
		Addr        string
		incarnation uint64
	}

	rows := make([]row, 0, len(canonical))
	for key, m := range canonical {
		// Calcolo "last"
		last := "-"
		if !m.LastSeen.IsZero() {
			last = time.Since(m.LastSeen).Truncate(time.Millisecond).String() + " ago"
		}

		// Ricava l'Addr anche per i "ID::" orfani
		addr := m.Addr
		if strings.HasPrefix(key, "ID::") && addr == "" {
			addr = m.Addr
		}

		rows = append(rows, row{
			ID:          m.ID,
			St:          m.State,
			HB:          m.Heartbeat,
			Last:        last,
			Addr:        addr,
			incarnation: m.Incarnation,
		})
	}

	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })

	// ================== INDICIZZA SERVIZI PER NODEID ==================
	svcsByNode := make(map[string][]*ServiceInstance, len(n.services))
	for _, s := range n.services {
		svcsByNode[s.NodeID] = append(svcsByNode[s.NodeID], s)
	}
	for _, list := range svcsByNode {
		sort.Slice(list, func(i, j int) bool {
			if list[i].Service != list[j].Service {
				return list[i].Service < list[j].Service
			}
			return list[i].InstanceID < list[j].InstanceID
		})
	}

	// ================== STAMPA ==================
	log.Printf("[VIEW] ----- %s -----", n.cfg.SelfID)
	for _, r := range rows {
		// riga membro (solo canonici, niente alias duplicati)
		log.Printf("  %-10s  %-7s  hb=%-6d  last=%-12s  %s with inc=%d", r.ID, r.St, r.HB, r.Last, r.Addr, r.incarnation)

		// servizi sotto il nodo (usiamo NodeID esatto)
		if list := svcsByNode[r.ID]; len(list) > 0 {
			for _, s := range list {
				ttl := s.TTLSeconds
				if ttl <= 0 {
					ttl = 15
				}
				age := now.Sub(s.LastUpdated).Truncate(time.Millisecond)
				expired := age > time.Duration(ttl)*time.Second

				status := "down"
				switch {
				case s.Tombstone:
					status = "tomb"
				case s.Up && !expired:
					status = "up"
				case s.Up && expired:
					status = "stale"
				default:
					status = "down"
				}

				log.Printf("      └─ svc=%-10s id=%-14s %-5s ver=%-3d ttl=%-2ds age=%-8s addr=%s",
					s.Service, s.InstanceID, status, s.Version, ttl, age, s.Addr)
			}
		}
	}

	// ================== RIEPILOGO SERVIZI (come prima) ==================
	type srow struct {
		K string
		S ServiceInstance
	}
	srows := make([]srow, 0, len(n.services))
	for k, v := range n.services {
		srows = append(srows, srow{K: k, S: *v})
	}
	sort.Slice(srows, func(i, j int) bool { return srows[i].K < srows[j].K })
	for _, sr := range srows {
		age := time.Since(sr.S.LastUpdated).Truncate(time.Millisecond)
		log.Printf("  [SVC] %-18s id=%-14s up=%-5v ver=%-3d ttl=%-2ds age=%-8s addr=%s node=%s",
			sr.S.Service, sr.S.InstanceID, sr.S.Up, sr.S.Version, sr.S.TTLSeconds, age, sr.S.Addr, sr.S.NodeID)
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	node, err := NewNode()
	if err != nil {
		log.Fatalf("Errore configurazione: %v", err)
	}
	node.run()
	select {}
}
