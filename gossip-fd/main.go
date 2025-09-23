// main.go
package main

import (
	"encoding/json"
	"errors"
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
	"sync/atomic"
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
// ===== EXPERIMENT TOGGLES (cambia solo qui tra le run) =====
const (
	EnablePeriodicGossip = true  // RUN A: solo periodico -> true; RUN B: true
	EnableEventGossip    = false // RUN A: false (solo periodico); RUN B: true (periodico + eventi)
)

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

// metriche semplici
type Metrics struct {
	periodic uint64 // # invii periodici
	event    uint64 // # invii da evento
}

type MemberState string

const (
	StateAlive   MemberState = "ALIVE"
	StateSuspect MemberState = "SUSPECT"
	StateDead    MemberState = "DEAD"
)

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
	ID           string      `json:"id"`
	Addr         string      `json:"addr"`
	Heartbeat    uint64      `json:"hb"`
	State        MemberState `json:"state"`
	LastSeenUnix int64       `json:"ls"`
}

// ---------------- Service Registry ----------------

type ServiceAnnouncement struct {
	Service         string `json:"service"` // es: "calc"
	InstanceID      string `json:"id"`      // es: "node1-calc"
	NodeID          string `json:"node"`    // chi ospita
	Addr            string `json:"addr"`    // es: "node1:18080"
	Version         uint64 `json:"ver"`     // contatore monotono
	TTLSeconds      int    `json:"ttl"`     // es: 15
	Up              bool   `json:"up"`      // true se attivo
	LastUpdatedUnix int64  `json:"ts"`      // unix sec
	Tombstone       bool   `json:"tomb"`    // per rimozione (non usato molto qui)
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
	SentAtUnix int64                 `json:"sent_at"`
}

// ---------------- Config ----------------

type Config struct {
	SelfID            string
	SelfAddr          string
	GossipInterval    time.Duration
	HeartbeatInterval time.Duration
	SuspectTimeout    time.Duration
	DeadTimeout       time.Duration
	FanoutK           int
	MaxDigestPeers    int

	// Service discovery
	APIPort          int    // HTTP API per discovery
	ServicesCSV      string // es: "calc"
	CalcPort         int    // porta del servizio demo calc
	ServiceTTL       int    // TTL in secondi (annunci)
	MaxServiceDigest int    // quanti annunci piggyback per messaggio

}

type Node struct {
	cfg     Config
	conn    *net.UDPConn
	mu      sync.Mutex
	members map[string]*Member

	// service registry
	services   map[string]*ServiceInstance // key = svcKey
	selfHB     uint64
	udpPort    int
	lastSvcVer map[string]uint64 // ultima versione vista per chiave
	//struct eventi
	eventCh chan struct{}
	//metriche
	metricPeriodic uint64 // # batch gossip inviati per motivo "periodic"
	metricEvent    uint64 // # batch gossip inviati per motivo "event"
}

// ---------------- Utilità env ----------------

func mustEnv(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func parseDurationEnv(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func parseIntEnv(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// ---------------- Node init ----------------

func NewNode() (*Node, error) {
	id := mustEnv("SELF_ID", "")
	addr := mustEnv("SELF_ADDR", "") // "node1:9000"
	if id == "" || addr == "" {
		return nil, errors.New("SELF_ID e SELF_ADDR sono obbligatori")
	}
	_, portStr, _ := strings.Cut(addr, ":")
	if portStr == "" {
		return nil, fmt.Errorf("SELF_ADDR senza porta: %s", addr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("porta invalida in SELF_ADDR: %v", err)
	}

	cfg := Config{
		SelfID:            id,
		SelfAddr:          addr,
		GossipInterval:    parseDurationEnv("GOSSIP_INTERVAL", 700*time.Millisecond),
		HeartbeatInterval: parseDurationEnv("HEARTBEAT_INTERVAL", 500*time.Millisecond),
		SuspectTimeout:    parseDurationEnv("SUSPECT_TIMEOUT", 2500*time.Millisecond),
		DeadTimeout:       parseDurationEnv("DEAD_TIMEOUT", 6000*time.Millisecond),
		MaxDigestPeers:    parseIntEnv("MAX_DIGEST", 64),
		APIPort:           parseIntEnv("API_PORT", 8080),
		ServicesCSV:       mustEnv("SERVICES", ""),
		CalcPort:          parseIntEnv("CALC_PORT", 18080),
		ServiceTTL:        parseIntEnv("SERVICE_TTL", 15),
		MaxServiceDigest:  parseIntEnv("MAX_SERVICE_DIGEST", 64),
	}

	udpAddr := net.UDPAddr{IP: net.IPv4zero, Port: port}
	conn, err := net.ListenUDP("udp", &udpAddr)
	if err != nil {
		return nil, fmt.Errorf("errore ListenUDP: %w", err)
	}

	n := &Node{
		cfg:      cfg,
		conn:     conn,
		members:  make(map[string]*Member),
		services: make(map[string]*ServiceInstance),
		udpPort:  port,
	}
	if EnableEventGossip {
		n.eventCh = make(chan struct{}, 64) // BUFFERIZZATO: evita blocchi
	}
	n.lastSvcVer = make(map[string]uint64)

	now := time.Now()
	n.members[id] = &Member{
		ID:        id,
		Addr:      addr,
		Heartbeat: 0,
		LastSeen:  now,
		State:     StateAlive,
	}

	// seed peers
	seeds := strings.Split(strings.TrimSpace(os.Getenv("SEEDS")), ",")
	for _, s := range seeds {
		s = strings.TrimSpace(s)
		if s == "" || s == addr {
			continue
		}
		host, portStr, _ := strings.Cut(s, ":")
		if host == "" || portStr == "" {
			continue
		}
		peerID := host
		n.members[peerID] = &Member{
			ID:        peerID,
			Addr:      s,
			Heartbeat: 0,
			LastSeen:  time.Time{},
			State:     StateSuspect,
		}
	}
	return n, nil
}

func (n *Node) run() {
	log.Printf("[BOOT] %s su %s | API :%d | services=%q | seeds=%s",
		n.cfg.SelfID, n.cfg.SelfAddr, n.cfg.APIPort, n.cfg.ServicesCSV, os.Getenv("SEEDS"))

	// membership
	go n.receiveLoop()
	go n.heartbeatLoop()
	go n.gossipLoop()
	go n.suspicionLoop()

	// service API + eventuale demo service
	go n.startDiscoveryAPI(n.cfg.APIPort)
	n.maybeStartDemoServices()

	// refresh/TTL per servizi locali
	go n.serviceRefreshLoop()
	// --- avvio metriche ---
	go n.metricsLoop()
	// stampa periodica view
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		n.printView()
	}
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

		// raccolgo le transizioni e quante volte devo triggerare eventi,
		// ma SENZA fare I/O o send sul canale sotto lock
		type tr struct {
			old      MemberState
			new      MemberState
			id       string
			lastSeen time.Time
			delta    time.Duration
		}
		var transitions []tr
		deadTriggers := 0

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
				if newSt == StateDead {
					deadTriggers++
				}
			}
		}
		n.mu.Unlock()
		// --- fine sezione critica ---

		// logging delle transizioni (fuori dal lock)
		for _, x := range transitions {
			log.Printf("[STATE] %s -> %s (peer=%s, lastSeen=%s, Δ=%s)",
				x.old, x.new, x.id, x.lastSeen.Format(time.RFC3339), x.delta.Truncate(time.Millisecond))
			if x.new == StateDead {
				log.Printf("[EVENTO] Nodo %s è diventato DEAD. Scatenato gossip immediato.", x.id)
			}
		}

		// trigger degli eventi (una volta per ogni passaggio a DEAD, come prima)
		for i := 0; i < deadTriggers; i++ {
			n.onEventTrigger()
		}

		// scadenze servizi (fa lock all’interno, ma qui siamo fuori dalla nostra sezione critica)
		n.pruneExpiredServices()
	}
}

// Canale per eventi che triggerano un gossip immediato
func (n *Node) onEventTrigger() {
	if !EnableEventGossip || n.eventCh == nil {
		return
	}
	select {
	case n.eventCh <- struct{}{}:
	default: // coalesce se pieno, niente blocco
	}
}

// ---------------- Gossip send/recv ----------------
func (n *Node) metricsLoop() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()

	for range t.C {
		p := atomic.LoadUint64(&n.metricPeriodic)
		e := atomic.LoadUint64(&n.metricEvent)
		log.Printf("[METRICS] gossip_batches periodic=%d event=%d", p, e)
	}
}

func (n *Node) gossipLoop() {
	t := time.NewTicker(n.cfg.GossipInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			n.sendGossip("periodic")

		case <-n.eventCh:
			// Coalesce di eventuali ulteriori trigger arrivati quasi insieme
			drained := 0
			for drained < 32 { // limite di sicurezza per non loopare infinito
				select {
				case <-n.eventCh:
					drained++
				default:
					drained = 32 // esci
				}
			}
			n.sendGossip("event")
		}
	}
}

// Funzione che invia gossip periodico
func (n *Node) sendGossip(origin string) {
	// normalizza il tag di origine per i log
	if origin == "" {
		origin = "unknown"
	}

	switch origin {
	case "periodic":
		atomic.AddUint64(&n.metricPeriodic, 1)
	case "event":
		atomic.AddUint64(&n.metricEvent, 1)
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

// Funzione che invia il gossip immediato quando si verifica un evento critico
func (n *Node) sendImmediateGossip() {
	log.Println("[GOSSIP IMMEDIATO] Gossip inviato immediatamente a causa di un evento critico.")

	// Calcola il fanout dinamico
	fanoutK := n.calculateDynamicFanout()

	// Seleziona i nodi target in base al fanout dinamico
	targets := n.pickRandomTargets(fanoutK)
	if len(targets) == 0 {
		log.Println("[GOSSIP IMMEDIATO] Nessun nodo disponibile per inviare gossip.")
		return
	}

	// Costruisci il messaggio da inviare
	msg := n.buildMessage(false /* isReply */)

	// Invia i messaggi ai nodi target in modo concorrente
	for _, peer := range targets {
		go func(p *Member) {
			if err := n.sendTo(p.Addr, msg); err != nil {
				log.Printf("[SEND-ERR] to %s: %v", p.Addr, err)
			}
		}(peer)
	}
}

func (n *Node) buildMessage(isReply bool) GossipMessage {
	n.mu.Lock()
	defer n.mu.Unlock()

	// membership digest
	sums := make([]MemberSummary, 0, len(n.members))
	for _, m := range n.members {
		sums = append(sums, MemberSummary{
			ID:           m.ID,
			Addr:         m.Addr,
			Heartbeat:    m.Heartbeat,
			State:        m.State,
			LastSeenUnix: m.LastSeen.Unix(),
		})
	}
	sort.Slice(sums, func(i, j int) bool {
		ai := sums[i].State == StateAlive
		aj := sums[j].State == StateAlive
		if ai != aj {
			return ai && !aj
		}
		return sums[i].LastSeenUnix > sums[j].LastSeenUnix
	})
	if len(sums) > n.cfg.MaxDigestPeers {
		sums = sums[:n.cfg.MaxDigestPeers]
	}

	// service digest: prendi le istanze più "recenti"
	sann := make([]ServiceAnnouncement, 0, len(n.services))
	for _, s := range n.services {
		sann = append(sann, ServiceAnnouncement{
			Service:         s.Service,
			InstanceID:      s.InstanceID,
			NodeID:          s.NodeID,
			Addr:            s.Addr,
			Version:         s.Version,
			TTLSeconds:      s.TTLSeconds,
			Up:              s.Up,
			LastUpdatedUnix: s.LastUpdated.Unix(),
			Tombstone:       s.Tombstone,
		})
	}
	sort.Slice(sann, func(i, j int) bool { return sann[i].LastUpdatedUnix > sann[j].LastUpdatedUnix })
	if len(sann) > n.cfg.MaxServiceDigest {
		sann = sann[:n.cfg.MaxServiceDigest]
	}

	return GossipMessage{
		FromID:     n.cfg.SelfID,
		ReturnAddr: n.cfg.SelfAddr,
		IsReply:    isReply,
		Membership: sums,
		Services:   sann,
		SentAtUnix: time.Now().Unix(),
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

		mu := n.mergeMembership(&gm)
		su := n.mergeServices(&gm)

		if !gm.IsReply {
			reply := n.buildMessage(true)
			ret := gm.ReturnAddr
			if ret == "" {
				ret = src.String()
			}
			if err := n.sendTo(ret, reply); err != nil {
				log.Printf("[REPLY-ERR] to %s: %v", ret, err)
			}
		}

		if mu+su > 0 {
			//log.Printf("[MERGE] da=%s, membri=%d, servizi=%d", gm.FromID, mu, su)
		}
	}
}

// ---------------- Merge logic ----------------

func (n *Node) mergeMembership(gm *GossipMessage) int {
	now := time.Now()
	updated := 0
	//log.Println("[SYNC] Acquisizione del mutex per merge dei membri")
	n.mu.Lock()
	defer func() {
		n.mu.Unlock()
		//log.Println("[SYNC] Mutex rilasciato dopo merge dei membri")
	}()

	if gm.FromID != "" {
		if _, ok := n.members[gm.FromID]; !ok {
			n.members[gm.FromID] = &Member{
				ID:       gm.FromID,
				Addr:     gm.ReturnAddr,
				State:    StateAlive,
				LastSeen: now,
			}
			updated++
			//log.Printf("[MERGE] Nuovo membro aggiunto: %s", gm.FromID)
		}
	}

	for _, s := range gm.Membership {
		m, ok := n.members[s.ID]
		if !ok {
			m = &Member{ID: s.ID, Addr: s.Addr}
			n.members[s.ID] = m
			updated++
			log.Printf("[MERGE] Nuovo membro aggiunto: %s", s.ID)
		}
		if s.Heartbeat > m.Heartbeat {
			old := m.State
			m.State = StateAlive
			m.Heartbeat = s.Heartbeat
			m.LastSeen = now
			if s.Addr != "" {
				m.Addr = s.Addr
			}
			if old != StateAlive {
				log.Printf("[RECOVER] %s: %s -> ALIVE (hb=%d)", s.ID, old, s.Heartbeat)
			}
			updated++
		} else if s.LastSeenUnix > 0 {
			ls := time.Unix(s.LastSeenUnix, 0)
			if ls.After(m.LastSeen) {
				m.LastSeen = ls
			}
		}
	}
	return updated
}

func (n *Node) mergeServices(gm *GossipMessage) int {
	now := time.Now()
	updated := 0

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, ann := range gm.Services {
		key := svcKey(ann.Service, ann.InstanceID)
		cur, ok := n.services[key]

		// monotonicità globale: scarta se la versione è <= dell'ultima vista
		last := n.lastSvcVer[key]
		if ann.Version <= last {
			continue
		}
		// ridondante, ma sicuro: se esiste cur e la versione non supera cur.Version, ignora
		if ok && ann.Version <= cur.Version {
			continue
		}

		// accetta annuncio
		inst := &ServiceInstance{
			Service:     ann.Service,
			InstanceID:  ann.InstanceID,
			NodeID:      ann.NodeID,
			Addr:        ann.Addr,
			Version:     ann.Version,
			TTLSeconds:  ann.TTLSeconds,
			Up:          ann.Up && !ann.Tombstone,
			Tombstone:   ann.Tombstone,
			LastUpdated: time.Unix(ann.LastUpdatedUnix, 0),
		}
		// normalizza LastUpdated
		if inst.LastUpdated.IsZero() || inst.LastUpdated.After(now) {
			inst.LastUpdated = now
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

		if EnableEventGossip {
			n.onEventTrigger() // diffondi subito la novità
		}

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

		if EnableEventGossip {
			n.onEventTrigger()
		}

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

	type row struct {
		ID   string
		St   MemberState
		HB   uint64
		Last string
		Addr string
	}

	// --- costruisci righe membri ordinate ---
	rows := make([]row, 0, len(n.members))
	for _, m := range n.members {
		last := "-"
		if !m.LastSeen.IsZero() {
			last = time.Since(m.LastSeen).Truncate(time.Millisecond).String() + " ago"
		}
		rows = append(rows, row{ID: m.ID, St: m.State, HB: m.Heartbeat, Last: last, Addr: m.Addr})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })

	// --- indicizza i servizi per NodeID per stampa per-nodo ---
	// nota: teniamo i puntatori per non copiare strutture
	svcsByNode := make(map[string][]*ServiceInstance, len(n.services))
	for _, s := range n.services {
		svcsByNode[s.NodeID] = append(svcsByNode[s.NodeID], s)
	}
	// ordina i servizi per (Service, InstanceID) per ogni nodo
	for _, list := range svcsByNode {
		sort.Slice(list, func(i, j int) bool {
			if list[i].Service != list[j].Service {
				return list[i].Service < list[j].Service
			}
			return list[i].InstanceID < list[j].InstanceID
		})
	}

	log.Printf("[VIEW] ----- %s -----", n.cfg.SelfID)
	for _, r := range rows {
		// stampa riga membro
		log.Printf("  %-10s  %-7s  hb=%-6d  last=%-12s  %s", r.ID, r.St, r.HB, r.Last, r.Addr)

		// stampa, sotto il nodo, i servizi offerti da quel nodo (se presenti)
		if list := svcsByNode[r.ID]; len(list) > 0 {
			for _, s := range list {
				// --- calcolo stato e metadati per stampa leggibile ---
				ttl := s.TTLSeconds
				if ttl <= 0 {
					ttl = 15 // fallback coerente col resto del codice
				}
				age := now.Sub(s.LastUpdated).Truncate(time.Millisecond)
				expired := age > time.Duration(ttl)*time.Second

				status := "down"
				switch {
				case s.Tombstone:
					status = "tomb" // deregistrato/mark rimozione
				case s.Up && !expired:
					status = "up" // attivo e fresco
				case s.Up && expired:
					status = "stale" // attivo ma oltre TTL (potenziale scadenza)
				default:
					status = "down"
				}

				// nota: indentazione per raggruppare visivamente i servizi sotto il nodo
				log.Printf("      └─ svc=%-10s id=%-14s %-5s ver=%-3d ttl=%-2ds age=%-8s addr=%s",
					s.Service, s.InstanceID, status, s.Version, ttl, age, s.Addr)
			}
		}
	}

	// --- stampa compatta dei servizi noti (lasciata come in origine) ---
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

// opzionale: scateno subito un giro di gossip per diffondere il cambio servizio
func (n *Node) triggerGossipForServiceChange() {
	if EnableEventGossip {
		n.onEventTrigger()
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
