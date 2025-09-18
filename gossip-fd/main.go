// main.go
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	services map[string]*ServiceInstance // key = svcKey
	selfHB   uint64
	udpPort  int
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
		FanoutK:           parseIntEnv("FANOUT_K", 2),
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
			if d > n.cfg.DeadTimeout {
				m.State = StateDead
			} else if d > n.cfg.SuspectTimeout {
				m.State = StateSuspect
			} else {
				m.State = StateAlive
			}
			if m.State != old {
				log.Printf("[STATE] %s -> %s (peer=%s, lastSeen=%s, Δ=%s)",
					old, m.State, id, m.LastSeen.Format(time.RFC3339), d.Truncate(time.Millisecond))
			}
		}
		n.mu.Unlock()
		// scadenze di servizi remoti (TTL)
		n.pruneExpiredServices()
	}
}

// ---------------- Gossip send/recv ----------------

func (n *Node) gossipLoop() {
	t := time.NewTicker(n.cfg.GossipInterval)
	defer t.Stop()
	for range t.C {
		targets := n.pickRandomTargets(n.cfg.FanoutK)
		if len(targets) == 0 {
			continue
		}
		msg := n.buildMessage(false /* isReply */)
		for _, peer := range targets {
			go func(p *Member) {
				if err := n.sendTo(p.Addr, msg); err != nil {
					log.Printf("[SEND-ERR] to %s: %v", p.Addr, err)
				}
			}(peer)
		}
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
			log.Printf("[MERGE] da=%s, membri=%d, servizi=%d", gm.FromID, mu, su)
		}
	}
}

// ---------------- Merge logic ----------------

func (n *Node) mergeMembership(gm *GossipMessage) int {
	now := time.Now()
	updated := 0

	n.mu.Lock()
	defer n.mu.Unlock()

	if gm.FromID != "" {
		if _, ok := n.members[gm.FromID]; !ok {
			n.members[gm.FromID] = &Member{
				ID:       gm.FromID,
				Addr:     gm.ReturnAddr,
				State:    StateAlive,
				LastSeen: now,
			}
			updated++
		}
	}

	for _, s := range gm.Membership {
		m, ok := n.members[s.ID]
		if !ok {
			m = &Member{ID: s.ID, Addr: s.Addr}
			n.members[s.ID] = m
			updated++
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

		// ignora annunci vecchi
		if ok && ann.Version <= cur.Version {
			continue
		}
		// applica aggiornamento
		n.services[key] = &ServiceInstance{
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
		if n.services[key].LastUpdated.IsZero() || n.services[key].LastUpdated.After(now) {
			n.services[key].LastUpdated = now
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
	if !ok {
		cur = &ServiceInstance{
			Service:     service,
			InstanceID:  instanceID,
			NodeID:      n.cfg.SelfID,
			Addr:        addr,
			Version:     1,
			TTLSeconds:  ttl,
			Up:          true,
			LastUpdated: now,
		}
		n.services[key] = cur
		log.Printf("[SVC] registrato %s id=%s addr=%s ttl=%ds", service, instanceID, addr, ttl)
		return
	}
	// refresh: bump versione e ts
	cur.Version++
	cur.Up = true
	cur.LastUpdated = now
	if addr != "" {
		cur.Addr = addr
	}
}

func (n *Node) deregisterLocalService(service, instanceID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	key := svcKey(service, instanceID)
	if cur, ok := n.services[key]; ok {
		cur.Version++
		cur.Up = false
		cur.Tombstone = true
		cur.LastUpdated = time.Now()
		log.Printf("[SVC] deregistrato %s id=%s", service, instanceID)
	}
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

	// GET /discover?service=calc
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

func (n *Node) pickRandomTargets(k int) []*Member {
	n.mu.Lock()
	defer n.mu.Unlock()

	peers := make([]*Member, 0, len(n.members))
	for id, m := range n.members {
		if id == n.cfg.SelfID {
			continue
		}
		if m.State == StateAlive || m.State == StateSuspect {
			peers = append(peers, m)
		}
	}
	if len(peers) == 0 {
		return nil
	}
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	if len(peers) > k {
		peers = peers[:k]
	}
	return peers
}

func (n *Node) printView() {
	n.mu.Lock()
	defer n.mu.Unlock()

	type row struct {
		ID   string
		St   MemberState
		HB   uint64
		Last string
		Addr string
	}
	rows := make([]row, 0, len(n.members))
	for _, m := range n.members {
		last := "-"
		if !m.LastSeen.IsZero() {
			last = time.Since(m.LastSeen).Truncate(time.Millisecond).String() + " ago"
		}
		rows = append(rows, row{ID: m.ID, St: m.State, HB: m.Heartbeat, Last: last, Addr: m.Addr})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })

	log.Printf("[VIEW] ----- %s -----", n.cfg.SelfID)
	for _, r := range rows {
		log.Printf("  %-10s  %-7s  hb=%-6d  last=%-12s  %s", r.ID, r.St, r.HB, r.Last, r.Addr)
	}

	// stampa compatta dei servizi noti
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
