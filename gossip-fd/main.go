// main.go
package main

import (
	"SDCC_Gossiping/utils"
	"bytes"
	"encoding/json"
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
	ID          string      `json:"id"`
	Addr        string      `json:"addr"`
	Heartbeat   uint64      `json:"hb"`
	Incarnation uint64      `json:"inc"`
	State       MemberState `json:"state"`
}

// ---------------- Service Registry ----------------

// chiave stabile per mappa locale
func svcKey(svc, id string) string { return svc + "##" + id }

// ---------------- Gossip Message ----------------

// ---------------- Config ----------------

type Node struct {
	cfg     utils.NodeConfig
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

// ---------------- Node init ----------------

func NewNode() (*Node, error) {
	cfg, err := utils.GetNodeConfig()
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

	// 1) Bootstrap dei peer dal registry
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

	// ================== RIEPILOGO SERVIZI ==================
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
