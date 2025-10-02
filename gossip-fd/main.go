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

type MemberState string

const (
	StateAlive   MemberState = "ALIVE"
	StateSuspect MemberState = "SUSPECT"
	StateDead    MemberState = "DEAD"
)

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

// chiave stabile per mappa locale
func svcKey(svc, id string) string { return svc + "##" + id }

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

	// per possibile eliminazione nodi
	votes             map[string]*voteBucket       // key = targetID
	DeathCertificates map[string]*certificateEntry // key = targetID
	recentlyEvicted   map[string]time.Time         // anti-rumore log/cleanup
}

func NewNode() (*Node, error) {
	cfg, err := utils.GetNodeConfig() //recuper della configurazione da env del nodo
	if err != nil {                   //senza una config corretta non si crea il nodo
		return nil, err
	}

	udpAddr := net.UDPAddr{IP: net.IPv4zero, Port: cfg.UDPPort} //Apre una socket UDP
	conn, err := net.ListenUDP("udp", &udpAddr)                 //si mette in ascolto
	if err != nil {
		return nil, fmt.Errorf("errore ListenUDP: %w", err)
	}

	n := &Node{ //creo la struttura nodo con tutti i campi necessari
		cfg:               cfg,
		conn:              conn,
		members:           make(map[string]*Member),               //lista di peers conosciuti
		services:          make(map[string]*ServiceInstance),      //servizi proprietari del nodo
		udpPort:           cfg.UDPPort,                            //la propria porta
		votes:             make(map[string]*voteBucket),           //voti proprietari del nodo
		DeathCertificates: make(map[string]*certificateEntry),     //certificati di morte proprietari
		recentlyEvicted:   make(map[string]time.Time),             //lista di nodi recentemente eliminati dalla membership
		httpClient:        &http.Client{Timeout: 1 * time.Second}, //http client per le chiamate al registry
		lastSvcVer:        make(map[string]uint64),                //versioni viste di un servizio
	}

	now := time.Now()
	n.members[cfg.SelfID] = &Member{ //vado a creare la entry di me stesso
		ID:          n.cfg.SelfID,   //ID del nodo
		Addr:        n.cfg.SelfAddr, //host:port
		Heartbeat:   0,              //heartbeat da diffondere per mantenimento alive monotono
		LastSeen:    now,            //necessario per rilevazione suspect/dead
		Incarnation: 1,              //necessario casistica recover di un nodo
		State:       StateAlive,     //stato attuale del nodo alive/suspect/dead
	}
	return n, nil
}

func (n *Node) run() {
	log.Printf("[BOOT] %s su %s | API :%d | services=%q | registry=%s | seeds=%s",
		n.cfg.SelfID, n.cfg.SelfAddr, n.cfg.APIPort, n.cfg.ServicesCSV, n.cfg.RegistryURL, os.Getenv("SEEDS"))
	// stringa host del nodo senza la porta
	selfHost, _, _ := strings.Cut(n.cfg.SelfAddr, ":")
	// recupero lista peer dal registry
	if peers, err := n.bootstrapFromRegistry(); err != nil {
		log.Printf("[ERROR] Errore nel bootstrap dal registry: %v", err)
	} else {
		for _, p := range peers {
			// p è un addr HTTP "host:porta" restituito dal registry
			host, _, _ := strings.Cut(p, ":")
			if host == "" || host == selfHost {
				// salta indirizzi vuoti o riferimenti a se stesso,
				continue
			}
			n.mu.Lock()
			if m, ok := n.members[host]; ok { //se gia presente nella membership
				if m.Addr != p {
					m.Addr = p //se il peer gia presente nella membership ma con indirizzo errato aggiorno
				}
				// se per qualche motivo era DEAD al boot, riportalo a SUSPECT
				if m.State == StateDead {
					m.State = StateSuspect
					m.LastSeen = time.Time{}
				}
			} else { //se assente nella membership
				// nuovo peer in vista iniziale
				n.members[host] = &Member{
					ID:        host, // ID canonico = solo host
					Addr:      p,    // indirizzo completo host:porta per UDP/HTTP
					Heartbeat: 0,
					LastSeen:  time.Time{},
					State:     StateSuspect, //non conoscendone lo stato lo imposto a suspect
				}
			}
			n.mu.Unlock()
		}
	}

	//avvio goroutine di membership/failure detector
	go n.receiveLoop()
	go n.heartbeatLoop()
	go n.gossipLoop()
	go n.suspicionLoop()

	//avvio API HTTP e servizi demo
	go n.startDiscoveryAPI(n.cfg.APIPort)
	n.maybeStartDemoServices()

	go n.serviceRefreshLoop()

	//Stampa periodicamente la view della membership
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
	host, _, _ := strings.Cut(n.cfg.SelfAddr, ":")
	joinAddr := fmt.Sprintf("%s:%d", host, n.cfg.APIPort)

	// Corpo della richiesta POST con id e indirizzo HTTP del nodo
	body := map[string]string{
		"id":   n.cfg.SelfID,
		"addr": joinAddr,
	}
	b, _ := json.Marshal(body)

	// Creazione della richiesta HTTP
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("errore nella creazione della richiesta: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Invio della richiesta al registry
	resp, err := n.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("errore nel contattare il registry: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("errore nel rispondere dal registry: status %d", resp.StatusCode)
	}

	// Decodifica della risposta JSON che contiene la lista dei peer
	var respBody struct {
		Peers []string `json:"peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		return nil, fmt.Errorf("errore nella decodifica della risposta del registry: %v", err)
	}

	// Ritorno la lista dei peer
	return respBody.Peers, nil
}

func (n *Node) receiveLoop() {
	buf := make([]byte, 64*1024)
	for {
		nRead, src, err := n.conn.ReadFromUDP(buf) //leggo dalla porta e nel caso scrivo su buf
		if err != nil {
			log.Printf("[RECV-ERR] %v", err)
			continue
		}
		data := make([]byte, nRead)
		copy(data, buf[:nRead])

		var gm GossipMessage
		if err := json.Unmarshal(data, &gm); err != nil { //eseguo unmarshal del messaggio
			log.Printf("[DECODE-ERR] from %s: %v", src.String(), err)
			continue
		}
		//Applica prima i certificati e i voti
		certApplied := n.ingestCertificates(gm.Certs)
		votesPromoted := n.ingestVotes(gm.Votes)
		if certApplied > 0 || votesPromoted > 0 {
			log.Printf("[DEATH-CERTIFICATE] cert_applied=%d, certs_promoted=%d (da %s)", certApplied, votesPromoted, gm.FromID)
		}
		//FILTRO anti-resurrezione: rimuovi dal digest membership le entry coperte da certificato attivo
		filtered := 0
		func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			now := time.Now()
			dst := gm.Membership[:0]
			for _, s := range gm.Membership {
				if e, ok := n.DeathCertificates[s.ID]; ok && now.Before(e.ExpiresAt) {
					// se l'evidenza nel digest NON è più nuova del certificato, scarta l'entry
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
			//log.Printf("[DEATH] filtrate %d entry membership coperte da certificati attivi (da %s)", filtered, gm.FromID)
		}
		// Merge membership e merge dei servizi
		mu := n.mergeMembership(&gm)
		su := n.mergeServices(&gm)

		//PUSH-PULL
		if !gm.IsReply { //se non è una reply allora rispondo
			ret := strings.TrimSpace(gm.ReturnAddr)
			if ret == "" {
				ret = src.String()
			}
			// evita di rispondere a te stesso
			if ret != "" && ret != n.cfg.SelfAddr {
				reply := n.buildReplyDiff(&gm)
				//se ho anche una sola differenza allora invio effettivamente la risposta altrimenti no
				if len(reply.Membership) > 0 || len(reply.Services) > 0 || len(reply.Certs) > 0 || len(reply.Votes) > 0 {
					if err := n.sendTo(ret, reply); err != nil {
						log.Printf("[REPLY-ERR] to %s: %v", ret, err)
					} else {
						//se non ho errori in fase di invio allora stampo il log
						log.Printf("[REPLY] to=%s members=%d services=%d death-certificate=%d votes=%d",
							ret, len(reply.Membership), len(reply.Services), len(reply.Certs), len(reply.Votes))
					}
				}

			}
		}
		if mu+su == 0 {
			log.Printf("[MERGE] da=%s, membri=%d, servizi=%d non effettuata", gm.FromID, mu, su)
		}
	}
}

func (n *Node) startDiscoveryAPI(port int) {
	mux := http.NewServeMux()
	// --- POST /service/register ---
	// Aggiunge/refresh-a un'istanza locale di un servizio
	mux.HandleFunc("/service/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		var req svcRegisterReq
		//acquisisco tutti i dettagli del servizio da registrare
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
		//registro effettivamente il servizio
		n.registerLocalService(req.Service, req.InstanceID, req.Addr, req.TTL)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})

	// --- POST /service/deregister ---
	// Elimina deregistrando un istanza di un servizio attivo sul nodo
	mux.HandleFunc("/service/deregister", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		var req svcDeregisterReq
		//acquisisco i dati del servizio da deregistrare
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
		//finalizzo l operazione
		n.deregisterLocalService(req.Service, req.InstanceID)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})

	// --- GET /services/local ---
	//rispondo con i servizi attivi localmente sul nodo
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

	// --- GET /discover?service=calc ---
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
			// scarta i servizi che hanno superato il ttl
			if now.Sub(s.LastUpdated) > time.Duration(s.TTLSeconds)*time.Second {
				continue
			}
			// il nodo che ospita il servizio deve essere Alive
			m, ok := n.members[s.NodeID]
			if !ok || m.State != StateAlive { //se non presente nella membership o se non Alive salta
				continue
			}
			out = append(out, Out{ //finalizzo la risposta
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
			go startCalcService(n, n.cfg.CalcPort) //fai partire il servizio
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
	// Calcolo del fanout con log2
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
		if m.State != StateDead {
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
	// Ritorna solo i primi 'k' peer
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	return peers[:k]
}

func (n *Node) printView() {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := time.Now()
	//canonical := make(map[string]*Member, len(n.members))

	type row struct {
		ID          string
		St          MemberState
		HB          uint64
		Last        string
		Addr        string
		incarnation uint64
	}

	rows := make([]row, 0, len(n.members))
	for _, m := range n.members {
		// Calcolo "last"
		last := "-"
		if !m.LastSeen.IsZero() {
			last = time.Since(m.LastSeen).Truncate(time.Millisecond).String() + " ago"
		}
		rows = append(rows, row{
			ID:          m.ID,
			St:          m.State,
			HB:          m.Heartbeat,
			Last:        last,
			Addr:        m.Addr,
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
		// riga membro
		log.Printf("  %-10s  %-7s  hb=%-6d  last=%-12s  %s with inc=%d", r.ID, r.St, r.HB, r.Last, r.Addr, r.incarnation)

		// servizi sotto il nodo
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
