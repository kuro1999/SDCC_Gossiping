package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Registry struct {
	mu            sync.Mutex
	nodes         map[string]*regEntry
	votes         map[string]map[string]time.Time // suspectID -> (voterID -> ts)
	ttl           time.Duration                   // opzionale per LastSeen (già presente)
	voteTTL       time.Duration                   // durata massima di validità di un voto
	notifyTimeout time.Duration                   // timeout per notifiche push ai nodi
}

type regEntry struct {
	ID       string
	Addr     string
	LastSeen time.Time
}

type regJoinResp struct {
	Peers []string `json:"peers"`
}

type config struct {
	port int
}

// majorityLocked calcola la maggioranza sul numero corrente di nodi.
// Deve essere chiamata a lock già acquisito.
func (r *Registry) majorityLocked() (maj, total int) {
	total = len(r.nodes)
	maj = total/2 + 1
	return
}

// notifyEviction invia la notifica di rimozione ai nodi target (best-effort, non bloccante).
func (r *Registry) notifyEviction(suspectID string, targets []string) {
	// corpo in JSON: { "id": "<suspectID>" }
	body, _ := json.Marshal(struct {
		ID string `json:"id"`
	}{ID: suspectID})

	for _, addr := range targets {
		// invio in goroutine, con timeout per non bloccare il registry
		go func(addr string) {
			ctx, cancel := context.WithTimeout(context.Background(), r.notifyTimeout)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost,
				"http://"+addr+"/api/membership/evict", bytes.NewReader(body))
			if err != nil {
				log.Printf("[REGISTRY] notify build error to %s: %v", addr, err)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("[REGISTRY] notify error to %s: %v", addr, err)
				return
			}
			io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}(addr)
	}
}

func (r *Registry) startRegistry(port int) {
	r.nodes = make(map[string]*regEntry)
	r.votes = make(map[string]map[string]time.Time)

	// valori sensati di default; se vuoi, leggili da env
	r.ttl = 90 * time.Second
	r.voteTTL = 2 * r.ttl
	r.notifyTimeout = 800 * time.Millisecond

	mux := http.NewServeMux()

	// POST /api/vote-dead
	// Body: { "voter_id": "...", "suspect_id": "..." }
	mux.HandleFunc("/api/vote-dead", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		var in struct {
			VoterID   string `json:"voter_id"`
			SuspectID string `json:"suspect_id"`
		}
		if err := json.NewDecoder(req.Body).Decode(&in); err != nil || in.VoterID == "" || in.SuspectID == "" {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}

		now := time.Now()

		r.mu.Lock()
		// valida che il voter esista nel registry (evita voti di sconosciuti)
		if _, ok := r.nodes[in.VoterID]; !ok {
			r.mu.Unlock()
			http.Error(w, "unknown voter", http.StatusBadRequest)
			return
		}

		// crea bucket di voti per questo suspect se non esiste
		if _, ok := r.votes[in.SuspectID]; !ok {
			r.votes[in.SuspectID] = make(map[string]time.Time)
		}

		// scarta voti scaduti (vecchi) per questo suspect
		cutoff := now.Add(-r.voteTTL)
		for v, ts := range r.votes[in.SuspectID] {
			if ts.Before(cutoff) {
				delete(r.votes[in.SuspectID], v)
			}
		}

		// registra il voto in modo idempotente (1 voto per voter->suspect)
		if _, dup := r.votes[in.SuspectID][in.VoterID]; !dup {
			r.votes[in.SuspectID][in.VoterID] = now
			log.Printf("[REGISTRY] vote: %s -> suspect=%s", in.VoterID, in.SuspectID)
		}

		// calcola tally e maggioranza
		maj, total := r.majorityLocked()
		tally := len(r.votes[in.SuspectID])

		// se si è raggiunta la maggioranza, rimuovi il suspect e notifica i nodi
		evicted := false
		var targets []string
		if tally >= maj {
			if _, present := r.nodes[in.SuspectID]; present {
				// prepara target per la notifica (tutti meno il suspect)
				for id, e := range r.nodes {
					if id != in.SuspectID {
						targets = append(targets, e.Addr)
					}
				}
				delete(r.nodes, in.SuspectID)
				delete(r.votes, in.SuspectID)
				evicted = true
				log.Printf("[REGISTRY] EVICT %s by majority", in.SuspectID)
			} else {
				// se non è più presente, pulisci i voti “orfani”
				delete(r.votes, in.SuspectID)
			}
		}

		// risposta JSON
		resp := struct {
			Tally    int  `json:"tally"`
			Majority int  `json:"majority"`
			Total    int  `json:"total"`
			Evicted  bool `json:"evicted"`
		}{
			Tally:    tally,
			Majority: maj,
			Total:    total,
			Evicted:  evicted,
		}

		r.mu.Unlock() // sblocca prima delle notifiche

		// notifica best-effort fuori lock
		if evicted && len(targets) > 0 {
			go r.notifyEviction(in.SuspectID, targets)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	// POST /api/join
	mux.HandleFunc("/api/join", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		var in struct{ ID, Addr string }
		if err := json.NewDecoder(req.Body).Decode(&in); err != nil || in.ID == "" || in.Addr == "" {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}

		// Provide the peer list in the network
		peers := []string{}
		r.mu.Lock()
		for id, entry := range r.nodes {
			if id != in.ID {
				peers = append(peers, entry.Addr)
			}
		}
		r.mu.Unlock()
		if len(peers) == 0 {
			log.Printf("no peers found, giving an empty list to node: %s", in.ID)
		} else {
			log.Printf("giving existing peers to %s: ", in.ID)
			for _, id := range peers {
				log.Printf("\t%s", id)
			}
		}

		r.mu.Lock()
		// Add the new node to the registry after given the list of peers in the network
		r.nodes[in.ID] = &regEntry{ID: in.ID, Addr: in.Addr, LastSeen: time.Now()}
		r.mu.Unlock()

		// Respond with the list of peers
		resp := regJoinResp{Peers: peers}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	http.Handle("/", mux)
	log.Printf("[REGISTRY] listening on port %d", port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		log.Fatalf("[REGISTRY] Error: %v", err)
	}
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

func main() {
	cfg := config{
		port: parseIntEnv("PORT", 8089),
	}

	reg := &Registry{}
	reg.startRegistry(cfg.port)

	select {}
}
