package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Registry struct {
	mu    sync.Mutex
	nodes map[string]*regEntry
	ttl   time.Duration
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

func (r *Registry) startRegistry(port int) {
	r.nodes = make(map[string]*regEntry)

	mux := http.NewServeMux()

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
