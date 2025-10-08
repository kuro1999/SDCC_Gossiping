package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Registry struct {
	mu         sync.Mutex
	nodes      map[string]*regEntry
	port       int
	reg_fanout int
}

type regEntry struct {
	ID       string
	Addr     string
	LastSeen time.Time
}

type regJoinResp struct {
	Peers []string `json:"peers"`
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

		// fornisce una sotto lista causale di peer nella rete
		peers := []string{}
		r.mu.Lock()
		for id, entry := range r.nodes {
			if id != in.ID {
				peers = append(peers, entry.Addr)
			}
		}
		r.mu.Unlock()

		// Calcolo dinamico del numero di peer da restituire (log2(N) + 1)
		N := len(peers)
		var k int
		if N == 0 {
			log.Printf("no peers found, giving an empty list to node: %s", in.ID)
		} else {
			if r.reg_fanout == 0 {
				k = int(math.Log2(float64(N))) + 1
			} else {
				k = r.reg_fanout
			}
			if k > N {
				k = N
			}
			rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
			if len(peers) >= k {
				peers = peers[:k]
			}
			log.Printf("giving k=%d peers to %s:", k, in.ID)
			for _, p := range peers {
				log.Printf("\t%s", p)
			}
		}

		// Aggiunge il nuovo nodo al registro dopo avergli dato la lista dei peer
		r.mu.Lock()
		r.nodes[in.ID] = &regEntry{ID: in.ID, Addr: in.Addr, LastSeen: time.Now()}
		r.mu.Unlock()

		// Risponde con la lista dei peer
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
	reg := &Registry{
		nodes:      make(map[string]*regEntry),
		port:       parseIntEnv("REGISTRY_PORT", 8089),
		reg_fanout: parseIntEnv("REG_FANOUT", 0), //se 0 allora dinamico else preso da config
	}
	reg.startRegistry(reg.port)

	select {}
}
