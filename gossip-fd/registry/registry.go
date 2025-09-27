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
	mu    sync.Mutex
	nodes map[string]*regEntry
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

		// Calcolo dinamico del numero di peer da restituire (log2(N) + 1)
		N := len(peers)
		k := int(math.Log2(float64(N))) + 1
		if k > N {
			k = N // Se k è maggiore del numero di peer, restituisci tutti
		}
		if k < 1 {
			k = 1 // Assicurati che k sia almeno 1
		}

		// Se ci sono peer disponibili, seleziona k peer casuali
		if N > 0 {
			rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
			if len(peers) >= k { // <-- guardia extra
				peers = peers[:k]
			}
			log.Printf("giving k=%d peers to %s:", k, in.ID) // <-- ordine fix
			for _, p := range peers {
				log.Printf("\t%s", p)
			}
		} else {
			log.Printf("no peers found, giving an empty list to node: %s", in.ID)
		}

		// Aggiungi il nuovo nodo al registro dopo avergli dato la lista dei peer
		r.mu.Lock()
		r.nodes[in.ID] = &regEntry{ID: in.ID, Addr: in.Addr, LastSeen: time.Now()}
		r.mu.Unlock()

		// Rispondi con la lista dei peer
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
