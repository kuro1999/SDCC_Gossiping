package registry

import (
	"encoding/json"
	"sync"

	"SDCC_gossiping/gossip"
)

type ServiceInfo struct {
	ID      string   `json:"id"`
	Address string   `json:"address"`
	Tags    []string `json:"tags"`
}

type eventType string

const (
	evtAdd    eventType = "add"
	evtRemove eventType = "remove"
)

type event struct {
	Type eventType   `json:"type"`
	Svc  ServiceInfo `json:"svc"`
}

type Registry struct {
	g        *gossip.Node
	services map[string]ServiceInfo
	mu       sync.RWMutex
}

func New(g *gossip.Node) *Registry {
	r := &Registry{
		g:        g,
		services: make(map[string]ServiceInfo),
	}
	go r.watchGossip()
	return r
}

func (r *Registry) Register(s ServiceInfo) {
	r.mu.Lock()
	r.services[s.ID] = s
	r.mu.Unlock()
	// propaga l'evento via gossip
	data, _ := json.Marshal(event{Type: evtAdd, Svc: s})
	r.g.SendToRandomPeer(data)
}

func (r *Registry) Unregister(id string) {
	r.mu.Lock()
	s, ok := r.services[id]
	if ok {
		delete(r.services, id)
	}
	r.mu.Unlock()
	if ok {
		data, _ := json.Marshal(event{Type: evtRemove, Svc: s})
		r.g.SendToRandomPeer(data)
	}
}

func (r *Registry) Lookup(tag string) []ServiceInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []ServiceInfo
	for _, svc := range r.services {
		if tag == "" {
			out = append(out, svc)
		} else {
			for _, t := range svc.Tags {
				if t == tag {
					out = append(out, svc)
					break
				}
			}
		}
	}
	return out
}

func (r *Registry) watchGossip() {
	for raw := range r.g.NextGossipMessage() {
		var ev event
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		r.mu.Lock()
		if ev.Type == evtAdd {
			r.services[ev.Svc.ID] = ev.Svc
		} else {
			delete(r.services, ev.Svc.ID)
		}
		r.mu.Unlock()
	}
}
