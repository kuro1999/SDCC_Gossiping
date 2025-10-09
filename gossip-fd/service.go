package main

import (
	"log"
	"time"
)

type svcRegisterReq struct {
	Service    string `json:"service"`
	InstanceID string `json:"instance_id"`
	Addr       string `json:"addr"`
}

type svcDeregisterReq struct {
	Service    string `json:"service"`
	InstanceID string `json:"instance_id"`
}

type ServiceAnnouncement struct {
	Service    string        `json:"service"` // "calc"
	InstanceID string        `json:"id"`      // "node1-calc"
	NodeID     string        `json:"node"`    // chi ospita il servizio
	Addr       string        `json:"addr"`    // "node1:18080"
	Version    uint64        `json:"ver"`     // contatore monotono
	TTLSeconds time.Duration `json:"ttl"`     // configurabile da env
	Up         bool          `json:"up"`      // true se attivo
	Tombstone  bool          `json:"tomb"`    // segnala sevizio inattivo
}

type ServiceInstance struct {
	Service     string
	InstanceID  string
	NodeID      string
	Addr        string
	Version     uint64
	TTLSeconds  time.Duration
	Up          bool
	LastUpdated time.Time
	ExpiresAt   time.Time
	Tombstone   bool
}

func (n *Node) registerLocalService(service, instanceID, addr string, ttl time.Duration) {
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
	if v := n.lastSvcVer[key]; v > base {
		base = v
	}
	nextVer := base + 1

	if !ok {
		// nuova entry locale
		cur = &ServiceInstance{
			Service:     service,
			InstanceID:  instanceID,
			NodeID:      n.cfg.SelfID,
			Addr:        addr,
			Version:     nextVer,
			Up:          true,
			Tombstone:   false,
			LastUpdated: now,          // solo per metriche/log
			ExpiresAt:   now.Add(ttl), // vero deadline locale
			TTLSeconds:  ttl,
		}
		n.services[key] = cur
		n.lastSvcVer[key] = nextVer

		log.Printf("[SVC] registered %s id=%s addr=%s ttl=%v ver=%d",
			service, instanceID, addr, ttl, nextVer)
		return
	}

	// refresh/ri-attivazione su entry esistente
	cur.Version = nextVer
	cur.Up = true
	cur.Tombstone = false
	cur.LastUpdated = now
	cur.TTLSeconds = ttl
	cur.ExpiresAt = now.Add(ttl) // estensione della deadline locale

	if addr != "" && addr != cur.Addr {
		cur.Addr = addr
	}

	n.lastSvcVer[key] = nextVer

	log.Printf("[SVC] refresh %s id=%s addr=%s ttl=%v ver=%d",
		service, instanceID, cur.Addr, cur.TTLSeconds, nextVer)
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
			Addr:        "",
			Version:     nextVer,
			TTLSeconds:  n.cfg.ServiceTTL,
			Up:          false,
			Tombstone:   true,
			LastUpdated: now,
			ExpiresAt:   now,
		}
	} else {
		cur.Version = nextVer
		cur.Up = false
		cur.Tombstone = true
		cur.LastUpdated = now
		cur.ExpiresAt = now
	}
	n.lastSvcVer[key] = nextVer
	log.Printf("[SVC] deregistered %s id=%s ver=%d", service, instanceID, nextVer)
}

func (n *Node) pruneExpiredServices() {
	now := time.Now()
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, s := range n.services {
		// normalizza TTL se mancante nel config
		//Timeout scaduto e tombstone = false, marco il servizio come DOWN
		if s.Up && !s.Tombstone && now.After(s.ExpiresAt) {
			// solo il proprietario può aumentarne la versione
			if s.NodeID == n.cfg.SelfID {
				base := s.Version
				if v := n.lastSvcVer[k]; v > base {
					base = v
				}
				s.Version = base + 1
				n.lastSvcVer[k] = s.Version
			}
			s.Up = false
			log.Printf("[SVC] timeout -> DOWN %s id=%s ver=%d", s.Service, s.InstanceID, s.Version)
		}
		//elimina dopo 3*TTL dall’ultimo update
		age := now.Sub(s.LastUpdated).Seconds()
		ttl := s.TTLSeconds.Seconds()
		if age > 3*ttl {
			delete(n.services, k)
			log.Printf("[SVC] GC %s/%s (last=%f) (now%f)", s.Service, s.InstanceID, age, 3*ttl)
		}
	}
}

func (n *Node) serviceRefreshLoop() {
	t := time.NewTicker(n.cfg.ServiceRefreshTimeout)
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

func (n *Node) mergeServices(gm *GossipMessage) int {
	now := time.Now()
	updated := 0
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, ann := range gm.Services {
		key := svcKey(ann.Service, ann.InstanceID)
		cur, _ := n.services[key]

		last := n.lastSvcVer[key]
		if ann.Version <= last {
			continue
		}
		if cur != nil && ann.Version <= cur.Version {
			continue
		}

		up := ann.Up && !ann.Tombstone

		n.services[key] = &ServiceInstance{
			Service:     ann.Service,
			InstanceID:  ann.InstanceID,
			NodeID:      ann.NodeID,
			Addr:        ann.Addr,
			Version:     ann.Version,
			TTLSeconds:  ann.TTLSeconds,
			Up:          up,
			Tombstone:   ann.Tombstone,
			LastUpdated: now,                     // <-- arrivo locale
			ExpiresAt:   now.Add(ann.TTLSeconds), // <-- scadenza locale
		}
		if ann.Version > n.lastSvcVer[key] {
			n.lastSvcVer[key] = ann.Version
		}
		updated++
	}
	return updated
}
