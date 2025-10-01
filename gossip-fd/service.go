package main

import (
	"log"
	"time"
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

type ServiceAnnouncement struct {
	Service    string `json:"service"` // es: "calc"
	InstanceID string `json:"id"`      // es: "node1-calc"
	NodeID     string `json:"node"`    // chi ospita
	Addr       string `json:"addr"`    // es: "node1:18080"
	Version    uint64 `json:"ver"`     // contatore monotono
	TTLSeconds int    `json:"ttl"`     // es: 15
	Up         bool   `json:"up"`      // true se attivo
	Tombstone  bool   `json:"tomb"`    // per rimozione (non usato molto qui)
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

func (n *Node) mergeServices(gm *GossipMessage) int {
	now := time.Now() // usato solo localmente per l'ultimo "heard"
	updated := 0

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, ann := range gm.Services {
		key := svcKey(ann.Service, ann.InstanceID)
		cur, ok := n.services[key]

		// monotonicità globale: scarta se la versione è <= dell'ultima vista
		last := n.lastSvcVer[key]
		if ann.Version <= last {
			// NOTA: se vuoi che announce a stessa versione fungano da "keepalive",
			// puoi rimuovere questo check o gestire un ramo speciale per il solo refresh del LastUpdated.
			continue
		}
		// ridondante ma sicuro: se esiste cur e la versione non supera cur.Version, ignora
		if ok && ann.Version <= cur.Version {
			continue
		}

		// normalizza TTL (durata locale, non timestamp)
		ttl := ann.TTLSeconds
		if ttl <= 0 {
			ttl = n.cfg.ServiceTTL
		}

		up := ann.Up && !ann.Tombstone
		inst := &ServiceInstance{
			Service:     ann.Service,
			InstanceID:  ann.InstanceID,
			NodeID:      ann.NodeID,
			Addr:        ann.Addr,
			Version:     ann.Version,
			TTLSeconds:  ttl,
			Up:          up,
			Tombstone:   ann.Tombstone,
			LastUpdated: now, // solo clock locale
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
