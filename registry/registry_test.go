package registry

import (
	"testing"
	"time"

	"SDCC_gossiping/gossip"
)

func TestRegistryPropagates(t *testing.T) {
	// 1. Configuro due nodi gossip su porte diverse
	g1, err := gossip.Config("A", "127.0.0.1:9001", 100*time.Millisecond, []string{"127.0.0.1:9002"})
	if err != nil {
		t.Fatalf("gossip.Config A failed: %v", err)
	}
	g2, err := gossip.Config("B", "127.0.0.1:9002", 100*time.Millisecond, []string{"127.0.0.1:9001"})
	if err != nil {
		t.Fatalf("gossip.Config B failed: %v", err)
	}

	// 2. Avvio entrambi i nodi
	if err := g1.Start(); err != nil {
		t.Fatalf("g1.Start failed: %v", err)
	}
	if err := g2.Start(); err != nil {
		t.Fatalf("g2.Start failed: %v", err)
	}

	// 3. Creo due registry che usano quei nodi
	r1 := New(g1)
	r2 := New(g2)

	// 4. Registra un servizio su r1
	svc := ServiceInfo{
		ID:      "S1",
		Address: "1.2.3.4:8080",
		Tags:    []string{"t1"},
	}
	r1.Register(svc)

	// 5. Attendo la propagazione via gossip
	time.Sleep(300 * time.Millisecond)

	// 6. Verifico che r2 veda il servizio
	found := false
	for _, s := range r2.Lookup("") {
		if s.ID == "S1" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("r2 non ha ricevuto l'evento di Register, services: %+v", r2.Lookup(""))
	}
}
